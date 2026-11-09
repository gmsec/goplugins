package namingregister

import (
	"context"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gmsec/micro/naming"
	"github.com/gmsec/micro/registry"
	"github.com/google/uuid"
	"github.com/xxjwxc/public/mylog"
	"github.com/xxjwxc/public/tools"
)

// NamingRegister dns default register
type NamingRegister struct {
	opts registry.Options
	sync.Mutex
	node    NamingClient
	address string

	// 心跳控制:done 关闭后心跳 goroutine 退出,避免每次 Register 泄漏一个 goroutine + ticker
	done chan struct{}
}

func NewDNSNamingRegistry(nameCli NamingClient, opts ...registry.Option) registry.RegNaming {
	options := registry.Options{
		Context:          context.Background(),
		Timeout:          time.Millisecond * 100,
		NodeID:           uuid.New().String(),
		KeepHeartTimeout: time.Second * 30,
		ServiceName:      "gmsec.service",
	}
	for _, o := range opts {
		o(&options)
	}

	return &NamingRegister{
		opts: options,
		node: nameCli,
	}
}

func (r *NamingRegister) String() string {
	return r.opts.ServiceName
}

// GetPort 获取当前注册端口的端口号
func (r *NamingRegister) GetPort() int {
	r.Lock()
	addrs := r.opts.Addrs
	r.Unlock()

	if len(addrs) == 0 {
		return 0
	}

	_, pt, err := net.SplitHostPort(addrs[0])
	if err != nil {
		return 0
	}
	port, _ := strconv.Atoi(pt)
	return port
}

// Init init option
func (r *NamingRegister) Init(opts ...registry.Option) error {
	for _, o := range opts {
		o(&r.opts)
	}

	return nil
}

// Options get opts list
func (r *NamingRegister) Options() registry.Options {
	r.Lock()
	defer r.Unlock()
	return r.opts
}

// Deregister 注销
func (r *NamingRegister) Deregister() error {
	r.Lock()
	defer r.Unlock()

	r.stopHeartbeatLocked()

	if r.node != nil {
		gr := &GRPCResolver{Client: r.node, HeartTimeout: r.opts.KeepHeartTimeout * 10}
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		if err := gr.Update(ctx, r.opts.ServiceName, naming.Update{
			Op:   naming.Delete,
			Addr: r.address,
		}); err != nil {
			mylog.Error(err)
		}
		cancel()
		if err := r.node.Close(); err != nil {
			mylog.Error(err)
		}
		r.node = nil
	}

	return nil
}

// Register register & add new node
func (r *NamingRegister) Register(address string, Metadata interface{}) error {
	r.Lock()

	// ip fix
	if strings.HasPrefix(address, "[::]") {
		address = tools.GetLocalIP() + address[4:]
	} else if strings.HasPrefix(address, ":") {
		address = tools.GetLocalIP() + address
	}
	r.address = address
	r.startHeartbeatLocked(address)
	node := r.node
	serviceName := r.opts.ServiceName
	r.Unlock()

	if node == nil { // 已 Deregister 后不可再注册,返回明确错误而不是 nil panic
		return ErrClientClosed
	}

	gr := &GRPCResolver{Client: node}
	up := naming.Update{
		Op:   naming.Add,
		Addr: address,
		// Metadata 保留时间戳是为了兼容滚动升级期间的旧客户端(它们仍按它判活),
		// 新链路不再依赖这个值:活性和清理由 etcd 的 lease / mDNS 的记录过期负责。
		Metadata: time.Now().Unix(),
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	return gr.Update(ctx, serviceName, up)
}

// startHeartbeatLocked 启动心跳,调用方必须已持有 r.Lock()。
//
// 心跳的职责只是周期性刷新 value(携带新的时间戳),不再负责保活:
// etcd 的存活由 lease 的 KeepAlive 维持,mDNS 由 DNS 记录的 TTL 维持。
// 这样即使这里漏了一次,节点也不会被误判为下线。
func (r *NamingRegister) startHeartbeatLocked(address string) {
	r.stopHeartbeatLocked()

	node, serviceName := r.node, r.opts.ServiceName
	if node == nil {
		return
	}

	interval := r.opts.KeepHeartTimeout
	if interval <= 0 { // time.NewTicker 不接受非正数
		interval = time.Second * 30
	}

	done := make(chan struct{})
	r.done = done

	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for {
			select {
			case <-done: // 注销时退出
				return
			case <-ticker.C:
				up := naming.Update{
					Op:       naming.Add,
					Addr:     address,
					Metadata: time.Now().Unix(),
				}
				ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
				(&GRPCResolver{Client: node}).Update(ctx, serviceName, up) // 发送心跳
				cancel()
			}
		}
	}()
}

// stopHeartbeatLocked 停止心跳,调用方必须已持有 r.Lock()
func (r *NamingRegister) stopHeartbeatLocked() {
	if r.done != nil {
		close(r.done)
		r.done = nil
	}
}

// Resolve resolve begin
func (r *NamingRegister) Resolve(target string) (naming.Watcher, error) {
	r.Lock()
	node := r.node
	heartTimeout := r.opts.KeepHeartTimeout * 10
	r.Unlock()

	if node == nil {
		return nil, ErrClientClosed
	}

	t := &GRPCResolver{Client: node.New(target), HeartTimeout: heartTimeout}

	// HeartTimeout 现在只作为 firstNext 里 Get 的超时上限,不再用于判活。
	return t.Resolve(target)
}

// Close close watcher
func (r *NamingRegister) Close() { r.Deregister() }
