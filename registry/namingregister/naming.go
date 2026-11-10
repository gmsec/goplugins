package namingregister

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gmsec/micro/naming"
	"github.com/gmsec/micro/registry"
	"github.com/google/uuid"
	"github.com/xxjwxc/public/tools"
)

const (
	// DefaultServiceName 默认服务名,集中定义避免散落硬编码;
	// 可通过 registry.WithServiceName(...) 覆盖。
	DefaultServiceName = "gmsec.service"
	// DefaultKeepHeartTimeout 默认心跳刷新周期,可通过
	// registry.WithKeepHeartTimeout(...) 覆盖。
	DefaultKeepHeartTimeout = 30 * time.Second
	// minOpTimeout 单次注册/注销/发现操作的最小超时。
	// Options.Timeout 默认 100ms(语义更接近 RegisterTTL),直接套用会让所有 RPC
	// 变成 100ms 超时,所以低于 1s 时统一抬到 1s。
	minOpTimeout = time.Second
)

// requestTimeout 把历史上散落的硬编码 3s 收敛到 Options.Timeout,并给 1s 兜底。
func requestTimeout(opts registry.Options) time.Duration {
	if opts.Timeout < minOpTimeout {
		return minOpTimeout
	}
	return opts.Timeout
}

// opContext 统一从 opts.Context 派生带超时的 ctx,不再到处 context.Background(),
// 让上游的取消/超时能传递到每一次注册与注销 RPC。
func opContext(opts registry.Options) (context.Context, context.CancelFunc) {
	parent := opts.Context
	if parent == nil {
		parent = context.Background()
	}
	return context.WithTimeout(parent, requestTimeout(opts))
}

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
		KeepHeartTimeout: DefaultKeepHeartTimeout,
		ServiceName:      DefaultServiceName,
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
	r.Lock()
	defer r.Unlock()
	return r.opts.ServiceName
}

// GetPort 获取当前注册端口的端口号
func (r *NamingRegister) GetPort() int {
	r.Lock()
	opts := r.opts
	r.Unlock()

	if len(opts.Addrs) == 0 {
		return 0
	}

	_, pt, err := net.SplitHostPort(opts.Addrs[0])
	if err != nil {
		return 0
	}
	port, _ := strconv.Atoi(pt)
	return port
}

// Init init option
//
// 与 GetPort/Options/Register/Resolve/Deregister 一样持锁写 r.opts,
// 消除 Init 无锁写 + 其它方法持锁读之间的 data race。
func (r *NamingRegister) Init(opts ...registry.Option) error {
	r.Lock()
	defer r.Unlock()
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
//
// 摘流量(gr.Update)与关闭客户端(node.Close)的失败不再只打日志后 return nil,
// 而是用 errors.Join 收集并返回,让上层能感知"摘流量失败"。
func (r *NamingRegister) Deregister() error {
	r.Lock()
	defer r.Unlock()

	r.stopHeartbeatLocked()

	var errs []error
	if r.node != nil {
		opts := r.opts
		gr := &GRPCResolver{Client: r.node, Timeout: requestTimeout(opts)}
		ctx, cancel := opContext(opts)
		if err := gr.Update(ctx, opts.ServiceName, naming.Update{
			Op:   naming.Delete,
			Addr: r.address,
		}); err != nil {
			errs = append(errs, fmt.Errorf("摘流量失败: %w", err))
		}
		cancel()
		if err := r.node.Close(); err != nil {
			errs = append(errs, fmt.Errorf("关闭命名客户端失败: %w", err))
		}
		r.node = nil
	}

	return errors.Join(errs...)
}

// Register register & add new node
//
// Metadata 形参是 registry.RegNaming 接口保留的调用方元数据提示,本实现刻意
// 不使用它:naming.Update.Metadata 由本实现占用为心跳时间戳(兼容滚动升级期间
// 的旧客户端),透传外部值会覆盖该语义。服务名/心跳/超时等请改用
// registry.WithServiceName / WithKeepHeartTimeout / WithTimeout 等 Option 配置。
// 参数名用 _ 明确表示该值被忽略,消除"传了会生效"的误导。
func (r *NamingRegister) Register(address string, _ interface{}) error {
	// ip fix
	if strings.HasPrefix(address, "[::]") {
		address = tools.GetLocalIP() + address[4:]
	} else if strings.HasPrefix(address, ":") {
		address = tools.GetLocalIP() + address
	}

	r.Lock()
	r.address = address
	opts := r.opts
	node := r.node
	r.startHeartbeatLocked(address, opts)
	r.Unlock()

	if node == nil { // 已 Deregister 后不可再注册,返回明确错误而不是 nil panic
		return ErrClientClosed
	}

	gr := &GRPCResolver{Client: node, Timeout: requestTimeout(opts)}
	up := naming.Update{
		Op:   naming.Add,
		Addr: address,
		// Metadata 保留时间戳是为了兼容滚动升级期间的旧客户端(它们仍按它判活),
		// 新链路不再依赖这个值:活性和清理由 etcd 的 lease / mDNS 的记录过期负责。
		Metadata: time.Now().Unix(),
	}
	ctx, cancel := opContext(opts)
	defer cancel()

	return gr.Update(ctx, opts.ServiceName, up)
}

// startHeartbeatLocked 启动心跳,调用方必须已持有 r.Lock()。
//
// 心跳的职责只是周期性刷新 value(携带新的时间戳),不再负责保活:
// etcd 的存活由 lease 的 KeepAlive 维持,mDNS 由 DNS 记录的 TTL 维持。
// 这样即使这里漏了一次,节点也不会被误判为下线。
func (r *NamingRegister) startHeartbeatLocked(address string, opts registry.Options) {
	r.stopHeartbeatLocked()

	node, serviceName := r.node, opts.ServiceName
	if node == nil {
		return
	}

	interval := opts.KeepHeartTimeout
	if interval <= 0 { // time.NewTicker 不接受非正数
		interval = DefaultKeepHeartTimeout
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
				ctx, cancel := opContext(opts)
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
	opts := r.opts
	r.Unlock()

	if node == nil {
		return nil, ErrClientClosed
	}

	t := &GRPCResolver{Client: node.New(target), Timeout: requestTimeout(opts)}
	return t.Resolve(target)
}

// Close close watcher
//
// 返回 error 而非丢弃 Deregister() 的结果,让上层能感知"摘流量失败"。
// RegNaming 接口本身没有 Close(),旧的 `reg.Close()` 语句式调用仍可编译,兼容。
func (r *NamingRegister) Close() error { return r.Deregister() }
