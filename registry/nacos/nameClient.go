package nacos

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strconv"
	"sync"
	"time"

	"github.com/gmsec/goplugins/registry/namingregister"
	"github.com/gmsec/micro/naming"
	"github.com/nacos-group/nacos-sdk-go/clients/naming_client"
	"github.com/nacos-group/nacos-sdk-go/model"
	"github.com/nacos-group/nacos-sdk-go/vo"
	"github.com/xxjwxc/public/mylog"
	"github.com/xxjwxc/public/tools"
)

// watchBuffer 是每个 watcher 独占的事件队列长度。
// 带缓冲 + select(ctx.Done()) 让 SDK 的 SubscribeCallback 永不阻塞:消费端慢、
// 或 Close() 之后回调会丢事件/直接返回,而不是卡住 SDK 回调线程。
const watchBuffer = 16

// namingClient 每个实例(含 New() 派生出来的 watcher)独占一个事件 channel。
// 不再有包级 map 与 init() 副作用:同名服务的多个 watcher 各收各的,不会互相
// 抢事件,也不存在并发读写全局 map 的 fatal error: concurrent map read and map write。
type namingClient struct {
	client      naming_client.INamingClient
	serviceName string
	// ownClient 仅构造函数创建的根实例为 true,表示由它负责关闭 SDK client。
	// New() 派生的 watcher 共享 SDK client,Close() 绝不提前关闭它。
	ownClient bool

	mu        sync.RWMutex
	isFirst   bool
	started   bool
	closed    bool
	ch        chan []*naming.Update // 带缓冲,永不 close;用 done 通知关闭
	done      chan struct{}         // Close() 时 close,唤醒阻塞的 send/recv
	subParam  *vo.SubscribeParam    // 保存 Subscribe 的 param,Unsubscribe 必须传同一指针
	cancel    context.CancelFunc
	closeOnce sync.Once
}

// newNamingClient 统一构造,保证 done 永远非 nil(Close 可无条件 close)。
func newNamingClient(client naming_client.INamingClient, serviceName string, ownClient bool) *namingClient {
	return &namingClient{
		client:      client,
		serviceName: serviceName,
		ownClient:   ownClient,
		done:        make(chan struct{}),
	}
}

// Put puts a key-value pair
func (nc *namingClient) Put(ctx context.Context, serviceName string, val naming.Update) error {
	ipPort, err := net.ResolveTCPAddr("tcp", val.Addr)
	if err != nil {
		return err
	}

	success, err := nc.client.RegisterInstance(vo.RegisterInstanceParam{
		Ip:          ipPort.IP.String(),
		Port:        uint64(ipPort.Port),
		ServiceName: serviceName,
		Weight:      10,
		Enable:      true,
		Healthy:     true,
		Ephemeral:   true,
		Metadata: map[string]string{
			"heart": fmt.Sprintf("%v", time.Now().Unix()),
			"op":    fmt.Sprintf("%v", val.Op),
		},
	})
	if err != nil {
		return err
	}
	if !success {
		return fmt.Errorf("nacos register error : addr:%v", val.Addr)
	}
	return nil
}

// Delete deletes a key, or optionally using WithRange(end), [key, end).
func (nc *namingClient) Delete(ctx context.Context, serviceName string, val naming.Update) error {
	ipPort, err := net.ResolveTCPAddr("tcp", val.Addr)
	if err != nil {
		return err
	}

	success, err := nc.client.DeregisterInstance(vo.DeregisterInstanceParam{
		Ip:          ipPort.IP.String(),
		Port:        uint64(ipPort.Port),
		ServiceName: serviceName,
		Ephemeral:   true,
	})
	if err != nil {
		return err
	}
	if !success {
		return fmt.Errorf("nacos deregister error : addr:%v", val.Addr)
	}
	return nil
}

// Get retrieves keys.
func (nc *namingClient) Get(ctx context.Context, serviceName string) ([]*naming.Update, error) {
	// SelectInstances 只返回满足 healthy/enable/weight 条件的实例列表。
	instances, err := nc.client.SelectInstances(vo.SelectInstancesParam{
		ServiceName: serviceName,
		HealthyOnly: true,
	})
	if err != nil {
		return nil, err
	}

	out := make([]*naming.Update, 0, len(instances))
	for _, v := range instances {
		f64, _ := strconv.ParseFloat(v.Metadata["heart"], 64)
		out = append(out, &naming.Update{
			Op:       naming.Add,
			Addr:     fmt.Sprintf("%v:%v", v.Ip, v.Port),
			Metadata: f64,
		})
	}
	return out, nil
}

// Watchering Watcher is init
func (nc *namingClient) Watchering() bool {
	nc.mu.RLock()
	defer nc.mu.RUnlock()
	return nc.isFirst
}

// Watch 为当前实例订阅 serviceName:每个 watcher 一个独立的带缓冲 channel,
// 事件不再被同名服务的其它 watcher 抢占。Subscribe 同步执行并返回错误,
// 不再用常驻 goroutine 无限重试(那是 goroutine 泄漏的来源)。
func (nc *namingClient) Watch(ctx context.Context, serviceName string) error {
	nc.mu.Lock()
	if nc.closed {
		nc.mu.Unlock()
		return namingregister.ErrClientClosed
	}
	if nc.started {
		nc.mu.Unlock()
		return nil
	}
	ch := make(chan []*naming.Update, watchBuffer)
	wctx, cancel := context.WithCancel(ctx)
	done := nc.done
	nc.ch = ch
	nc.cancel = cancel
	nc.serviceName = serviceName
	nc.isFirst = true
	nc.started = true
	nc.mu.Unlock()

	param := &vo.SubscribeParam{
		ServiceName: serviceName,
		SubscribeCallback: func(services []model.SubscribeService, err error) {
			mylog.Infof("services:%v", tools.JSONDecode(services))
			var updates []*naming.Update
			for _, v := range services {
				f64, _ := strconv.ParseFloat(v.Metadata["heart"], 64)
				op := naming.Add
				if v.Metadata["op"] == "1" || !v.Enable || !v.Valid {
					op = naming.Delete
				}
				updates = append(updates, &naming.Update{
					Op:       op,
					Addr:     fmt.Sprintf("%v:%v", v.Ip, v.Port),
					Metadata: f64,
				})
			}
			if len(updates) == 0 {
				return
			}
			// ch 永不 close,从根上杜绝 send on closed channel;
			// done / wctx.Done() 保证 Close() 之后回调立即退出,不阻塞 SDK 回调线程。
			select {
			case ch <- updates:
			case <-done:
			case <-wctx.Done():
			}
		},
	}

	// Subscribe 可能在本 goroutine 内同步触发一次 callback,故调用时不能持有 nc.mu。
	if err := nc.client.Subscribe(param); err != nil {
		cancel()
		nc.mu.Lock()
		nc.started = false
		nc.ch = nil
		nc.mu.Unlock()
		return err
	}

	nc.mu.Lock()
	if nc.closed { // Watch 与 Close 竞态:立刻撤销刚加的回调,避免泄漏
		nc.mu.Unlock()
		_ = nc.client.Unsubscribe(param)
		return namingregister.ErrClientClosed
	}
	nc.subParam = param
	nc.mu.Unlock()
	return nil
}

// WatcherNext 兼容旧接口:不带 ctx,等价于 context.Background()。
func (nc *namingClient) WatcherNext() ([]*naming.Update, error) {
	return nc.WatcherNextWithContext(context.Background())
}

// WatcherNextWithContext 阻塞等待事件时同时监听 ctx.Done() 与 done,
// 因此 Close()/cancel() 能立刻唤醒它,不会永久卡在 <-ch。
func (nc *namingClient) WatcherNextWithContext(ctx context.Context) ([]*naming.Update, error) {
	nc.mu.RLock()
	ch := nc.ch
	done := nc.done
	nc.mu.RUnlock()
	if ch == nil {
		return nil, namingregister.ErrWatcherClosed
	}
	select {
	case updates := <-ch:
		return updates, nil
	case <-done:
		return nil, namingregister.ErrWatcherClosed
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// Close 注销 Subscribe 回调、唤醒阻塞的 Next、释放实例资源。
// 用 sync.Once 保证幂等;SDK client 仅由 ownClient 的根实例关闭,
// 避免派生 watcher 提前关闭共享连接。
func (nc *namingClient) Close() error {
	var retErr error
	nc.closeOnce.Do(func() {
		nc.mu.Lock()
		nc.closed = true
		subParam := nc.subParam
		cancel := nc.cancel
		own := nc.ownClient
		close(nc.done)
		nc.mu.Unlock()

		if cancel != nil {
			cancel()
		}
		if subParam != nil {
			if err := nc.client.Unsubscribe(subParam); err != nil {
				retErr = errors.Join(retErr, fmt.Errorf("nacos unsubscribe: %w", err))
				mylog.Error(err)
			}
		}
		if own {
			// 当前钉死的 nacos-sdk-go v1.0.10 的 INamingClient 未暴露 Close();
			// 用可选接口断言:待依赖升级到带 Close() 的版本后自动生效,
			// 避免 SDK 内部连接/协程泄漏。
			if c, ok := nc.client.(interface{ Close() error }); ok {
				if err := c.Close(); err != nil {
					retErr = errors.Join(retErr, fmt.Errorf("nacos client close: %w", err))
				}
			}
		}
	})
	return retErr
}

// New 派生的 watcher 共享 SDK client,但不拥有它(ownClient=false)。
func (nc *namingClient) New(serviceName string) namingregister.NamingClient {
	return newNamingClient(nc.client, serviceName, false)
}
