package etcdv3

import (
	"context"
	"encoding/json"
	"sync"

	"github.com/gmsec/goplugins/registry/namingregister"
	"github.com/gmsec/micro/naming"
	clientv3 "go.etcd.io/etcd/client/v3"
	etcd "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// leaseTTL 是注册 key 的租约时长(秒)。
//
// 历史实现只做 Put,不绑定 lease:进程崩溃后 key 永久残留,
// 只能靠消费者按 metadata 时间戳判活、再手动 Delete。那条链路有三个硬伤:
// 跨机时钟漂移会误判、任意消费者都能删别人的节点、没有消费者时死节点永不清理。
// 绑定 lease 之后,活性和清理都交给 etcd,上层不再需要判活。
//
// 30s 是折中值:15s 在容器/跨机房抖动时容易误删,60s 又会让崩溃节点的摘除变慢。
// 声明成变量是为了让一致性测试能缩短到秒级验证"崩溃后自动消失"。
var leaseTTL int64 = 30

type namingClient struct {
	client *clientv3.Client

	// mu 保护下面的租约字段:Put 会被注册流程和心跳 goroutine 同时调用
	mu       sync.Mutex
	leaseID  clientv3.LeaseID
	cancelKA context.CancelFunc
	fullKey  string

	// 下面三个字段只在 watcher goroutine 里访问(Get -> Watch -> WatcherNext 串行)
	wch      etcd.WatchChan
	watchKey string
	revision int64
}

// Put puts a key-value pair.
// 首次调用会申请租约并启动续期,之后的调用(心跳)复用同一个租约。
func (nc *namingClient) Put(ctx context.Context, key string, val naming.Update) error {
	v, err := json.Marshal(val)
	if err != nil {
		return status.Error(codes.InvalidArgument, err.Error())
	}
	fullKey := key + "//" + val.Addr

	nc.mu.Lock()
	if nc.leaseID == 0 {
		resp, err := nc.client.Grant(ctx, leaseTTL)
		if err != nil {
			nc.mu.Unlock()
			return err
		}
		kaCtx, cancel := context.WithCancel(context.Background())
		ch, err := nc.client.KeepAlive(kaCtx, resp.ID)
		if err != nil {
			cancel()
			nc.mu.Unlock()
			return err
		}
		nc.leaseID = resp.ID
		nc.cancelKA = cancel
		nc.fullKey = fullKey
		go nc.keepAlive(ch, resp.ID)
	}
	leaseID := nc.leaseID
	nc.mu.Unlock()

	_, err = nc.client.Put(ctx, fullKey, string(v), clientv3.WithLease(leaseID))
	return err
}

// keepAlive 消费租约续期的响应流。
// etcd 重启或租约被回收时 channel 会关闭/返回 nil,这里把 leaseID 清零,
// 让下一次 Put(心跳或重新注册)重新申请租约,而不是拿着失效 ID 空写。
func (nc *namingClient) keepAlive(ch <-chan *clientv3.LeaseKeepAliveResponse, id clientv3.LeaseID) {
	for {
		resp, ok := <-ch
		if !ok || resp == nil {
			nc.mu.Lock()
			if nc.leaseID == id {
				nc.leaseID = 0
				nc.fullKey = ""
			}
			nc.mu.Unlock()
			return
		}
	}
}

// Delete deletes a key, or optionally using WithRange(end), [key, end).
func (nc *namingClient) Delete(ctx context.Context, key string, val naming.Update) error {
	nc.mu.Lock()
	leaseID := nc.leaseID
	cancelKA := nc.cancelKA
	fullKey := nc.fullKey
	nc.leaseID = 0
	nc.cancelKA = nil
	nc.fullKey = ""
	nc.mu.Unlock()

	if cancelKA != nil {
		cancelKA()
	}

	// 绑过租约就 revoke:它同时取消续期并让 key 立即消失。
	// 直接 Delete(key) 而留着续期 goroutine,key 会被 KeepAlive 再写回来。
	if leaseID != 0 {
		_, err := nc.client.Revoke(ctx, leaseID)
		return err
	}

	if fullKey == "" {
		fullKey = key + "//" + val.Addr
	}
	_, err := nc.client.Delete(ctx, fullKey)
	return err
}

// Get retrieves keys.
func (nc *namingClient) Get(ctx context.Context, key string) ([]*naming.Update, error) {
	// 不用 WithSerializable:服务发现要的是当前真实实例集,
	// 读旧快照会让客户端基于过期列表做判断(历史上会据此删掉健康节点)。
	resp, err := nc.client.Get(ctx, key+"//", etcd.WithPrefix())
	if err != nil {
		return nil, err
	}
	if resp.Header != nil {
		nc.revision = resp.Header.Revision
	}

	out := make([]*naming.Update, 0, len(resp.Kvs))
	for _, kv := range resp.Kvs {
		var jupdate naming.Update
		if err := json.Unmarshal(kv.Value, &jupdate); err != nil {
			continue
		}
		out = append(out, &jupdate)
	}

	return out, nil
}

// Watchering Watcher is init
func (nc *namingClient) Watchering() bool {
	return nc.wch != nil
}

// Watch start watch
func (nc *namingClient) Watch(ctx context.Context, key string) error {
	opts := []etcd.OpOption{etcd.WithPrefix(), etcd.WithPrevKV()}
	// 从上次 Get 之后的第一个 revision 开始:否则重连期间产生的增删事件会全部丢失,
	// 客户端会一直停留在断线前的地址表上。
	if nc.revision > 0 {
		opts = append(opts, etcd.WithRev(nc.revision+1))
	}
	nc.watchKey = key
	nc.wch = nc.client.Watch(ctx, key+"//", opts...)
	return nil
}

// WatcherNext watching next
func (nc *namingClient) WatcherNext() ([]*naming.Update, error) {
	wch := nc.wch
	if wch == nil {
		return nil, status.Error(codes.Unavailable, namingregister.ErrWatcherClosed.Error())
	}
	wr, ok := <-wch
	if !ok {
		return nil, status.Error(codes.Unavailable, namingregister.ErrWatcherClosed.Error())
	}

	// revision 被压缩(通常是 watcher 断了很久),增量已经不可用。
	// 回退成一次全量 Get 并重建 watch,对上层表现为"拿到一批 Add"。
	if wr.CompactRevision != 0 {
		list, err := nc.Get(context.Background(), nc.watchKey)
		if err != nil {
			return nil, err
		}
		if err := nc.Watch(context.Background(), nc.watchKey); err != nil {
			return nil, err
		}
		return list, nil
	}

	if err := wr.Err(); err != nil {
		return nil, err
	}

	return decodeEvents(wr.Events), nil
}

// decodeEvents 把 etcd 事件转成命名更新。
// Delete 事件从 PrevKv 取值:key 已经被删了,Kv 是空的。
func decodeEvents(events []*clientv3.Event) []*naming.Update {
	updates := make([]*naming.Update, 0, len(events))
	for _, e := range events {
		var jupdate naming.Update
		var err error
		switch e.Type {
		case etcd.EventTypePut:
			err = json.Unmarshal(e.Kv.Value, &jupdate)
			jupdate.Op = naming.Add
		case etcd.EventTypeDelete:
			if e.PrevKv == nil { // 没有 PrevKV 就无法还原地址,跳过而不是 panic
				continue
			}
			err = json.Unmarshal(e.PrevKv.Value, &jupdate)
			jupdate.Op = naming.Delete
		default:
			continue
		}

		if err == nil {
			updates = append(updates, &jupdate)
		}
	}
	return updates
}

// New new watching client.
// 只共享底层 ClientConn,不继承租约:租约属于注册端的那个实例。
func (nc *namingClient) New(serviceName string) namingregister.NamingClient {
	return &namingClient{
		client: nc.client,
	}
}

// Close close.
// 只停续期,不关闭共享的 etcd client(它可能还被其它客户端用着)。
func (nc *namingClient) Close() error {
	nc.mu.Lock()
	cancelKA := nc.cancelKA
	nc.cancelKA = nil
	nc.mu.Unlock()

	if cancelKA != nil {
		cancelKA()
	}
	return nil
}
