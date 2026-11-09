package etcdv3

import (
	"context"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gmsec/goplugins/registry/namingregister"
	"github.com/gmsec/micro/naming"
	"github.com/gmsec/micro/registry"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// conformanceEndpoints 从环境变量读 etcd 地址,没配置就跳过。
// 本地验证:
//
//	docker run -d --rm -p 2379:2379 quay.io/coreos/etcd:v3.5.15 etcd \
//	  --advertise-client-urls=http://0.0.0.0:2379 --listen-client-urls=http://0.0.0.0:2379
//	GMSEC_ETCD_ENDPOINTS=127.0.0.1:2379 go test ./registry/etcdv3/ -run Conformance -v
func conformanceEndpoints(t *testing.T) []string {
	t.Helper()
	raw := os.Getenv("GMSEC_ETCD_ENDPOINTS")
	if raw == "" {
		t.Skip("未设置 GMSEC_ETCD_ENDPOINTS,跳过 etcd 一致性测试")
	}
	return strings.Split(raw, ",")
}

func newTestClient(t *testing.T) *clientv3.Client {
	t.Helper()
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   conformanceEndpoints(t),
		DialTimeout: 3 * time.Second,
	})
	if err != nil {
		t.Fatalf("连接 etcd 失败: %v", err)
	}
	return cli
}

func newTestRegister(cli *clientv3.Client, service string) registry.RegNaming {
	return namingregister.NewDNSNamingRegistry(&namingClient{client: cli},
		registry.WithServiceName(service))
}

// watcherSnapshot 在后台持续消费 watcher,返回一个当前存活地址的快照函数。
func watcherSnapshot(w naming.Watcher) func() map[string]bool {
	var (
		mu   sync.Mutex
		live = make(map[string]bool)
	)
	go func() {
		for {
			ups, err := w.Next()
			if err != nil {
				return
			}
			mu.Lock()
			for _, u := range ups {
				switch u.Op {
				case naming.Add:
					live[u.Addr] = true
				case naming.Delete:
					delete(live, u.Addr)
				}
			}
			mu.Unlock()
		}
	}()

	return func() map[string]bool {
		mu.Lock()
		defer mu.Unlock()
		cp := make(map[string]bool, len(live))
		for k, v := range live {
			cp[k] = v
		}
		return cp
	}
}

func eventually(t *testing.T, d time.Duration, desc string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("超时(%v):%s", d, desc)
}

// TestEtcdConformance 覆盖注册中心的四条核心语义:
// 注册可见、多实例可见、主动注销立即消失、进程崩溃后租约到期自动消失。
func TestEtcdConformance(t *testing.T) {
	cli := newTestClient(t)
	defer cli.Close()

	// 缩短租约,让"崩溃后自动消失"能在测试时间内验证
	oldTTL := leaseTTL
	leaseTTL = 2
	defer func() { leaseTTL = oldTTL }()

	service := "conformance.etcd." + time.Now().Format("150405.000000")
	defer cli.Delete(context.Background(), service+"//", clientv3.WithPrefix())

	reg1 := newTestRegister(cli, service)
	addr1 := "127.0.0.1:19001"
	if err := reg1.Register(addr1, nil); err != nil {
		t.Fatalf("注册失败: %v", err)
	}
	defer reg1.Deregister()

	w, err := reg1.Resolve(service)
	if err != nil {
		t.Fatalf("Resolve 失败: %v", err)
	}
	defer w.Close()

	snap := watcherSnapshot(w)

	// 1) 注册后能被发现
	eventually(t, 5*time.Second, "注册后应能发现实例", func() bool {
		return snap()[addr1]
	})

	// 2) 第二个实例
	reg2 := newTestRegister(cli, service)
	addr2 := "127.0.0.1:19002"
	if err := reg2.Register(addr2, nil); err != nil {
		t.Fatalf("第二个实例注册失败: %v", err)
	}
	eventually(t, 5*time.Second, "第二个实例应能发现", func() bool {
		return snap()[addr1] && snap()[addr2]
	})

	// 3) 主动注销应立即消失(Revoke,不等 TTL)
	if err := reg2.Deregister(); err != nil {
		t.Fatalf("注销失败: %v", err)
	}
	eventually(t, 5*time.Second, "注销后应立即消失", func() bool {
		return snap()[addr1] && !snap()[addr2]
	})

	// 4) 不注销直接停进程 -> 租约到期后自动消失
	crashCli := newTestClient(t)
	defer crashCli.Close()
	crashNC := &namingClient{client: crashCli}
	crashReg := namingregister.NewDNSNamingRegistry(crashNC, registry.WithServiceName(service))
	addr3 := "127.0.0.1:19003"
	if err := crashReg.Register(addr3, nil); err != nil {
		t.Fatalf("崩溃实例注册失败: %v", err)
	}
	eventually(t, 5*time.Second, "崩溃实例应能发现", func() bool {
		return snap()[addr3]
	})

	// 只停 KeepAlive、不 Revoke,等价于进程被 kill
	crashNC.Close()

	eventually(t, 15*time.Second, "租约到期后崩溃实例应自动消失", func() bool {
		return !snap()[addr3]
	})
}

// TestLeaseReuse 验证心跳不会每次重新申请租约。
func TestLeaseReuse(t *testing.T) {
	cli := newTestClient(t)
	defer cli.Close()

	oldTTL := leaseTTL
	leaseTTL = 2
	defer func() { leaseTTL = oldTTL }()

	service := "conformance.etcd.lease." + time.Now().Format("150405.000000")
	defer cli.Delete(context.Background(), service+"//", clientv3.WithPrefix())

	nc := &namingClient{client: cli}
	reg := namingregister.NewDNSNamingRegistry(nc, registry.WithServiceName(service))
	addr := "127.0.0.1:19011"
	if err := reg.Register(addr, nil); err != nil {
		t.Fatalf("注册失败: %v", err)
	}
	defer reg.Deregister()

	nc.mu.Lock()
	first := nc.leaseID
	nc.mu.Unlock()
	if first == 0 {
		t.Fatal("注册后应绑定租约")
	}

	// 再写一次(模拟心跳)不应换租约
	if err := nc.Put(context.Background(), service, naming.Update{Op: naming.Add, Addr: addr}); err != nil {
		t.Fatalf("心跳写入失败: %v", err)
	}
	nc.mu.Lock()
	second := nc.leaseID
	nc.mu.Unlock()
	if second != first {
		t.Fatalf("心跳不应重新申请租约: %d -> %d", first, second)
	}
}
