package etcdv3

import (
	"fmt"

	"github.com/gmsec/goplugins/registry/namingregister"
	"github.com/gmsec/micro/registry"
	"github.com/xxjwxc/public/mylog"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// NewEtcdv3NamingRegistryE 创建注册中心;连接失败以 error 返回,不再 os.Exit(1)。
func NewEtcdv3NamingRegistryE(conf clientv3.Config, opts ...registry.Option) (registry.RegNaming, error) {
	etcdCli, err := clientv3.New(conf)
	if err != nil {
		return nil, fmt.Errorf("连接 etcd 服务器失败: %w", err)
	}

	cli := newNamingClient(etcdCli, true)
	return namingregister.NewDNSNamingRegistry(cli, opts...), nil
}

// NewEtcdv3NamingRegistry 兼容旧签名:失败时用 mylog 记录错误并返回 nil,
// 不再用 fmt.Printf + os.Exit(1) 直接终止进程。
// 需要在调用方感知错误的场景请改用 NewEtcdv3NamingRegistryE。
func NewEtcdv3NamingRegistry(conf clientv3.Config, opts ...registry.Option) registry.RegNaming {
	reg, err := NewEtcdv3NamingRegistryE(conf, opts...)
	if err != nil {
		mylog.Error(err)
		return nil
	}
	return reg
}
