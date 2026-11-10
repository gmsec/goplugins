// Package nacos 提供 nacos 版的服务注册与发现实现。
//
// Deprecated: 当前版本只保证 mDNS(本地开发/内网)和 etcdv3(生产)两种实现
// 的语义一致与一致性测试覆盖。nacos 实现保留可用,但不再跟随
// naming.Update 语义的变化做同步维护,属于 best-effort:
//   - 客户端侧已不再做应用层判活/删 key,依赖 nacos 的 Ephemeral + Healthy 过滤;
//   - 包级全局 map 与共享无缓冲 channel 已移除:每个 watcher 实例独占一个
//     带缓冲事件队列,Close() 会注销 Subscribe 回调并唤醒阻塞的 Next();
//   - Subscribe 回调为带缓冲的 select 发送,消费端变慢不会卡住 SDK 回调线程。
//
// 新项目请优先使用 etcdv3。
package nacos

import (
	"fmt"

	"github.com/gmsec/goplugins/registry/namingregister"
	"github.com/gmsec/micro/registry"
	"github.com/nacos-group/nacos-sdk-go/clients"
	"github.com/nacos-group/nacos-sdk-go/common/constant"
	"github.com/nacos-group/nacos-sdk-go/vo"
	"github.com/xxjwxc/public/mylog"
)

// NewNacosNamingRegistryE 创建注册中心;连接失败以 error 返回,不再 os.Exit(1)。
func NewNacosNamingRegistryE(sconf []constant.ServerConfig, conf *constant.ClientConfig, opts ...registry.Option) (registry.RegNaming, error) {
	nacosClient, err := clients.NewNamingClient(vo.NacosClientParam{
		ClientConfig:  conf,
		ServerConfigs: sconf,
	})
	if err != nil {
		return nil, fmt.Errorf("连接 nacos 服务器失败: %w", err)
	}

	cli := newNamingClient(nacosClient, "", true)
	return namingregister.NewDNSNamingRegistry(cli, opts...), nil
}

// NewNacosNamingRegistry 兼容旧签名:失败时用 mylog 记录错误并返回 nil,
// 不再用 fmt.Printf + os.Exit(1) 直接终止进程。
// 需要在调用方感知错误的场景请改用 NewNacosNamingRegistryE。
func NewNacosNamingRegistry(sconf []constant.ServerConfig, conf *constant.ClientConfig, opts ...registry.Option) registry.RegNaming {
	reg, err := NewNacosNamingRegistryE(sconf, conf, opts...)
	if err != nil {
		mylog.Error(err)
		return nil
	}
	return reg
}
