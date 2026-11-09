// Package nacos 提供 nacos 版的服务注册与发现实现。
//
// Deprecated: 当前版本只保证 mDNS(本地开发/内网)和 etcdv3(生产)两种实现
// 的语义一致与一致性测试覆盖。nacos 实现保留可用,但不再跟随
// naming.Update 语义的变化做同步维护,属于 best-effort:
//   - 客户端侧已不再做应用层判活/删 key,依赖 nacos 的 Ephemeral + Healthy 过滤;
//   - 包级全局 _map 缓存未修,同进程订阅同名服务的多个客户端会互相抢事件;
//   - Subscribe 回调为阻塞发送,消费端变慢会卡住 SDK 回调线程。
//
// 新项目请优先使用 etcdv3。
package nacos

import (
	"fmt"
	"os"

	"github.com/gmsec/goplugins/registry/namingregister"
	"github.com/gmsec/micro/registry"
	"github.com/nacos-group/nacos-sdk-go/clients"
	"github.com/nacos-group/nacos-sdk-go/common/constant"
	"github.com/nacos-group/nacos-sdk-go/vo"
)

// NewNacosNamingRegistry returns a new default dns naming registry which is mdns
func NewNacosNamingRegistry(sconf []constant.ServerConfig, conf *constant.ClientConfig, opts ...registry.Option) registry.RegNaming {
	// a more graceful way to create naming client
	nacosClient, err := clients.NewNamingClient(
		vo.NacosClientParam{
			ClientConfig:  conf,
			ServerConfigs: sconf,
		},
	)
	if err != nil {
		fmt.Printf("连接 nacos 服务器失败: %+v\n", err)
		os.Exit(1)
	}

	cli := &namingClient{client: nacosClient}

	return namingregister.NewDNSNamingRegistry(cli, opts...)
}
