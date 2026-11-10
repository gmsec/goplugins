# goplugins
Community maintained plugins for Go gmsec

gmsec plugins

### 注册中心支持范围

| 实现 | 定位 | 支持级别 |
|---|---|---|
| `registry/etcdv3` | 生产环境 | 一等公民，参与一致性测试 |
| `micro` 内置 mDNS | 本地开发 / CI / 内网 | 一等公民，参与一致性测试 |
| `registry/nacos` | 已有 nacos 资产的适配 | **best-effort**，不保证语义一致 |

注册端不再自行判断节点活性：

- etcd：注册时申请 lease（30s），后台 KeepAlive 续期，进程崩溃后 key 自动过期；主动注销走 `Revoke`。
- mDNS：靠 DNS 记录过期 + 查询结果比对（静默 5s 判下线）。

`naming.Update.Metadata` 里的时间戳仅为兼容滚动升级期间的旧客户端保留，新链路不依赖它。

### etcd
```
clientv3 "go.etcd.io/etcd/client/v3"
"github.com/gmsec/goplugins/registry/etcdv3"

	reg := etcdv3.NewEtcdv3NamingRegistry(clientv3.Config{
		Endpoints:   config.GetEtcdInfo().Addrs,
		DialTimeout: time.Second * time.Duration(config.GetEtcdInfo().Timeout),
	})
```
### nacos（deprecated / best-effort）

> 保留可用，但不再跟随 `naming.Update` 语义变化同步维护。新项目请使用 etcd。
```
"github.com/gmsec/goplugins/registry/nacos"

	nacosCnf := config.GetNacosNamingInfo()
	var serverconfig []constant.ServerConfig
	for _, v := range nacosCnf.Addrs {
		ipPort, err := net.ResolveTCPAddr("tcp", v)
		if err != nil {
			panic(err)
		}
		serverconfig = append(serverconfig, *constant.NewServerConfig(
			ipPort.IP.String(),
			uint64(ipPort.Port),
		))
	}
	reg := nacos.NewNacosNamingRegistry(serverconfig, constant.NewClientConfig(constant.WithNamespaceId(nacosCnf.Namespace)))

```

> 说明：构造函数新增了返回 `error` 的 `NewEtcdv3NamingRegistryE` / `NewNacosNamingRegistryE`，
> 旧签名保留为兼容包装（失败时仅记录错误并返回 `nil`，不再 `os.Exit(1)`）。

### 依赖版本分叉与治理

`go.mod` 把 `github.com/gmsec/micro` 钉死在 `v0.0.0-20240126043355-ad68a05cb4f3`
（2024 年，原编译于 gRPC 1.53 / Go 1.18），而本仓库与本地 `micro` 工作副本已使用
Go 1.25 / gRPC 1.79.x。三者之间 gRPC、protobuf、`google.golang.org/genproto` 等
间接依赖存在**版本分叉风险**：

- 接口 / 生成代码差异可能导致 `micro` 与 `goplugins` 编译期或运行期不兼容；
- 旧版 `micro` 会把较低版本的 grpc / genproto 带进依赖图，与 `goplugins` 直接依赖的
  `google.golang.org/grpc v1.79.1` 冲突，可能出现符号缺失或 proto 注册冲突；
- `go build ./...` 通过并不代表运行期 ABI 一致（`grpc.ServiceDesc`、拦截器签名等尤其敏感）。

**推荐的治理方式**（本次不擅自升级依赖主版本，也不改动 `go.sum`，由维护方统一发 tag 后再落地）：

1. 本地联调时，在 `go.mod` 打开同版本 `replace`，让依赖指向同一份代码：

   ```go
   replace github.com/gmsec/micro => ../micro
   // 同理按需：replace github.com/xxjwxc/public => ../public
   ```

   仓库里这两行已以注释形式保留，去掉注释即可启用（路径按本机实际目录调整）。

2. 正式发布时，由 `micro` 侧发布与 Go 1.25 / gRPC 1.79 对齐的新 tag，`goplugins` 用
   `go get github.com/gmsec/micro@<new-tag>` 更新到该版本并重新生成 `go.sum`；
   不要用 `go get -u` 全量升级，避免把无关依赖一起抬版本。

3. 升级后务必回归 `go build ./...`、`go vet ./...`、`go test -race ./...`，以及
   `registry/etcdv3` 的一致性测试，确认运行期 ABI 一致。
