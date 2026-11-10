package api

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"

	"github.com/xxjwxc/public/mylog"
	"github.com/xxjwxc/public/tools"
	"google.golang.org/grpc/metadata"

	"github.com/gin-gonic/gin"
)

// gin key
type ginHTTPReq struct{}

// GetVersion Get the version by req url
func (c *Context) GetVersion() string { // 获取版本号
	return c.GetGinCtx().Param("version")
}

// WriteJSON 写入json对象
func (c *Context) WriteJSON(obj interface{}) {
	c.GetGinCtx().JSON(200, obj)
}

// WriteHeadToCtx 设置头数据到grpc header里面
func (c *Context) WriteHeadToCtx(g *gin.Context) {
	if g == nil {
		g = c.GetGinCtx()
	}
	if g == nil || g.Request == nil {
		return
	}

	js := tools.JSONDecode(g.Request.Header)
	// gRPC 的 metadata value 只能是 ASCII 可打印字符。
	// 请求头里出现了中文/其他非 ASCII 字符时，直接透传会导致整个调用报
	// "header key/value contains illegal characters"，所以在这里转义。
	headerData := metadata.Pairs("gmsec-httpheader", asciiSafe(js))
	c.Context = metadata.NewOutgoingContext(c.Context, headerData)
}

// asciiSafe 把非 ASCII 字符转成 JSON 的 \uXXXX 转义，保证可以放进 grpc metadata。
// 接收方解码 JSON 时会自动还原成原字符。
func asciiSafe(s string) string {
	if isASCII(s) {
		return s
	}

	var b strings.Builder
	b.Grow(len(s) * 2)
	for _, r := range s {
		if r < 0x80 {
			b.WriteRune(r)
			continue
		}
		if r > 0xFFFF { // 超出 BMP，拆成代理对
			r -= 0x10000
			fmt.Fprintf(&b, "\\u%04x\\u%04x", 0xD800+(r>>10), 0xDC00+(r&0x3FF))
			continue
		}
		fmt.Fprintf(&b, "\\u%04x", r)
	}
	return b.String()
}

func isASCII(s string) bool {
	for i := 0; i < len(s); i++ {
		if s[i] >= 0x80 {
			return false
		}
	}
	return true
}

// AddHeadToCtx 添加一层数据到ctx里面
func (c *Context) AddHeadToCtx(kv ...string) { // 后续也可以往后面添加数据
	c.Context = metadata.AppendToOutgoingContext(c.Context, kv...)
}

// GetGinCtx 获取 gin.Context
func (c *Context) GetGinCtx() *gin.Context {
	req := c.GetValue(ginHTTPReq{})
	if req != nil {
		if r, ok := req.(*gin.Context); ok {
			return r
		}
	}

	mylog.Info("using default gin.context")
	r, _ := gin.CreateTestContext(httptest.NewRecorder())
	r.Request = &http.Request{
		Method: "POST",
		Header: GetKeyValues(c),
	}
	c.SetValue(ginHTTPReq{}, r)
	return r
}

// defaultBlockedHeaders 是从 gmsec-httpheader 还原请求头时默认阻断的头。
// 这些头一旦被不可信的 gRPC 调用方通过 metadata 原样注入,会造成身份伪造、
// 请求走私、转发链伪造或缓存污染。key 一律按小写比较。
var defaultBlockedHeaders = map[string]struct{}{
	// 认证 / 凭据
	"authorization":       {},
	"proxy-authorization": {},
	"cookie":              {},
	"set-cookie":          {},
	"www-authenticate":    {},
	"proxy-authenticate":  {},
	// 请求目标 / 长度 / 期望
	"host":           {},
	"content-length": {},
	"expect":         {},
	"max-forwards":   {},
	// hop-by-hop(逐跳)头
	"connection":        {},
	"proxy-connection":  {},
	"keep-alive":        {},
	"transfer-encoding": {},
	"te":                {},
	"trailer":           {},
	"upgrade":           {},
	"via":               {},
	// 转发链伪造头(另外还统一拦截 x-forwarded-* 前缀)
	"forwarded":          {},
	"x-forwarded-for":    {},
	"x-forwarded-host":   {},
	"x-forwarded-proto":  {},
	"x-forwarded-port":   {},
	"x-forwarded-prefix": {},
	"x-forwarded-server": {},
	"x-real-ip":          {},
	"x-original-url":     {},
	"x-rewrite-url":      {},
	// 内部保留头
	"gmsec-httpheader": {},
}

// headerPolicy 头部过滤策略。allowlist 为 nil 时是黑名单模式(默认),
// 非 nil 时切换为白名单模式(仍先过黑名单)。
type headerPolicy struct {
	allowlist map[string]struct{}
	blocklist map[string]struct{}
}

func newHeaderPolicy() headerPolicy {
	b := make(map[string]struct{}, len(defaultBlockedHeaders))
	for k, v := range defaultBlockedHeaders {
		b[k] = v
	}
	return headerPolicy{blocklist: b}
}

// HeaderOption 配置 GetKeyValues 的头部过滤策略。
type HeaderOption func(*headerPolicy)

// WithHeaderAllowlist 设置可透传头的白名单(大小写不敏感)。设置后只有名单内
// 的头会透传,且仍会先排除内置的敏感/转发头;传空参数则回到默认黑名单模式。
// 适合只信任少数业务头(如 x-request-id、x-trace-id)的场景。
func WithHeaderAllowlist(keys ...string) HeaderOption {
	return func(p *headerPolicy) {
		if len(keys) == 0 {
			p.allowlist = nil
			return
		}
		m := make(map[string]struct{}, len(keys))
		for _, k := range keys {
			m[strings.ToLower(strings.TrimSpace(k))] = struct{}{}
		}
		p.allowlist = m
	}
}

// WithHeaderBlocklist 追加需要额外阻断的头(大小写不敏感),用于补充业务侧
// 自定义的敏感头。
func WithHeaderBlocklist(keys ...string) HeaderOption {
	return func(p *headerPolicy) {
		for _, k := range keys {
			p.blocklist[strings.ToLower(strings.TrimSpace(k))] = struct{}{}
		}
	}
}

var (
	headerPolicyMu      sync.RWMutex
	currentHeaderPolicy = newHeaderPolicy()
)

// SetHeaderPolicy 应用头部过滤配置(并发安全)。策略被整体替换,已发布的
// map 只读共享,不会在读取期间被修改。
//
//	api.SetHeaderPolicy(api.WithHeaderAllowlist("x-request-id", "x-trace-id"))
//	api.SetHeaderPolicy(api.WithHeaderBlocklist("x-internal-token"))
//	api.SetHeaderPolicy() // 恢复默认
func SetHeaderPolicy(opts ...HeaderOption) {
	headerPolicyMu.Lock()
	defer headerPolicyMu.Unlock()
	p := newHeaderPolicy()
	for _, o := range opts {
		o(&p)
	}
	currentHeaderPolicy = p
}

func loadHeaderPolicy() headerPolicy {
	headerPolicyMu.RLock()
	defer headerPolicyMu.RUnlock()
	return currentHeaderPolicy
}

// GetKeyValues 从 gmsec-httpheader metadata 还原请求头,但不再原样照搬:
//   - 阻断认证/凭据、hop-by-hop、请求目标类头,以及所有 x-forwarded-* 前缀头;
//   - 若配置了白名单,则只放行白名单内的头;
//   - 丢弃含 CR/LF/NUL 的值,防止头部值注入。
//
// 这样不可信的 gRPC 调用方无法借 metadata 注入 Host/Authorization 或伪造转发链。
func GetKeyValues(ctx context.Context) map[string][]string {
	m := make(map[string][]string)
	// 调用服务端方法的时候可以在后面传参数
	md, _ := metadata.FromIncomingContext(ctx)
	if v, ok := md["gmsec-httpheader"]; ok && len(v) > 0 {
		tools.JSONEncode(v[0], &m)
	}

	p := loadHeaderPolicy()
	for k, vals := range m {
		lk := strings.ToLower(strings.TrimSpace(k))
		if lk == "" {
			delete(m, k)
			continue
		}
		// 1) 内置黑名单 + 转发链前缀,一律阻断
		if _, blocked := p.blocklist[lk]; blocked || strings.HasPrefix(lk, "x-forwarded-") {
			delete(m, k)
			continue
		}
		// 2) 白名单模式:只放行名单内的头
		if p.allowlist != nil {
			if _, allowed := p.allowlist[lk]; !allowed {
				delete(m, k)
				continue
			}
		}
		// 3) 过滤含 CR/LF/NUL 的值,防止头部值注入
		clean := vals[:0]
		for _, v := range vals {
			if strings.ContainsAny(v, "\r\n\x00") {
				continue
			}
			clean = append(clean, v)
		}
		if len(clean) == 0 {
			delete(m, k)
			continue
		}
		m[k] = clean
	}
	return m
}

// NewCtx Create a new custom context
func NewCtx(c *gin.Context) *Context { // 新建一个自定义context
	// 从 HTTP 请求派生上下文,让超时/取消/链路追踪信息能自动透传到下游,
	// 而不是像历史实现那样用 context.TODO() 把上游信息全部截断。
	base := context.Background()
	if c != nil && c.Request != nil {
		base = c.Request.Context()
	}
	ctx := &Context{base}
	if c != nil { // context.WithValue 不允许 value 为 nil
		ctx.SetValue(ginHTTPReq{}, c)
	}
	return ctx
}

// NewAPIFunc default of custom handlefunc
func NewAPIFunc(c *gin.Context) interface{} {
	return NewCtx(c)
}
