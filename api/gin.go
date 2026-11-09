package api

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"

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

func GetKeyValues(ctx context.Context) map[string][]string {
	m := make(map[string][]string)
	// 调用服务端方法的时候可以在后面传参数
	md, _ := metadata.FromIncomingContext(ctx)
	if v, ok := md["gmsec-httpheader"]; ok {
		if len(v) > 0 {
			tools.JSONEncode(v[0], &m)
		}
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
