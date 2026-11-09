package plugin

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/xxjwxc/public/dev"
	"github.com/xxjwxc/public/mylog"

	"github.com/gin-gonic/gin"
	"github.com/gmsec/micro"
	"github.com/soheilhy/cmux"
)

// shutdownTimeout 优雅关闭 HTTP 服务时等待在途请求的最长时间
const shutdownTimeout = 5 * time.Second

// WithGin gin model init. gen 初始化模式
func WithGin(router *gin.Engine) Option {
	return func(o *options) {
		o.router = router
	}
}

// WithMicro micro model init. micro 初始化模式
func WithMicro(service micro.Service) Option {
	return func(o *options) {
		o.service = service
	}
}

// WithAddr addr model init. 地址初始化
func WithAddr(addr string) Option {
	return func(o *options) {
		o.addr = addr
	}
}

// Run 启动服务:gRPC 与 HTTP 共用一个端口(cmux 分流)。
func Run(opts ...Option) (*server, error) {
	var s server
	for _, f := range opts {
		f(&s.opt)
	}

	if s.opt.service == nil {
		return nil, fmt.Errorf("goplugins: service is nil")
	}

	if len(s.opt.addr) > 0 {
		s.opt.service.Server().SetAddress(s.opt.addr)
	}
	addr := s.opt.service.Server().GetAddress()

	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("goplugins: failed to listen %v: %w", addr, err)
	}
	s.lis = lis
	s.mux = cmux.New(lis)

	// 所有 matcher 必须在 mux.Serve() 之前注册,否则存在丢连接/竞争的风险
	grpcl := s.mux.MatchWithWriters(cmux.HTTP2MatchHeaderFieldSendSettings("content-type", "application/grpc"))
	s.opt.service.Server().SetListener(grpcl)

	var httpl net.Listener
	if s.opt.router != nil {
		httpl = s.mux.Match(cmux.HTTP1Fast())
		s.httpSrv = &http.Server{Handler: s.opt.router}
	}

	s.wg.Add(1)
	go func() { // grpc
		defer s.wg.Done()
		if err := s.opt.service.Run(); err != nil {
			mylog.Error(err)
		}
		// service.Run 返回意味着收到停止信号(ctrl+c)或其他终止,
		// 这里触发整体优雅退出,让 Wait() 能够返回。
		s.shutdown()
	}()

	if httpl != nil {
		s.wg.Add(1)
		go func() { // http
			defer s.wg.Done()
			if err := s.httpSrv.Serve(httpl); err != nil && !errors.Is(err, http.ErrServerClosed) {
				debugPrintError(err)
			}
		}()
	}

	return &s, nil
}

// RunHTTP 只启动http
func RunHTTP(opts ...Option) (*server, error) {
	var s server
	for _, f := range opts {
		f(&s.opt)
	}

	if len(s.opt.addr) == 0 {
		return nil, fmt.Errorf("goplugins: addr is nil")
	}
	if s.opt.router == nil {
		return nil, fmt.Errorf("goplugins: gin router is nil")
	}

	s.httpSrv = &http.Server{Addr: s.opt.addr, Handler: s.opt.router}
	s.wg.Add(1)
	go func() { // http
		defer s.wg.Done()
		if err := s.httpSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			debugPrintError(err)
		}
	}()

	return &s, nil
}

func debugPrintError(err error) {
	if err != nil {
		if dev.IsDev() {
			// 写入 gin 的错误输出流(默认 os.Stderr),与 gin 自身日志保持一致
			fmt.Fprintf(gin.DefaultErrorWriter, "[GIN-debug] [ERROR] %v\n", err)
		}
	}
}

type server struct {
	opt     options
	wg      sync.WaitGroup
	mux     cmux.CMux
	lis     net.Listener
	httpSrv *http.Server

	stopOnce sync.Once
}

// Wait 等待服务退出(阻塞)。收到停止信号后会自动完成优雅关闭并返回。
func (s *server) Wait() {
	if s.mux != nil {
		if err := s.mux.Serve(); err != nil && !isClosedError(err) {
			mylog.Error(err)
		}
	}

	s.wg.Wait()
}

// Stop 主动优雅停止服务
func (s *server) Stop() {
	s.shutdown()
	s.wg.Wait()
}

// shutdown 幂等的关闭动作:先摘流量,再关入口 listener
func (s *server) shutdown() {
	s.stopOnce.Do(func() {
		// 1. 通知 micro 停止 gRPC 服务(内部幂等)
		if s.opt.service != nil {
			s.opt.service.NotifyStop()
		}

		// 2. 优雅关闭 HTTP,等待在途请求完成
		if s.httpSrv != nil {
			ctx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
			if err := s.httpSrv.Shutdown(ctx); err != nil {
				mylog.Error(err)
			}
			cancel()
		}

		// 3. 关闭根 listener,让 mux.Serve() 返回,否则进程无法退出
		if s.lis != nil {
			if err := s.lis.Close(); err != nil {
				mylog.Error(err)
			}
		}
	})
}

// isClosedError 判断是否属于"已经关闭"类错误,这类错误不需要当成故障打印
func isClosedError(err error) bool {
	return errors.Is(err, net.ErrClosed) || errors.Is(err, http.ErrServerClosed)
}

type options struct {
	service micro.Service
	router  *gin.Engine
	addr    string
}

// Option option list
type Option func(*options)
