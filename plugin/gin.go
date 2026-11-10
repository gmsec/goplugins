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

// shutdownTimeout 优雅关闭 HTTP / gRPC 时等待在途请求的最长时间
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
	s.errCh = make(chan error, 1)

	// 所有 matcher 必须在 mux.Serve() 之前注册,否则存在丢连接/竞争的风险
	grpcl := s.mux.MatchWithWriters(cmux.HTTP2MatchHeaderFieldSendSettings("content-type", "application/grpc"))
	s.opt.service.Server().SetListener(grpcl)

	var httpl net.Listener
	if s.opt.router != nil {
		httpl = s.mux.Match(cmux.HTTP1Fast())
		s.httpSrv = &http.Server{Handler: s.opt.router}
	}

	// cmux.Serve() 必须在 Run() 里就启动并纳入 wg:
	// 否则只调 Run() 不调 Wait() 时,matcher listener 收不到任何连接,服务静默不可用。
	s.wg.Add(1)
	go func() { // cmux
		defer s.wg.Done()
		if err := s.mux.Serve(); err != nil && !isClosedError(err) {
			err = fmt.Errorf("goplugins: cmux serve: %w", err)
			s.reportErr(err)
			mylog.Error(err)
			s.abort()
		}
	}()

	s.wg.Add(1)
	go func() { // grpc
		defer s.wg.Done()
		if err := s.opt.service.Run(); err != nil {
			// 启动/运行期错误:通过 errCh 回传给调用方,并做最小清理,
			// 但不把它当成"收到停止信号后的优雅停止"(历史上无条件 shutdown
			// 会把启动失败伪装成一次正常退出,且不暴露任何错误)。
			err = fmt.Errorf("goplugins: service run: %w", err)
			s.reportErr(err)
			mylog.Error(err)
			s.abort()
			return
		}
		// Run() 正常返回 == 真正的停止信号(ctrl+c / Context.Done)
		s.shutdown()
	}()

	if httpl != nil {
		s.wg.Add(1)
		go func() { // http
			defer s.wg.Done()
			if err := s.httpSrv.Serve(httpl); err != nil && !isClosedError(err) {
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

	// 同步 bind:拿到 listener 之后再打日志,保证日志对应的是"端口已监听";
	// 端口占用等错误当场通过返回值暴露,不用等 Err() 通道。
	lis, err := net.Listen("tcp", s.opt.addr)
	if err != nil {
		return nil, fmt.Errorf("goplugins: failed to listen %v: %w", s.opt.addr, err)
	}
	s.lis = lis
	s.httpSrv = &http.Server{Addr: s.opt.addr, Handler: s.opt.router}
	s.errCh = make(chan error, 1)

	// 改用 http.Server 之后不再经过 gin 的 Run(),这里按 gin 的调试输出约定
	// 把 "Listening and serving HTTP on :xx" 补回来。
	debugPrintListening(s.opt.addr)

	s.wg.Add(1)
	go func() { // http
		defer s.wg.Done()
		if err := s.httpSrv.Serve(lis); err != nil && !isClosedError(err) {
			err = fmt.Errorf("goplugins: http serve: %w", err)
			s.reportErr(err)
			mylog.Error(err)
			s.abort()
		}
	}()

	return &s, nil
}

// debugPrintListening 复现 gin.Engine.Run 里的启动日志:
// RunHTTP 不再调用 router.Run(),这行日志会丢失,按 gin 的输出约定补回来。
func debugPrintListening(addr string) {
	if !gin.IsDebugging() {
		return
	}
	fmt.Fprintf(gin.DefaultWriter, "[GIN-debug] Listening and serving HTTP on %s\n", addr)
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
	errCh   chan error

	stopOnce sync.Once
}

// Err 返回服务错误通道(缓冲 1,只保留第一个错误,通道不会关闭)。
// 收到非 nil 错误表示启动/运行异常,调用方可据此决策(必要时调用 Stop 清理);
// 保持 Run()/RunHTTP() 的签名不变,错误改由这里回传。
func (s *server) Err() <-chan error { return s.errCh }

func (s *server) reportErr(err error) {
	if err == nil || s.errCh == nil {
		return
	}
	select {
	case s.errCh <- err:
	default: // 只保留第一个错误,避免写满阻塞
	}
}

// Wait 等待服务退出(阻塞)。mux.Serve() 已在 Run() 的 goroutine 里运行,
// 收到停止信号后由 shutdown() 关闭入口并完成优雅关闭,各 goroutine 退出后返回。
func (s *server) Wait() {
	s.wg.Wait()
}

// Stop 主动优雅停止服务
func (s *server) Stop() {
	s.shutdown()
	s.wg.Wait()
}

// shutdown 幂等的完整优雅关闭:只有"真正的停止信号"才调用。
func (s *server) shutdown() { s.finish(true) }

// abort 启动/运行异常时的最小清理:关掉入口、解除 wg 阻塞,但不走优雅流程,
// 避免把失败伪装成一次正常停止。
func (s *server) abort() { s.finish(false) }

func (s *server) finish(graceful bool) {
	s.stopOnce.Do(func() {
		// 1. 先关根 listener:cmux 停止接收新连接,Serve() 随之返回,
		//    matcher listener 也会被 cmux 关闭,gRPC/HTTP 的 Serve 一并退出。
		if s.lis != nil {
			if err := s.lis.Close(); err != nil && !isClosedError(err) {
				mylog.Error(err)
			}
		}

		if !graceful {
			// 异常路径:立即关闭,不等在途请求;并通知 micro 让 Run() 退出。
			if s.httpSrv != nil {
				if err := s.httpSrv.Close(); err != nil && !isClosedError(err) {
					mylog.Error(err)
				}
			}
			if s.opt.service != nil {
				if gs := s.opt.service.Server().GetServer(); gs != nil {
					gs.Stop()
				}
				s.opt.service.NotifyStop()
			}
			return
		}

		// 2. gRPC 优雅停止:GracefulStop 可能因为连接迟迟不释放而永久阻塞,
		//    放到独立 goroutine + 超时兜底,超时后强制 Stop()。
		if s.opt.service != nil {
			if gs := s.opt.service.Server().GetServer(); gs != nil {
				done := make(chan struct{})
				go func() {
					gs.GracefulStop()
					close(done)
				}()
				select {
				case <-done:
				case <-time.After(shutdownTimeout):
					gs.Stop()
				}
			}
		}

		// 3. 优雅关闭 HTTP,等待在途请求完成。
		if s.httpSrv != nil {
			ctx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
			if err := s.httpSrv.Shutdown(ctx); err != nil {
				mylog.Error(err)
			}
			cancel()
		}

		// 4. 最后通知 micro:service.Run() 从信号等待中返回,deregister 后退出,
		//    grpc goroutine 的 wg.Done 随之触发,Wait() 才能返回。
		if s.opt.service != nil {
			s.opt.service.NotifyStop()
		}
	})
}

// isClosedError 判断是否属于"已经关闭"类错误,这类错误不需要当成故障打印
func isClosedError(err error) bool {
	return errors.Is(err, net.ErrClosed) ||
		errors.Is(err, http.ErrServerClosed) ||
		errors.Is(err, cmux.ErrListenerClosed) ||
		errors.Is(err, cmux.ErrServerClosed)
}

type options struct {
	service micro.Service
	router  *gin.Engine
	addr    string
}

// Option option list
type Option func(*options)
