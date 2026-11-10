// Copyright 2016 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package namingregister

import (
	"context"
	"fmt"
	"time"

	"github.com/gmsec/micro/naming"
	"github.com/xxjwxc/public/mylog"
	"github.com/xxjwxc/public/tools"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var (
	// ErrWatcherClosed watcher 已关闭
	ErrWatcherClosed = fmt.Errorf("naming: watch closed")
	// ErrClientClosed naming client 未初始化或已关闭
	ErrClientClosed = fmt.Errorf("naming: client is nil or closed")
)

// GRPCResolver creates a grpc.Watcher for a target to track its resolution changes.
type GRPCResolver struct {
	// Client is an initialized etcd client.
	Client NamingClient
	// Timeout 是 firstNext 里 Get 等单次操作的超时上限(由 Options.Timeout 收敛而来)。
	Timeout time.Duration
	// HeartTimeout 保留字段仅为兼容旧调用方,已不再用于 Get 超时上限。
	// Deprecated: 请使用 Timeout。
	HeartTimeout time.Duration
}

func (gr *GRPCResolver) Update(ctx context.Context, target string, nm naming.Update) (err error) {
	if gr.Client == nil {
		return ErrClientClosed
	}
	switch nm.Op {
	case naming.Add:
		err = gr.Client.Put(ctx, target, nm)
	case naming.Delete:
		err = gr.Client.Delete(ctx, target, nm)
	default:
		return status.Error(codes.InvalidArgument, "naming: bad naming op")
	}
	return err
}

func (gr *GRPCResolver) Resolve(target string) (naming.Watcher, error) {
	if gr.Client == nil {
		return nil, ErrClientClosed
	}
	ctx, cancel := context.WithCancel(context.Background())
	w := &gRPCWatcher{
		c:           gr.Client,
		target:      target,
		serviceName: target,
		ctx:         ctx,
		cancel:      cancel,
		Timeout:     gr.Timeout,
	}
	return w, nil
}

type gRPCWatcher struct {
	c           NamingClient
	target      string
	ctx         context.Context
	cancel      context.CancelFunc
	err         error
	Timeout     time.Duration
	serviceName string
}

// Next gets the next set of updates from the etcd resolver.
// Calls to Next should be serialized; concurrent calls are not safe since
// there is no way to reconcile the update ordering.
//
// 这里只负责把注册中心的变更转给上层,不再判断"谁还活着":
// etcd 用 lease 到期删 key、mDNS 用记录过期,两者都会自然产生 Delete 事件。
// 历史上的 metadata 时间戳判活已移除,它带来的时钟漂移、误删和写放大
// 都随着原生机制接管而消失。
func (gw *gRPCWatcher) Next() ([]*naming.Update, error) {
	// watcher 已关闭:立即返回,否则调用方(grpc resolver)会永久阻塞
	if err := gw.ctx.Err(); err != nil {
		return nil, err
	}

	if !gw.c.Watchering() {
		// first Next() returns all addresses
		return gw.firstNext()
	}
	if gw.err != nil {
		return nil, gw.err
	}

	var (
		list []*naming.Update
		err  error
	)
	// 底层若支持 ContextWatcher,把 gw.ctx 传下去:Close()/cancel() 才能真正
	// 唤醒阻塞在 <-ch 的 Next()。未实现的旧 NamingClient 仍走 WatcherNext()。
	if cw, ok := gw.c.(ContextWatcher); ok {
		list, err = cw.WatcherNextWithContext(gw.ctx)
	} else {
		list, err = gw.c.WatcherNext()
	}
	if err != nil {
		// 真实错误必须回传,不能被吞成 (nil, nil) 让调用方空转。
		gw.err = err
		return nil, err
	}

	if len(list) > 0 {
		mylog.Debugf("update(%v):%v", gw.serviceName, tools.JSONDecode(list))
	}

	return list, nil
}

func (gw *gRPCWatcher) firstNext() ([]*naming.Update, error) {
	// Use serialized request so resolution still works if the target etcd
	// server is partitioned away from the quorum.
	// 超时收敛到 Options.Timeout(见 naming.go requestTimeout),不再硬编码 3s;
	// 同时以 gw.ctx 为父 ctx,Close()/cancel() 可以打断 Get。
	timeout := gw.Timeout
	if timeout < minOpTimeout {
		timeout = minOpTimeout
	}
	ctx, cancel := context.WithTimeout(gw.ctx, timeout)
	defer cancel()

	resp, err := gw.c.Get(ctx, gw.target)
	if gw.err = err; err != nil {
		return nil, err
	}

	if err := gw.c.Watch(gw.ctx, gw.target); err != nil {
		gw.err = err
		return nil, err
	}

	return resp, nil
}

func (gw *gRPCWatcher) Close() {
	// 先 cancel:让可能阻塞在 Next()/WatcherNextWithContext() 的调用方立刻返回,
	// 再关闭底层客户端。顺序反过来会让 Close() 在极端情况下等不到 Next() 退出。
	gw.cancel()
	_ = gw.c.Close()
}
