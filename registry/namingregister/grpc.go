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
	Client       NamingClient
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
	w := &gRPCWatcher{c: gr.Client, target: target, serviceName: target, ctx: ctx, cancel: cancel, HeartTimeout: gr.HeartTimeout}
	return w, nil
}

type gRPCWatcher struct {
	c      NamingClient
	target string
	ctx    context.Context
	cancel context.CancelFunc
	//wch          etcd.WatchChan
	err          error
	HeartTimeout time.Duration
	serviceName  string
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

	// process new events on target/*
	list, err := gw.c.WatcherNext()
	if err != nil {
		// 历史实现这里错写成 return nil, gw.err(gw.err 此刻为 nil),
		// 导致真实错误被吞掉,调用方拿到 (nil, nil) 后空转。
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
	// 加超时,避免 Get 在网络异常时无限阻塞(历史实现直接传无超时的 ctx)
	timeout := gw.HeartTimeout
	if timeout <= 0 || timeout > 3*time.Second {
		timeout = 3 * time.Second
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
	gw.c.Close()
	gw.cancel()
}
