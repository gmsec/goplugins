package namingregister

import (
	"context"

	"github.com/gmsec/micro/naming"
)

type NamingClient interface {
	// Put puts a key-value pair
	Put(ctx context.Context, serviceName string, val naming.Update) error
	// Delete deletes a key, or optionally using WithRange(end), [key, end).
	Delete(ctx context.Context, serviceName string, val naming.Update) error

	// Get retrieves keys.
	Get(ctx context.Context, serviceName string) ([]*naming.Update, error)

	// Watchering Watcher is init
	Watchering() bool

	// Watch start watch
	Watch(ctx context.Context, serviceName string) error

	// WatcherNext watching next
	WatcherNext() ([]*naming.Update, error)

	// New new watching client
	New(serviceName string) NamingClient

	// Close close
	Close() error
}

// ContextWatcher 是 NamingClient 的可选增强接口(保持 NamingClient 本身不变,
// 下游已有实现无需改动即可继续工作)。实现后 GRPCResolver 会把 watcher 的 ctx
// 传进来,使 Close()/cancel() 能唤醒阻塞在 <-ch 的 Next();未实现该接口时
// 仍走旧的 WatcherNext()。
//
// 约定:实现应把 ctx.Done() 纳入阻塞等待的 select,并在压缩/重建 watch 时
// 复用同一个 ctx,而不是 context.Background()。
type ContextWatcher interface {
	WatcherNextWithContext(ctx context.Context) ([]*naming.Update, error)
}
