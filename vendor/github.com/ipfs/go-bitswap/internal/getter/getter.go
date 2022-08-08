package getter

import (
	"context"
	"errors"

	notifications "github.com/ipfs/go-bitswap/internal/notifications"
	logging "github.com/ipfs/go-log"

	blocks "github.com/ipfs/go-block-format"
	cid "github.com/ipfs/go-cid"
	ipld "github.com/ipfs/go-ipld-format"
)

var log = logging.Logger("bitswap")

// GetBlocksFunc is any function that can take an array of CIDs and return a
// channel of incoming blocks.
type GetBlocksFunc func(context.Context, []cid.Cid) (<-chan blocks.Block, error)

// SyncGetBlock takes a block cid and an async function for getting several
// blocks that returns a channel, and uses that function to return the
// block syncronously.
/*
 接受一个区块cid和一个用于获取几个区块的异步函数，该函数返回一个channel，并使用该函数同步地返回区块。
 */
func SyncGetBlock(p context.Context, k cid.Cid, gb GetBlocksFunc) (blocks.Block, error) {
	if !k.Defined() {
		log.Error("undefined cid in GetBlock")
		return nil, ipld.ErrNotFound{Cid: k}
	}

	// Any async work initiated by this function must end when this function
	// returns. To ensure this, derive a new context. Note that it is okay to
	// listen on parent in this scope, but NOT okay to pass |parent| to
	// functions called by this one. Otherwise those functions won't return
	// when this context's cancel func is executed. This is difficult to
	// enforce. May this comment keep you safe.
	/*
	任何由该函数发起的异步工作必须在该函数返回时结束。为了确保这一点，要派生出一个新的上下文。
	请注意，在这个作用域中可以监听parent，但不可以把|parent|传递给被这个作用域调用的函数。
	否则，当这个上下文的取消函数被执行时，这些函数将不会返回。这是很难执行的。
	愿这个评论能保证你的安全
	 */
	ctx, cancel := context.WithCancel(p)
	defer cancel()

	promise, err := gb(ctx, []cid.Cid{k})
	if err != nil {
		return nil, err
	}

	select {
	case block, ok := <-promise:
		if !ok {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			default:
				return nil, errors.New("promise channel was closed")
			}
		}
		return block, nil
	case <-p.Done():
		return nil, p.Err()
	}
}

// WantFunc is any function that can express a want for set of blocks.
type WantFunc func(context.Context, []cid.Cid)

// AsyncGetBlocks take a set of block cids, a pubsub channel for incoming
// blocks, a want function, and a close function, and returns a channel of
// incoming blocks.
/*
接收一组 块的 cids、一个用于接收区块的pubsub channel、
一个want函数和一个close函数，并返回一个接收区块的通道
 */
func AsyncGetBlocks(ctx context.Context, sessctx context.Context, keys []cid.Cid, notif notifications.PubSub,
	want WantFunc, cwants func([]cid.Cid)) (<-chan blocks.Block, error) {

	// If there are no keys supplied, just return a closed channel
	// 如果没有keys 提供，返回一个关闭的channel
	if len(keys) == 0 {
		out := make(chan blocks.Block)
		close(out)
		return out, nil
	}

	// Use a PubSub notifier to listen for incoming blocks for each key
	// 使用一个PubSub通知器来监听每个密钥的传入块
	remaining := cid.NewSet()
	promise := notif.Subscribe(ctx, keys...)
	for _, k := range keys {
		log.Debugw("Bitswap.GetBlockRequest.Start", "cid", k)
		remaining.Add(k)
	}

	// Send the want request for the keys to the network
	// 向网络发送 keys 的需求请求
	want(ctx, keys)

	out := make(chan blocks.Block)
	go handleIncoming(ctx, sessctx, remaining, promise, out, cwants)
	return out, nil
}

// Listens for incoming blocks, passing them to the out channel.
// If the context is cancelled or the incoming channel closes, calls cfun with
// any keys corresponding to blocks that were never received.
/*
 侦听传入的块列表，将它们传递给out channel。
 如果上下文被取消或传入的通道关闭，则用任何与从未收到的blocks对应的keys去调用cfun
 */
func handleIncoming(ctx context.Context, sessctx context.Context, remaining *cid.Set,
	in <-chan blocks.Block, out chan blocks.Block, cfun func([]cid.Cid)) {

	ctx, cancel := context.WithCancel(ctx)

	// Clean up before exiting this function, and call the cancel function on
	// any remaining keys
	// 在退出这个函数之前进行清理，并在任何剩余的keys上调用cancel函数
	defer func() {
		cancel()
		close(out)
		// can't just defer this call on its own, arguments are resolved *when* the defer is created
		// 不能单独推迟这个调用，参数是在创建推迟的时候解决的
		cfun(remaining.Keys())
	}()

	for {
		select {
		case blk, ok := <-in:
			// If the channel is closed, we're done (note that PubSub closes
			// the channel once all the keys have been received)
			// 如果通道被关闭，我们就完成并返回（注意PubSub在收到所有密钥后就会关闭通道)
			if !ok {
				return
			}

			remaining.Remove(blk.Cid())
			select {
			case out <- blk:
			case <-ctx.Done():
				return
			case <-sessctx.Done():
				return
			}
		case <-ctx.Done():
			return
		case <-sessctx.Done():
			return
		}
	}
}
