// Package balanced provides methods to build balanced DAGs, which are generalistic
// DAGs in which all leaves (nodes representing chunks of data) are at the same
// distance from the root. Nodes can have only a maximum number of children; to be
// able to store more leaf data nodes balanced DAGs are extended by increasing its
// depth (and having more intermediary nodes).
//
// Internal nodes are always represented by UnixFS nodes (of type `File`) encoded
// inside DAG nodes (see the `go-unixfs` package for details of UnixFS). In
// contrast, leaf nodes with data have multiple possible representations: UnixFS
// nodes as above, raw nodes with just the file data (no format) and Filestore
// nodes (that directly link to the file on disk using a format stored on a raw
// node, see the `go-ipfs/filestore` package for details of Filestore.)
//
// In the case the entire file fits into just one node it will be formatted as a
// (single) leaf node (without parent) with the possible representations already
// mentioned. This is the only scenario where the root can be of a type different
// that the UnixFS node.
//
// Notes:
// 1. In the implementation. `FSNodeOverDag` structure is used for representing
//    the UnixFS node encoded inside the DAG node.
//    (see https://github.com/ipfs/go-ipfs/pull/5118.)
// 2. `TFile` is used for backwards-compatibility. It was a bug causing the leaf
//    nodes to be generated with this type instead of `TRaw`. The former one
//    should be used (like the trickle builder does).
//    (See https://github.com/ipfs/go-ipfs/pull/5120.)
//
//                                                 +-------------+
//                                                 |   Root 4    |
//                                                 +-------------+
//                                                       |
//                            +--------------------------+----------------------------+
//                            |                                                       |
//                      +-------------+                                         +-------------+
//                      |   Node 2    |                                         |   Node 5    |
//                      +-------------+                                         +-------------+
//                            |                                                       |
//              +-------------+-------------+                           +-------------+
//              |                           |                           |
//       +-------------+             +-------------+             +-------------+
//       |   Node 1    |             |   Node 3    |             |   Node 6    |
//       +-------------+             +-------------+             +-------------+
//              |                           |                           |
//       +------+------+             +------+------+             +------+
//       |             |             |             |             |
//  +=========+   +=========+   +=========+   +=========+   +=========+
//  | Chunk 1 |   | Chunk 2 |   | Chunk 3 |   | Chunk 4 |   | Chunk 5 |
//  +=========+   +=========+   +=========+   +=========+   +=========+
//
package balanced

import (
	"errors"

	ft "github.com/ipfs/go-unixfs"
	h "github.com/ipfs/go-unixfs/importer/helpers"

	ipld "github.com/ipfs/go-ipld-format"
)

// Layout builds a balanced DAG layout. In a balanced DAG of depth 1, leaf nodes
// with data are added to a single `root` until the maximum number of links is
// reached. Then, to continue adding more data leaf nodes, a `newRoot` is created
// pointing to the old `root` (which will now become and intermediary node),
// increasing the depth of the DAG to 2. This will increase the maximum number of
// data leaf nodes the DAG can have (`Maxlinks() ^ depth`). The `fillNodeRec`
// function will add more intermediary child nodes to `newRoot` (which already has
// `root` as child) that in turn will have leaf nodes with data added to them.
// After that process is completed (the maximum number of links is reached),
// `fillNodeRec` will return and the loop will be repeated: the `newRoot` created
// will become the old `root` and a new root will be created again to increase the
// depth of the DAG. The process is repeated until there is no more data to add
// (i.e. the DagBuilderHelper’s Done() function returns true).
//
// The nodes are filled recursively, so the DAG is built from the bottom up. Leaf
// nodes are created first using the chunked file data and its size. The size is
// then bubbled up to the parent (internal) node, which aggregates all the sizes of
// its children and bubbles that combined size up to its parent, and so on up to
// the root. This way, a balanced DAG acts like a B-tree when seeking to a byte
// offset in the file the graph represents: each internal node uses the file size
// of its children as an index when seeking.
//
//      `Layout` creates a root and hands it off to be filled:
//
//             +-------------+
//             |   Root 1    |
//             +-------------+
//                    |
//       ( fillNodeRec fills in the )
//       ( chunks on the root.      )
//                    |
//             +------+------+
//             |             |
//        + - - - - +   + - - - - +
//        | Chunk 1 |   | Chunk 2 |
//        + - - - - +   + - - - - +
//
//                           ↓
//      When the root is full but there's more data...
//                           ↓
//
//             +-------------+
//             |   Root 1    |
//             +-------------+
//                    |
//             +------+------+
//             |             |
//        +=========+   +=========+   + - - - - +
//        | Chunk 1 |   | Chunk 2 |   | Chunk 3 |
//        +=========+   +=========+   + - - - - +
//
//                           ↓
//      ...Layout's job is to create a new root.
//                           ↓
//
//                            +-------------+
//                            |   Root 2    |
//                            +-------------+
//                                  |
//                    +-------------+ - - - - - - - - +
//                    |                               |
//             +-------------+            ( fillNodeRec creates the )
//             |   Node 1    |            ( branch that connects    )
//             +-------------+            ( "Root 2" to "Chunk 3."  )
//                    |                               |
//             +------+------+             + - - - - -+
//             |             |             |
//        +=========+   +=========+   + - - - - +
//        | Chunk 1 |   | Chunk 2 |   | Chunk 3 |
//        +=========+   +=========+   + - - - - +
//     这就是默克尔树 不是图。。。特么的
func Layout(db *h.DagBuilderHelper) (ipld.Node, error) {
	if db.Done() {
		// No data, return just an empty node.
		root, err := db.NewLeafNode(nil, ft.TFile)
		if err != nil {
			return nil, err
		}
		// This works without Filestore support (`ProcessFileStore`).
		// TODO: Why? Is there a test case missing?

		return root, db.Add(root)
	}

	// The first `root` will be a single leaf node with data
	// (corner case), after that subsequent `root` nodes will
	// always be internal nodes (with a depth > 0) that can
	// be handled by the loop.
	//  第一个`root`将是一个带有数据的单叶结点 (角的情况），之后的`根'节点总是内部节点（深度>0），可以被循环处理。
	/* 构建第一个根节点, 该根节点也包含data */
	root, fileSize, err := db.NewLeafDataNode(ft.TFile)
	if err != nil {
		return nil, err
	}

	// Each time a DAG of a certain `depth` is filled (because it
	// has reached its maximum capacity of `db.Maxlinks()` per node)
	// extend it by making it a sub-DAG of a bigger DAG with `depth+1`.
	// 每次某个深度的DAG被填满（因为它已经达到了每个节点的最大容量`db.Maxlinks()`），就把它扩展为一个深度为`1`的更大DAG的子DAG。
	/* 如果只需要一个block，那么直接返回root即可 */
	/* 否则，从depth开始构建 Merkle Tree,下面会详细描述该过程 */
	for depth := 1; !db.Done(); depth++ {

		// Add the old `root` as a child of the `newRoot`. 每次进来都创建一个新root作为父节点
		newRoot := db.NewFSNodeOverDag(ft.TFile) // DAG上的文件节点
		newRoot.AddChild(root, fileSize, db)

		// Fill the `newRoot` (that has the old `root` already as child)
		// and make it the current `root` for the next iteration (when
		// it will become "old").
		// 填充 "newRoot"（已经有旧的 "root "作为子节点），并使其成为下一次迭代的当前 "root"（当它成为 "旧 "时. db里面可以一直读取下一段数据
		root, fileSize, err = fillNodeRec(db, newRoot, depth)
		if err != nil {
			return nil, err
		}
	}

	return root, db.Add(root)
}

// fillNodeRec will "fill" the given internal (non-leaf) `node` with data by
// adding child nodes to it, either leaf data nodes (if `depth` is 1) or more
// internal nodes with higher depth (and calling itself recursively on them
// until *they* are filled with data). The data to fill the node with is
// provided by DagBuilderHelper.
// fillNodeRec将用数据 "填充 "给定的内部（非叶子）"节点"，向其添加子节点，要么是叶子数据节点（如果`深度'为1），
// 要么是更高深度的内部节点（并递归调用它们，直到*它们*被填充数据）。填充节点的数据由DagBuilderHelper提供。

// `node` represents a (sub-)DAG root that is being filled. If called recursively,
// it is `nil`, a new node is created. If it has been called from `Layout` (see
// diagram below) it points to the new root (that increases the depth of the DAG),
// it already has a child (the old root). New children will be added to this new
// root, and those children will in turn be filled (calling `fillNodeRec`
// recursively).
// `node`代表一个正在被填充的（子）DAG根。如果被递归调用，它是`nil`，一个新的节点被创建。如果它是从`Layout`调用的（见下图），
//  它指向新的根（增加了DAG的深度），它已经有一个子节点（旧的根）。
//  新的子节点将被添加到这个新的根上，而这些子节点将反过来被填充（递归调用`fillNodeRec`）。
//
//                      +-------------+
//                      |   `node`    |
//                      |  (new root) |
//                      +-------------+
//                            |
//              +-------------+ - - - - - - + - - - - - - - - - - - +
//              |                           |                       |
//      +--------------+             + - - - - -  +           + - - - - -  +
//      |  (old root)  |             |  new child |           |            |
//      +--------------+             + - - - - -  +           + - - - - -  +
//              |                          |                        |
//       +------+------+             + - - + - - - +
//       |             |             |             |
//  +=========+   +=========+   + - - - - +    + - - - - +
//  | Chunk 1 |   | Chunk 2 |   | Chunk 3 |    | Chunk 4 |
//  +=========+   +=========+   + - - - - +    + - - - - +
//
// The `node` to be filled uses the `FSNodeOverDag` abstraction that allows adding
// child nodes without packing/unpacking the UnixFS layer node (having an internal
// `ft.FSNode` cache).
// 要填充的 "节点 "使用 "FSNodeOverDag "抽象，该抽象允许添加子节点，而无需对UnixFS层节点进行打包/解包（有一个内部的`ft.FSNode`缓存）

// It returns the `ipld.Node` representation of the passed `node` filled with
// children and the `nodeFileSize` with the total size of the file chunk (leaf)
// nodes stored under this node (parent nodes store this to enable efficient
// seeking through the DAG when reading data later).
// 它返回所传递的 "node"的 "ipld.Node "表示，其中充满了子节点，
// "nodeFileSize "表示存储在该节点下的文件块（叶子）节点的总大小（父节点存储这个，以便以后读取数据时能够在DAG中进行有效搜索

// warning: **children** pinned indirectly, but input node IS NOT pinned.
// 子节点被固定， 但是输入节点不一定被固定
func fillNodeRec(db *h.DagBuilderHelper, node *h.FSNodeOverDag, depth int) (filledNode ipld.Node, nodeFileSize uint64, err error) {
	if depth < 1 {
		return nil, 0, errors.New("attempt to fillNode at depth < 1")
	}

	if node == nil {
		node = db.NewFSNodeOverDag(ft.TFile)
	}

	// Child node created on every iteration to add to parent `node`.
	// It can be a leaf node or another internal node.
	// // 在每次迭代中创建的子节点添加到父节点`node`。
	//	// 它可以是一个叶子节点或其他内部节点。
	var childNode ipld.Node
	// File size from the child node needed to update the `FSNode`
	// in `node` when adding the child.
	//  添加子节点时，需要更新`node`中的`FSNode`的文件大小
	var childFileSize uint64

	// While we have room and there is data available to be added.
	// 虽然我们有空间，并且有数据可以添加
	for node.NumChildren() < db.Maxlinks() && !db.Done() {

		if depth == 1 {
			// Base case: add leaf node with data.
			childNode, childFileSize, err = db.NewLeafDataNode(ft.TFile)
			if err != nil {
				return nil, 0, err
			}
		} else {
			// Recursion case: create an internal node to in turn keep
			// descending in the DAG and adding child nodes to it. 递归案例：创建一个内部节点，依次在DAG中不断下降并向其添加子节点. 这里相当于重写创建带有数据的子节点
			childNode, childFileSize, err = fillNodeRec(db, nil, depth-1)
			if err != nil {
				return nil, 0, err
			}
		}

		err = node.AddChild(childNode, childFileSize, db)
		if err != nil {
			return nil, 0, err
		}
	}

	nodeFileSize = node.FileSize()

	// Get the final `dag.ProtoNode` with the `FSNode` data encoded inside. 获得最终的`dag.ProtoNode'，里面有`FSNode'数据编码
	filledNode, err = node.Commit()
	if err != nil {
		return nil, 0, err
	}

	return filledNode, nodeFileSize, nil
}
