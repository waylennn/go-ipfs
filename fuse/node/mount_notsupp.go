//go:build (!nofuse && openbsd) || (!nofuse && netbsd) || (!nofuse && plan9)
// +build !nofuse,openbsd !nofuse,netbsd !nofuse,plan9

package node

import (
	"errors"

	core "github.com/ipfs/ipfs-banana/core"
)

func Mount(node *core.IpfsNode, fsdir, nsdir string) error {
	return errors.New("FUSE not supported on OpenBSD or NetBSD. See #5334 (https://github.com/ipfs/ipfs-banana/issues/5334).")
}
