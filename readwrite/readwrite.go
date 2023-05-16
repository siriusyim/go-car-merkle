package readwrite

import (
	"github.com/ipfs/go-cid"
	"github.com/ipld/go-car/v2/blockstore"

	"github.com/filecoin-project/boost-gfm/stores"

	bstore "github.com/ipfs/go-ipfs-blockstore"
	carv2 "github.com/ipld/go-car/v2"
)

func ReadWriteFilestore(path string, cb stores.WriteAction, roots ...cid.Cid) (stores.ClosableBlockstore, error) {
	rw, err := NewReadWrite(path, roots,
		cb,
		carv2.ZeroLengthSectionAsEOF(true),
		carv2.StoreIdentityCIDs(true),
		blockstore.UseWholeCIDs(true),
	)
	if err != nil {
		return nil, err
	}

	bs, err := stores.FilestoreOf(rw)
	if err != nil {
		return nil, err
	}

	return &closableBlockstore{Blockstore: bs, closeFn: rw.Finalize}, nil
}

type closableBlockstore struct {
	bstore.Blockstore
	closeFn func() error
}

func (c *closableBlockstore) Close() error {
	return c.closeFn()
}

type WrapReadWrite struct {
	*stores.ReadWrite
}

func NewReadWrite(path string, roots []cid.Cid, cb stores.WriteAction, opts ...carv2.Option) (*WrapReadWrite, error) {
	wr, err := stores.NewReadWriteTraceFile(path, roots, cb, opts...)
	if err != nil {
		return nil, err
	}

	return &WrapReadWrite{
		wr,
	}, nil
}
