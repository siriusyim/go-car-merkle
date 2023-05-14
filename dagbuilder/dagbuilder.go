package dagbuilder

import (
	"context"

	chunker "github.com/ipfs/go-ipfs-chunker"
	ihelper "github.com/ipfs/go-unixfs/importer/helpers"
	"github.com/ipld/go-ipld-prime"
)

type DagBuilder struct {
	*ihelper.DagBuilderHelper
}

func WrappedDagBuilder(params *ihelper.DagBuilderParams, spl chunker.Splitter) (*ihelper.DagBuilderHelper, error) {
	db, err := params.New(spl)
	if err != nil {
		return nil, err
	}
	return &DagBuilder{
		db,
	}, nil
}

func (db *DagBuilder) Add(node ipld.Node) error {
	return db.GetDagServ().Add(context.TODO(), node)
}

/*
func sizedStore(ls *ipld.LinkSystem, lp datamodel.LinkPrototype, n datamodel.Node) (datamodel.Link, uint64, error) {
    var byteCount int
    lnk, err := wrappedLinkSystem(ls, func(bc int) {
        byteCount = bc
    }).Store(ipld.LinkContext{}, lp, n)
    return lnk, uint64(byteCount), err
}

type byteCounter struct {
    w  io.Writer
    bc int
}

func (bc *byteCounter) Write(p []byte) (int, error) {
    bc.bc += len(p)
    return bc.w.Write(p)
}

func wrappedLinkSystem(ls *ipld.LinkSystem, byteCountCb func(byteCount int)) *ipld.LinkSystem {
    wrappedEncoder := func(encoder codec.Encoder) codec.Encoder {
        return func(node datamodel.Node, writer io.Writer) error {
            bc := byteCounter{w: writer}
            err := encoder(node, &bc)
            if err == nil {
                byteCountCb(bc.bc)
            }
            return err
        }
    }
    wrappedEncoderChooser := func(lp datamodel.LinkPrototype) (codec.Encoder, error) {
        encoder, err := ls.EncoderChooser(lp)
        if err != nil {
            return nil, err
        }
        return wrappedEncoder(encoder), nil
    }
    return &ipld.LinkSystem{
        EncoderChooser:     wrappedEncoderChooser,
        DecoderChooser:     ls.DecoderChooser,
        HasherChooser:      ls.HasherChooser,
        StorageWriteOpener: ls.StorageWriteOpener,
        StorageReadOpener:  ls.StorageReadOpener,
        TrustedStorage:     ls.TrustedStorage,
        NodeReifier:        ls.NodeReifier,
        KnownReifiers:      ls.KnownReifiers,
    }
}
*/
