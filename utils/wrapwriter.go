package utils

import (
	"io"

	"github.com/ipfs/go-cid"
)

type WriteAction func(path string, cid cid.Cid, count int, total uint64)

type WrapWriter struct {
	io.Writer
	path   string
	offset uint64
	count  int
	cb     WriteAction
}

func (bc *WrapWriter) Write(p []byte) (int, error) {
	n, err := bc.Writer.Write(p)
	if err == nil {
		size := len(p)
		bc.count = size
		c := cid.Undef
		if size == 38 {
			c, _ = cid.Parse(p)
		}
		bc.cb(bc.path, c, bc.count, bc.offset)
		bc.offset += uint64(size)
		return n, nil
	}

	return n, err
}

func WrappedWriter(w io.Writer, path string, cb WriteAction) io.Writer {
	wrapped := WrapWriter{
		Writer: w,
		path:   path,
		cb:     cb,
	}
	return &wrapped
}
