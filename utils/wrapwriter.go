package utils

import (
	"io"

	"github.com/ipfs/go-cid"
)

type WriteAction func(path string, cid cid.Cid, count int, total int)

type WrapWriter struct {
	io.Writer
	path  string
	total int
	count int
	cb    WriteAction
}

func (bc *WrapWriter) Write(p []byte) (int, error) {
	n, err := bc.Writer.Write(p)
	if err == nil {
		bc.total += len(p)
		bc.count = len(p)
		c := cid.Undef
		if len(p) == 38 {
			c, _ = cid.Parse(p)
		}
		bc.cb(bc.path, c, bc.count, bc.total)
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
