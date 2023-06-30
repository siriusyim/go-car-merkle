package utils

import (
	"io"

	"github.com/ipfs/go-cid"
)

type WriteAfterAction func(path string, cid cid.Cid, count int, total uint64)

type WriteBeforeAction func([]byte, io.Writer) ([]byte, error)

func DefaultWriteAfterAction(path string, cid cid.Cid, count int, total uint64) {}

func DefaultWriteBeforeAction(buf []byte, w io.Writer) ([]byte, error) { return buf, nil }

type WrapWriter struct {
	io.Writer
	path   string
	offset uint64
	count  int
	after  WriteAfterAction
	before WriteBeforeAction
}

func (bc *WrapWriter) Write(p []byte) (int, error) {
	buf, err := bc.before(p, bc.Writer)
	if err != nil {
		return 0, err
	}

	n, err := bc.Writer.Write(buf)
	if err == nil {
		size := len(p)
		bc.count = size
		c := cid.Undef
		if size == 38 {
			c, _ = cid.Parse(p)
		}
		bc.after(bc.path, c, bc.count, bc.offset)
		bc.offset += uint64(size)
		return n, nil
	}

	return n, err
}

func WrappedWriter(w io.Writer, path string, acb WriteAfterAction, bcb WriteBeforeAction) io.Writer {
	wrapped := WrapWriter{
		Writer: w,
		path:   path,
		after:  acb,
		before: bcb,
	}
	return &wrapped
}
