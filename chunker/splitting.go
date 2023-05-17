package chunker

import (
	"io"

	chunkers "github.com/ipfs/go-ipfs-chunker"
	pool "github.com/libp2p/go-buffer-pool"
	"github.com/siriusyim/go-car-merkle/utils"
)

const UnixfsChunkSize uint64 = 1 << 20

type SplitterAction func(srcPath string, offset uint64, size uint32, eof bool)

func DefaultSplitterAction(srcPath string, offset uint64, size uint32, eof bool) {}

type sliceSplitter struct {
	r    io.Reader
	size uint32
	err  error

	//记录原文件路径
	srcPath string
	//允许外部传入回调函数获取原始文件读取信息
	cb SplitterAction
	//记录当前文件读取offset
	offset uint64

	utils.Subject
}

// NewSliceSplitter returns a new size-based Splitter with the given block size.
func NewSliceSplitter(r io.Reader, size int64, srcPath string, cb SplitterAction, call bool) chunkers.Splitter {
	var callback SplitterAction
	if call {
		callback = cb
	} else {
		callback = DefaultSplitterAction
	}
	return &sliceSplitter{
		srcPath: srcPath,
		r:       r,
		size:    uint32(size),
		cb:      callback,
		offset:  0,
		Subject: utils.NewSubject(),
	}
}

// NextBytes produces a new chunk.
func (ss *sliceSplitter) NextBytes() ([]byte, error) {
	if ss.err != nil {
		return nil, ss.err
	}

	full := pool.Get(int(ss.size))
	n, err := io.ReadFull(ss.r, full)
	switch err {
	case io.ErrUnexpectedEOF:
		ss.err = io.EOF
		small := make([]byte, n)
		copy(small, full)
		pool.Put(full)
		ss.record(uint32(n), false)
		return small, nil
	case nil:
		ss.record(ss.size, false)
		return full, nil
	default:
		pool.Put(full)
		return nil, err
	}
}

func (ss *sliceSplitter) record(size uint32, eof bool) {
	ss.cb(ss.srcPath, ss.offset, size, eof)
	ss.offset += uint64(size)
}

// Reader returns the io.Reader associated to this Splitter.
func (ss *sliceSplitter) Reader() io.Reader {
	return ss.r
}

var _ chunkers.Splitter = &sliceSplitter{}
