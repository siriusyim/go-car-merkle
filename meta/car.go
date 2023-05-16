package meta

import (
	"io"

	"github.com/ipfs/go-cid"
	chunker "github.com/ipfs/go-ipfs-chunker"
	mc "github.com/siriusyim/go-car-merkle/chunker"
	"github.com/siriusyim/go-car-merkle/utils"
)

type CarMeta struct {
	Path       string  `json:"path"`
	Offset     int     `json:"offset"`
	Size       int     `json:"size"`
	DistOffset int     `json:"distoffset"`
	Cid        cid.Cid `json:"cid"`
}

type MetaService struct {
	spl    chunker.Splitter
	writer io.Writer
	metas  map[cid.Cid]*CarMeta
	sa     mc.SplitterAction
	wa     utils.WriteAction
}

func New() *MetaService {
	return &MetaService{
		metas: make(map[cid.Cid]*CarMeta, 0),
	}
}

func (ms *MetaService) GetSplitter(r io.Reader, srcPath string) chunker.Splitter {
	spl := mc.NewSliceSplitter(r, int64(mc.UnixfsChunkSize), srcPath, ms.splitterAction)
	ms.spl = spl
	return spl
}

func (ms *MetaService) splitterAction(srcPath string, offset uint64, size uint32, eof bool) {
	//TODO
	return
}

func (ms *MetaService) GetWriter(w io.Writer, path string) io.Writer {
	writer := utils.WrappedWriter(w, path, ms.writeAction)
	ms.writer = writer
	return writer
}

func (ms *MetaService) writeAction(path string, cid cid.Cid, count int, total int) {
	//TODO
	return
}
