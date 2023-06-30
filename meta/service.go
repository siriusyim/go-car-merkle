package meta

import (
	"fmt"
	"io"
	"sync"

	commcid "github.com/filecoin-project/go-fil-commcid"
	commp "github.com/filecoin-project/go-fil-commp-hashhash"
	"github.com/ipfs/go-cid"
	chunker "github.com/ipfs/go-ipfs-chunker"
	ihelper "github.com/ipfs/go-unixfs/importer/helpers"
	pb "github.com/ipfs/go-unixfs/pb"

	mc "github.com/siriusyim/go-car-merkle/chunker"
	mh "github.com/siriusyim/go-car-merkle/dagbuilder"
	"github.com/siriusyim/go-car-merkle/types"
	"github.com/siriusyim/go-car-merkle/utils"
)

const DefaultMaxCommpBuffSizePad = uint64(1 << 20)
const DefaultMaxCommpBuffSize = uint64(DefaultMaxCommpBuffSizePad - (DefaultMaxCommpBuffSizePad / 128))

type MetaService struct {
	spl    chunker.Splitter
	writer io.Writer
	helper ihelper.Helper

	metas map[cid.Cid]*types.CarMeta
	lk    sync.Mutex

	splCh chan *types.SrcData

	maxCommpBuffSize uint64

	commpBuffSize uint64
	calc          *commp.Calc
	commpHashs    []cid.Cid
}

func New() *MetaService {
	return &MetaService{
		metas:            make(map[cid.Cid]*types.CarMeta, 0),
		splCh:            make(chan *types.SrcData),
		maxCommpBuffSize: DefaultMaxCommpBuffSize,
		commpHashs:       make([]cid.Cid, 0),
	}
}

func (ms *MetaService) GetHelper(params *ihelper.DagBuilderParams, spl chunker.Splitter) (ihelper.Helper, error) {
	db, err := mh.WrappedDagBuilder(params, spl, ms.helperAction)
	if err != nil {
		return nil, err
	}
	ms.helper = db
	return db, nil
}

func (ms *MetaService) helperAction(c cid.Cid, nodeType pb.Data_DataType) {
	var cm types.CarMeta
	select {
	case meta := <-ms.splCh:
		{
			cm.SrcPath = meta.Path
			cm.SrcOffset = meta.Offset
			cm.Size = meta.Size
			cm.NodeType = nodeType
			cm.Cid = c
		}
	}

	fmt.Println("<<<<<< Read srcPath:", cm.SrcPath, " offset:", cm.SrcOffset, " size: ", cm.Size)
	ms.insertMeta(c, &cm)
	return
}

func (ms *MetaService) GetSplitter(r io.Reader, srcPath string, call bool) chunker.Splitter {
	spl := mc.NewSliceSplitter(r, int64(mc.UnixfsChunkSize), srcPath, ms.splitterAction, call)
	ms.spl = spl
	return spl
}

func (ms *MetaService) splitterAction(srcPath string, offset uint64, size uint32, eof bool) {
	go func() {
		ms.splCh <- &types.SrcData{
			Path:   srcPath,
			Offset: offset,
			Size:   size,
		}
	}()
	return
}

func (ms *MetaService) GetCarWriter(w io.Writer, path string, call bool) io.Writer {
	if !call {
		return w
	}
	writer := utils.WrappedWriter(w, path, ms.carWriteAfterAction, utils.DefaultWriteBeforeAction)
	ms.writer = writer
	return writer
}

func (ms *MetaService) carWriteAfterAction(dstpath string, c cid.Cid, count int, offset uint64) {
	fmt.Println(">>>>>> Write dstPath:", dstpath, " count:", count, " offset: ", offset, " cid: ", c.String())
	if _, ok := ms.metas[c]; !ok {
		fmt.Printf("meta cid: %s is not exist\n", c.String())
		return
	}
	ms.updateMeta(c, dstpath, offset)
	return
}

func (ms *MetaService) GetPieceWriter(w io.Writer, path string, call bool) io.Writer {
	if !call {
		return w
	}
	writer := utils.WrappedWriter(w, path, utils.DefaultWriteAfterAction, ms.pieceWriteBeforeAction)
	ms.writer = writer
	return writer
}

func (ms *MetaService) SetPieceCalc(calc *commp.Calc) error {
	ms.calc = calc
	return nil
}

func (ms *MetaService) generatePieceCid() (cid.Cid, error) {
	rawCommP, _, err := ms.calc.Digest()
	if err != nil {
		return cid.Undef, err
	}
	commCid, err := commcid.DataCommitmentV1ToCID(rawCommP)
	if err != nil {
		return cid.Undef, err
	}
	return commCid, nil
}

func (ms *MetaService) pieceWriteBeforeAction(buf []byte, w io.Writer) ([]byte, error) {
	count := len(buf)
	if ms.commpBuffSize+uint64(count) >= ms.maxCommpBuffSize {

		toWriteLen := ms.commpBuffSize + uint64(count) - ms.maxCommpBuffSize
		if _, err := w.Write(buf[:toWriteLen]); err != nil {
			return nil, err
		}

		sliceCid, err := ms.generatePieceCid()
		if err != nil {
			return nil, err
		}

		ms.commpHashs = append(ms.commpHashs, sliceCid)

		ms.commpBuffSize = 0
		return buf[toWriteLen:], nil
	}

	ms.commpBuffSize += uint64(len(buf))
	//fmt.Println(">>>>>> Write piece:", dstpath, " count:", count, " offset: ", offset, " cid: ", c.String())
	return buf, nil
}

func (ms *MetaService) PrintPieceCids() {
	for _, v := range ms.commpHashs {
		fmt.Println("cid:", v.String())
	}
}
func (ms *MetaService) insertMeta(c cid.Cid, cm *types.CarMeta) error {
	ms.lk.Lock()
	defer ms.lk.Unlock()
	if _, ok := ms.metas[c]; ok {
		return fmt.Errorf("meta srcpath:%s offset: %d size: %d cid: %s exist", cm.SrcPath, cm.SrcOffset, cm.Size, c.String())
	}
	ms.metas[c] = cm
	return nil
}

func (ms *MetaService) updateMeta(c cid.Cid, dstpath string, offset uint64) error {
	ms.lk.Lock()
	defer ms.lk.Unlock()
	if _, ok := ms.metas[c]; !ok {
		return fmt.Errorf("meta cid: %s is not exist", c.String())
	}

	ms.metas[c].DstPath = dstpath
	ms.metas[c].DstOffset = offset

	return nil
}

func (ms *MetaService) PrintJson(path string) error {
	meta := &types.Meta{
		Metas: ms.metas,
	}
	vmr := meta.GetDstMetaInfo()
	if err := utils.WriteJson(path+"-dm.json", "\t", vmr); err != nil {
		return err
	}

	drs := vmr.GetDstRanges()
	return utils.WriteJson(path+"-dr.json", "\t", drs)
}
