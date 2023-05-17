package types

import (
	"sort"

	"encoding/binary"

	"github.com/ipfs/go-cid"
	pb "github.com/ipfs/go-unixfs/pb"
)

type CarMeta struct {
	SrcPath   string           `json:"srcpath"`
	SrcOffset uint64           `json:"srcoffset"`
	Size      uint32           `json:"size"`
	DstPath   string           `json:"dstpath"`
	DstOffset uint64           `json:"dstoffset"`
	NodeType  pb.Data_DataType `json:"nodetype"`
}

func (cm *CarMeta) GetDstRange(c cid.Cid) (uint64, uint64) {
	var sum uint64
	cidcount := len(c.Bytes())
	sum += uint64(cidcount)
	sum += uint64(cm.Size)
	buf := make([]byte, 8)

	lencount := binary.PutUvarint(buf, sum)
	start := cm.DstOffset - uint64(lencount)
	end := cm.DstOffset + uint64(cm.Size)
	return start, end

}

type SrcData struct {
	Path   string
	Offset uint64
	Size   uint32
}

type Range struct {
	Start uint64 `json:"start"`
	End   uint64 `json:"end"`
}
type DstMeta struct {
	Range     *Range           `json:"range"`
	SrcPath   string           `json:"srcpath"`
	SrcOffset uint64           `json:"srcoffset"`
	Size      uint32           `json:"size"`
	NodeType  pb.Data_DataType `json:"nodetype"`
}

type DstMetaInfo struct {
	DstMetas map[string][]*DstMeta `json:"verifyinfos"`
}

func (dmi *DstMetaInfo) GetDstRange(path string) []*Range {
	if _, ok := dmi.DstMetas[path]; !ok {
		return nil
	}
	dstMeta := dmi.DstMetas[path]
	if len(dstMeta) == 0 {
		return nil
	}
	var out []*Range
	tmp := &Range{
		Start: dstMeta[0].Range.Start,
		End:   dstMeta[0].Range.End,
	}
	for i, v := range dstMeta {
		if tmp.End == v.Range.Start && i != 0 {
			tmp.End = v.Range.End
		} else {
			out = append(out, tmp)
			tmp = &Range{
				Start: v.Range.Start,
				End:   v.Range.End,
			}
		}
	}
	out = append(out, tmp)
	return out
}

type Meta struct {
	Metas map[cid.Cid]*CarMeta `json:"metas"`
}

func (m *Meta) GetDstMetaInfo() *DstMetaInfo {
	vmr := &DstMetaInfo{
		DstMetas: make(map[string][]*DstMeta, 0),
	}

	for k, v := range m.Metas {
		if _, ok := vmr.DstMetas[v.DstPath]; !ok {
			vmr.DstMetas[v.DstPath] = make([]*DstMeta, 0)
		}

		start, end := v.GetDstRange(k)

		ran := &Range{
			Start: start,
			End:   end,
		}
		vmr.DstMetas[v.DstPath] = append(vmr.DstMetas[v.DstPath], &DstMeta{
			Range:     ran,
			SrcPath:   v.SrcPath,
			SrcOffset: v.SrcOffset,
			Size:      v.Size,
			NodeType:  v.NodeType,
		})
	}

	for _, v := range vmr.DstMetas {
		sort.Slice(v, func(i int, j int) bool {
			return v[i].Range.Start < v[j].Range.Start
		})
	}

	return vmr
}
