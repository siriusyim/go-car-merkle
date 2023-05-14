package types

import (
	pb "github.com/ipfs/go-unixfs/pb"
)

type Slice struct {
	Path     string           `json:"path"`
	Offset   int              `json:"offset"`
	Size     int              `json:"size"`
	NodeType pb.Data_DataType `json:"nodetype"`
}
