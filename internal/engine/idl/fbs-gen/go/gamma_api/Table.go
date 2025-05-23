// Code generated by the FlatBuffers compiler. DO NOT EDIT.

package gamma_api

import (
	flatbuffers "github.com/google/flatbuffers/go"
)

type Table struct {
	_tab flatbuffers.Table
}

func GetRootAsTable(buf []byte, offset flatbuffers.UOffsetT) *Table {
	n := flatbuffers.GetUOffsetT(buf[offset:])
	x := &Table{}
	x.Init(buf, n+offset)
	return x
}

func (rcv *Table) Init(buf []byte, i flatbuffers.UOffsetT) {
	rcv._tab.Bytes = buf
	rcv._tab.Pos = i
}

func (rcv *Table) Table() flatbuffers.Table {
	return rcv._tab
}

func (rcv *Table) Name() []byte {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(4))
	if o != 0 {
		return rcv._tab.ByteVector(o + rcv._tab.Pos)
	}
	return nil
}

func (rcv *Table) Fields(obj *FieldInfo, j int) bool {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(6))
	if o != 0 {
		x := rcv._tab.Vector(o)
		x += flatbuffers.UOffsetT(j) * 4
		x = rcv._tab.Indirect(x)
		obj.Init(rcv._tab.Bytes, x)
		return true
	}
	return false
}

func (rcv *Table) FieldsLength() int {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(6))
	if o != 0 {
		return rcv._tab.VectorLen(o)
	}
	return 0
}

func (rcv *Table) VectorsInfo(obj *VectorInfo, j int) bool {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(8))
	if o != 0 {
		x := rcv._tab.Vector(o)
		x += flatbuffers.UOffsetT(j) * 4
		x = rcv._tab.Indirect(x)
		obj.Init(rcv._tab.Bytes, x)
		return true
	}
	return false
}

func (rcv *Table) VectorsInfoLength() int {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(8))
	if o != 0 {
		return rcv._tab.VectorLen(o)
	}
	return 0
}

func (rcv *Table) IndexType() []byte {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(10))
	if o != 0 {
		return rcv._tab.ByteVector(o + rcv._tab.Pos)
	}
	return nil
}

func (rcv *Table) IndexParams() []byte {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(12))
	if o != 0 {
		return rcv._tab.ByteVector(o + rcv._tab.Pos)
	}
	return nil
}

func (rcv *Table) RefreshInterval() int32 {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(14))
	if o != 0 {
		return rcv._tab.GetInt32(o + rcv._tab.Pos)
	}
	return 0
}

func (rcv *Table) MutateRefreshInterval(n int32) bool {
	return rcv._tab.MutateInt32Slot(14, n)
}

func TableStart(builder *flatbuffers.Builder) {
	builder.StartObject(6)
}
func TableAddName(builder *flatbuffers.Builder, name flatbuffers.UOffsetT) {
	builder.PrependUOffsetTSlot(0, flatbuffers.UOffsetT(name), 0)
}
func TableAddFields(builder *flatbuffers.Builder, fields flatbuffers.UOffsetT) {
	builder.PrependUOffsetTSlot(1, flatbuffers.UOffsetT(fields), 0)
}
func TableStartFieldsVector(builder *flatbuffers.Builder, numElems int) flatbuffers.UOffsetT {
	return builder.StartVector(4, numElems, 4)
}
func TableAddVectorsInfo(builder *flatbuffers.Builder, vectorsInfo flatbuffers.UOffsetT) {
	builder.PrependUOffsetTSlot(2, flatbuffers.UOffsetT(vectorsInfo), 0)
}
func TableStartVectorsInfoVector(builder *flatbuffers.Builder, numElems int) flatbuffers.UOffsetT {
	return builder.StartVector(4, numElems, 4)
}
func TableAddIndexType(builder *flatbuffers.Builder, indexType flatbuffers.UOffsetT) {
	builder.PrependUOffsetTSlot(3, flatbuffers.UOffsetT(indexType), 0)
}
func TableAddIndexParams(builder *flatbuffers.Builder, indexParams flatbuffers.UOffsetT) {
	builder.PrependUOffsetTSlot(4, flatbuffers.UOffsetT(indexParams), 0)
}
func TableAddRefreshInterval(builder *flatbuffers.Builder, refreshInterval int32) {
	builder.PrependInt32Slot(5, refreshInterval, 0)
}
func TableEnd(builder *flatbuffers.Builder) flatbuffers.UOffsetT {
	return builder.EndObject()
}
