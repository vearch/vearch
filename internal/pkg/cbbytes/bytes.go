// Copyright 2019 The Vearch Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package cbbytes

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
)

// IEC Sizes.
const (
	Byte = 1 << (iota * 10)
	KB
	MB
	GB
	TB
	PB
	EB
)

// SI Sizes.
const (
	IByte = 1
	IKB   = IByte * 1000
	IMB   = IKB * 1000
	IGB   = IMB * 1000
	ITB   = IGB * 1000
	IPB   = ITB * 1000
	IEB   = IPB * 1000
)

var (
	siSizes  = []string{"B", "kB", "MB", "GB", "TB", "PB", "EB"}
	iecSizes = []string{"B", "KiB", "MiB", "GiB", "TiB", "PiB", "EiB"}
)

func logn(n, b float64) float64 {
	return math.Log(n) / math.Log(b)
}

func humanFormat(s uint64, base float64, sizes []string) string {
	if s < 10 {
		return fmt.Sprintf("%dB", s)
	}
	e := math.Floor(logn(float64(s), base))
	suffix := sizes[int(e)]
	val := math.Floor(float64(s)/math.Pow(base, e)*10+0.5) / 10
	f := "%.0f%s"
	if val < 10 {
		f = "%.1f%s"
	}
	return fmt.Sprintf(f, val, suffix)
}

// FormatByte convert uint64 to human-readable byte strings
func FormatByte(s uint64) string {
	return humanFormat(s, 1000, siSizes)
}

// FormatIByte convert uint64 to human-readable byte strings
func FormatIByte(s uint64) string {
	return humanFormat(s, 1024, iecSizes)
}

func VectorToByte(vector []float32) ([]byte, error) {
	return FloatArrayByte(vector)
}

func VectorBinaryToByte(vector []uint8) ([]byte, error) {
	byteArr, error := UInt8ArrayToByteArray(vector)
	return byteArr, error
}

func ByteToVectorForFloat32(bs []byte) ([]float32, error) {
	float32s, err := ByteToFloat32Array(bs)
	if err != nil {
		return nil, err
	}
	return float32s, nil
}

func ByteToVectorBinary(bs []byte, dimension int) ([]int32, error) {
	featureLength := int(dimension / 8)
	result := make([]int32, featureLength)
	for i := range featureLength {
		result[i] = int32(bs[i])
	}
	return result, nil
}

func FloatArrayByte(fa []float32) (code []byte, err error) {
	buf := &bytes.Buffer{}
	if err = binary.Write(buf, binary.LittleEndian, fa); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func UInt8ArrayByte(in []uint8) (code []byte, err error) {
	buf := &bytes.Buffer{}
	for i := range in {
		if err = binary.Write(buf, binary.LittleEndian, in[i]); err != nil {
			return nil, err
		}
	}
	return buf.Bytes(), nil
}

func UInt8ArrayToByteArray(in []uint8) (code []byte, err error) {
	uint8Lenth := len(in)
	var byteArr = make([]byte, uint8Lenth)
	for i := range uint8Lenth {
		unit8Value := in[i]
		if unit8Value > 255 {
			return nil, fmt.Errorf("byte value overflows byte constant :%v", unit8Value)
		}
		byteArr[i] = unit8Value
	}
	return byteArr, nil
}

func FloatArray(fa []float32) (code string, err error) {
	buf := &bytes.Buffer{}
	for i := range fa {
		if err = binary.Write(buf, binary.LittleEndian, fa[i]); err != nil {
			return "", err
		}
	}
	return buf.String(), nil
}

func BoolToByte(b bool) []byte {
	if b {
		return UInt32ToByte(1)
	} else {
		return UInt32ToByte(0)
	}
}

func Float64ToByte(v float64) []byte {
	bs, _ := ValueToByte(v)
	return bs
}

func Float64ToByteNew(float float64) []byte {
	bits := math.Float64bits(float)
	bytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(bytes, bits)

	return bytes
}

func Float32ToByte(v float32) []byte {
	bs, _ := ValueToByte(v)
	return bs
}

func UInt32ToByte(v uint32) []byte {
	bs, _ := ValueToByte(v)
	return bs
}

func Int64ToByte(v int64) []byte {
	bs, _ := ValueToByte(v)
	return bs
}

func Int32ToByte(v int32) []byte {
	bs, _ := ValueToByte(v)
	return bs
}

func ValueToByte(fa interface{}) ([]byte, error) {
	buf := &bytes.Buffer{}
	if err := binary.Write(buf, binary.LittleEndian, fa); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func ArrayByteFloat(bs []byte) (result []float32) {
	length := len(bs) / 4
	result = make([]float32, length)

	for i := range result {
		result[i] = math.Float32frombits(binary.LittleEndian.Uint32(bs[i*4 : (i+1)*4]))
	}
	return result
}

// BitLen calculated bit length
func BitLen(x int64) (n int64) {
	for ; x >= 0x8000; x >>= 16 {
		n += 16
	}
	if x >= 0x80 {
		x >>= 8
		n += 8
	}
	if x >= 0x8 {
		x >>= 4
		n += 4
	}
	if x >= 0x2 {
		x >>= 2
		n += 2
	}
	if x >= 0x1 {
		n++
	}
	return
}

func ByteToFloat32(bytes []byte) float32 {
	bits := binary.LittleEndian.Uint32(bytes)
	return math.Float32frombits(bits)
}

func ByteToUInt64(bs []byte) uint64 {
	return binary.LittleEndian.Uint64(bs)
}

func ByteToFloat32Array(bytes []byte) ([]float32, error) {
	if len(bytes)%4 != 0 {
		return nil, fmt.Errorf("input bytes not a multiple of 4")
	}

	num := len(bytes) / 4

	result := make([]float32, num)
	for i := 0; i < num; i++ {
		result[i] = math.Float32frombits(binary.LittleEndian.Uint32(bytes[i*4:]))
	}
	return result, nil
}

func ByteToUInt8Array(bytes []byte) ([]uint8, error) {
	if len(bytes)%4 != 0 {
		return nil, fmt.Errorf("input bytes not a multiple of 4")
	}

	num := len(bytes) / 4

	result := make([]uint8, num)
	for i := range num {
		result[i] = uint8(binary.LittleEndian.Uint32(bytes[i*4:]))
	}
	return result, nil
}

func ByteToFloat64(bs []byte) float64 {
	if len(bs) == 4 {
		return float64(ByteToFloat32(bs))
	}
	bits := binary.LittleEndian.Uint64(bs)

	return math.Float64frombits(bits)
}

func ByteToFloat64New(bytes []byte) float64 {
	bits := binary.LittleEndian.Uint64(bytes)

	return math.Float64frombits(bits)
}

func Bytes2Int(bs []byte) int64 {
	return int64(binary.LittleEndian.Uint64(bs))
}

func Bytes2Int32(bs []byte) int32 {
	return int32(binary.LittleEndian.Uint32(bs))
}

func Bytes2Long(bs []byte) int64 {
	return int64(binary.LittleEndian.Uint64(bs))
}

func CloneBytes(b []byte) []byte {
	result := make([]byte, len(b))
	copy(result, b)
	return result
}

func BytesToInt32(bys []byte) int32 {
	bytebuff := bytes.NewBuffer(bys)
	var data uint8
	binary.Read(bytebuff, binary.BigEndian, &data)
	unit8V := uint8(data)
	return int32(unit8V)
}
