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

package bytes

import "C"
import (
	"bytes"
	"encoding/binary"
	"fmt"
	"github.com/tiglabs/log"
	"math"
	"unsafe"
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

var byteSizes = map[string]uint64{
	"b":   Byte,
	"kib": KB,
	"kb":  IKB,
	"mib": MB,
	"mb":  IMB,
	"gib": GB,
	"gb":  IGB,
	"tib": TB,
	"tb":  ITB,
	"pib": PB,
	"pb":  IPB,
	"eib": EB,
	"eb":  IEB,

	"":   Byte,
	"ki": KB,
	"k":  IKB,
	"mi": MB,
	"m":  IMB,
	"gi": GB,
	"g":  IGB,
	"ti": TB,
	"t":  ITB,
	"pi": PB,
	"p":  IPB,
	"ei": EB,
	"e":  IEB,
}

var (
	siSizes  = []string{"B", "kB", "MB", "GB", "TB", "PB", "EB"}
	iecSizes = []string{"B", "KiB", "MiB", "GiB", "TiB", "PiB", "EiB"}

	jsonPrefix = []byte("{")
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

func FloatArrayByte(fa []float32) (code []byte, err error) {
	buf := &bytes.Buffer{}
	for i := 0; i < len(fa); i++ {
		if err = binary.Write(buf, binary.LittleEndian, fa[i]); err != nil {
			return nil, err
		}
	}
	return buf.Bytes(), nil
}

func FloatArray(fa []float32) (code string, err error) {
	buf := &bytes.Buffer{}
	for i := 0; i < len(fa); i++ {
		if err = binary.Write(buf, binary.LittleEndian, fa[i]); err != nil {
			return "", err
		}
	}
	return buf.String(), nil
}

// Returns a slice of the bytes of the provided float64 slice.
// This allows highly performant access to large float64 slices for such things
// as computing hashes or simply writing the bytes to a file.
// BEWARE: this also means this []byte _is platform dependent_.
func UnsafeFloat32SliceAsByteSlice(floats []float32) []byte {
	lf := 4 * len(floats)
	// step by step
	pf := &(floats[0])                        // To pointer to the first byte of b
	up := unsafe.Pointer(pf)                  // To *special* unsafe.Pointer, it can be converted to any pointer
	pi := (*[1]byte)(up)                      // To pointer as byte array
	buf := (*pi)[:]                           // Creates slice to our array of 1 byte
	address := unsafe.Pointer(&buf)           // Capture the address to the slice structure
	lenAddr := uintptr(address) + uintptr(8)  // Capture the address where the length and cap size is stored
	capAddr := uintptr(address) + uintptr(16) // WARNING: This is fragile, depending on a go-internal structure.
	lenPtr := (*int)(unsafe.Pointer(lenAddr)) // Create pointers to the length and cap size
	capPtr := (*int)(unsafe.Pointer(capAddr)) //
	*lenPtr = lf                              // Assign the actual slice size and cap
	*capPtr = lf                              //

	return buf
}

func ValueToByte(fa interface{}) ([]byte, error) {
	buf := &bytes.Buffer{}
	if err := binary.Write(buf, binary.LittleEndian, fa); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func ByteArray2UInt64(bs []byte) uint64 {
	if len(bs) == 4 {
		return uint64(binary.LittleEndian.Uint32(bs))
	} else if len(bs) == 8 {
		return binary.LittleEndian.Uint64(bs)
	} else {
		log.Error("err byte to int len:[%d] , value:%v", len(bs), bs)
		return 0
	}
}

func ArrayByteFloat(bs []byte) (result []float32) {
	length := len(bs) / 4
	result = make([]float32, length)

	for i := 0; i < len(result); i++ {
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

func ByteToFloat64(bs []byte) float64 {
	if len(bs) == 4 {
		return float64(ByteToFloat32(bs))
	}
	bits := binary.LittleEndian.Uint64(bs)

	return math.Float64frombits(bits)
}

func Bytes2Int(bs []byte) int64 {
	return int64(binary.LittleEndian.Uint64(bs))
}

func CloneBytes(b []byte) []byte {
	return append([]byte(nil), b...)
}
