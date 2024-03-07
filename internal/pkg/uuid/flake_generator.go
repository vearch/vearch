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

package uuid

import (
	"crypto/rand"
	"encoding/base64"
	"encoding/binary"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/vearch/vearch/internal/pkg/bufalloc"
)

const (
	seqNumberMask = 0xffffff
)

// flakeGenerator is essentially flake ids.
// Format: use 6 bytes for timestamp, use 6 bytes for macAddress and use 3 bytes for sequence number.
// It reorder bytes in a way that does not make ids sort in order anymore.
type flakeGenerator struct {
	sync.Mutex
	// only use bottom 3 bytes for the sequence number, init with random int.
	seqNumber uint32
	// ensure clock moves forward
	lastTimestamp int64
	macAddr       []byte
}

// NewFlakeGenerator create a flakeGenerator instance
func NewFlakeGenerator() Generator {
	g := &flakeGenerator{macAddr: make([]byte, 6)}
	g.initSeqNumber()
	g.initMacAddr()
	return g
}

func (g *flakeGenerator) GetUUID() string {
	seqID := atomic.AddUint32(&g.seqNumber, 1) & seqNumberMask
	curTimestamp := time.Now().UnixNano() / 1e6

	g.Lock()
	if g.lastTimestamp > curTimestamp {
		curTimestamp = g.lastTimestamp
	}
	if seqID == 0 {
		// Always force the clock to increment whenever sequence number is 0
		curTimestamp++
	}
	g.lastTimestamp = curTimestamp
	g.Unlock()

	uuidBuf := bufalloc.AllocBuffer(15)

	uuidBuf.WriteByte(byte(curTimestamp >> 40))
	uuidBuf.WriteByte(byte(curTimestamp >> 32))
	uuidBuf.WriteByte(byte(curTimestamp >> 24))
	uuidBuf.WriteByte(byte(curTimestamp >> 16))
	uuidBuf.WriteByte(byte(curTimestamp >> 8))
	uuidBuf.WriteByte(byte(curTimestamp))
	uuidBuf.Write(g.macAddr)
	uuidBuf.WriteByte(byte(seqID >> 16))
	uuidBuf.WriteByte(byte(seqID >> 8))
	uuidBuf.WriteByte(byte(seqID))
	uid := base64.URLEncoding.EncodeToString(uuidBuf.Bytes())

	bufalloc.FreeBuffer(uuidBuf)
	return uid
}

func (g *flakeGenerator) initSeqNumber() {
	buf := make([]byte, 4)
	g.safeRandom(buf)
	g.seqNumber = binary.BigEndian.Uint32(buf)
}

func (g *flakeGenerator) initMacAddr() {
	if interfaces, err := net.Interfaces(); err == nil {
		for _, iface := range interfaces {
			if len(iface.HardwareAddr) >= 6 {
				copy(g.macAddr[:], iface.HardwareAddr)
				return
			}
		}
	}

	g.safeRandom(g.macAddr[:])
	g.macAddr[0] |= 0x01
}

func (g *flakeGenerator) safeRandom(dest []byte) {
	if _, err := rand.Read(dest); err != nil {
		panic(err)
	}
}
