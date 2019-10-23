package gammacb

import (
	bytes2 "bytes"
	"encoding/json"
	"fmt"
	"github.com/tiglabs/log"
	"github.com/tiglabs/raft/proto"
	"github.com/vearch/vearch/util/bytes"
	"io/ioutil"
	"math"
	"os"
	"path/filepath"
	"strconv"
)

const (
	buf_size        = 1024000
	send_over int32 = -1
)

var _ proto.Snapshot = &GammaSnapshot{}

type GammaSnapshot struct {
	sn     int64
	index  int
	path   string
	infos  []os.FileInfo
	reader *os.File
	size   int64
}

func (g *GammaSnapshot) Next() ([]byte, error) {
	if g.index >= len(g.infos) {
		return bytes.ValueToByte(send_over)
	}
	if g.reader == nil {
		info := g.infos[g.index]
		g.index = g.index + 1
		if info.IsDir() {
			log.Warn("dir:[%s] name:[%s] is dir , so skip sync", g.path, info.Name())
			return bytes.ValueToByte(g.index - 1)
		}
		g.size = info.Size()
		name := info.Name()

		reader, err := os.Open(filepath.Join(g.path, name))
		if err != nil {
			return nil, err
		}
		g.reader = reader
	}

	b := make([]byte, int64(math.Min(buf_size, float64(g.size))))

	size, err := g.reader.Read(b)
	if err != nil {
		return nil, err
	}

	g.size = g.size - int64(size)
	if g.size == 0 {
		if err := g.reader.Close(); err != nil {
			return nil, err
		}
		g.reader = nil
	}

	buffer := bytes2.Buffer{}

	toByte, err := bytes.ValueToByte(g.index - 1)
	if err != nil {
		return nil, err
	}
	buffer.Write(toByte)
	buffer.Write(b)

	return buffer.Bytes(), nil

}

func (g *GammaSnapshot) ApplyIndex() uint64 {
	return uint64(g.sn)
}

func (g *GammaSnapshot) Close() {
	if g.reader != nil {
		_ = g.reader.Close()
	}
}

func (ge *gammaEngine) NewSnapshot() (proto.Snapshot, error) {
	ge.lock.RLock()
	defer ge.lock.RUnlock()
	infos, _ := ioutil.ReadDir(ge.path)
	fileName := filepath.Join(ge.path, indexSn)

	b, err := ioutil.ReadFile(fileName)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, err
		} else {
			b = []byte("0")
		}
	}
	sn, err := strconv.ParseInt(string(b), 10, 64)
	if err != nil {
		return nil, err
	}
	if sn < 0 {
		return nil, fmt.Errorf("read sn:[%d] less than zero", sn)
	}
	return &GammaSnapshot{path: ge.path, sn: sn, infos: infos}, nil
}

func (ge *gammaEngine) ApplySnapshot(peers []proto.Peer, iter proto.SnapIterator) error {
	schema, err := iter.Next()
	if err != nil {
		return err
	}
	var names []string
	if err := json.Unmarshal(schema, &names); err != nil {
		return err
	}

	index := -1

	var out *os.File

	for {
		bs, err := iter.Next()
		if err != nil {
			return err
		}
		i := int(bytes.ByteArray2UInt64(bs[:4]))

		if i == int(send_over) {
			return nil
		}

		if i != index {
			if out != nil {
				if err := out.Close(); err != nil {
					return err
				}
			}
			if out, err = os.Create(filepath.Join(ge.path, names[i])); err != nil {
				return err
			}
			index = i
		}
		if _, err = out.Write(bs[4:]); err != nil {
			return err
		}
	}
}
