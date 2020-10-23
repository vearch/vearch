package gammacb

import (
	bytes2 "bytes"
	"encoding/json"
	"fmt"
	"github.com/vearch/vearch/util/log"
	"github.com/tiglabs/raft/proto"
	"github.com/vearch/vearch/util/cbbytes"
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
		return cbbytes.ValueToByte(send_over)
	}
	if g.reader == nil {
		info := g.infos[g.index]
		g.index = g.index + 1
		log.Debug("gamma snapshot dir info is [%+v] ", info)
		if info.IsDir() {
			log.Warn("dir:[%s] name:[%s] is dir , so skip sync", g.path, info.Name())
			return cbbytes.ValueToByte(g.index - 1)
		}
		g.size = info.Size()
		name := info.Name()

		reader, err := os.Open(filepath.Join(g.path, name))
		log.Debug("next reader info [%+v],path [%s],name [%s]", info, g.path, name)

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

	toByte, err := cbbytes.ValueToByte(g.index - 1)
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

	log.Debug("new snapshot ge path is [%+v]",fileName)

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
	log.Debug("apply snap shot names is [%s]", names)
	index := -1

	var out *os.File

	for {
		bs, err := iter.Next()
		if err != nil {
			return err
		}
		i := int(cbbytes.ByteArray2UInt64(bs[:4]))

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
			log.Debug("create file path is [%s] ,name is [%s]",ge.path, names[i])

			index = i
		}
		if _, err = out.Write(bs[4:]); err != nil {
			return err
		}
	}
}
