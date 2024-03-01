package gammacb

import (
	"fmt"
	"io"
	"io/fs"
	"math"
	"os"
	"path/filepath"
	"strconv"

	"github.com/cubefs/cubefs/depends/tiglabs/raft/proto"
	protobuf "github.com/golang/protobuf/proto"
	"github.com/vearch/vearch/internal/proto/vearchpb"
	"github.com/vearch/vearch/internal/util/errutil"
	"github.com/vearch/vearch/internal/util/fileutil"
	"github.com/vearch/vearch/internal/util/log"
)

const (
	// trasport 10M everytime
	buf_size = 1024000 * 10
)

var _ proto.Snapshot = &GammaSnapshot{}

type GammaSnapshot struct {
	sn           int64
	index        int64
	path         string
	infos        []fs.DirEntry
	absFileNames []string
	reader       *os.File
	size         int64
}

func (g *GammaSnapshot) Next() ([]byte, error) {
	var err error
	defer errutil.CatchError(&err)
	if int(g.index) >= len(g.absFileNames) && g.size == 0 {
		log.Debug("leader send over, leader finish snapshot.")
		snapShotMsg := &vearchpb.SnapshotMsg{
			Status: vearchpb.SnapshotStatus_Finish,
		}
		data, err := protobuf.Marshal(snapShotMsg)
		if err != nil {
			return data, err
		} else {
			return data, io.EOF
		}
	}
	if g.reader == nil {
		filePath := g.absFileNames[g.index]
		g.index = g.index + 1
		log.Debug("g.index is [%+v] ", g.index)
		log.Debug("g.absFileNames length is [%+v] ", len(g.absFileNames))
		info, _ := os.Stat(filePath)
		if info.IsDir() {
			log.Debug("dir:[%s] name:[%s] is dir , so skip sync", g.path, info.Name())
			snapShotMsg := &vearchpb.SnapshotMsg{
				Status: vearchpb.SnapshotStatus_Running,
			}
			return protobuf.Marshal(snapShotMsg)
		}
		g.size = info.Size()
		reader, err := os.Open(filePath)
		log.Debug("next reader info [%+v],path [%s],name [%s]", info, g.path, filePath)
		if err != nil {
			errutil.ThrowError(err)
			return nil, err
		}
		g.reader = reader
	}

	byteData := make([]byte, int64(math.Min(buf_size, float64(g.size))))
	size, err := g.reader.Read(byteData)
	if err != nil {
		errutil.ThrowError(err)
		return nil, err
	}
	g.size = g.size - int64(size)
	log.Debug("current g.size [%+v], info size [%+v]", g.size, size)
	if g.size == 0 {
		if err := g.reader.Close(); err != nil {
			errutil.ThrowError(err)

			return nil, err
		}
		g.reader = nil
	}
	// snapshot proto msg
	snapShotMsg := &vearchpb.SnapshotMsg{
		FileName: g.absFileNames[g.index-1],
		Data:     byteData,
		Status:   vearchpb.SnapshotStatus_Running,
	}
	return protobuf.Marshal(snapShotMsg)
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
	infos, _ := os.ReadDir(ge.path)
	fileName := filepath.Join(ge.path, indexSn)
	log.Debug("new snapshot ge path is [%+v]", fileName)
	b, err := os.ReadFile(fileName)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, err
		} else {
			b = []byte("0")
		}
	}

	// get all file names
	absFileNames, _ := fileutil.GetAllFileNames(ge.path)
	sn, err := strconv.ParseInt(string(b), 10, 64)
	if err != nil {
		return nil, err
	}
	if sn < 0 {
		return nil, fmt.Errorf("read sn:[%d] less than zero", sn)
	}
	return &GammaSnapshot{path: ge.path, sn: sn, infos: infos, absFileNames: absFileNames}, nil
}

func (ge *gammaEngine) ApplySnapshot(peers []proto.Peer, iter proto.SnapIterator) error {
	var err error
	defer errutil.CatchError(&err)
	var out *os.File
	for {
		bs, err := iter.Next()
		if err != nil && err != io.EOF {
			errutil.ThrowError(err)
			return err
		}
		if bs == nil {
			continue
		}

		msg := &vearchpb.SnapshotMsg{}
		err = protobuf.Unmarshal(bs, msg)
		errutil.ThrowError(err)
		if msg.Status == vearchpb.SnapshotStatus_Finish {
			if out != nil {
				if err := out.Close(); err != nil {
					errutil.ThrowError(err)
					return err
				}
				out = nil
			}
			log.Debug("follower receive finish.")
			return nil
		}
		if msg.Data == nil || len(msg.Data) == 0 {
			log.Debug("msg data is nil.")
			continue
		}
		if out != nil {
			if err := out.Close(); err != nil {
				errutil.ThrowError(err)
				return err
			}
			out = nil
		}
		// create dir
		fileDir := filepath.Dir(msg.FileName)
		_, exist := os.Stat(fileDir)
		if os.IsNotExist(exist) {
			log.Debug("create dir [%+v]", fileDir)
			err := os.MkdirAll(fileDir, os.ModePerm)
			errutil.ThrowError(err)
		}
		// create file, append write mode
		if out, err = os.OpenFile(msg.FileName, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0660); err != nil {
			errutil.ThrowError(err)
			return err
		}
		log.Debug("write file path is [%s] ,name is [%s], size is [%d]", ge.path, msg.FileName, len(msg.Data))
		if _, err = out.Write(msg.Data); err != nil {
			errutil.ThrowError(err)
			return err
		}
	}
}
