/*
 * Copyright 2017 Dgraph Labs, Inc. and Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package badger

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/dgraph-io/badger/protos"
	"github.com/dgraph-io/badger/y"
	"github.com/pkg/errors"
)

// Manifest represents the contents of the MANIFEST file in a Badger store.
//
// The MANIFEST file describes the startup state of the db -- all LSM files and what level they're
// at.
//
// It consists of a sequence of ManifestChangeSet objects.  Each of these is treated atomically,
// and contains a sequence of ManifestChange's (file creations/deletions) which we use to
// reconstruct the manifest at startup.
type Manifest struct {
	Levels []LevelManifest
	// 某个文件table id对应的level信息
	Tables map[uint64]TableManifest

	// Contains total number of creation and deletion changes in the manifest -- used to compute
	// whether it'd be useful to rewrite the manifest.
	// 统计信息，用来表示是否需要重写manifest文件
	Creations int
	Deletions int
}

func createManifest() Manifest {
	levels := make([]LevelManifest, 0)
	return Manifest{
		Levels: levels,
		Tables: make(map[uint64]TableManifest),
	}
}

// LevelManifest contains information about LSM tree levels
// in the MANIFEST file.
type LevelManifest struct {
	// 表示某一个level下的table id集合
	Tables map[uint64]struct{} // Set of table id's
}

// TableManifest contains information about a specific level
// in the LSM tree.
// 代表了某个level
type TableManifest struct {
	Level uint8
}

// manifestFile holds the file pointer (and other info) about the manifest file, which is a log
// file we append to.
// manifest文件的描述信息
type manifestFile struct {
	fp        *os.File
	directory string
	// We make this configurable so that unit tests can hit rewrite() code quickly
	deletionsRewriteThreshold int

	// Guards appends, which includes access to the manifest field.
	// 并发控制的锁内容
	appendLock sync.Mutex

	// Used to track the current state of the manifest, used when rewriting.
	// 用来重写一个db的操作的文件
	manifest Manifest
}

const (
	// ManifestFilename is the filename for the manifest file.
	ManifestFilename                  = "MANIFEST"
	manifestRewriteFilename           = "MANIFEST-REWRITE"
	manifestDeletionsRewriteThreshold = 10000
	manifestDeletionsRatio            = 10
)

// asChanges returns a sequence of changes that could be used to recreate the Manifest in its
// present state.
// 把manifest中的内容判断抽取出来成日志
func (m *Manifest) asChanges() []*protos.ManifestChange {
	changes := make([]*protos.ManifestChange, 0, len(m.Tables))
	for id, tm := range m.Tables {
		// 每一个create change都是新的对象或者结构体
		changes = append(changes, makeTableCreateChange(id, int(tm.Level)))
	}
	return changes
}

// 深度copy
func (m *Manifest) clone() Manifest {
	changeSet := protos.ManifestChangeSet{Changes: m.asChanges()}
	ret := createManifest()
	y.Check(applyChangeSet(&ret, &changeSet))
	return ret
}

// openOrCreateManifestFile opens a Badger manifest file if it exists, or creates on if
// one doesn’t.
func openOrCreateManifestFile(dir string) (ret *manifestFile, result Manifest, err error) {
	return helpOpenOrCreateManifestFile(dir, manifestDeletionsRewriteThreshold)
}

func helpOpenOrCreateManifestFile(dir string, deletionsThreshold int) (ret *manifestFile, result Manifest, err error) {
	// 查看文件是否存在
	path := filepath.Join(dir, ManifestFilename)
	fp, err := y.OpenExistingSyncedFile(path, false) // We explicitly sync in addChanges, outside the lock.
	// 出现错误的情况，有两种，一种是文件出现了损坏
	// 一种是文件不存在，也就是manifest 文件不存在
	// 文件不存在的可能有两种：1是没有manifest文件 2是没有manifest文件但是有rewrite manifest文件
	if err != nil {
		if !os.IsNotExist(err) {
			return nil, Manifest{}, err
		}
		// 文件已经存在，也就是manifest file已经存在
		// 开始的是m是空的
		m := createManifest()
		fp, netCreations, err := helpRewrite(dir, &m)
		if err != nil {
			return nil, Manifest{}, err
		}
		// 确保文件数量为0
		y.AssertTrue(netCreations == 0)
		// ？？？？很不明白为什么这里需要一个deep copy
		mf := &manifestFile{
			fp:                        fp,
			directory:                 dir,
			// manifestFile中存储的是m的deep copy
			manifest:                  m.clone(),
			deletionsRewriteThreshold: deletionsThreshold,
		}
		return mf, m, nil
	}
	// 如果文件已经存在，并且没有错我的内容，就需要replay manifest的信息
	// 如果manifest文件已经存在就replay这个文件
	manifest, truncOffset, err := ReplayManifestFile(fp)
	if err != nil {
		_ = fp.Close()
		return nil, Manifest{}, err
	}

	// replay 完以及后需要truncate一部分信息
	// Truncate file so we don't have a half-written entry at the end.
	if err := fp.Truncate(truncOffset); err != nil {
		_ = fp.Close()
		return nil, Manifest{}, err
	}
	// seek到文件尾部
	if _, err = fp.Seek(0, os.SEEK_END); err != nil {
		_ = fp.Close()
		return nil, Manifest{}, err
	}
	// 返回对应的结果
	mf := &manifestFile{
		fp:                        fp,
		directory:                 dir,
		manifest:                  manifest.clone(),
		deletionsRewriteThreshold: deletionsThreshold,
	}
	return mf, manifest, nil
}

func (mf *manifestFile) close() error {
	return mf.fp.Close()
}

// addChanges writes a batch of changes, atomically, to the file. By "atomically" that means when
// we replay the MANIFEST file, we'll either replay all the changes or none of them.  (The truth of
// this depends on the filesystem -- some might append garbage data if a system crash happens at
// the wrong time.)
func (mf *manifestFile) addChanges(changesParam []*protos.ManifestChange) error {
	changes := protos.ManifestChangeSet{Changes: changesParam}
	buf, err := changes.Marshal()
	if err != nil {
		return err
	}

	// Maybe we could use O_APPEND instead (on certain file systems)
	// 把修改内容 apply到manifest file中
	mf.appendLock.Lock()
	if err := applyChangeSet(&mf.manifest, &changes); err != nil {
		mf.appendLock.Unlock()
		return err
	}
	// Rewrite manifest if it'd shrink by 1/10 and it's big enough to care
	if mf.manifest.Deletions > mf.deletionsRewriteThreshold &&
		mf.manifest.Deletions > manifestDeletionsRatio*(mf.manifest.Creations-mf.manifest.Deletions) {
		if err := mf.rewrite(); err != nil {
			mf.appendLock.Unlock()
			return err
		}
	} else {
		var lenCrcBuf [8]byte
		binary.BigEndian.PutUint32(lenCrcBuf[0:4], uint32(len(buf)))
		binary.BigEndian.PutUint32(lenCrcBuf[4:8], crc32.Checksum(buf, y.CastagnoliCrcTable))
		buf = append(lenCrcBuf[:], buf...)
		if _, err := mf.fp.Write(buf); err != nil {
			mf.appendLock.Unlock()
			return err
		}
	}

	mf.appendLock.Unlock()
	return mf.fp.Sync()
}

// Has to be 4 bytes.  The value can never change, ever, anyway.
var magicText = [4]byte{'B', 'd', 'g', 'r'}

// The magic version number.
const magicVersion = 2

// 这个方法的作用，就是在dir目录下写一个manifestRewriteFilename文件，内容是根据manifest信息
// 如果m是空的话也会写一部分信息（每一个信息包含entry：crc+data内容）
// 最后再把manifestRewriteFilename重命名为ManifestFilename
// 返回的int表示m中文件的数量
func helpRewrite(dir string, m *Manifest) (*os.File, int, error) {
	rewritePath := filepath.Join(dir, manifestRewriteFilename)
	// We explicitly sync.
	fp, err := y.OpenTruncFile(rewritePath, false)
	if err != nil {
		return nil, 0, err
	}

	// 写 magic信息
	buf := make([]byte, 8)
	copy(buf[0:4], magicText[:])
	binary.BigEndian.PutUint32(buf[4:8], magicVersion)
	// m是空的
	netCreations := len(m.Tables)
	changes := m.asChanges()
	set := protos.ManifestChangeSet{Changes: changes}

	changeBuf, err := set.Marshal()
	if err != nil {
		fp.Close()
		return nil, 0, err
	}
	// crc校验
	var lenCrcBuf [8]byte
	binary.BigEndian.PutUint32(lenCrcBuf[0:4], uint32(len(changeBuf)))
	binary.BigEndian.PutUint32(lenCrcBuf[4:8], crc32.Checksum(changeBuf, y.CastagnoliCrcTable))
	// crc校验信息
	buf = append(buf, lenCrcBuf[:]...)
	// 数据信息
	buf = append(buf, changeBuf...)
	if _, err := fp.Write(buf); err != nil {
		fp.Close()
		return nil, 0, err
	}
	if err := fp.Sync(); err != nil {
		fp.Close()
		return nil, 0, err
	}

	// In Windows the files should be closed before doing a Rename.
	if err = fp.Close(); err != nil {
		return nil, 0, err
	}
	// rename rewrite到原始名字
	manifestPath := filepath.Join(dir, ManifestFilename)
	if err := os.Rename(rewritePath, manifestPath); err != nil {
		return nil, 0, err
	}
	// 打开文件
	fp, err = y.OpenExistingSyncedFile(manifestPath, false)
	if err != nil {
		return nil, 0, err
	}
	if _, err := fp.Seek(0, os.SEEK_END); err != nil {
		fp.Close()
		return nil, 0, err
	}
	if err := syncDir(dir); err != nil {
		fp.Close()
		return nil, 0, err
	}

	return fp, netCreations, nil
}

// Must be called while appendLock is held.
func (mf *manifestFile) rewrite() error {
	// In Windows the files should be closed before doing a Rename.
	if err := mf.fp.Close(); err != nil {
		return err
	}
	fp, netCreations, err := helpRewrite(mf.directory, &mf.manifest)
	if err != nil {
		return err
	}
	mf.fp = fp
	mf.manifest.Creations = netCreations
	mf.manifest.Deletions = 0

	return nil
}

type countingReader struct {
	wrapped *bufio.Reader
	count   int64
}

func (r *countingReader) Read(p []byte) (n int, err error) {
	n, err = r.wrapped.Read(p)
	r.count += int64(n)
	return
}

func (r *countingReader) ReadByte() (b byte, err error) {
	b, err = r.wrapped.ReadByte()
	if err == nil {
		r.count++
	}
	return
}

var (
	errBadMagic = errors.New("manifest has bad magic")
)

// ReplayManifestFile reads the manifest file and constructs two manifest objects.  (We need one
// immutable copy and one mutable copy of the manifest.  Easiest way is to construct two of them.)
// Also, returns the last offset after a completely read manifest entry -- the file must be
// truncated at that point before further appends are made (if there is a partial entry after
// that).  In normal conditions, truncOffset is the file size.
func ReplayManifestFile(fp *os.File) (ret Manifest, truncOffset int64, err error) {
	r := countingReader{wrapped: bufio.NewReader(fp)}

	var magicBuf [8]byte
	// 读取magic buff
	if _, err := io.ReadFull(&r, magicBuf[:]); err != nil {
		return Manifest{}, 0, errBadMagic
	}
	// 比较内容
	if bytes.Compare(magicBuf[0:4], magicText[:]) != 0 {
		return Manifest{}, 0, errBadMagic
	}
	//
	version := binary.BigEndian.Uint32(magicBuf[4:8])
	if version != magicVersion {
		return Manifest{}, 0,
			fmt.Errorf("manifest has unsupported version: %d (we support %d)", version, magicVersion)
	}

	offset := r.count

	build := createManifest()
	for {
		offset = r.count
		var lenCrcBuf [8]byte
		_, err := io.ReadFull(&r, lenCrcBuf[:])
		if err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				// 如果文件读完
				break
			}
			// 返回error
			return Manifest{}, 0, err
		}
		length := binary.BigEndian.Uint32(lenCrcBuf[0:4])
		var buf = make([]byte, length)

		if _, err := io.ReadFull(&r, buf); err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				// 文件读错误
				break
			}
			// 返回error
			return Manifest{}, 0, err
		}
		if crc32.Checksum(buf, y.CastagnoliCrcTable) != binary.BigEndian.Uint32(lenCrcBuf[4:8]) {
			// checksum出了问题
			break
		}

		var changeSet protos.ManifestChangeSet
		if err := changeSet.Unmarshal(buf); err != nil {
			return Manifest{}, 0, err
		}

		if err := applyChangeSet(&build, &changeSet); err != nil {
			return Manifest{}, 0, err
		}
	}

	return build, offset, err
}

func applyManifestChange(build *Manifest, tc *protos.ManifestChange) error {
	switch tc.Op {
	case protos.ManifestChange_CREATE:
		if _, ok := build.Tables[tc.Id]; ok {
			return fmt.Errorf("MANIFEST invalid, table %d exists", tc.Id)
		}
		build.Tables[tc.Id] = TableManifest{
			Level: uint8(tc.Level),
		}
		for len(build.Levels) <= int(tc.Level) {
			build.Levels = append(build.Levels, LevelManifest{make(map[uint64]struct{})})
		}
		build.Levels[tc.Level].Tables[tc.Id] = struct{}{}
		build.Creations++
	case protos.ManifestChange_DELETE:
		tm, ok := build.Tables[tc.Id]
		if !ok {
			return fmt.Errorf("MANIFEST removes non-existing table %d", tc.Id)
		}
		delete(build.Levels[tm.Level].Tables, tc.Id)
		delete(build.Tables, tc.Id)
		build.Deletions++
	default:
		return fmt.Errorf("MANIFEST file has invalid manifestChange op")
	}
	return nil
}

// This is not a "recoverable" error -- opening the KV store fails because the MANIFEST file is
// just plain broken.
func applyChangeSet(build *Manifest, changeSet *protos.ManifestChangeSet) error {
	for _, change := range changeSet.Changes {
		if err := applyManifestChange(build, change); err != nil {
			return err
		}
	}
	return nil
}

// 生成一个新文件的change信息
func makeTableCreateChange(id uint64, level int) *protos.ManifestChange {
	return &protos.ManifestChange{
		Id:    id,
		Op:    protos.ManifestChange_CREATE,
		Level: uint32(level),
	}
}

// 生成一个删除文件的信息的操作
func makeTableDeleteChange(id uint64) *protos.ManifestChange {
	return &protos.ManifestChange{
		Id: id,
		Op: protos.ManifestChange_DELETE,
	}
}
