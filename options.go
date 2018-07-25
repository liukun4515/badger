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
	"github.com/dgraph-io/badger/options"
	"github.com/dgraph-io/badger/y"
)

// NOTE: Keep the comments in the following to 75 chars width, so they
// format nicely in godoc.

// Options are params for creating DB object.
// option信息用来创建KV DB的时候使用
type Options struct {
	// 1. Mandatory flags
	// -------------------
	// Directory to store the data in. Should exist and be writable.
	Dir string
	// Directory to store the value log in. Can be the same as Dir. Should
	// exist and be writable.
	ValueDir string

	// 2. Frequently modified flags
	// -----------------------------
	// Sync all writes to disk. Setting this to true would slow down data
	// loading significantly.
	SyncWrites bool

	// How should LSM tree be accessed.
	TableLoadingMode options.FileLoadingMode

	// 3. Flags that user might want to review
	// ----------------------------------------
	// The following affect all levels of LSM tree.
	// 最大的一个sst文件大小
	// 一个sstable最大的大小为64M
	// 其实也就规定了一个memtable大小当达到64M的时候就会进行 flush compaction
	MaxTableSize        int64 // Each table (or file) is at most this size.
	// 层次之间的倍数
	LevelSizeMultiplier int   // Equals SizeOf(Li+1)/SizeOf(Li).
	// 最大的level数目
	MaxLevels           int   // Maximum number of levels of compaction.
	// If value size >= this threshold, only store value offsets in tree.
	// value大小的限制？？？不太懂是做什么的
	ValueThreshold int
	// Maximum number of tables to keep in memory, before stalling.
	// 当memtable大小大于这个值的时候就会停止
	NumMemtables int
	// The following affect how we handle LSM tree L0.
	// Maximum number of Level 0 tables before we start compacting.
	// l0文件的个数
	NumLevelZeroTables int

	// If we hit this number of Level 0 tables, we will stall until L0 is
	// compacted away.
	// 当l0的文件高于一定的数目就会stall
	NumLevelZeroTablesStall int

	// Maximum total size for L1.
	// l0文件的大小
	LevelOneSize int64

	// Size of single value log file.
	// 单个value log文件的大小
	ValueLogFileSize int64

	// Number of compaction workers to run concurrently.
	// 并发可以compactor的个数
	NumCompactors int

	// 4. Flags for testing purposes
	// ------------------------------
	DoNotCompact bool // Stops LSM tree from compactions.
	// 控制最大batch的个数和size
	// 因为如果一个batch太大，比如 N MB这样不可能wal做到原子性
	maxBatchCount int64 // max entries in batch
	maxBatchSize  int64 // max batch size in bytes
}

// DefaultOptions sets a list of recommended options for good performance.
// Feel free to modify these to suit your needs.
var DefaultOptions = Options{
	DoNotCompact:        false,
	// L1文件总大小256M
	LevelOneSize:        256 << 20,
	LevelSizeMultiplier: 10,
	TableLoadingMode:    options.LoadToRAM,
	// table.MemoryMap to mmap() the tables.
	// table.Nothing to not preload the tables.
	MaxLevels:               7,
	MaxTableSize:            64 << 20,
	// 最大的并发compactor的数目
	NumCompactors:           3,
	// l0文件个数目5个开始compaction
	NumLevelZeroTables:      5,
	// l0文件的数目达到10个就进行stall
	NumLevelZeroTablesStall: 10,
	// memtable最多为5个在队列中
	NumMemtables:            5,
	// sync write 关闭
	SyncWrites:              false,
	// Nothing to read/write value log using standard File I/O
	// MemoryMap to mmap() the value log files
	// value log的大小为1GB
	ValueLogFileSize: 1 << 30,
	ValueThreshold:   20,
}

// 估计一个entry的大小
func (opt *Options) estimateSize(entry *Entry) int {
	if len(entry.Value) < opt.ValueThreshold {
		return len(entry.Key) + len(entry.Value) + y.MetaSize + y.UserMetaSize + y.CasSize
	}
	return len(entry.Key) + 16 + y.MetaSize + y.UserMetaSize + y.CasSize
}
