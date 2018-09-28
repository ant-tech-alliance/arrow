// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#ifndef PLASMA_QUEUE_H
#define PLASMA_QUEUE_H

#include <functional>
#include <memory>
#include <string>
#include <vector>
#include <unordered_map>
#include <algorithm>

#include "arrow/buffer.h"
#include "arrow/status.h"
#include "arrow/util/macros.h"
#include "arrow/util/visibility.h"
#include "plasma/common.h"

using arrow::Buffer;
using arrow::Status;

namespace plasma {

static const uint64_t QUEUE_INVALID_OFFSET = -1;
static const uint64_t QUEUE_INVALID_SEQ_ID = -1;
static const uint32_t QUEUE_MAX_BLOCK_SIZE = 1000;

struct QueueHeader {
  QueueHeader()
    : first_block_offset(QUEUE_INVALID_OFFSET)
    , ref_count(0)
    , last_sealed_seq_id(0) {
  }
  // Offset of the first valid block relative to the start of the buffer. -1 for invalid.
  uint64_t first_block_offset;
  // Reference count to protect the first_block_offset field from being changed.
  uint32_t ref_count;
  // Last sealed sequence id for queue reader
  uint64_t last_sealed_seq_id;
};

struct QueueBlockHeader {
  QueueBlockHeader()
    : start_seq_id(0)
    , end_seq_id(0)
    , is_sealed(true)
    , next_block_offset(QUEUE_INVALID_OFFSET)
    , ref_count(0) {
    // Account for the space occupied by QueueBlockHeader. The item offsets
    // are relative to the start of this block.
    auto start_offset_in_block = sizeof(QueueBlockHeader);
    std::fill_n(item_offsets, sizeof(item_offsets)/sizeof(uint32_t), start_offset_in_block);
  }

  // Start seq ID of this block.
  uint64_t start_seq_id;
  // End seq ID of this block.
  uint64_t end_seq_id;
  // Indicates the block is sealed.
  bool is_sealed;
  // Offset of next block which is relative to start of the buffer. -1 indicates invalid.
  uint64_t next_block_offset;
  // Number of elements contained in this block. Note that it's possible that a block
  // contains less than QUEUE_MAX_BLOCK_SIZE items, e.g. if it reaches the end of Queue buffer.
  uint32_t ref_count;
  // Offset of items relative to the start of this block. This array has
  // QUEUE_MAX_BLOCK_SIZE + 1 elements as the last one accounts for the offset
  // for the end of this block (if this block has QUEUE_MAX_BLOCK_SIZE items).
  uint32_t item_offsets[QUEUE_MAX_BLOCK_SIZE + 1];
};

class PlasmaQueueWriter {
 public:
  // when reconstruct a writer, we should set from_exist as true
  PlasmaQueueWriter(uint8_t* buffer, uint64_t buffer_size, bool evict_without_client = false, bool initialize_from_buffer = false);
  // Try to add an item into the queue, if memory is not enough then try to
  // evict the block
  arrow::Status Append(uint8_t* data, uint32_t data_size, uint64_t& seq_id, uint64_t& data_offset,
    uint64_t& start_offset, uint64_t timestamp, bool& should_create_block, bool& try_to_evict);

  // Memory is enough and append the item from start_offset
  void Append(uint8_t* data, uint32_t data_size, uint64_t& seq_id, uint64_t& data_offset,
    uint64_t start_offset, uint64_t timestamp, bool should_create_block);
  // Evict blocks to get memory
  void EvictBlocks(uint64_t seq_id);

  uint8_t* GetBuffer() { return buffer_; }
  uint64_t GetBufferSize() { return buffer_size_; }

  void SetLastSeal(uint64_t last_seal) { queue_header_->last_sealed_seq_id = last_seal;}
  uint64_t GetLastSeal() { return queue_header_->last_sealed_seq_id;}

  bool GetEvictWithoutClient() { return evict_without_client_; }
  uint64_t GetTimeStampSize() { return time_stamp_size_; }

 private:
  // Find the start address(new_start_offset) to write next item
  bool FindStartOffset(uint32_t data_size, uint64_t& new_start_offset);

  bool Allocate(uint64_t& start_offset, uint32_t data_size, uint64_t &seq_id, bool& try_to_evict);

  arrow::Status InitializeFromBuffer(bool evict_without_client);

  // Points to start of the buffer.
  uint8_t* buffer_;
  // Size of the ring buffer.
  uint64_t buffer_size_;
  // Points to the start of the buffer
  QueueHeader* queue_header_;

  uint64_t seq_id_;
  // next item offset of the current block
  uint32_t next_index_in_block_;
  // offset of last block relative to the start of the buffer
  uint64_t last_block_offset_;
  // indicate whether evict the items when the queue is full without client, default is false
  bool evict_without_client_;
  const uint64_t time_stamp_size_;

}; // PlasmaQueueWriter

// Reader of plasma queue. Only used for plasma store to re-generate queue item
// notifications when necessary. There is no coordination with queue writer.
class PlasmaQueueReader {
 public:
  PlasmaQueueReader(std::shared_ptr<Buffer> buffer, uint64_t start_block_offset,
      uint64_t start_seq_id);

  Status GetNext(uint8_t** data, uint32_t* data_size, uint64_t* data_offset, uint64_t* seq_id, uint64_t* timestamp);
  uint64_t GetLastSeal() { return queue_header_->last_sealed_seq_id;}
 private:
  // Buffer pointer which holds the reference to queue buffer.
  std::shared_ptr<Buffer> queue_buffer_;
  // Points to start of the ring buffer.
  uint8_t* buffer_;
  // Size of the ring buffer.
  uint64_t buffer_size_;
  // Points to the start of ring buffer, this is mostly to facilitate
  // writing the queue data structure.
  QueueHeader* queue_header_;
  // Current sequence id.
  uint64_t curr_seq_id_;
  // Current block header;
  QueueBlockHeader* curr_block_header_;
  const uint64_t time_stamp_size_;
}; // PlasmaQueueReader

}  // namespace plasma

#endif  // PLASMA_QUEUE_H
