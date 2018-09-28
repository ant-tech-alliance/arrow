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

// PLASMA CLIENT: Client library for using the plasma store and manager

#include "plasma/plasma_queue.h"

#ifdef _WIN32
#include <Win32_Interop/win32_types.h>
#endif

#include <fcntl.h>
#include <netinet/in.h>
#include <stdio.h>
#include <stdlib.h>
#include <strings.h>
#include <sys/ioctl.h>
#include <sys/mman.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <sys/un.h>
#include <time.h>
#include <unistd.h>

#include <algorithm>
#include <deque>
#include <mutex>
#include <unordered_map>
#include <vector>

#include "arrow/buffer.h"
#include "arrow/util/thread-pool.h"

#include "plasma/common.h"
#include "plasma/fling.h"
#include "plasma/io.h"
#include "plasma/malloc.h"
#include "plasma/plasma.h"
#include "plasma/protocol.h"

namespace plasma {

using flatbuf::PlasmaError;

PlasmaQueueWriter::PlasmaQueueWriter(uint8_t* buffer, uint64_t buffer_size,
                                     bool evict_without_client, bool initialize_from_buffer) :
  buffer_(buffer),
  buffer_size_(buffer_size),
  queue_header_(reinterpret_cast<QueueHeader*>(buffer_)),
  seq_id_(0),
  next_index_in_block_(0),
  last_block_offset_(QUEUE_INVALID_OFFSET),
  evict_without_client_(evict_without_client),
  time_stamp_size_(sizeof(uint64_t)) {

  ARROW_CHECK(buffer_ != nullptr);

  // Data is in buffer, we should reconstruct the queue from the buffer data
  if (initialize_from_buffer) {
    ARROW_CHECK_OK(InitializeFromBuffer(evict_without_client));
  } else {
    // Initialize the queue header properly.
    *queue_header_ = QueueHeader();
    next_index_in_block_ = 0;
  }
}

Status PlasmaQueueWriter::InitializeFromBuffer(bool evict_without_client) {
  if (queue_header_ == nullptr) {
    return Status::Invalid("Initialize queue writer from buffer failed, the input buffer is invalid.");
  }
  auto first_block_offset = queue_header_->first_block_offset;
  // Queue in buffer is empty, just return
  if (first_block_offset == QUEUE_INVALID_OFFSET) {
    return Status::OK();
  }

  uint64_t next_block_offset = QUEUE_INVALID_OFFSET;
  auto curr_block_header = reinterpret_cast<QueueBlockHeader*> (buffer_ + first_block_offset);
  // Find the last block and reconstruct the writer information
  while(curr_block_header->next_block_offset != QUEUE_INVALID_OFFSET) {
    next_block_offset = curr_block_header->next_block_offset;
    curr_block_header = reinterpret_cast<QueueBlockHeader*> (buffer_ + next_block_offset);
  }
  // Now already reaches the last block
  seq_id_ = curr_block_header->end_seq_id;
  last_block_offset_ = next_block_offset;
  next_index_in_block_ = curr_block_header->end_seq_id - curr_block_header->start_seq_id + 1;

  evict_without_client_ = evict_without_client;
  return Status::OK();
}

bool PlasmaQueueWriter::FindStartOffset(uint32_t data_size, uint64_t& new_start_offset) {
  if (queue_header_->first_block_offset == QUEUE_INVALID_OFFSET) {
    // Queue is empty. Start immediately after QueueHeader.
    new_start_offset = sizeof(QueueHeader);
    return true;
  }

  bool should_create_block = false;
  if (next_index_in_block_ >= QUEUE_MAX_BLOCK_SIZE) {
    // Already reaches the end of current block. Create a new one.
    should_create_block = true;
    // Don't return yet, calculate the start offset for next block below.
    next_index_in_block_ = QUEUE_MAX_BLOCK_SIZE;
    // Account for the space needed for block header.
    data_size += sizeof(QueueBlockHeader);
  }

  QueueBlockHeader* curr_block_header = reinterpret_cast<QueueBlockHeader*>(buffer_ + last_block_offset_);
  auto curr_block_end_offset = last_block_offset_ + curr_block_header->item_offsets[next_index_in_block_];
  if (curr_block_end_offset + data_size > buffer_size_) {
    // It would reach the end of buffer if we append the new data to the current block.
    // So we need to start from the begining, which is right after QueueHeader.
    should_create_block = true;
    new_start_offset = sizeof(QueueHeader);
  } else {
    // Still ok to append to the current end.
    new_start_offset = curr_block_end_offset;
  }

  return should_create_block;
}

void PlasmaQueueWriter::EvictBlocks(uint64_t seqid) {
  QueueBlockHeader * curr_block_header;
  while(queue_header_->first_block_offset != QUEUE_INVALID_OFFSET) {
    curr_block_header = reinterpret_cast<QueueBlockHeader *>(buffer_ + queue_header_->first_block_offset);
    if(curr_block_header->end_seq_id > seqid) {
      break;
    }
    queue_header_->first_block_offset = curr_block_header->next_block_offset;
  }
  curr_block_header = reinterpret_cast<QueueBlockHeader *>(buffer_ + queue_header_->first_block_offset);
  // all blocks in the queue have been evicted
  if (queue_header_->first_block_offset == QUEUE_INVALID_OFFSET) {
    last_block_offset_ = QUEUE_INVALID_OFFSET;
  }
}

bool PlasmaQueueWriter::Allocate(uint64_t& start_offset, uint32_t data_size, uint64_t& seq_id, bool& try_to_evict) {
  // deal with four situations
  auto queue_header_size = sizeof(QueueHeader);
  int64_t fake_first_block_offset = queue_header_->first_block_offset;
  if (start_offset != queue_header_size) {
    if (start_offset > queue_header_->first_block_offset) {
      return true;
    }
  } else {
    if (queue_header_->first_block_offset == QUEUE_INVALID_OFFSET) {
      return start_offset + data_size <= buffer_size_;
    }
    if (last_block_offset_ < queue_header_->first_block_offset) {
      // Item from first block offset to the buffer end should be evicted
      try_to_evict = true;
      QueueBlockHeader* block_at_buffer_start = reinterpret_cast<QueueBlockHeader*>(buffer_ + queue_header_size);
      // evict sequence id
      seq_id = block_at_buffer_start->start_seq_id - 1;
      fake_first_block_offset = queue_header_size;
    }
  }
  //if (start_offset + data_size <= queue_header_->first_block_offset) {
  if (start_offset + data_size <= fake_first_block_offset) {
    // Enough space before first_block, no need for eviction.
    return true;
  }
  // Evict blocks, we should send the message to plasma store to check whether we can
  // evict the queue blocks, so we shouldn't evict the item directly. We use a fake first block
  // offset here to save the evict position
  // TODO: solve race between reader & writer on first_block_offset.
  while (fake_first_block_offset != QUEUE_INVALID_OFFSET) {
    // auto curr_first_block = reinterpret_cast<QueueBlockHeader*>(buffer_ + queue_header_->first_block_offset);
    auto curr_first_block = reinterpret_cast<QueueBlockHeader*>(buffer_ + fake_first_block_offset);
    // record the seq id to evict
    // if all consumed item seq id is greater than it, we can evict
    seq_id = curr_first_block->end_seq_id;

    // TODO: not clear here
    if (curr_first_block->ref_count > 0) {
      // current block is in-use, cannot evict.
      return false;
    }

    // XXX: Remove the current block for more space.
    // queue_header_->first_block_offset = curr_first_block->next_block_offset;
    fake_first_block_offset = curr_first_block->next_block_offset;

    auto next_block_offset = curr_first_block->next_block_offset;
    if (next_block_offset == queue_header_size || next_block_offset == QUEUE_INVALID_OFFSET) {
      // This indicates the next block is round back at the start of the buffer, or
      // this is the last block.
      next_block_offset = buffer_size_;
    }

    if (start_offset + data_size <= next_block_offset) {
      // OK, we could satisfy the space requirement by removing current block.
      try_to_evict = true;
      break;
    }
  }

  if (fake_first_block_offset == QUEUE_INVALID_OFFSET) {
    last_block_offset_ = QUEUE_INVALID_OFFSET;
    if(data_size <= (buffer_size_ - queue_header_size)) {
      try_to_evict = true;
    }
    // This indicates that all the existing blocks have been evicted. In this case
    // check if the whole buffer can contain the new item.
    return data_size <= (buffer_size_ - queue_header_size);
  }

  return false;
}

arrow::Status PlasmaQueueWriter::Append(uint8_t* data, uint32_t data_size, uint64_t& seq_id,
  uint64_t& data_offset, uint64_t& start_offset, uint64_t timestamp, bool& should_create_block, bool & try_to_evict) {
  // add time stamp to each item
  uint32_t required_size = data_size + time_stamp_size_;

  start_offset = sizeof(QueueHeader);
  should_create_block = FindStartOffset(required_size, start_offset);

  if (should_create_block) {
     required_size += sizeof(QueueBlockHeader);
  }

  // Try to allocate the space for new item.
  seq_id = QUEUE_INVALID_SEQ_ID;
  bool succeed = Allocate(start_offset, required_size, seq_id, try_to_evict);
  if (!succeed || try_to_evict) {
    return Status::OutOfMemory("Could not allocate space in queue buffer!");
  }

  Append(data, data_size, seq_id, data_offset, start_offset, timestamp, should_create_block);
  return Status::OK();
}

// Memory is enough, just append the item into the queue
void PlasmaQueueWriter::Append(uint8_t* data, uint32_t data_size, uint64_t& seq_id,
  uint64_t& data_offset, uint64_t start_offset, uint64_t timestamp, bool should_create_block) {
  seq_id_++;
  seq_id = seq_id_;

  uint8_t* dest_addr;
  if (should_create_block) {
    // 1. initialize new block.
    auto new_block_header = reinterpret_cast<QueueBlockHeader*>(buffer_ + start_offset);
    *new_block_header = QueueBlockHeader();
    new_block_header->start_seq_id = seq_id_;
    new_block_header->end_seq_id = seq_id_;

    next_index_in_block_ = 0;
    // append new data.
    dest_addr = reinterpret_cast<uint8_t*>(new_block_header) + new_block_header->item_offsets[next_index_in_block_];
    new_block_header->item_offsets[next_index_in_block_ + 1] =
        new_block_header->item_offsets[next_index_in_block_] + data_size + time_stamp_size_;

    // hook up previous block with new one.
    if (queue_header_->first_block_offset == QUEUE_INVALID_OFFSET) {
      queue_header_->first_block_offset = start_offset;
      last_block_offset_ = start_offset;
    } else {
      QueueBlockHeader* last_block_header = reinterpret_cast<QueueBlockHeader*>(buffer_ + last_block_offset_);
      last_block_header->next_block_offset = start_offset;
      last_block_offset_ = start_offset;
    }
  } else {
    // ASSERT(last_block_offset_ + curr_block_header->item_offsets[next_index_in_block_] == start_offset);
    QueueBlockHeader* last_block_header = reinterpret_cast<QueueBlockHeader*>(buffer_ + last_block_offset_);

    last_block_header->end_seq_id = seq_id_;

    dest_addr = reinterpret_cast<uint8_t*>(last_block_header) + last_block_header->item_offsets[next_index_in_block_];
    last_block_header->item_offsets[next_index_in_block_ + 1] =
        last_block_header->item_offsets[next_index_in_block_] + data_size + time_stamp_size_;
  }

  uint64_t* stamp_addr = reinterpret_cast<uint64_t*>(dest_addr);
  *stamp_addr = ((timestamp != -1) ? timestamp : 0);

  next_index_in_block_++;
  data_offset = dest_addr - buffer_;
  if (data != nullptr) {
    memcpy(dest_addr + time_stamp_size_, data, data_size);
  }
}

PlasmaQueueReader::PlasmaQueueReader(std::shared_ptr<Buffer> buffer,
                                     uint64_t start_block_offset,
                                     uint64_t start_seq_id) :
  queue_buffer_(buffer),
  buffer_(buffer->mutable_data()),
  buffer_size_(buffer->size()),
  queue_header_(reinterpret_cast<QueueHeader*>(buffer_)),
  curr_seq_id_(start_seq_id),
  curr_block_header_(nullptr),
  time_stamp_size_(sizeof(uint64_t)) {

  auto curr_block_offset = start_block_offset;

  while (curr_block_offset != QUEUE_INVALID_OFFSET) {
    auto block_header = reinterpret_cast<QueueBlockHeader*>(buffer_ + curr_block_offset);

    if (!block_header->is_sealed || start_seq_id < block_header->start_seq_id) {
      break;
    }

    if (start_seq_id <= block_header->end_seq_id) {
       curr_block_header_ = block_header;
       curr_seq_id_ = start_seq_id;
       break;
    }

    curr_block_offset = block_header->next_block_offset;
  }
}

arrow::Status PlasmaQueueReader::GetNext(uint8_t **data, uint32_t *data_size,
                                         uint64_t* data_offset, uint64_t *seq_id,
                                         uint64_t* timestamp) {

  // Check if still in current block.
  // if not, move to next block. Add refcnt to next block, and release refcnt to current block.
  if (curr_block_header_ == nullptr) {
    return Status::PlasmaObjectNonexistent("Sequence id in this block is not found.");
  }

  // curr_seq_id >= curr_block_header_->seq_id, but it's possible it reaches the end of this block
  // and arrives at the first of next block (if it exists).
  if (curr_seq_id_ > curr_block_header_->end_seq_id) {
    auto next_block_offset = curr_block_header_->next_block_offset;
    if (next_block_offset == QUEUE_INVALID_OFFSET) {
      // next block is empty. return failure.
      return Status::PlasmaObjectNonexistent("Next block is not found.");
    }
    curr_block_header_ = reinterpret_cast<QueueBlockHeader*>(buffer_ + next_block_offset);
  }

  if (!curr_block_header_->is_sealed) {
    return Status::PlasmaObjectNonexistent("Required block is not sealed.");
  }

  auto index = curr_seq_id_ - curr_block_header_->start_seq_id;
  uint8_t * start_addr = reinterpret_cast<uint8_t*>(curr_block_header_) + curr_block_header_->item_offsets[index];
  *data =  start_addr + time_stamp_size_;
  *timestamp = *(reinterpret_cast<uint64_t*>(start_addr));
  *data_size = curr_block_header_->item_offsets[index + 1] - curr_block_header_->item_offsets[index] - time_stamp_size_;
  // offset of the item, including the timestamp and actual data
  *data_offset = static_cast<uint64_t>(reinterpret_cast<uint8_t*>(curr_block_header_) - buffer_) +
      curr_block_header_->item_offsets[index];
  *seq_id = curr_seq_id_++;

  return Status::OK();
}

}  // namespace plasma
