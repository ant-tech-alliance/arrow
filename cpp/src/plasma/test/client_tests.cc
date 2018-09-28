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

#include <assert.h>
#include <signal.h>
#include <stdlib.h>
#include <sys/time.h>
#include <sys/types.h>
#include <unistd.h>

#include <chrono>
#include <random>
#include <thread>
#include <iostream>

#include <gtest/gtest.h>

#include "arrow/test-util.h"

#include "plasma/client.h"
#include "plasma/common.h"
#include "plasma/plasma.h"
#include "plasma/protocol.h"
#include "plasma/test-util.h"

namespace plasma {

std::string test_executable;  // NOLINT

void AssertObjectBufferEqual(const ObjectBuffer& object_buffer,
                             const std::vector<uint8_t>& metadata,
                             const std::vector<uint8_t>& data) {
  arrow::AssertBufferEqual(*object_buffer.metadata, metadata);
  arrow::AssertBufferEqual(*object_buffer.data, data);
}

class TestPlasmaStore : public ::testing::Test {
 public:
  // TODO(pcm): At the moment, stdout of the test gets mixed up with
  // stdout of the object store. Consider changing that.
  void SetUp() {
    uint64_t seed = std::chrono::high_resolution_clock::now().time_since_epoch().count();
    std::mt19937 rng(static_cast<uint32_t>(seed));
    std::string store_index = std::to_string(rng());
    store_socket_name_ = "/tmp/store" + store_index;
    // store_socket_name_ = "/tmp/store1234";

    std::string plasma_directory =
        test_executable.substr(0, test_executable.find_last_of("/"));
    std::string plasma_command = plasma_directory +
                                 "/plasma_store_server -m 1000000000 -s " +
                                 store_socket_name_ + " 1> /dev/null 2> /dev/null &";
    system(plasma_command.c_str());
    // prevent the warning when the store setup hasn't finished
    usleep(100*1000);
    ARROW_CHECK_OK(client_.Connect(store_socket_name_, ""));
    ARROW_CHECK_OK(client2_.Connect(store_socket_name_, ""));
    ARROW_CHECK_OK(client3_.Connect(store_socket_name_, ""));
  }
  virtual void TearDown() {
    ARROW_CHECK_OK(client_.Disconnect());
    ARROW_CHECK_OK(client2_.Disconnect());
    // Kill all plasma_store processes
    // TODO should only kill the processes we launched
#ifdef COVERAGE_BUILD
    // Ask plasma_store to exit gracefully and give it time to write out
    // coverage files
    system("killall -TERM plasma_store_server");
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
#endif
    system("killall -KILL plasma_store_server");
  }

  void CreateObject(PlasmaClient& client, const ObjectID& object_id,
                    const std::vector<uint8_t>& metadata,
                    const std::vector<uint8_t>& data) {
    std::shared_ptr<Buffer> data_buffer;
    ARROW_CHECK_OK(client.Create(object_id, data.size(), &metadata[0], metadata.size(),
                                 &data_buffer));
    for (size_t i = 0; i < data.size(); i++) {
      data_buffer->mutable_data()[i] = data[i];
    }
    ARROW_CHECK_OK(client.Seal(object_id));
    ARROW_CHECK_OK(client.Release(object_id));
  }

  const std::string& GetStoreSocketName() const { return store_socket_name_; }

 protected:
  PlasmaClient client_;
  PlasmaClient client2_;
  PlasmaClient client3_;
  std::string store_socket_name_;
};

TEST_F(TestPlasmaStore, NewSubscriberTest) {
  PlasmaClient local_client, local_client2;

  ARROW_CHECK_OK(local_client.Connect(store_socket_name_, ""));
  ARROW_CHECK_OK(local_client2.Connect(store_socket_name_, ""));

  ObjectID object_id = random_object_id();

  // Test for the object being in local Plasma store.
  // First create object.
  int64_t data_size = 100;
  uint8_t metadata[] = {5};
  int64_t metadata_size = sizeof(metadata);
  std::shared_ptr<Buffer> data;
  ARROW_CHECK_OK(
      local_client.Create(object_id, data_size, metadata, metadata_size, &data));
  ARROW_CHECK_OK(local_client.Seal(object_id));

  // Test that new subscriber client2 can receive notifications about existing objects.
  int fd = -1;
  ARROW_CHECK_OK(local_client2.Subscribe(&fd));
  ASSERT_GT(fd, 0);

  ObjectID object_id2 = random_object_id();
  int64_t data_size2 = 0;
  int64_t metadata_size2 = 0;
  ARROW_CHECK_OK(
      local_client2.GetNotification(fd, &object_id2, &data_size2, &metadata_size2));
  ASSERT_EQ(object_id, object_id2);
  ASSERT_EQ(data_size, data_size2);
  ASSERT_EQ(metadata_size, metadata_size2);

  // Delete the object.
  ARROW_CHECK_OK(local_client.Release(object_id));
  ARROW_CHECK_OK(local_client.Delete(object_id));

  ARROW_CHECK_OK(
      local_client2.GetNotification(fd, &object_id2, &data_size2, &metadata_size2));
  ASSERT_EQ(object_id, object_id2);
  ASSERT_EQ(-1, data_size2);
  ASSERT_EQ(-1, metadata_size2);

  ARROW_CHECK_OK(local_client2.Disconnect());
  ARROW_CHECK_OK(local_client.Disconnect());
}

TEST_F(TestPlasmaStore, SealErrorsTest) {
  ObjectID object_id = random_object_id();

  Status result = client_.Seal(object_id);
  ASSERT_TRUE(result.IsPlasmaObjectNonexistent());

  // Create object.
  std::vector<uint8_t> data(100, 0);
  CreateObject(client_, object_id, {42}, data);

  // Trying to seal it again.
  result = client_.Seal(object_id);
  ASSERT_TRUE(result.IsPlasmaObjectAlreadySealed());
}

TEST_F(TestPlasmaStore, DeleteTest) {
  ObjectID object_id = random_object_id();

  // Test for deleting non-existance object.
  Status result = client_.Delete(object_id);
  ARROW_CHECK_OK(result);

  // Test for the object being in local Plasma store.
  // First create object.
  int64_t data_size = 100;
  uint8_t metadata[] = {5};
  int64_t metadata_size = sizeof(metadata);
  std::shared_ptr<Buffer> data;
  ARROW_CHECK_OK(client_.Create(object_id, data_size, metadata, metadata_size, &data));
  ARROW_CHECK_OK(client_.Seal(object_id));

  result = client_.Delete(object_id);
  ARROW_CHECK_OK(result);
  bool has_object = false;
  ARROW_CHECK_OK(client_.Contains(object_id, &has_object));
  ASSERT_TRUE(has_object);

  // Avoid race condition of Plasma Manager waiting for notification.
  ARROW_CHECK_OK(client_.Release(object_id));
  // object_id is marked as to-be-deleted, when it is not in use, it will be deleted.
  ARROW_CHECK_OK(client_.Contains(object_id, &has_object));
  ASSERT_FALSE(has_object);
  ARROW_CHECK_OK(client_.Delete(object_id));
}

TEST_F(TestPlasmaStore, DeleteObjectsTest) {
  ObjectID object_id1 = random_object_id();
  ObjectID object_id2 = random_object_id();

  // Test for deleting non-existance object.
  Status result = client_.Delete(std::vector<ObjectID>{object_id1, object_id2});
  ARROW_CHECK_OK(result);
  // Test for the object being in local Plasma store.
  // First create object.
  int64_t data_size = 100;
  uint8_t metadata[] = {5};
  int64_t metadata_size = sizeof(metadata);
  std::shared_ptr<Buffer> data;
  ARROW_CHECK_OK(client_.Create(object_id1, data_size, metadata, metadata_size, &data));
  ARROW_CHECK_OK(client_.Seal(object_id1));
  ARROW_CHECK_OK(client_.Create(object_id2, data_size, metadata, metadata_size, &data));
  ARROW_CHECK_OK(client_.Seal(object_id2));
  // Release the ref count of Create function.
  ARROW_CHECK_OK(client_.Release(object_id1));
  ARROW_CHECK_OK(client_.Release(object_id2));
  // Increase the ref count by calling Get using client2_.
  std::vector<ObjectBuffer> object_buffers;
  ARROW_CHECK_OK(client2_.Get({object_id1, object_id2}, 0, &object_buffers));
  // Objects are still used by client2_.
  result = client_.Delete(std::vector<ObjectID>{object_id1, object_id2});
  ARROW_CHECK_OK(result);
  // The object is used and it should not be deleted right now.
  bool has_object = false;
  ARROW_CHECK_OK(client_.Contains(object_id1, &has_object));
  ASSERT_TRUE(has_object);
  ARROW_CHECK_OK(client_.Contains(object_id2, &has_object));
  ASSERT_TRUE(has_object);
  // Decrease the ref count by deleting the PlasmaBuffer (in ObjectBuffer).
  // client2_ won't send the release request immediately because the trigger
  // condition is not reached. The release is only added to release cache.
  object_buffers.clear();
  // The reference count went to zero, but the objects are still in the release
  // cache.
  ARROW_CHECK_OK(client_.Contains(object_id1, &has_object));
  ASSERT_TRUE(has_object);
  ARROW_CHECK_OK(client_.Contains(object_id2, &has_object));
  ASSERT_TRUE(has_object);
  // The Delete call will flush release cache and send the Delete request.
  result = client2_.Delete(std::vector<ObjectID>{object_id1, object_id2});
  ARROW_CHECK_OK(client_.Contains(object_id1, &has_object));
  ASSERT_FALSE(has_object);
  ARROW_CHECK_OK(client_.Contains(object_id2, &has_object));
  ASSERT_FALSE(has_object);
}

TEST_F(TestPlasmaStore, ContainsTest) {
  ObjectID object_id = random_object_id();

  // Test for object non-existence.
  bool has_object;
  ARROW_CHECK_OK(client_.Contains(object_id, &has_object));
  ASSERT_FALSE(has_object);

  // Test for the object being in local Plasma store.
  // First create object.
  std::vector<uint8_t> data(100, 0);
  CreateObject(client_, object_id, {42}, data);
  // Avoid race condition of Plasma Manager waiting for notification.
  std::vector<ObjectBuffer> object_buffers;
  ARROW_CHECK_OK(client_.Get({object_id}, -1, &object_buffers));
  ARROW_CHECK_OK(client_.Contains(object_id, &has_object));
  ASSERT_TRUE(has_object);
}

TEST_F(TestPlasmaStore, GetTest) {
  std::vector<ObjectBuffer> object_buffers;

  ObjectID object_id = random_object_id();

  // Test for object non-existence.
  ARROW_CHECK_OK(client_.Get({object_id}, 0, &object_buffers));
  ASSERT_EQ(object_buffers.size(), 1);
  ASSERT_FALSE(object_buffers[0].metadata);
  ASSERT_FALSE(object_buffers[0].data);
  EXPECT_FALSE(client_.IsInUse(object_id));

  // Test for the object being in local Plasma store.
  // First create object.
  std::vector<uint8_t> data = {3, 5, 6, 7, 9};
  CreateObject(client_, object_id, {42}, data);
  ARROW_CHECK_OK(client_.FlushReleaseHistory());
  EXPECT_FALSE(client_.IsInUse(object_id));

  object_buffers.clear();
  ARROW_CHECK_OK(client_.Get({object_id}, -1, &object_buffers));
  ASSERT_EQ(object_buffers.size(), 1);
  ASSERT_EQ(object_buffers[0].device_num, 0);
  AssertObjectBufferEqual(object_buffers[0], {42}, {3, 5, 6, 7, 9});

  // Metadata keeps object in use
  {
    auto metadata = object_buffers[0].metadata;
    object_buffers.clear();
    ::arrow::AssertBufferEqual(*metadata, {42});
    ARROW_CHECK_OK(client_.FlushReleaseHistory());
    EXPECT_TRUE(client_.IsInUse(object_id));
  }
  // Object is automatically released
  ARROW_CHECK_OK(client_.FlushReleaseHistory());
  EXPECT_FALSE(client_.IsInUse(object_id));
}

TEST_F(TestPlasmaStore, LegacyGetTest) {
  // Test for old non-releasing Get() variant
  ObjectID object_id = random_object_id();
  {
    ObjectBuffer object_buffer;

    // Test for object non-existence.
    ARROW_CHECK_OK(client_.Get(&object_id, 1, 0, &object_buffer));
    ASSERT_FALSE(object_buffer.metadata);
    ASSERT_FALSE(object_buffer.data);
    EXPECT_FALSE(client_.IsInUse(object_id));

    // First create object.
    std::vector<uint8_t> data = {3, 5, 6, 7, 9};
    CreateObject(client_, object_id, {42}, data);
    ARROW_CHECK_OK(client_.FlushReleaseHistory());
    EXPECT_FALSE(client_.IsInUse(object_id));

    ARROW_CHECK_OK(client_.Get(&object_id, 1, -1, &object_buffer));
    AssertObjectBufferEqual(object_buffer, {42}, {3, 5, 6, 7, 9});
  }
  // Object needs releasing manually
  ARROW_CHECK_OK(client_.FlushReleaseHistory());
  EXPECT_TRUE(client_.IsInUse(object_id));
  ARROW_CHECK_OK(client_.Release(object_id));
  ARROW_CHECK_OK(client_.FlushReleaseHistory());
  EXPECT_FALSE(client_.IsInUse(object_id));
}

TEST_F(TestPlasmaStore, MultipleGetTest) {
  ObjectID object_id1 = random_object_id();
  ObjectID object_id2 = random_object_id();
  std::vector<ObjectID> object_ids = {object_id1, object_id2};
  std::vector<ObjectBuffer> object_buffers;

  int64_t data_size = 4;
  uint8_t metadata[] = {5};
  int64_t metadata_size = sizeof(metadata);
  std::shared_ptr<Buffer> data;
  ARROW_CHECK_OK(client_.Create(object_id1, data_size, metadata, metadata_size, &data));
  data->mutable_data()[0] = 1;
  ARROW_CHECK_OK(client_.Seal(object_id1));

  ARROW_CHECK_OK(client_.Create(object_id2, data_size, metadata, metadata_size, &data));
  data->mutable_data()[0] = 2;
  ARROW_CHECK_OK(client_.Seal(object_id2));

  ARROW_CHECK_OK(client_.Get(object_ids, -1, &object_buffers));
  ASSERT_EQ(object_buffers[0].data->data()[0], 1);
  ASSERT_EQ(object_buffers[1].data->data()[0], 2);
}

TEST_F(TestPlasmaStore, AbortTest) {
  ObjectID object_id = random_object_id();
  std::vector<ObjectBuffer> object_buffers;

  // Test for object non-existence.
  ARROW_CHECK_OK(client_.Get({object_id}, 0, &object_buffers));
  ASSERT_FALSE(object_buffers[0].data);

  // Test object abort.
  // First create object.
  int64_t data_size = 4;
  uint8_t metadata[] = {5};
  int64_t metadata_size = sizeof(metadata);
  std::shared_ptr<Buffer> data;
  uint8_t* data_ptr;
  ARROW_CHECK_OK(client_.Create(object_id, data_size, metadata, metadata_size, &data));
  data_ptr = data->mutable_data();
  // Write some data.
  for (int64_t i = 0; i < data_size / 2; i++) {
    data_ptr[i] = static_cast<uint8_t>(i % 4);
  }
  // Attempt to abort. Test that this fails before the first release.
  Status status = client_.Abort(object_id);
  ASSERT_TRUE(status.IsInvalid());
  // Release, then abort.
  ARROW_CHECK_OK(client_.Release(object_id));
  ARROW_CHECK_OK(client_.FlushReleaseHistory());
  EXPECT_TRUE(client_.IsInUse(object_id));

  ARROW_CHECK_OK(client_.Abort(object_id));
  ARROW_CHECK_OK(client_.FlushReleaseHistory());
  EXPECT_FALSE(client_.IsInUse(object_id));

  // Test for object non-existence after the abort.
  ARROW_CHECK_OK(client_.Get({object_id}, 0, &object_buffers));
  ASSERT_FALSE(object_buffers[0].data);

  // Create the object successfully this time.
  CreateObject(client_, object_id, {42, 43}, {1, 2, 3, 4, 5});

  // Test that we can get the object.
  ARROW_CHECK_OK(client_.Get({object_id}, -1, &object_buffers));
  AssertObjectBufferEqual(object_buffers[0], {42, 43}, {1, 2, 3, 4, 5});
  ARROW_CHECK_OK(client_.Release(object_id));
}

TEST_F(TestPlasmaStore, MultipleClientTest) {
  ObjectID object_id = random_object_id();
  std::vector<ObjectBuffer> object_buffers;

  // Test for object non-existence on the first client.
  bool has_object;
  ARROW_CHECK_OK(client_.Contains(object_id, &has_object));
  ASSERT_FALSE(has_object);

  // Test for the object being in local Plasma store.
  // First create and seal object on the second client.
  int64_t data_size = 100;
  uint8_t metadata[] = {5};
  int64_t metadata_size = sizeof(metadata);
  std::shared_ptr<Buffer> data;
  ARROW_CHECK_OK(client2_.Create(object_id, data_size, metadata, metadata_size, &data));
  ARROW_CHECK_OK(client2_.Seal(object_id));
  // Test that the first client can get the object.
  ARROW_CHECK_OK(client_.Get({object_id}, -1, &object_buffers));
  ASSERT_TRUE(object_buffers[0].data);
  ARROW_CHECK_OK(client_.Contains(object_id, &has_object));
  ASSERT_TRUE(has_object);

  // Test that one client disconnecting does not interfere with the other.
  // First create object on the second client.
  object_id = random_object_id();
  ARROW_CHECK_OK(client2_.Create(object_id, data_size, metadata, metadata_size, &data));
  // Disconnect the first client.
  ARROW_CHECK_OK(client_.Disconnect());
  // Test that the second client can seal and get the created object.
  ARROW_CHECK_OK(client2_.Seal(object_id));
  ARROW_CHECK_OK(client2_.Get({object_id}, -1, &object_buffers));
  ASSERT_TRUE(object_buffers[0].data);
  ARROW_CHECK_OK(client2_.Contains(object_id, &has_object));
  ASSERT_TRUE(has_object);
}

TEST_F(TestPlasmaStore, QueueReaderTest) {

  ObjectID object_id = random_object_id();
  std::vector<ObjectBuffer> object_buffers;

  // Test for object non-existence on the first client.
  bool has_object;
  ARROW_CHECK_OK(client_.Contains(object_id, &has_object));
  ASSERT_FALSE(has_object);

  // Test for the object being in local Plasma store.
  // First create and seal object on the second client.
  int64_t queue_size = 100 * 1024;
  std::shared_ptr<Buffer> queue_item_buf;
  ARROW_CHECK_OK(client2_.CreateQueue(object_id, queue_size, &queue_item_buf));

  // ARROW_CHECK_OK(client2_.Seal(object_id));
  // Test that the first client can get the object.
  int notify_fd;
  ARROW_CHECK_OK(client_.GetQueue(object_id, -1, &notify_fd));
  ARROW_CHECK_OK(client_.Contains(object_id, &has_object));
  ASSERT_TRUE(has_object);

  uint8_t item1[] = { 1, 2, 3, 4, 5 };
  int64_t item1_size = sizeof(item1);
  for (int i = 0;i<item1_size;i++) {
    ARROW_CHECK_OK(client2_.PushQueueItem(object_id, &item1[i], sizeof(uint8_t)));
  }

  uint64_t start_block_offset = sizeof(QueueHeader);
  std::unique_ptr<PlasmaQueueReader> queue_reader(
    new PlasmaQueueReader(queue_item_buf, start_block_offset,1));
  
  uint64_t seq_id, data_offset, timestamp;
  uint32_t data_size;
  uint8_t * data = nullptr;
  for(uint64_t i = 0; i<item1_size; ++i) {
    ARROW_CHECK_OK(queue_reader->GetNext(&data, &data_size, &data_offset, &seq_id, &timestamp));
    ASSERT_TRUE((*data) == (item1[i]));
  }

  uint8_t item2[] = { 6, 7, 8, 9, 10};
  int64_t item2_size = sizeof(item2);
  for(uint64_t i = 0;i<item2_size;i++) {
    ARROW_CHECK_OK(client2_.PushQueueItem(object_id, &item2[i], item2_size));
  }
  for(uint64_t i = 0;i<item2_size;i++) {
    ARROW_CHECK_OK(queue_reader->GetNext(&data, &data_size, &data_offset, &seq_id, &timestamp));
    ASSERT_TRUE((*data) == (item2[i]));
  }
}

TEST_F(TestPlasmaStore, PushQueueFirstTest) {
  ObjectID object_id = random_object_id();
  std::vector<ObjectBuffer> object_buffers;

  // Test for object non-existence on the first client.
  bool has_object;
  ARROW_CHECK_OK(client_.Contains(object_id, &has_object));
  ASSERT_FALSE(has_object);

  // Test for the object being in local Plasma store.
  // Create the writer
  int64_t queue_size = 100 * 1024;
  std::shared_ptr<Buffer> queue_item_buf;
  ARROW_CHECK_OK(client_.CreateQueue(object_id, queue_size, &queue_item_buf));

  // Push queue before subscribe queue
  uint8_t item[] = { 1, 2, 3, 4, 5, 6, 7};
  int64_t item_size = sizeof(item);
  for (int i = 0;i<item_size;i++) {
    ARROW_CHECK_OK(client_.PushQueueItem(object_id, &item[i], sizeof(uint8_t)));
  }

  // Test that the first client can get the object.
  int notify_fd;
  ARROW_CHECK_OK(client2_.GetQueue(object_id, -1, &notify_fd));
  ARROW_CHECK_OK(client2_.Contains(object_id, &has_object));
  ASSERT_TRUE(has_object);
  uint8_t* buff = nullptr;
  uint32_t buff_size = 0;
  uint64_t seq_id = -1;
  uint64_t timestamp;
  for (int i = 0;i<item_size;i++) {
    ARROW_CHECK_OK(client2_.GetQueueItem(object_id, buff, buff_size, seq_id, timestamp));
    ASSERT_TRUE((*buff) == item[i]);
  }
}
/*
TEST_F(TestPlasmaStore, RecreateQueueTest) {
  ObjectID object_id = random_object_id();
  std::vector<ObjectBuffer> object_buffers;

  // Test for object non-existence on the first client.
  bool has_object;
  ARROW_CHECK_OK(client_.Contains(object_id, &has_object));
  ASSERT_FALSE(has_object);

  // Test for the object being in local Plasma store.
  // Create the writer
  int64_t queue_size = 100 * 1024;
  std::shared_ptr<Buffer> queue_item_buf;
  ARROW_CHECK_OK(client_.CreateQueue(object_id, queue_size, &queue_item_buf));

  // Push queue before subscribe queue
  uint8_t item[] = { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 1, 2, 3, 4, 5, 6, 7};
  int64_t item_size = sizeof(item);
  for (int i = 0;i<6;i++) {
    ARROW_CHECK_OK(client_.PushQueueItem(object_id, &item[i], sizeof(uint8_t)));
  }
  // close reader client_
  client_.Disconnect();

  // Test that the reader client2_ can get the object.
  int notify_fd;
  ARROW_CHECK_OK(client2_.GetQueue(object_id, -1, &notify_fd));
  ARROW_CHECK_OK(client2_.Contains(object_id, &has_object));
  ASSERT_TRUE(has_object);
  uint8_t* buff = nullptr;
  uint32_t buff_size = 0;
  uint64_t seq_id = -1;
  uint64_t timestamp;
  for (int i = 0;i<6;i++) {
    ARROW_CHECK_OK(client2_.GetQueueItem(object_id, buff, buff_size, seq_id, timestamp));
    ASSERT_TRUE((*buff) == item[i]);
  }

  std::shared_ptr<arrow::Buffer> new_buffer;
  // Recreate queue writer and write other data
  ARROW_CHECK_OK(client3_.CreateQueue(object_id, 0, &new_buffer, 0, false, true));
  for (int i = 6;i<item_size;i++) {
    ARROW_CHECK_OK(client3_.PushQueueItem(object_id, &item[i], sizeof(uint8_t)));
  }

  // Reader try to read the data
  for (int i = 6;i<item_size;i++) {
    ARROW_CHECK_OK(client2_.GetQueueItem(object_id, buff, buff_size, seq_id, timestamp));
    ASSERT_TRUE((*buff) == item[i]);
  }
  
  // Read the data to ensure the queue is reconstruct correctly
  std::unique_ptr<PlasmaQueueReader> queue_reader(
    new PlasmaQueueReader(new_buffer, sizeof(QueueHeader), 1));

  uint64_t data_offset;
  uint32_t data_size;
  uint8_t * data = nullptr;
  for(uint64_t i = 0; i<item_size; ++i) {
    ARROW_CHECK_OK(queue_reader->GetNext(&data, &data_size, &data_offset, &seq_id, &timestamp));
    ASSERT_TRUE((*data) == (item[i]));
  }
}
*/
TEST_F(TestPlasmaStore, TimestampTest) {
  ObjectID object_id = random_object_id();
  std::vector<ObjectBuffer> object_buffers;

  // Test for object non-existence on the first client.
  bool has_object;
  ARROW_CHECK_OK(client_.Contains(object_id, &has_object));
  ASSERT_FALSE(has_object);

  // Test for the object being in local Plasma store.
  // Create the writer
  int64_t queue_size = 100 * 1024;
  std::shared_ptr<Buffer> queue_item_buf;
  ARROW_CHECK_OK(client_.CreateQueue(object_id, queue_size, &queue_item_buf));

  // Push queue before subscribe queue
  uint8_t item[] = { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
  int64_t item_size = sizeof(item);
  for (int i = 0;i<item_size;i++) {
    ARROW_CHECK_OK(client_.PushQueueItem(object_id, &item[i], sizeof(uint8_t), item[i]/3*100000));
  }

  // Test that the reader client2_ can get the object.
  int notify_fd;
  ARROW_CHECK_OK(client2_.GetQueue(object_id, -1, &notify_fd));
  ARROW_CHECK_OK(client2_.Contains(object_id, &has_object));
  ASSERT_TRUE(has_object);
  uint8_t* buff = nullptr;
  uint32_t buff_size = 0;
  uint64_t seq_id = -1;
  uint64_t timestamp;
  for (int i = 0;i<item_size;i++) {
    ARROW_CHECK_OK(client2_.GetQueueItem(object_id, buff, buff_size, seq_id, timestamp));
    ASSERT_TRUE((*buff) == item[i]);
    ASSERT_TRUE(item[i]/3*100000 == timestamp);
  }
}

TEST_F(TestPlasmaStore, EvictQueueItemTest) {
  // one writer and two reader
  ObjectID object_id = random_object_id();
  std::vector<ObjectBuffer> object_buffers;

  auto queue_header_size = sizeof(QueueHeader);
  auto block_header_size = sizeof(QueueBlockHeader);

  auto time_stamp_size = sizeof(uint64_t);
  // This is the size of a queue item
  auto data_size = sizeof(uint64_t) + time_stamp_size;
  auto fragmented_data_size = data_size >> 1;
  int data_num = 1024;
  // Test for object non-existence on the first client.
  bool has_object;
  ARROW_CHECK_OK(client_.Contains(object_id, &has_object));
  ASSERT_FALSE(has_object);

  // Test for the object being in local Plasma store.
  // Create the writer
  // Calculate the queue size for evicting data, each item includes a timestamp and the actual data
  int64_t queue_size = queue_header_size + block_header_size * 2 + data_size * data_num + fragmented_data_size;
  std::shared_ptr<Buffer> queue_item_buf;
  ARROW_CHECK_OK(client_.CreateQueue(object_id, queue_size, &queue_item_buf));
  QueueHeader* queue_header = reinterpret_cast<QueueHeader*>(queue_item_buf->mutable_data());
  
  ASSERT_TRUE(queue_item_buf->size() == queue_size);

  int notify_fd;
  ARROW_CHECK_OK(client2_.GetQueue(object_id, -1, &notify_fd));
  ARROW_CHECK_OK(client2_.Contains(object_id, &has_object));
  ASSERT_TRUE(has_object);

  ARROW_CHECK_OK(client3_.GetQueue(object_id, -1, &notify_fd));
  ARROW_CHECK_OK(client3_.Contains(object_id, &has_object));
  ASSERT_TRUE(has_object);
  usleep(10*1000);
  // push 1024 data, block 1:1000 items, block 2:24 items
  uint64_t *data = new uint64_t();
  for (uint64_t i = 1;i<=data_num;i++) {
    *data = i;
    ARROW_CHECK_OK(client_.PushQueueItem(object_id, (uint8_t*)data, sizeof(uint64_t)));
  }
  delete data;

  ASSERT_TRUE(queue_header->first_block_offset == queue_header_size);

  // check 1024 data is ok
  uint64_t start_block_offset = sizeof(QueueHeader);
  std::unique_ptr<PlasmaQueueReader> queue_reader(
    new PlasmaQueueReader(queue_item_buf, start_block_offset,1));
  
  uint64_t seq_id, data_offset, timestamp;
  uint32_t dsize;
  data = nullptr;
  for(uint64_t i = 1; i<= data_num; ++i) {
    ARROW_CHECK_OK(queue_reader->GetNext(reinterpret_cast<uint8_t**>(&data), &dsize, &data_offset, &seq_id, &timestamp));
    ASSERT_TRUE((*data) == (uint64_t)(i));
    // std::cout<<"data: "<<(*data)<<", timestamp:" << timestamp<<std::endl;
  }

  // check data position in the memory
  QueueBlockHeader* bh1 = reinterpret_cast<QueueBlockHeader*>(queue_item_buf->mutable_data() + queue_header_size);
  QueueBlockHeader* bh2 = reinterpret_cast<QueueBlockHeader*>(queue_item_buf->mutable_data() + queue_header_size +
    + block_header_size + QUEUE_MAX_BLOCK_SIZE * data_size);
  int index = data_num - QUEUE_MAX_BLOCK_SIZE;
  ASSERT_TRUE(bh1->start_seq_id == 1);
  ASSERT_TRUE(bh1->end_seq_id == QUEUE_MAX_BLOCK_SIZE);
  ASSERT_TRUE(bh2->start_seq_id == (QUEUE_MAX_BLOCK_SIZE + 1));
  ASSERT_TRUE(bh2->end_seq_id == data_num);
  ASSERT_TRUE(bh1->next_block_offset == queue_header_size + block_header_size + data_size * QUEUE_MAX_BLOCK_SIZE);
  ASSERT_TRUE(bh2->next_block_offset == (uint64_t)-1);
  ASSERT_TRUE(bh2->item_offsets[index] == (block_header_size + data_size * index));

  // reader read the data and check the information record in the plasma store
  ObjectBuffer ob;
  uint64_t seq_id2, seq_id3;
  for(uint64_t i = 1; i<= QUEUE_MAX_BLOCK_SIZE;i++) {
    ARROW_CHECK_OK(client2_.GetQueueItem(object_id, &ob, seq_id2, timestamp));
  }
  // insert a long data and try to evict data, we can not evict data yet
  bool is_first_item = false;
  uint8_t long_str[] = "1234567890";

  std::shared_ptr<Buffer> buffer;
  // The buffer returned by create queue item will be empty
  Status s = client_.CreateQueueItem(object_id, sizeof(long_str), &buffer, seq_id, is_first_item);
  ASSERT_TRUE(s.IsOutOfMemory());
  s = client_.SealQueueItem(object_id, seq_id, buffer, is_first_item);
  ASSERT_TRUE(s.IsInvalid());

  // The append should fail because the reader has not consume the item
  ASSERT_TRUE(bh1->start_seq_id == 1);
  ASSERT_TRUE(bh1->end_seq_id == QUEUE_MAX_BLOCK_SIZE);
  // create queue item failed, the seq id should be the evict seq id
  ASSERT_TRUE(seq_id == QUEUE_MAX_BLOCK_SIZE);
  ASSERT_TRUE(is_first_item == false);
  // reader client2 consume the 1001 item
  ARROW_CHECK_OK(client2_.GetQueueItem(object_id, &ob, seq_id2, timestamp));
  // try to evict the first block, this should fail because client3 do not read the item
  s = client_.CreateQueueItem(object_id, sizeof(long_str), &buffer, seq_id, is_first_item);
  ASSERT_TRUE(s.IsOutOfMemory());
  s = client_.SealQueueItem(object_id, seq_id, buffer, is_first_item);
  ASSERT_TRUE(s.IsInvalid());

  ASSERT_TRUE(bh1->start_seq_id == 1);
  ASSERT_TRUE(bh1->end_seq_id == QUEUE_MAX_BLOCK_SIZE);
  ASSERT_TRUE(seq_id == QUEUE_MAX_BLOCK_SIZE);

  for(uint64_t i = 1; i<=(QUEUE_MAX_BLOCK_SIZE + 1);i++) {
    ARROW_CHECK_OK(client3_.GetQueueItem(object_id, &ob, seq_id3, timestamp));
  }
  // make sure the reader have consumed the item
  usleep(100*1000);
  // Evict block, now client2 and client3 all consumed the queue items, this will evict the block
  ARROW_CHECK_OK(client_.CreateQueueItem(object_id, sizeof(long_str), &buffer, seq_id, is_first_item));
  ARROW_CHECK_OK(client_.SealQueueItem(object_id, seq_id, buffer, is_first_item));

  ASSERT_TRUE(seq_id == (data_num + 1));
  ASSERT_TRUE(bh1->start_seq_id == (data_num+1));
  ASSERT_TRUE(bh1->end_seq_id == (data_num+1));
  ASSERT_TRUE(bh2->start_seq_id == (QUEUE_MAX_BLOCK_SIZE+1) && bh2->end_seq_id == data_num);
  // after evict data, first block become bh2
  ASSERT_TRUE((uint64_t)bh2 == (uint64_t)(queue_item_buf->mutable_data() + queue_header->first_block_offset));
  // try to disconncet reader client 3
  ARROW_CHECK_OK(client3_.Disconnect());

  // write two new block, make last block is close to the first block, insert 1020 item
  uint32_t buf_size = 3;
  for(uint32_t i = 1; i<= (QUEUE_MAX_BLOCK_SIZE + 20) ;i++) {
    ARROW_CHECK_OK(client_.CreateQueueItem(object_id, buf_size, &buffer, seq_id, is_first_item));
    ARROW_CHECK_OK(client_.SealQueueItem(object_id, seq_id, buffer, is_first_item));
  }
  ASSERT_TRUE(bh1->start_seq_id == 1025 && bh1->end_seq_id == 2024);

  // consume and evict data
  for(uint32_t i = 1;i<1018;i++) {
    ARROW_CHECK_OK(client2_.GetQueueItem(object_id, &ob, seq_id2, timestamp));
  }

  // try to evict queue block
  Status status = client_.CreateQueueItem(object_id, 8000, &buffer, seq_id, is_first_item);
  ASSERT_TRUE(status.IsOutOfMemory());
  ARROW_CHECK_OK(client_.SealQueueItem(object_id, seq_id, buffer, is_first_item));

}

TEST_F(TestPlasmaStore, QueuePushAndGetTest) {
  uint64_t timestamp;
  ObjectID object_id = random_object_id();
  std::vector<ObjectBuffer> object_buffers;

  // Test for object non-existence on the first client.
  bool has_object;
  ARROW_CHECK_OK(client_.Contains(object_id, &has_object));
  ASSERT_FALSE(has_object);

  // Test for the object being in local Plasma store.
  // First create and seal object on the second client.
  int64_t queue_size = 100 * 1024;
  std::shared_ptr<Buffer> data;
  ARROW_CHECK_OK(client2_.CreateQueue(object_id, queue_size, &data));
  // ARROW_CHECK_OK(client2_.Seal(object_id));
  // Test that the first client can get the object.
  int notify_fd;
  ARROW_CHECK_OK(client_.GetQueue(object_id, -1, &notify_fd));
  ARROW_CHECK_OK(client_.Contains(object_id, &has_object));
  ASSERT_TRUE(has_object);

  uint8_t item1[] = { 1, 2, 3, 4, 5 };
  int64_t item1_size = sizeof(item1);
  ARROW_CHECK_OK(client2_.PushQueueItem(object_id, item1, item1_size));

  uint8_t item2[] = { 6, 7, 8, 9, 10 };
  int64_t item2_size = sizeof(item2);
  ARROW_CHECK_OK(client2_.PushQueueItem(object_id, item2, item2_size));

  uint8_t* buff = nullptr;
  uint32_t buff_size = 0;
  uint64_t seq_id = -1;

  ARROW_CHECK_OK(client_.GetQueueItem(object_id, buff, buff_size, seq_id, timestamp));
  ASSERT_TRUE(seq_id == 1);
  ASSERT_TRUE(buff_size == item1_size);
  for (auto i = 0; i < buff_size; i++) {
    ASSERT_TRUE(buff[i] == item1[i]);
  }
   
  ARROW_CHECK_OK(client_.GetQueueItem(object_id, buff, buff_size, seq_id, timestamp));
  ASSERT_TRUE(seq_id == 2);
  ASSERT_TRUE(buff_size == item2_size);
  for (auto i = 0; i < buff_size; i++) {
    ASSERT_TRUE(buff[i] == item2[i]);
  }
}

TEST_F(TestPlasmaStore, QueueCreateAndGetTest) {
  uint64_t timestamp;
  ObjectID object_id = random_object_id();
  ObjectBuffer object_buffer;

  // Test for object non-existence on the first client.
  bool has_object;
  ARROW_CHECK_OK(client_.Contains(object_id, &has_object));
  ASSERT_FALSE(has_object);

  // Test for the object being in local Plasma store.
  // First create and seal object on the second client.
  int64_t queue_size = 10 * 1024;
  std::shared_ptr<Buffer> data;
  ARROW_CHECK_OK(client2_.CreateQueue(object_id, queue_size, &data));
  // ARROW_CHECK_OK(client2_.Seal(object_id));
  // Test that the first client can get the object.
  int notify_fd;
  ARROW_CHECK_OK(client_.GetQueue(object_id, -1, &notify_fd));
  ARROW_CHECK_OK(client_.Contains(object_id, &has_object));
  ASSERT_TRUE(has_object);

  uint64_t seq_id = -1;

  uint8_t item1[] = { 1, 2, 3, 4, 5 };
  int64_t item1_size = sizeof(item1);

  ARROW_CHECK_OK(client2_.CreateQueueItem(object_id, item1_size, &data, seq_id));
  memcpy(data->mutable_data(), item1, item1_size);
  ARROW_CHECK_OK(client2_.SealQueueItem(object_id, seq_id, data));
  

  uint8_t item2[] = { 6, 7, 8, 9 };
  int64_t item2_size = sizeof(item2);
  ARROW_CHECK_OK(client2_.CreateQueueItem(object_id, item2_size, &data, seq_id));
  memcpy(data->mutable_data(), item2, item2_size);
  ARROW_CHECK_OK(client2_.SealQueueItem(object_id, seq_id, data));

  uint8_t* buff = nullptr;
  uint32_t buff_size = 0;

  ARROW_CHECK_OK(client_.GetQueueItem(object_id, &object_buffer, seq_id, timestamp));
  ASSERT_TRUE(seq_id == 1);
  ASSERT_TRUE(object_buffer.data->size() == item1_size);
  for (auto i = 0; i < item1_size; i++) {
    ASSERT_TRUE(object_buffer.data->data()[i] == item1[i]);
  }
  
  std::shared_ptr<Buffer> buffer;
  ARROW_CHECK_OK(client_.GetQueueItem(object_id, &buffer, seq_id, timestamp));
  ASSERT_TRUE(seq_id == 2);
  ASSERT_TRUE(buffer->size() == item2_size);
  for (auto i = 0; i < item2_size; i++) {
    ASSERT_TRUE(buffer->data()[i] == item2[i]);
  }
}

TEST_F(TestPlasmaStore, BatchQueueItemPerfTest) {

  int64_t queue_size = 100 * 1024 * 1024;

  std::vector<uint64_t> items;
  items.resize(100 * 1000);
  for (int i = 0; i < items.size(); i++) {
    items[i] = i;
  }

  std::vector<ObjectID> object_ids;
  object_ids.resize(100 * 1000);
  for (int i = 0; i < object_ids.size(); i++) {
    object_ids[i] = random_object_id();
  }  
  std::vector<ObjectBuffer> object_buffers;
  using namespace std::chrono;
  duration<double> push_time, get_time;

  //
  // Plasma queue performance test.
  //
  ARROW_LOG(INFO) << "[start] Plasma queue performance test";

  ObjectID object_id = random_object_id();

  uint64_t timestamp;
  // Test for object non-existence on the first client.
  bool has_object;
  ARROW_CHECK_OK(client_.Contains(object_id, &has_object));
  ASSERT_FALSE(has_object);

  // Test for the object being in local Plasma store.
  // First create and seal object on the second client.

  std::shared_ptr<Buffer> data;
  ARROW_CHECK_OK(client2_.CreateQueue(object_id, queue_size, &data));
  // ARROW_CHECK_OK(client2_.Seal(object_id));
  // Test that the first client can get the object.
  int notify_fd;
  ARROW_CHECK_OK(client_.GetQueue(object_id, -1, &notify_fd));
  ARROW_CHECK_OK(client_.Contains(object_id, &has_object));
  ASSERT_TRUE(has_object);

  
  auto curr_time = std::chrono::system_clock::now();
  ARROW_LOG(INFO) << "PushQueueItem started ";
  for (int i = 0; i < items.size(); i++) {
    uint8_t* data = reinterpret_cast<uint8_t*>(&items[i]);
    uint32_t data_size = static_cast<uint32_t>(sizeof(uint64_t));
    ARROW_CHECK_OK(client2_.PushQueueItem(object_id, data, data_size));
  }

  push_time =
      std::chrono::system_clock::now() - curr_time;
  ARROW_LOG(INFO) << "PushQueueItem takes " << push_time.count()
                << " seconds";

  curr_time = std::chrono::system_clock::now();
  ARROW_LOG(INFO) << "GetQueueItem started ";
  for (int i = 0; i < items.size(); i++) {
    uint8_t* buff = nullptr;
    uint32_t buff_size = 0;
    uint64_t seq_id = -1;

    ARROW_CHECK_OK(client_.GetQueueItem(object_id, buff, buff_size, seq_id, timestamp));
    ASSERT_TRUE(seq_id == i + 1);
    ASSERT_TRUE(buff_size == sizeof(uint64_t));
    uint64_t value = *(uint64_t*)(buff);
    ASSERT_TRUE(value == items[i]);
  }
  get_time =
      std::chrono::system_clock::now() - curr_time;
  ARROW_LOG(INFO) << "GetQueueItem takes " << get_time.count()
                << " seconds";
  ARROW_LOG(INFO) << "[done] Plasma queue performance test";

}

TEST_F(TestPlasmaStore, BatchQueueItemPerfTest2) {

  int64_t queue_size = 100 * 1024 * 1024;

  std::vector<uint64_t> items;
  items.resize(100 * 1000);
  for (int i = 0; i < items.size(); i++) {
    items[i] = i;
  }

  std::vector<ObjectID> object_ids;
  object_ids.resize(100 * 1000);
  for (int i = 0; i < object_ids.size(); i++) {
    object_ids[i] = random_object_id();
  }  
  std::vector<ObjectBuffer> object_buffers;
  using namespace std::chrono;
  duration<double> push_time, get_time;

  //
  // Plasma queue performance test.
  //
  ARROW_LOG(INFO) << "[start] Plasma queue performance test 2";

  ObjectID object_id = random_object_id();

  // Test for object non-existence on the first client.
  bool has_object;
  ARROW_CHECK_OK(client_.Contains(object_id, &has_object));
  ASSERT_FALSE(has_object);

  // Test for the object being in local Plasma store.
  // First create and seal object on the second client.

  std::shared_ptr<Buffer> buffer;
  ARROW_CHECK_OK(client2_.CreateQueue2(object_id, queue_size, &buffer));

  auto curr_time = std::chrono::system_clock::now();
  ARROW_LOG(INFO) << "PushQueueItem started ";
  for (int i = 0; i < items.size(); i++) {
    uint8_t* data = reinterpret_cast<uint8_t*>(&items[i]);
    uint32_t data_size = static_cast<uint32_t>(sizeof(uint64_t));
    ARROW_CHECK_OK(client2_.CreateQueueItem2(object_id, i, data_size, 0, &buffer));
    memcpy(buffer->mutable_data(), data, data_size);
    ARROW_CHECK_OK(client2_.SealQueueItem2(object_id, i));
  }

  push_time =
      std::chrono::system_clock::now() - curr_time;
  ARROW_LOG(INFO) << "PushQueueItem takes " << push_time.count()
                << " seconds";
}


TEST_F(TestPlasmaStore, BatchCreateObjectPerfTest) {


  using namespace std::chrono;
  int64_t queue_size = 100 * 1024 * 1024;

  std::vector<uint64_t> items;
  items.resize(100 * 1000);
  for (int i = 0; i < items.size(); i++) {
    items[i] = i;
  }
  auto begin_time = std::chrono::system_clock::now();
  ARROW_LOG(INFO) << "Generate object ids started ";

  std::vector<ObjectID> object_ids;
  object_ids.resize(100 * 1000);
  for (int i = 0; i < object_ids.size(); i++) {
    object_ids[i] = random_object_id();
  } 

  duration<double> id_time =
      std::chrono::system_clock::now() - begin_time;
  ARROW_LOG(INFO) << "Generate object ids takes " << id_time.count()
                << " seconds";


  std::vector<ObjectBuffer> object_buffers;
  duration<double> push_time, get_time;

  //
  // Plasma object performance test.
  //
  ARROW_LOG(INFO) << "[start] Plasma object performance test";

  auto curr_time = std::chrono::system_clock::now();
  ARROW_LOG(INFO) << "Push objects started ";

  std::vector<ObjectID> object_ids_1k(1000);
  std::vector<uint64_t> data_sizes_1k(1000);
  
  std::vector<std::shared_ptr<Buffer>> data_buffers;
  data_buffers.reserve(1000);
  for (int i = 0; i < items.size(); i+= 1000) {
    data_buffers.clear();

    for (int j = 0; j < 1000; j++) {
      object_ids_1k[j] = object_ids[i + j];
      data_sizes_1k[j] = sizeof(uint64_t);
    }

    ARROW_CHECK_OK(client2_.CreateBatch(object_ids_1k, data_sizes_1k, data_buffers, false));

    for (int j = 0; j < 1000; j++) {
      //ARROW_LOG(INFO) << " write [" << j << "] pointer = " << (uint64_t)(data_buffers[j]->mutable_data())
      //                << " item = " << items[i + j] << " size = " << data_sizes_1k[j];
      memcpy(data_buffers[j]->mutable_data(), &items[i + j], data_sizes_1k[j]);
      // ARROW_CHECK_OK(client2_.Seal(object_ids[i + j]));
    }    
  }

  push_time =
      std::chrono::system_clock::now() - curr_time;
  ARROW_LOG(INFO) << "Push object takes " << push_time.count()
                << " seconds";

  for (int i = 0; i < items.size(); i++) {
    ARROW_CHECK_OK(client2_.Seal(object_ids[i]));
  }

  curr_time = std::chrono::system_clock::now();
  ARROW_LOG(INFO) << "Get object started ";
  for (int i = 0; i < items.size(); i+= 1000) {
    object_buffers.clear();

    for (int j = 0; j < 1000; j++) {
      object_ids_1k[j] = object_ids[i + j];
      data_sizes_1k[j] = sizeof(uint64_t);
    }

    ARROW_CHECK_OK(client_.Get(object_ids_1k, -1, &object_buffers));
    ASSERT_TRUE(object_buffers.size() == 1000);

    for (int j = 0; j < 1000; j++) {
      //ARROW_LOG(INFO) << " write [" << j << "] pointer = " << (uint64_t)(data_buffers[j]->mutable_data())
      //                << " item = " << items[i + j] << " size = " << data_sizes_1k[j];
      
      ASSERT_TRUE(object_buffers[j].data->size() == sizeof(uint64_t));
      ASSERT_TRUE(memcmp(object_buffers[j].data->data(), &items[i + j], sizeof(uint64_t)) == 0);
    }
  }
  get_time =
    std::chrono::system_clock::now() - curr_time;
  ARROW_LOG(INFO) << "Get object takes " << get_time.count()
                << " seconds";
  ARROW_LOG(INFO) << "[done] Plasma object performance test";

  for (int i = 0; i < items.size(); i++) {
    ARROW_CHECK_OK(client2_.Release(object_ids[i]));
  }
}

TEST_F(TestPlasmaStore, BatchObjectPerfTest) {

  int64_t queue_size = 100 * 1024 * 1024;

  std::vector<uint64_t> items;
  items.resize(100 * 1000);
  for (int i = 0; i < items.size(); i++) {
    items[i] = i;
  }

  std::vector<ObjectID> object_ids;
  object_ids.resize(100 * 1000);
  for (int i = 0; i < object_ids.size(); i++) {
    object_ids[i] = random_object_id();
  }  
  std::vector<ObjectBuffer> object_buffers;
  using namespace std::chrono;
  duration<double> push_time, get_time;

  //
  // Plasma object performance test.
  //
  ARROW_LOG(INFO) << "[start] Plasma object performance test";

  auto curr_time = std::chrono::system_clock::now();
  ARROW_LOG(INFO) << "Push objects started ";

  std::shared_ptr<Buffer> data_buffer;
  for (int i = 0; i < items.size(); i++) {
    uint8_t* data = reinterpret_cast<uint8_t*>(&items[i]);
    uint32_t data_size = static_cast<uint32_t>(sizeof(uint64_t));
    ARROW_CHECK_OK(client2_.Create(object_ids[i], data_size, nullptr, 0, &data_buffer));
    memcpy(data_buffer->mutable_data(), data, data_size);
    // ARROW_CHECK_OK(client2_.Seal(object_ids[i]));
  }

  push_time =
      std::chrono::system_clock::now() - curr_time;
  ARROW_LOG(INFO) << "Push object takes " << push_time.count()
                << " seconds";

  for (int i = 0; i < items.size(); i++) {
    ARROW_CHECK_OK(client2_.Seal(object_ids[i]));
  }

  curr_time = std::chrono::system_clock::now();
  ARROW_LOG(INFO) << "Get object started ";
  for (int i = 0; i < items.size(); i++) {
    uint8_t* data = reinterpret_cast<uint8_t*>(&items[i]);
    uint32_t data_size = static_cast<uint32_t>(sizeof(uint64_t));
    ARROW_CHECK_OK(client_.Get({object_ids[i]}, -1, &object_buffers));
    ASSERT_TRUE(object_buffers.size() == 1);
    ASSERT_TRUE(object_buffers[0].data->size() == data_size);
    ASSERT_TRUE(memcmp(object_buffers[0].data->data(), data, data_size) == 0);
  }
  get_time =
    std::chrono::system_clock::now() - curr_time;
  ARROW_LOG(INFO) << "Get object takes " << get_time.count()
                << " seconds";
  ARROW_LOG(INFO) << "[done] Plasma object performance test";

  for (int i = 0; i < items.size(); i++) {
    ARROW_CHECK_OK(client2_.Release(object_ids[i]));
  }
}

TEST_F(TestPlasmaStore, QueueBatchCreateAndGetTest) {

  ObjectID object_id = random_object_id();
  std::vector<ObjectBuffer> object_buffers;
  uint64_t timestamp;
  // Test for object non-existence on the first client.
  bool has_object;
  ARROW_CHECK_OK(client_.Contains(object_id, &has_object));
  ASSERT_FALSE(has_object);

  // Test for the object being in local Plasma store.
  // First create and seal object on the second client.
  int64_t queue_size = 1024 * 1024;
  std::shared_ptr<Buffer> data;
  ARROW_CHECK_OK(client2_.CreateQueue(object_id, queue_size, &data));
  // ARROW_CHECK_OK(client2_.Seal(object_id));
  // Test that the first client can get the object.
  int notify_fd;
  ARROW_CHECK_OK(client_.GetQueue(object_id, -1, &notify_fd));
  ARROW_CHECK_OK(client_.Contains(object_id, &has_object));
  ASSERT_TRUE(has_object);

  std::vector<uint64_t> items;
  items.resize(3000);
  for (int i = 0; i < items.size(); i++) {
    items[i] = i;
  }

  for (int i = 0; i < items.size(); i++) {
    uint64_t seq_id = -1;
    uint8_t* item = reinterpret_cast<uint8_t*>(&items[i]);
    uint32_t item_size = static_cast<uint32_t>(sizeof(uint64_t));
    
    ARROW_CHECK_OK(client2_.CreateQueueItem(object_id, item_size, &data, seq_id));
    memcpy(data->mutable_data(), item, item_size);
    ARROW_CHECK_OK(client2_.SealQueueItem(object_id, seq_id, data));
  }

  for (int i = 0; i < items.size(); i++) {
    uint8_t* buff = nullptr;
    uint32_t buff_size = 0;
    uint64_t seq_id = -1;

    ARROW_CHECK_OK(client_.GetQueueItem(object_id, buff, buff_size, seq_id, timestamp));
    ASSERT_TRUE(seq_id == i + 1);
    ASSERT_TRUE(buff_size == sizeof(uint64_t));
    uint64_t value = *(uint64_t*)(buff);
    ASSERT_TRUE(value == items[i]);
  }
}

TEST_F(TestPlasmaStore, QueueSubscriberTest) {
  PlasmaClient local_client1, local_client2, local_client3;

  ARROW_CHECK_OK(local_client1.Connect(store_socket_name_, ""));
  ARROW_CHECK_OK(local_client2.Connect(store_socket_name_, ""));
  ARROW_CHECK_OK(local_client3.Connect(store_socket_name_, ""));

  // Client1 creates queue object.
  int64_t queue_size = 1024 * 1024;
  std::shared_ptr<Buffer> data;
  ObjectID object_id = random_object_id();
  ARROW_CHECK_OK(local_client1.CreateQueue(object_id, queue_size, &data));

  // Client2 subscribes updates for all queues.
  /*
  int fd2 = -1;
  ARROW_CHECK_OK(local_client2.SubscribeQueue(&fd2));
  ASSERT_GT(fd2, 0);
  */

  // Client3 subscribes updates for this queue object.
  int fd3 = -1;
  ARROW_CHECK_OK(local_client3.SubscribeQueue(object_id, &fd3));
  ASSERT_GT(fd3, 0);

  // Client1 pushes queue items.
  std::vector<uint64_t> items;
  items.resize(100);
  for (int i = 0; i < items.size(); i++) {
    items[i] = i;
  }

  for (int i = 0; i < items.size(); i++) {
    uint8_t* data = reinterpret_cast<uint8_t*>(&items[i]);
    uint32_t data_size = static_cast<uint32_t>(sizeof(uint64_t));
    ARROW_CHECK_OK(local_client1.PushQueueItem(object_id, data, data_size));
  }

  // Verify client2 receives all notifications.
  /*
  for (int i = 0; i < items.size(); i++) {
    uint64_t seq_id = 0;
    uint64_t data_offset = 0;
    uint32_t data_size = 0;
    ARROW_CHECK_OK(local_client2.GetQueueNotification(fd2, &seq_id, &data_offset, &data_size));
    ASSERT_TRUE(seq_id == i + 1);
    ASSERT_TRUE(data_size == sizeof(uint64_t));
  }

  // Verify client2 receives all notifications.
  for (int i = 0; i < items.size(); i++) {
    uint64_t seq_id = 0;
    uint64_t data_offset = 0;
    uint32_t data_size = 0;
    ARROW_CHECK_OK(local_client3.GetQueueNotification(fd3, &seq_id, &data_offset, &data_size));
    ASSERT_TRUE(seq_id == i + 1);
    ASSERT_TRUE(data_size == sizeof(uint64_t));
  }
  */
  ARROW_CHECK_OK(local_client1.Release(object_id));

  ARROW_CHECK_OK(local_client3.Disconnect());
  ARROW_CHECK_OK(local_client2.Disconnect());
  ARROW_CHECK_OK(local_client1.Disconnect());
}

TEST_F(TestPlasmaStore, ManyObjectTest) {
  // Create many objects on the first client. Seal one third, abort one third,
  // and leave the last third unsealed.
  std::vector<ObjectID> object_ids;
  for (int i = 0; i < 100; i++) {
    ObjectID object_id = random_object_id();
    object_ids.push_back(object_id);

    // Test for object non-existence on the first client.
    bool has_object;
    ARROW_CHECK_OK(client_.Contains(object_id, &has_object));
    ASSERT_FALSE(has_object);

    // Test for the object being in local Plasma store.
    // First create and seal object on the first client.
    int64_t data_size = 100;
    uint8_t metadata[] = {5};
    int64_t metadata_size = sizeof(metadata);
    std::shared_ptr<Buffer> data;
    ARROW_CHECK_OK(client_.Create(object_id, data_size, metadata, metadata_size, &data));

    if (i % 3 == 0) {
      // Seal one third of the objects.
      ARROW_CHECK_OK(client_.Seal(object_id));
      // Test that the first client can get the object.
      ARROW_CHECK_OK(client_.Contains(object_id, &has_object));
      ASSERT_TRUE(has_object);
    } else if (i % 3 == 1) {
      // Abort one third of the objects.
      ARROW_CHECK_OK(client_.Release(object_id));
      ARROW_CHECK_OK(client_.Abort(object_id));
    }
  }
  // Disconnect the first client. All unsealed objects should be aborted.
  ARROW_CHECK_OK(client_.Disconnect());

  // Check that the second client can query the object store for the first
  // client's objects.
  int i = 0;
  for (auto const& object_id : object_ids) {
    bool has_object;
    ARROW_CHECK_OK(client2_.Contains(object_id, &has_object));
    if (i % 3 == 0) {
      // The first third should be sealed.
      ASSERT_TRUE(has_object);
    } else {
      // The rest were aborted, so the object is not in the store.
      ASSERT_FALSE(has_object);
    }
    i++;
  }
}

#ifndef ARROW_NO_DEPRECATED_API
TEST_F(TestPlasmaStore, DeprecatedApiTest) {
  int64_t default_delay = PLASMA_DEFAULT_RELEASE_DELAY;
  ARROW_CHECK(default_delay == plasma::kPlasmaDefaultReleaseDelay);
}
#endif  // ARROW_NO_DEPRECATED_API

#ifdef PLASMA_GPU
using arrow::gpu::CudaBuffer;
using arrow::gpu::CudaBufferReader;
using arrow::gpu::CudaBufferWriter;

namespace {

void AssertCudaRead(const std::shared_ptr<Buffer>& buffer,
                    const std::vector<uint8_t>& expected_data) {
  std::shared_ptr<CudaBuffer> gpu_buffer;
  const size_t data_size = expected_data.size();

  ARROW_CHECK_OK(CudaBuffer::FromBuffer(buffer, &gpu_buffer));
  ASSERT_EQ(gpu_buffer->size(), data_size);

  CudaBufferReader reader(gpu_buffer);
  std::vector<uint8_t> read_data(data_size);
  int64_t read_data_size;
  ARROW_CHECK_OK(reader.Read(data_size, &read_data_size, read_data.data()));
  ASSERT_EQ(read_data_size, data_size);

  for (size_t i = 0; i < data_size; i++) {
    ASSERT_EQ(read_data[i], expected_data[i]);
  }
}

}  // namespace

TEST_F(TestPlasmaStore, GetGPUTest) {
  ObjectID object_id = random_object_id();
  std::vector<ObjectBuffer> object_buffers;

  // Test for object non-existence.
  ARROW_CHECK_OK(client_.Get({object_id}, 0, &object_buffers));
  ASSERT_EQ(object_buffers.size(), 1);
  ASSERT_FALSE(object_buffers[0].data);

  // Test for the object being in local Plasma store.
  // First create object.
  uint8_t data[] = {4, 5, 3, 1};
  int64_t data_size = sizeof(data);
  uint8_t metadata[] = {42};
  int64_t metadata_size = sizeof(metadata);
  std::shared_ptr<Buffer> data_buffer;
  std::shared_ptr<CudaBuffer> gpu_buffer;
  ARROW_CHECK_OK(
      client_.Create(object_id, data_size, metadata, metadata_size, &data_buffer, 1));
  ARROW_CHECK_OK(CudaBuffer::FromBuffer(data_buffer, &gpu_buffer));
  CudaBufferWriter writer(gpu_buffer);
  ARROW_CHECK_OK(writer.Write(data, data_size));
  ARROW_CHECK_OK(client_.Seal(object_id));

  object_buffers.clear();
  ARROW_CHECK_OK(client_.Get({object_id}, -1, &object_buffers));
  ASSERT_EQ(object_buffers.size(), 1);
  ASSERT_EQ(object_buffers[0].device_num, 1);
  // Check data
  AssertCudaRead(object_buffers[0].data, {4, 5, 3, 1});
  // Check metadata
  AssertCudaRead(object_buffers[0].metadata, {42});
}

TEST_F(TestPlasmaStore, MultipleClientGPUTest) {
  ObjectID object_id = random_object_id();
  std::vector<ObjectBuffer> object_buffers;

  // Test for object non-existence on the first client.
  bool has_object;
  ARROW_CHECK_OK(client_.Contains(object_id, &has_object));
  ASSERT_FALSE(has_object);

  // Test for the object being in local Plasma store.
  // First create and seal object on the second client.
  int64_t data_size = 100;
  uint8_t metadata[] = {5};
  int64_t metadata_size = sizeof(metadata);
  std::shared_ptr<Buffer> data;
  ARROW_CHECK_OK(
      client2_.Create(object_id, data_size, metadata, metadata_size, &data, 1));
  ARROW_CHECK_OK(client2_.Seal(object_id));
  // Test that the first client can get the object.
  ARROW_CHECK_OK(client_.Get({object_id}, -1, &object_buffers));
  ARROW_CHECK_OK(client_.Contains(object_id, &has_object));
  ASSERT_TRUE(has_object);

  // Test that one client disconnecting does not interfere with the other.
  // First create object on the second client.
  object_id = random_object_id();
  ARROW_CHECK_OK(
      client2_.Create(object_id, data_size, metadata, metadata_size, &data, 1));
  // Disconnect the first client.
  ARROW_CHECK_OK(client_.Disconnect());
  // Test that the second client can seal and get the created object.
  ARROW_CHECK_OK(client2_.Seal(object_id));
  object_buffers.clear();
  ARROW_CHECK_OK(client2_.Contains(object_id, &has_object));
  ASSERT_TRUE(has_object);
  ARROW_CHECK_OK(client2_.Get({object_id}, -1, &object_buffers));
  ASSERT_EQ(object_buffers.size(), 1);
  ASSERT_EQ(object_buffers[0].device_num, 1);
  AssertCudaRead(object_buffers[0].metadata, {5});
}

#endif  // PLASMA_GPU

}  // namespace plasma

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  plasma::test_executable = std::string(argv[0]);
  return RUN_ALL_TESTS();
}
