//
// Copyright (C) 2026 The Android Open Source Project
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
#include "update_engine/payload_consumer/parallel_payload_applier.h"

#include <fcntl.h>
#include <unistd.h>

#include <string>
#include <vector>

#include <gtest/gtest.h>
#include <android-base/unique_fd.h>

#include "update_engine/common/download_action.h"
#include "update_engine/common/fake_boot_control.h"
#include "update_engine/common/fake_hardware.h"
#include "update_engine/common/fake_prefs.h"
#include "update_engine/common/hash_calculator.h"
#include "update_engine/common/test_utils.h"
#include "update_engine/common/utils.h"
#include "update_engine/payload_consumer/delta_performer.h"
#include "update_engine/payload_consumer/payload_constants.h"
#include "update_engine/payload_generator/annotated_operation.h"
#include "update_engine/payload_generator/blob_file_writer.h"
#include "update_engine/payload_generator/delta_diff_generator.h"
#include "update_engine/payload_generator/extent_ranges.h"
#include "update_engine/payload_generator/extent_utils.h"
#include "update_engine/payload_generator/payload_file.h"
#include "update_engine/payload_generator/payload_generation_config.h"

namespace chromeos_update_engine {

namespace {

constexpr size_t kPartBlocks = 4;
constexpr size_t kPartSize = kPartBlocks * kBlockSize;

class TestDelegate : public DownloadActionDelegate {
 public:
  void BytesReceived(uint64_t, uint64_t received, uint64_t total) override {
    bytes_received_calls++;
    last_received = received;
    last_total = total;
  }
  bool ShouldCancel(ErrorCode*) override { return false; }
  void DownloadComplete() override {}

  int bytes_received_calls{0};
  uint64_t last_received{0};
  uint64_t last_total{0};
};

brillo::Blob TestContent() {
  brillo::Blob content(kPartSize);
  test_utils::FillWithData(&content);
  return content;
}

// Writes a signed-less full payload containing one partition "root" whose
// single REPLACE operation carries |content|.
bool WriteTestPayload(const std::string& payload_path,
                      const brillo::Blob& content,
                      const brillo::Blob& stored_blob,
                      InstallOperation::Type op_type,
                      uint32_t minor_version,
                      uint64_t* metadata_size) {
  ScopedTempFile blob_file("parallel_applier_blob.XXXXXX", true);
  off64_t blob_size = 0;
  BlobFileWriter blob_writer(blob_file.fd(), &blob_size);

  PayloadGenerationConfig config;
  config.version.major = kBrilloMajorPayloadVersion;
  config.version.minor = minor_version;
  config.block_size = kBlockSize;

  PayloadFile payload;
  if (!payload.Init(config))
    return false;

  ScopedTempFile image_file("parallel_applier_image.XXXXXX", true);
  if (!utils::WriteFile(
          image_file.path().c_str(), content.data(), content.size()))
    return false;
  PartitionConfig old_part("root");
  PartitionConfig new_part("root");
  new_part.path = image_file.path();
  new_part.size = content.size();

  std::vector<AnnotatedOperation> aops(1);
  AnnotatedOperation& aop = aops[0];
  aop.name = "root-op";
  aop.op.set_type(op_type);
  *aop.op.add_dst_extents() = ExtentForRange(0, content.size() / kBlockSize);
  if (minor_version == kFullPayloadMinorVersion) {
    brillo::Blob hashed = stored_blob.empty() ? content : stored_blob;
    if (!aop.SetOperationBlob(hashed, &blob_writer))
      return false;
    brillo::Blob hash;
    if (!HashCalculator::RawHashOfData(hashed, &hash))
      return false;
    aop.op.set_data_sha256_hash(hash.data(), hash.size());
  }
  if (!payload.AddPartition(old_part, new_part, std::move(aops), {}, {}))
    return false;
  return payload.WritePayload(
      payload_path, blob_file.path(), "", metadata_size);
}

bool WriteMultiPartTestPayload(
    const std::string& payload_path,
    const std::vector<std::pair<std::string, brillo::Blob>>& partitions,
    uint32_t minor_version,
    uint64_t* metadata_size) {
  ScopedTempFile blob_file("parallel_applier_blob.XXXXXX", true);
  off64_t blob_size = 0;
  BlobFileWriter blob_writer(blob_file.fd(), &blob_size);

  PayloadGenerationConfig config;
  config.version.major = kBrilloMajorPayloadVersion;
  config.version.minor = minor_version;
  config.block_size = kBlockSize;

  PayloadFile payload;
  if (!payload.Init(config))
    return false;

  for (const auto& part : partitions) {
    ScopedTempFile image_file(
        "parallel_applier_image_" + part.first + ".XXXXXX", true);
    if (!utils::WriteFile(
            image_file.path().c_str(), part.second.data(), part.second.size()))
      return false;
    PartitionConfig old_part(part.first);
    PartitionConfig new_part(part.first);
    new_part.path = image_file.path();
    new_part.size = part.second.size();

    std::vector<AnnotatedOperation> aops(1);
    AnnotatedOperation& aop = aops[0];
    aop.name = part.first + "-op";
    aop.op.set_type(InstallOperation::REPLACE);
    size_t num_blocks = part.second.size() / kBlockSize;
    *aop.op.add_dst_extents() = ExtentForRange(0, num_blocks);

    if (minor_version == kFullPayloadMinorVersion) {
      if (!aop.SetOperationBlob(part.second, &blob_writer))
        return false;
      brillo::Blob hash;
      if (!HashCalculator::RawHashOfData(part.second, &hash))
        return false;
      aop.op.set_data_sha256_hash(hash.data(), hash.size());
    }
    if (!payload.AddPartition(old_part, new_part, std::move(aops), {}, {}))
      return false;
  }
  return payload.WritePayload(
      payload_path, blob_file.path(), "", metadata_size);
}

}  // namespace

class ParallelPayloadApplierTest : public ::testing::Test {
 protected:
  void SetUp() override {
    content_ = TestContent();
    install_plan_.source_slot = 0;
    install_plan_.target_slot = 1;
    // The test payloads are unsigned and no public key is installed on the
    // host, so metadata signature validation must not be mandatory for the
    // stock metadata stage. Operation hashes are still mandatory, through
    // Config::hash_checks_mandatory.
    install_plan_.hash_checks_mandatory = false;
    install_plan_.payloads.resize(1);
    fake_boot_control_.SetPartitionDevice("root", 1, target_file_.path());
    fake_boot_control_.SetPartitionDevice("root", 0, "/dev/null");
  }

  ParallelPayloadApplier::Config MakeConfig(const std::string& payload_path,
                                            uint64_t payload_size) {
    ParallelPayloadApplier::Config config;
    config.payload_fd = open(payload_path.c_str(), O_RDONLY | O_CLOEXEC);
    EXPECT_GE(config.payload_fd, 0);
    config.base_offset = 0;
    config.payload_size = payload_size;
    config.prefs = &prefs_;
    config.boot_control = &fake_boot_control_;
    config.hardware = &fake_hardware_;
    config.delegate = &delegate_;
    config.hash_checks_mandatory = true;
    config.interactive = true;
    install_plan_.payloads[0].size = payload_size;
    install_plan_.payloads[0].metadata_size = metadata_size_;
    config.payload = &install_plan_.payloads[0];
    return config;
  }

  brillo::Blob content_;
  ScopedTempFile payload_file_{"parallel_applier_payload.XXXXXX"};
  ScopedTempFile target_file_{"parallel_applier_target.XXXXXX"};
  FakePrefs prefs_;
  FakeBootControl fake_boot_control_;
  FakeHardware fake_hardware_;
  TestDelegate delegate_;
  InstallPlan install_plan_;
  uint64_t metadata_size_{0};
};

TEST_F(ParallelPayloadApplierTest, ProbeAcceptsFullPayload) {
  ASSERT_TRUE(WriteTestPayload(payload_file_.path(),
                               content_,
                               {},
                               InstallOperation::REPLACE_XZ,
                               kFullPayloadMinorVersion,
                               &metadata_size_));
  const uint64_t size = utils::FileSize(payload_file_.path());
  ParallelPayloadApplier::Config config =
      MakeConfig(payload_file_.path(), size);
  EXPECT_TRUE(
      ParallelPayloadApplier::ProbeEligible(config.payload_fd, &config));
  EXPECT_GT(config.data_begin, 0u);
  close(config.payload_fd);
}

TEST_F(ParallelPayloadApplierTest, ProbeRejectsDeltaPayload) {
  ASSERT_TRUE(WriteTestPayload(payload_file_.path(),
                               content_,
                               {},
                               InstallOperation::REPLACE_XZ,
                               kSourceMinorPayloadVersion,
                               &metadata_size_));
  ParallelPayloadApplier::Config config =
      MakeConfig(payload_file_.path(), utils::FileSize(payload_file_.path()));
  EXPECT_FALSE(
      ParallelPayloadApplier::ProbeEligible(config.payload_fd, &config));
  close(config.payload_fd);
}

TEST_F(ParallelPayloadApplierTest, ProbeRejectsUnsupportedOperation) {
  ASSERT_TRUE(WriteTestPayload(payload_file_.path(),
                               content_,
                               {},
                               InstallOperation::PUFFDIFF,
                               kFullPayloadMinorVersion,
                               &metadata_size_));
  ParallelPayloadApplier::Config config =
      MakeConfig(payload_file_.path(), utils::FileSize(payload_file_.path()));
  EXPECT_FALSE(
      ParallelPayloadApplier::ProbeEligible(config.payload_fd, &config));
  close(config.payload_fd);
}

TEST_F(ParallelPayloadApplierTest, ApplyWritesPartition) {
  ASSERT_TRUE(WriteTestPayload(payload_file_.path(),
                               content_,
                               {},
                               InstallOperation::REPLACE,
                               kFullPayloadMinorVersion,
                               &metadata_size_));
  const uint64_t size = utils::FileSize(payload_file_.path());
  ParallelPayloadApplier::Config config =
      MakeConfig(payload_file_.path(), size);
  ASSERT_TRUE(
      ParallelPayloadApplier::ProbeEligible(config.payload_fd, &config));

  ParallelPayloadApplier applier;
  EXPECT_EQ(ErrorCode::kSuccess,
            applier.Apply(std::move(config), &install_plan_));

  brillo::Blob written;
  ASSERT_TRUE(utils::ReadFile(target_file_.path(), &written));
  ASSERT_EQ(content_.size(), written.size());
  EXPECT_EQ(content_, written);
  ASSERT_EQ(1u, install_plan_.partitions.size());
  EXPECT_EQ("root", install_plan_.partitions[0].name);
  EXPECT_FALSE(install_plan_.partitions[0].target_hash.empty());
}

TEST_F(ParallelPayloadApplierTest, ApplyFailsOnCorruptedBlob) {
  ASSERT_TRUE(WriteTestPayload(payload_file_.path(),
                               content_,
                               {},
                               InstallOperation::REPLACE,
                               kFullPayloadMinorVersion,
                               &metadata_size_));
  android::base::unique_fd wfd(
      open(payload_file_.path().c_str(), O_WRONLY | O_CLOEXEC));
  ASSERT_GE(wfd.get(), 0);
  ASSERT_TRUE(utils::PWriteAll(wfd.get(), "\xff", 1, metadata_size_ + 1));

  const uint64_t size = utils::FileSize(payload_file_.path());
  ParallelPayloadApplier::Config config =
      MakeConfig(payload_file_.path(), size);
  ASSERT_TRUE(
      ParallelPayloadApplier::ProbeEligible(config.payload_fd, &config));

  ParallelPayloadApplier applier;
  EXPECT_EQ(ErrorCode::kDownloadOperationHashMismatch,
            applier.Apply(std::move(config), &install_plan_));
}

// Operations larger than DeltaPerformer::kMaxPayloadBufferSize are streamed
// from the payload instead of being read into memory.
TEST_F(ParallelPayloadApplierTest, ApplyWritesLargeOperation) {
  brillo::Blob large(DeltaPerformer::kMaxPayloadBufferSize + 1024 * 1024);
  test_utils::FillWithData(&large);
  ASSERT_TRUE(WriteTestPayload(payload_file_.path(),
                               large,
                               {},
                               InstallOperation::REPLACE,
                               kFullPayloadMinorVersion,
                               &metadata_size_));
  const uint64_t size = utils::FileSize(payload_file_.path());
  ParallelPayloadApplier::Config config =
      MakeConfig(payload_file_.path(), size);
  ASSERT_TRUE(
      ParallelPayloadApplier::ProbeEligible(config.payload_fd, &config));

  ParallelPayloadApplier applier;
  EXPECT_EQ(ErrorCode::kSuccess,
            applier.Apply(std::move(config), &install_plan_));

  brillo::Blob written;
  ASSERT_TRUE(utils::ReadFile(target_file_.path(), &written));
  EXPECT_EQ(large, written);
}

TEST_F(ParallelPayloadApplierTest, ApplyFailsOnCorruptedLargeOperation) {
  brillo::Blob large(DeltaPerformer::kMaxPayloadBufferSize + 1024 * 1024);
  test_utils::FillWithData(&large);
  ASSERT_TRUE(WriteTestPayload(payload_file_.path(),
                               large,
                               {},
                               InstallOperation::REPLACE,
                               kFullPayloadMinorVersion,
                               &metadata_size_));
  android::base::unique_fd wfd(
      open(payload_file_.path().c_str(), O_WRONLY | O_CLOEXEC));
  ASSERT_GE(wfd.get(), 0);
  ASSERT_TRUE(utils::PWriteAll(wfd.get(), "\xff", 1, metadata_size_ + 1));

  const uint64_t size = utils::FileSize(payload_file_.path());
  ParallelPayloadApplier::Config config =
      MakeConfig(payload_file_.path(), size);
  ASSERT_TRUE(
      ParallelPayloadApplier::ProbeEligible(config.payload_fd, &config));

  ParallelPayloadApplier applier;
  EXPECT_EQ(ErrorCode::kDownloadOperationHashMismatch,
            applier.Apply(std::move(config), &install_plan_));
}

TEST_F(ParallelPayloadApplierTest,
       ApplyFailsWhenOperationDataIsOutsidePayload) {
  ASSERT_TRUE(WriteTestPayload(payload_file_.path(),
                               content_,
                               {},
                               InstallOperation::REPLACE,
                               kFullPayloadMinorVersion,
                               &metadata_size_));
  // Cut the end of the data blob off; the metadata stays intact.
  const uint64_t size = utils::FileSize(payload_file_.path()) - 1;
  ASSERT_EQ(0, truncate(payload_file_.path().c_str(), size));
  ParallelPayloadApplier::Config config =
      MakeConfig(payload_file_.path(), size);
  ASSERT_TRUE(
      ParallelPayloadApplier::ProbeEligible(config.payload_fd, &config));

  ParallelPayloadApplier applier;
  EXPECT_EQ(ErrorCode::kDownloadTransferError,
            applier.Apply(std::move(config), &install_plan_));
}

TEST_F(ParallelPayloadApplierTest, ApplyReportsProgressThroughDelegate) {
  ASSERT_TRUE(WriteTestPayload(payload_file_.path(),
                               content_,
                               {},
                               InstallOperation::REPLACE,
                               kFullPayloadMinorVersion,
                               &metadata_size_));
  const uint64_t size = utils::FileSize(payload_file_.path());
  ParallelPayloadApplier::Config config =
      MakeConfig(payload_file_.path(), size);
  ASSERT_TRUE(
      ParallelPayloadApplier::ProbeEligible(config.payload_fd, &config));

  ParallelPayloadApplier applier;
  EXPECT_EQ(ErrorCode::kSuccess,
            applier.Apply(std::move(config), &install_plan_));
  EXPECT_GT(delegate_.bytes_received_calls, 0);
  EXPECT_EQ(size, delegate_.last_total);
  EXPECT_EQ(size, delegate_.last_received);
}

TEST_F(ParallelPayloadApplierTest, ApplyReturnsCanceledWhenCanceled) {
  ASSERT_TRUE(WriteTestPayload(payload_file_.path(),
                               content_,
                               {},
                               InstallOperation::REPLACE,
                               kFullPayloadMinorVersion,
                               &metadata_size_));
  const uint64_t size = utils::FileSize(payload_file_.path());
  ParallelPayloadApplier::Config config =
      MakeConfig(payload_file_.path(), size);
  ASSERT_TRUE(
      ParallelPayloadApplier::ProbeEligible(config.payload_fd, &config));

  ParallelPayloadApplier applier;
  applier.Cancel();
  EXPECT_EQ(ErrorCode::kUserCanceled,
            applier.Apply(std::move(config), &install_plan_));
}

TEST_F(ParallelPayloadApplierTest, ApplyWritesMultiplePartitions) {
  brillo::Blob content1(kPartSize);
  test_utils::FillWithData(&content1);
  brillo::Blob content2(kPartSize * 2);
  test_utils::FillWithData(&content2);
  content2[0] = ~content2[0];  // Ensure distinction

  std::vector<std::pair<std::string, brillo::Blob>> parts = {
      {"system", content1}, {"vendor", content2}};

  ASSERT_TRUE(WriteMultiPartTestPayload(
      payload_file_.path(), parts, kFullPayloadMinorVersion, &metadata_size_));

  ScopedTempFile target_system{"parallel_applier_target_system.XXXXXX"};
  ScopedTempFile target_vendor{"parallel_applier_target_vendor.XXXXXX"};
  fake_boot_control_.SetPartitionDevice("system", 1, target_system.path());
  fake_boot_control_.SetPartitionDevice("vendor", 1, target_vendor.path());
  fake_boot_control_.SetPartitionDevice("system", 0, "/dev/null");
  fake_boot_control_.SetPartitionDevice("vendor", 0, "/dev/null");

  install_plan_.partitions.clear();
  const uint64_t size = utils::FileSize(payload_file_.path());
  ParallelPayloadApplier::Config config =
      MakeConfig(payload_file_.path(), size);
  ASSERT_TRUE(
      ParallelPayloadApplier::ProbeEligible(config.payload_fd, &config));

  ParallelPayloadApplier applier;
  EXPECT_EQ(ErrorCode::kSuccess,
            applier.Apply(std::move(config), &install_plan_));

  brillo::Blob written1, written2;
  ASSERT_TRUE(utils::ReadFile(target_system.path(), &written1));
  ASSERT_TRUE(utils::ReadFile(target_vendor.path(), &written2));
  EXPECT_EQ(content1, written1);
  EXPECT_EQ(content2, written2);
  ASSERT_EQ(2u, install_plan_.partitions.size());
}
}  // namespace chromeos_update_engine
