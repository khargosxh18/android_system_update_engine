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
#include <cstring>

#include <algorithm>
#include <chrono>
#include <memory>
#include <utility>

#include <android-base/unique_fd.h>
#include <base/logging.h>
#include <base/time/time.h>

#include "update_engine/common/constants.h"
#include "update_engine/common/download_action.h"
#include "update_engine/common/error_code_utils.h"
#include "update_engine/common/hash_calculator.h"
#include "update_engine/common/utils.h"
#include "update_engine/payload_consumer/delta_performer.h"
#include "update_engine/payload_consumer/partition_writer.h"
#include "update_engine/payload_consumer/payload_constants.h"
#include "update_engine/payload_consumer/payload_metadata.h"

namespace chromeos_update_engine {

namespace {

constexpr size_t kMaxThreads = 8;
constexpr size_t kProgressIntervalMs = 200;
constexpr size_t kPayloadHashReadBufferSize = 4 * 1024 * 1024;

bool OpSupported(const InstallOperation& op) {
  switch (op.type()) {
    case InstallOperation::REPLACE:
    case InstallOperation::REPLACE_BZ:
    case InstallOperation::REPLACE_XZ:
    case InstallOperation::REPLACE_ZSTD:
    case InstallOperation::ZERO:
    case InstallOperation::DISCARD:
      return true;
    default:
      return false;
  }
}

bool ReadFullyAt(int fd, brillo::Blob* out, uint64_t offset) {
  ssize_t bytes_read = 0;
  return utils::PReadAll(fd, out->data(), out->size(), offset, &bytes_read) &&
         bytes_read == static_cast<ssize_t>(out->size());
}

// Hashes |length| bytes at |offset| without holding them in memory. Uses
// pread(), so it is safe on an fd shared between workers.
ErrorCode HashFdRange(int fd,
                      uint64_t offset,
                      uint64_t length,
                      brillo::Blob* hash) {
  HashCalculator calc;
  brillo::Blob buffer(std::min<uint64_t>(length, kPayloadHashReadBufferSize));
  uint64_t done = 0;
  while (done < length) {
    const size_t to_read = std::min<uint64_t>(buffer.size(), length - done);
    ssize_t bytes_read = 0;
    if (!utils::PReadAll(
            fd, buffer.data(), to_read, offset + done, &bytes_read) ||
        bytes_read != static_cast<ssize_t>(to_read))
      return ErrorCode::kDownloadTransferError;
    if (!calc.Update(buffer.data(), bytes_read))
      return ErrorCode::kDownloadOperationHashVerificationError;
    done += bytes_read;
  }
  if (!calc.Finalize())
    return ErrorCode::kDownloadOperationHashVerificationError;
  *hash = calc.raw_hash();
  return ErrorCode::kSuccess;
}

}  // namespace

ParallelPayloadApplier::~ParallelPayloadApplier() {
  Cancel();
  for (std::thread& worker : workers_) {
    if (worker.joinable())
      worker.join();
  }
}

// static
bool ParallelPayloadApplier::ProbeEligible(int payload_fd, Config* config) {
  brillo::Blob header(kMaxPayloadHeaderSize);
  if (!ReadFullyAt(payload_fd, &header, config->base_offset)) {
    LOG(ERROR) << "Parallel sideload: unable to read payload header";
    return false;
  }
  PayloadMetadata metadata;
  ErrorCode error = ErrorCode::kSuccess;
  if (metadata.ParsePayloadHeader(header, &error) !=
      MetadataParseResult::kSuccess) {
    LOG(ERROR) << "Parallel sideload: bad payload header: "
               << utils::ErrorCodeToString(error);
    return false;
  }
  const uint64_t metadata_size =
      metadata.GetMetadataSize() + metadata.GetMetadataSignatureSize();
  VLOG(1) << "Parallel sideload: manifest size " << metadata_size << " bytes";

  brillo::Blob metadata_bytes(metadata_size);
  if (!ReadFullyAt(payload_fd, &metadata_bytes, config->base_offset)) {
    LOG(ERROR) << "Parallel sideload: unable to read payload metadata";
    return false;
  }
  if (!metadata.GetManifest(metadata_bytes, &config->manifest)) {
    LOG(ERROR) << "Parallel sideload: unable to parse payload manifest";
    return false;
  }
  const DeltaArchiveManifest& manifest = config->manifest;
  if (manifest.minor_version() != kFullPayloadMinorVersion) {
    LOG(INFO) << "Parallel sideload: not a full payload, minor version "
              << manifest.minor_version();
    return false;
  }
  if (manifest.partial_update() || manifest.partitions().empty()) {
    LOG(INFO) << "Parallel sideload: partial or empty payload not supported";
    return false;
  }
  for (const PartitionUpdate& partition : manifest.partitions()) {
    if (partition.has_old_partition_info()) {
      LOG(INFO) << "Parallel sideload: delta partition "
                << partition.partition_name();
      return false;
    }
    for (const InstallOperation& op : partition.operations()) {
      if (!OpSupported(op)) {
        LOG(INFO) << "Parallel sideload: unsupported operation type "
                  << static_cast<int>(op.type()) << " ("
                  << InstallOperationTypeName(op.type()) << ") in partition "
                  << partition.partition_name();
        return false;
      }
    }
  }
  config->data_begin = config->base_offset + metadata_size;
  return true;
}

ErrorCode ParallelPayloadApplier::Apply(Config config,
                                        InstallPlan* install_plan) {
  config_ = std::move(config);
  install_plan_ = install_plan;
  android::base::unique_fd fd(config_.payload_fd);
  config_.payload_fd = -1;

  uint64_t total = 0;
  if (config_.payload_size) {
    total = config_.payload_size;
  } else {
    const off64_t file_size = utils::FileSize(fd.get());
    if (file_size < 0) {
      LOG(ERROR) << "Parallel sideload: unable to stat payload fd";
      return ErrorCode::kDownloadTransferError;
    }
    total = static_cast<uint64_t>(file_size) - config_.base_offset;
  }
  const uint64_t metadata_bytes = config_.data_begin - config_.base_offset;
  payload_data_size_ = total > metadata_bytes ? total - metadata_bytes : 0;
  block_size_ = config_.manifest.block_size();

  // Run the stock metadata stage through DeltaPerformer, fed with the payload
  // metadata only: metadata size/signature checks, ValidateManifest() (version
  // and timestamp checks), the SPL downgrade check, the VABC manifest
  // overrides, PreparePartitionsForUpdate() and ParsePartitions() all happen
  // exactly as in the sequential path. Only the data blobs are applied by the
  // workers below.
  {
    DeltaPerformer performer(config_.prefs,
                             config_.boot_control,
                             config_.hardware,
                             config_.delegate,
                             install_plan,
                             config_.payload,
                             config_.interactive,
                             config_.update_certificates_path);
    brillo::Blob metadata(config_.data_begin - config_.base_offset);
    if (!ReadFullyAt(fd.get(), &metadata, config_.base_offset)) {
      LOG(ERROR) << "Parallel sideload: unable to read payload metadata";
      return ErrorCode::kDownloadTransferError;
    }
    ErrorCode error = ErrorCode::kSuccess;
    if (!performer.Write(metadata.data(), metadata.size(), &error) ||
        !performer.IsManifestValid()) {
      LOG(ERROR) << "Parallel sideload: metadata stage failed: "
                 << utils::ErrorCodeToString(error);
      return error == ErrorCode::kSuccess
                 ? ErrorCode::kDownloadManifestParseError
                 : error;
    }
    LOG_IF(WARNING, performer.Close() != 0) << "Error closing the performer.";
  }

  // Move partition data out; scalar manifest fields (signatures_offset,
  // block_size) remain valid and are used by ValidateOperationHash.
  partitions_.assign(
      std::make_move_iterator(config_.manifest.mutable_partitions()->begin()),
      std::make_move_iterator(config_.manifest.mutable_partitions()->end()));
  if (install_plan->partitions.size() != partitions_.size()) {
    LOG(ERROR) << "Parallel sideload: install plan partition count mismatch";
    return ErrorCode::kInstallDeviceOpenError;
  }

  // IsDynamicPartition() lazily fills a cache inside the dynamic partition
  // control object, so resolve it here, before any worker thread exists.
  auto* dynamic_control = config_.boot_control->GetDynamicPartitionControl();
  for (const InstallPlan::Partition& install_part : install_plan->partitions) {
    is_dynamic_.push_back(dynamic_control->IsDynamicPartition(
        install_part.name, install_plan->target_slot));
  }

  order_.reserve(partitions_.size());
  for (size_t i = 0; i < partitions_.size(); i++)
    order_.push_back(i);
  // Longest Processing Time (LPT) first scheduling
  std::stable_sort(order_.begin(), order_.end(), [this](size_t a, size_t b) {
    return partitions_[a].new_partition_info().size() >
           partitions_[b].new_partition_info().size();
  });

  bytes_done_ = config_.data_begin - config_.base_offset;
  size_t num_threads = config_.num_threads;
  if (num_threads == 0) {
    long online = sysconf(_SC_NPROCESSORS_ONLN);
    if (online < 1)
      online = 1;
    num_threads = std::min<size_t>(static_cast<size_t>(online), kMaxThreads);
  }
  num_threads = std::max<size_t>(1, std::min(num_threads, order_.size()));
  LOG(INFO) << "Parallel sideload: applying " << partitions_.size()
            << " partitions with " << num_threads << " workers";

  running_ = num_threads;
  uint64_t last_reported = 0;
  for (size_t i = 0; i < num_threads; i++)
    workers_.emplace_back(&ParallelPayloadApplier::WorkerMain, this, fd.get());

  // UI progress pump: Sleep dictates our 200ms UI tick rate and prevents
  // flooding the main message loop with progress updates.
  while (running_.load() > 0) {
    std::this_thread::sleep_for(std::chrono::milliseconds(kProgressIntervalMs));
    ReportProgress(&last_reported, total);
  }
  for (std::thread& worker : workers_)
    worker.join();
  workers_.clear();
  ReportProgress(&last_reported, total);

  ErrorCode result = first_error_.load();
  if (result == ErrorCode::kSuccess && cancelled_)
    result = ErrorCode::kUserCanceled;
  if (result == ErrorCode::kSuccess)
    result = VerifyPayloadHash(fd.get(), total);
  if (result == ErrorCode::kSuccess) {
    // The signature blob is not counted, so report the full size explicitly.
    bytes_done_ = total;
    ReportProgress(&last_reported, total);
  }

  LOG(INFO) << "Parallel sideload: finished with "
            << utils::ErrorCodeToString(result);
  return result;
}

void ParallelPayloadApplier::Cancel() {
  cancelled_ = true;
}

void ParallelPayloadApplier::WorkerMain(int fd) {
  while (!cancelled_ && first_error_.load() == ErrorCode::kSuccess) {
    const size_t slot = next_partition_.fetch_add(1);
    if (slot >= order_.size())
      break;
    ApplyPartition(fd, order_[slot]);
  }
  running_--;
}

void ParallelPayloadApplier::ApplyPartition(int fd, size_t index) {
  const PartitionUpdate& partition = partitions_[index];
  const InstallPlan::Partition& install_part = install_plan_->partitions[index];
  auto* dynamic_control = config_.boot_control->GetDynamicPartitionControl();

  // Concurrent initialization of PartitionWriter is safe here because:
  // 1. DeltaPerformer's metadata stage already completed all mutating
  //    liblp/device-mapper work on the main thread.
  // 2. IsDynamicPartition() is pre-resolved to avoid lazy-cache race
  // conditions.
  // 3. VABC (Snapshot Compression) is inherently disabled for full payloads
  //    (minor version 0), so UpdateUsesSnapshotCompression() will not mutate
  //    state.
  auto writer = partition_writer::CreatePartitionWriter(partition,
                                                        install_part,
                                                        dynamic_control,
                                                        block_size_,
                                                        config_.interactive,
                                                        is_dynamic_[index]);
  if (!writer->Init(install_plan_, false /* source_may_exist */, 0)) {
    LOG(ERROR) << "Parallel sideload: unable to open partition "
               << partition.partition_name();
    SetError(ErrorCode::kInstallDeviceOpenError);
    return;
  }

  brillo::Blob blob;
  for (const InstallOperation& op : partition.operations()) {
    if (cancelled_ || first_error_.load() != ErrorCode::kSuccess)
      return;
    if (op.type() == InstallOperation::ZERO ||
        op.type() == InstallOperation::DISCARD) {
      if (!writer->PerformZeroOrDiscardOperation(op)) {
        SetError(ErrorCode::kDownloadOperationExecutionError);
        return;
      }
      continue;
    }

    // Same rule as DeltaPerformer: the operation data must lie inside the
    // payload. There is no other size limit, as in the sequential path.
    if (op.data_offset() > payload_data_size_ ||
        op.data_length() > payload_data_size_ - op.data_offset()) {
      LOG(ERROR) << "Parallel sideload: operation data at offset "
                 << op.data_offset() << " with length " << op.data_length()
                 << " is outside the payload";
      SetError(ErrorCode::kDownloadTransferError);
      return;
    }

    const uint64_t data_pos = config_.data_begin + op.data_offset();
    if (op.data_length() <= DeltaPerformer::kMaxPayloadBufferSize) {
      blob.resize(op.data_length());
      if (!ReadFullyAt(fd, &blob, data_pos)) {
        LOG(ERROR) << "Parallel sideload: short read for operation at offset "
                   << op.data_offset();
        SetError(ErrorCode::kDownloadTransferError);
        return;
      }
      const ErrorCode hash_error = ValidateOperationHash(op, fd, &blob);
      if (hash_error != ErrorCode::kSuccess) {
        SetError(hash_error);
        return;
      }
      if (!writer->PerformReplaceOperation(op, blob.data(), blob.size())) {
        SetError(ErrorCode::kDownloadOperationExecutionError);
        return;
      }
    } else {
      // Like DeltaPerformer, do not keep large operations in memory: verify
      // the hash by streaming the data, then let the PartitionWriter stream it
      // from the payload in chunks, so the data is checked before any of it is
      // written.
      const ErrorCode hash_error = ValidateOperationHash(op, fd, nullptr);
      if (hash_error != ErrorCode::kSuccess) {
        SetError(hash_error);
        return;
      }
      if (!writer->PerformReplaceOperation(
              op, fd, static_cast<off_t>(data_pos), op.data_length())) {
        SetError(ErrorCode::kDownloadOperationExecutionError);
        return;
      }
    }
    bytes_done_ += op.data_length();
  }

  if (!writer->FinishedInstallOps()) {
    SetError(ErrorCode::kDownloadWriteError);
    return;
  }
  const int err = writer->Close();
  if (err != 0) {
    LOG(ERROR) << "Parallel sideload: error closing partition "
               << partition.partition_name() << ": " << strerror(-err);
    SetError(ErrorCode::kDownloadWriteError);
    return;
  }
  LOG(INFO) << "Parallel sideload: wrote partition "
            << partition.partition_name();
}

ErrorCode ParallelPayloadApplier::ValidateOperationHash(
    const InstallOperation& op, int fd, const brillo::Blob* blob) const {
  if (!op.has_data_sha256_hash()) {
    if (!op.data_length())
      return ErrorCode::kSuccess;
    if (config_.manifest.signatures_offset() &&
        config_.manifest.signatures_offset() == op.data_offset())
      return ErrorCode::kSuccess;
    if (config_.hash_checks_mandatory) {
      LOG(ERROR) << "Parallel sideload: missing mandatory operation hash";
      return ErrorCode::kDownloadOperationHashMissingError;
    }
    LOG(WARNING) << "Parallel sideload: no operation hash to validate";
    return ErrorCode::kSuccess;
  }
  brillo::Blob expected(op.data_sha256_hash().begin(),
                        op.data_sha256_hash().end());
  brillo::Blob actual;
  if (blob != nullptr) {
    if (!HashCalculator::RawHashOfBytes(
            blob->data(), op.data_length(), &actual))
      return ErrorCode::kDownloadOperationHashVerificationError;
  } else {
    const ErrorCode error = HashFdRange(
        fd, config_.data_begin + op.data_offset(), op.data_length(), &actual);
    if (error != ErrorCode::kSuccess)
      return error;
  }
  if (actual != expected) {
    LOG(ERROR) << "Parallel sideload: hash mismatch for operation at offset "
               << op.data_offset();
    return ErrorCode::kDownloadOperationHashMismatch;
  }
  return ErrorCode::kSuccess;
}

ErrorCode ParallelPayloadApplier::VerifyPayloadHash(int fd,
                                                    uint64_t total) const {
  if (install_plan_->payloads.empty() ||
      install_plan_->payloads[0].hash.empty()) {
    LOG(INFO) << "Parallel sideload: no payload hash provided, skipping";
    return ErrorCode::kSuccess;
  }
  HashCalculator calc;
  brillo::Blob buffer(kPayloadHashReadBufferSize);
  uint64_t offset = 0;
  while (offset < total) {
    const size_t to_read = std::min<size_t>(buffer.size(), total - offset);
    ssize_t bytes_read = 0;
    if (!utils::PReadAll(fd,
                         buffer.data(),
                         to_read,
                         config_.base_offset + offset,
                         &bytes_read) ||
        bytes_read != static_cast<ssize_t>(to_read))
      return ErrorCode::kDownloadTransferError;
    if (!calc.Update(buffer.data(), bytes_read))
      return ErrorCode::kDownloadPayloadVerificationError;
    offset += bytes_read;
  }
  if (!calc.Finalize())
    return ErrorCode::kDownloadPayloadVerificationError;
  if (calc.raw_hash() != install_plan_->payloads[0].hash) {
    LOG(ERROR) << "Parallel sideload: payload hash mismatch";
    return ErrorCode::kPayloadHashMismatchError;
  }
  LOG(INFO) << "Parallel sideload: payload hash matches value in payload";
  return ErrorCode::kSuccess;
}

void ParallelPayloadApplier::SetError(ErrorCode code) {
  ErrorCode expected = ErrorCode::kSuccess;
  first_error_.compare_exchange_strong(expected, code);
}

void ParallelPayloadApplier::ReportProgress(uint64_t* last_reported,
                                            uint64_t total) const {
  if (config_.delegate == nullptr || total == 0)
    return;
  const uint64_t done = std::min(bytes_done_.load(), total);
  if (done > *last_reported) {
    config_.delegate->BytesReceived(done - *last_reported, done, total);
    *last_reported = done;
  }
}

}  // namespace chromeos_update_engine
