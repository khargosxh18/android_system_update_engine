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
#ifndef UPDATE_ENGINE_PAYLOAD_CONSUMER_PARALLEL_PAYLOAD_APPLIER_H_
#define UPDATE_ENGINE_PAYLOAD_CONSUMER_PARALLEL_PAYLOAD_APPLIER_H_

#include <atomic>
#include <string>
#include <thread>
#include <vector>

#include <android-base/macros.h>
#include <brillo/secure_blob.h>

#include "update_engine/common/error_code.h"
#include "update_engine/payload_consumer/install_plan.h"
#include "update_engine/update_metadata.pb.h"

namespace chromeos_update_engine {

class BootControlInterface;
class DownloadActionDelegate;
class HardwareInterface;
class PrefsInterface;

// Applies a full payload read from a local file or fd by decoding and writing
// independent partitions concurrently. Recovery sideload only: the streaming
// DeltaPerformer path remains the fallback for everything else.
//
// The applier reuses the stock preparation and write stack (the DeltaPerformer
// metadata stage, which runs PreparePartitionsForUpdate and
// InstallPlan::ParsePartitions, plus PartitionWriter) and enforces the same
// per-operation hash policy, so its on-disk result and failure semantics match
// the sequential path exactly.
// It is not resumable: any error aborts the whole attempt.
class ParallelPayloadApplier {
 public:
  struct Config {
    // Fd of the file containing the payload (zip or raw payload.bin).
    // Ownership passes to Apply() once ProbeEligible() succeeded.
    int payload_fd = -1;
    // Offset of the CrAU payload inside |payload_fd|.
    uint64_t base_offset = 0;
    // Size of the CrAU payload; 0 means "until end of file".
    uint64_t payload_size = 0;
    // Worker count; 0 selects min(online CPUs, 8).
    size_t num_threads = 0;

    // Payload entry of the install plan; the stock metadata stage needs it.
    InstallPlan::Payload* payload = nullptr;

    PrefsInterface* prefs = nullptr;
    BootControlInterface* boot_control = nullptr;
    HardwareInterface* hardware = nullptr;
    DownloadActionDelegate* delegate = nullptr;
    std::string update_certificates_path;

    bool hash_checks_mandatory = false;
    bool interactive = true;

    // Filled by ProbeEligible().
    DeltaArchiveManifest manifest;
    // Absolute offset of the first data blob inside |payload_fd|.
    uint64_t data_begin = 0;
  };

  ParallelPayloadApplier() = default;
  ~ParallelPayloadApplier();

  // Read-only eligibility check. Parses the payload manifest at
  // |config->base_offset| and fills |config->manifest| and |config->data_begin|
  // only if the payload is a full, non-partial update composed exclusively of
  // operations this applier can decode. Never modifies device state.
  static bool ProbeEligible(int payload_fd, Config* config);

  // Blocks until every partition is written and flushed, or the first error.
  // Takes ownership of |config.payload_fd|. Reports progress through
  // |config.delegate|. The caller owns |install_plan| and must keep it alive.
  ErrorCode Apply(Config config, InstallPlan* install_plan);

  // Asks workers to stop before their next operation. Apply() then returns
  // ErrorCode::kUserCanceled once drained.
  void Cancel();

 private:
  void WorkerMain(int fd);
  void ApplyPartition(int fd, size_t index);
  // Checks the operation hash against |blob|, or, when |blob| is null,
  // against the data streamed from |fd|.
  ErrorCode ValidateOperationHash(const InstallOperation& op,
                                  int fd,
                                  const brillo::Blob* blob) const;
  ErrorCode VerifyPayloadHash(int fd, uint64_t total) const;
  void SetError(ErrorCode code);
  void ReportProgress(uint64_t* last_reported, uint64_t total) const;

  Config config_;
  InstallPlan* install_plan_{nullptr};
  std::vector<PartitionUpdate> partitions_;
  // Indices into |partitions_|, largest partition first.
  std::vector<size_t> order_;
  // Per partition, resolved before any worker starts: IsDynamicPartition()
  // lazily fills a cache inside the dynamic partition control object.
  std::vector<bool> is_dynamic_;
  uint32_t block_size_{0};
  // Number of payload bytes after the metadata; operation data must lie
  // inside it, as DeltaPerformer requires.
  uint64_t payload_data_size_{0};

  std::atomic<size_t> next_partition_{0};
  std::atomic<size_t> running_{0};
  std::atomic<uint64_t> bytes_done_{0};
  std::atomic<ErrorCode> first_error_{ErrorCode::kSuccess};
  std::atomic<bool> cancelled_{false};
  std::vector<std::thread> workers_;

  DISALLOW_COPY_AND_ASSIGN(ParallelPayloadApplier);
};

}  // namespace chromeos_update_engine

#endif  // UPDATE_ENGINE_PAYLOAD_CONSUMER_PARALLEL_PAYLOAD_APPLIER_H_
