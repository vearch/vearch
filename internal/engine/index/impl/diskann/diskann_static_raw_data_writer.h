#pragma once
#include <fstream>
#include <limits>
#include "vector/raw_vector.h"

namespace vearch {

class DiskANNRawDataWriter {
 public:
  static Status WriteToFile(RawVector *raw_vec, int64_t num_vecs,
                       int dimension, const std::string &output_path) {
    const int64_t maxValue =
        static_cast<int64_t>(std::numeric_limits<uint32_t>::max());
    if (num_vecs > maxValue) {
      LOG(ERROR) << "DiskANNRawDataWriter: num_vecs exceeds uint32_t max, would truncate file header";
      return Status::ParamError("DiskANNRawDataWriter: num_vecs exceeds uint32_t max, would truncate file header");
    }
    std::ofstream writer(output_path, std::ios::binary);
    if (!writer.is_open()) {
      return Status::IOError("Cannot open file: " + output_path);
    }

    uint32_t npts = static_cast<uint32_t>(num_vecs);
    uint32_t dim = static_cast<uint32_t>(dimension);
    writer.write(reinterpret_cast<char *>(&npts), sizeof(uint32_t));
    writer.write(reinterpret_cast<char *>(&dim), sizeof(uint32_t));
    if (!writer.good()) {
      writer.close();
      return Status::IOError("Failed to write header to file: " + output_path);
    }

    const size_t vec_bytes = dim * sizeof(float);
    const int64_t batch_size = 1000;
    std::vector<float> zeros(dim, 0.0f);

    for (int64_t offset = 0; offset < num_vecs; offset += batch_size) {
      int64_t cur_batch = std::min(batch_size, num_vecs - offset);

      std::vector<int64_t> vids(cur_batch);
      for (int64_t j = 0; j < cur_batch; j++) {
        vids[j] = offset + j;
      }

      ScopeVectors scope_vecs;
      int ret = raw_vec->Gets(vids, scope_vecs);
      if (ret != 0) {
        writer.close();
        LOG(ERROR) << "DiskANNRawDataWriter: RawVector::Gets failed at offset " +
                               std::to_string(offset) + ", ret=" +
                               std::to_string(ret);
        return Status::IOError("RawVector::Gets failed at offset " +
                               std::to_string(offset) + ", ret=" +
                               std::to_string(ret));
      }

      for (int64_t j = 0; j < cur_batch; j++) {
        const uint8_t *vec =
            (j < (int64_t)scope_vecs.Size()) ? scope_vecs.Get(j) : nullptr;
        if (vec == nullptr) {
          writer.write(reinterpret_cast<const char *>(zeros.data()), vec_bytes);
        } else {
          writer.write(reinterpret_cast<const char *>(vec), vec_bytes);
        }
      }
    }
    writer.close();
    if (writer.fail()) {
      LOG(ERROR) << "DiskANNRawDataWriter: Failed to close file cleanly: " + output_path;
      return Status::IOError("Failed to close file cleanly: " + output_path);
    }
    return Status::OK();
  }
};
}