/** Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * 	http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#pragma once

#include <assert.h>
#include <errno.h>
#include <glog/logging.h>
#include <algorithm>
#include <cstdio>
#include <cstring>
#include <sstream>
#include <stdexcept>
#include <string>
#include <vector>

namespace neug {

void ensure_directory_exists(const std::string& dir_path);

bool read_string_from_file(const std::string& file_path, std::string& content);

bool write_string_to_file(const std::string& content,
                          const std::string& file_path);

/**
 * Copy file from src to dst.
 * @param src source file path
 * @param dst destination file path
 * @param overwrite whether to clean up the dst file if it already exists
 * @param recursive whether to copy directories recursively
 */
void copy_directory(const std::string& src, const std::string& dst,
                    bool overwrite = false, bool recursive = true);

void remove_directory(const std::string& dir_path);

void write_file(const std::string& filename, const void* buffer, size_t size,
                size_t num);

void read_file(const std::string& filename, void* buffer, size_t size,
               size_t num);

void write_statistic_file(const std::string& file_path, size_t capacity,
                          size_t size);

void read_statistic_file(const std::string& file_path, size_t& capacity,
                         size_t& size);

struct BufferWriter {
  FILE* fout;
  std::vector<char> buffer;
  size_t buffered_bytes;
  constexpr static size_t kBufferSize = 16UL << 20;  // 16MB

  BufferWriter(const BufferWriter&) = delete;
  BufferWriter& operator=(const BufferWriter&) = delete;

  BufferWriter(const std::string& filename)
      : buffer(kBufferSize), buffered_bytes(0) {
    fout = fopen(filename.c_str(), "wb");
    if (fout == nullptr) {
      std::stringstream ss;
      ss << "Failed to open file " << filename << ", " << strerror(errno);
      LOG(ERROR) << ss.str();
      throw std::runtime_error(ss.str());
    }
  }

  ~BufferWriter() {
    try {
      close();
    } catch (const std::exception& e) {
      LOG(ERROR) << "Exception in BufferWriter destructor: " << e.what();
    }
  }

  void write(const char* data, size_t bytes) {
    size_t offset = 0;
    while (bytes > 0) {
      size_t space_left = buffer.size() - buffered_bytes;
      if (space_left == 0) {
        flush();
        space_left = buffer.size();
      }
      size_t chunk = std::min(space_left, bytes);
      memcpy(buffer.data() + buffered_bytes, data + offset, chunk);
      buffered_bytes += chunk;
      offset += chunk;
      bytes -= chunk;

      if (buffered_bytes == buffer.size()) {
        flush();
      }
    }
  }

  void flush() {
    if (buffered_bytes > 0) {
      if (fwrite(buffer.data(), 1, buffered_bytes, fout) != buffered_bytes) {
        std::stringstream ss;
        ss << "Failed to write to file, error code: " << strerror(errno);
        LOG(ERROR) << ss.str();
        throw std::runtime_error(ss.str());
      }
      buffered_bytes = 0;
    }
  }

  void close() {
    flush();
    if (fout != nullptr) {
      if (fflush(fout) != 0) {
        std::stringstream ss;
        ss << "Failed to flush file, error code: " << strerror(errno);
        LOG(ERROR) << ss.str();
        throw std::runtime_error(ss.str());
      }
      if (fclose(fout) != 0) {
        std::stringstream ss;
        ss << "Failed to close file, error code: " << strerror(errno);
        LOG(ERROR) << ss.str();
        throw std::runtime_error(ss.str());
      }
      fout = nullptr;
    }
  }
};

}  // namespace neug
