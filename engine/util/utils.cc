/**
 * Copyright 2019 The Gamma Authors.
 *
 * This source code is licensed under the Apache License, Version 2.0 license
 * found in the LICENSE file in the root directory of this source tree.
 */

#include "utils.h"

#include <dirent.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <unistd.h>

#include <algorithm>
#include <fstream>
#include <sstream>
#include <stdexcept>

#include "util/log.h"

namespace utils {

long get_file_size(const char *path) {
  long filesize = -1;
  struct stat statbuff;
  if (stat(path, &statbuff) < 0) {
    return filesize;
  } else {
    filesize = statbuff.st_size;
  }
  return filesize;
}

long get_file_size(const std::string &path) {
  return get_file_size(path.c_str());
}

std::vector<std::string> split(const std::string &p_str,
                               const std::string &p_separator) {
  std::vector<std::string> ret;

  std::size_t begin = p_str.find_first_not_of(p_separator);
  while (std::string::npos != begin) {
    std::size_t end = p_str.find_first_of(p_separator, begin);
    if (std::string::npos == end) {
      ret.emplace_back(p_str.substr(begin, p_str.size() - begin));
      break;
    } else {
      ret.emplace_back(p_str.substr(begin, end - begin));
    }

    begin = p_str.find_first_not_of(p_separator, end);
  }

  return ret;
}

std::vector<std::string> split(const std::string &p_str,
                               const char *p_separator) {
  const std::string p_separator_str = std::string(p_separator);
  return split(p_str, p_separator_str);
}

int count_lines(const char *filename) {
  std::ifstream read_file;
  int n = 0;
  std::string tmp;
  read_file.open(filename, std::ios::in);
  if (read_file.fail()) {
    return 0;
  } else {
    while (getline(read_file, tmp, '\n')) {
      n++;
    }
    read_file.close();
    return n;
  }
}

void GenRandom(std::mt19937 &rng, unsigned *addr, unsigned size, unsigned N) {
  for (unsigned i = 0; i < size; ++i) {
    if (size >= N) {
      addr[i] = rng() % N;
    } else {
      addr[i] = rng() % (N - size);
    }
  }
  std::sort(addr, addr + size);
  for (unsigned i = 1; i < size; ++i) {
    if (addr[i] <= addr[i - 1]) {
      addr[i] = addr[i - 1] + 1;
    }
  }

  unsigned off = rng() % N;
  for (unsigned i = 0; i < size; ++i) {
    addr[i] = (addr[i] + off) % N;
  }
}

double elapsed() {
  struct timeval tv;
  gettimeofday(&tv, NULL);
  return tv.tv_sec + tv.tv_usec * 1e-6;
}

double getmillisecs() {
  struct timeval tv;
  gettimeofday(&tv, 0);
  return tv.tv_sec * 1e3 + tv.tv_usec * 1e-3;
}

int isFolderExist(const char *path) {
  DIR *dp;
  if ((dp = opendir(path)) == NULL) {
    return 0;
  }
  closedir(dp);
  return -1;
}

bool file_exist(const std::string &path) {
  if (access(path.c_str(), F_OK) != 0) {
    return false;  // not exist
  }
  return true;
}

int make_dir(const char *path) {
  if (!utils::isFolderExist(path)) {
    return mkdir(path, S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH);
  }
  return 0;
}

int remove_dir(const char *path) {
  DIR *d = opendir(path);
  size_t path_len = strlen(path);
  int r = -1;

  if (d) {
    struct dirent *p;

    r = 0;

    while (!r && (p = readdir(d))) {
      int r2 = -1;
      char *buf;
      size_t len;

      /* Skip the names "." and ".." as we don't want to recurse on them. */
      if (!strcmp(p->d_name, ".") || !strcmp(p->d_name, "..")) {
        continue;
      }

      len = path_len + strlen(p->d_name) + 2;
      buf = static_cast<char *>(malloc(len));

      if (buf) {
        struct stat statbuf;

        snprintf(buf, len, "%s/%s", path, p->d_name);

        if (!stat(buf, &statbuf)) {
          if (S_ISDIR(statbuf.st_mode)) {
            r2 = remove_dir(buf);
          } else {
            r2 = unlink(buf);
          }
        }

        free(buf);
      }

      r = r2;
    }

    closedir(d);
  }

  if (!r) {
    r = rmdir(path);
  }

  return r;
}

int move_dir(const char *src, const char *dst, bool backup) {
  std::string option = "";
  if (backup) {
    option += "--backup=t";
  }
  std::string cmd = std::string("/bin/mv ") + option + " " + src + " " + dst;
  return system(cmd.c_str());
}

inline bool is_folder(const char *dir_name) {
  auto dir = opendir(dir_name);
  if (dir) {
    closedir(dir);
    return true;
  }
  return false;
}

inline bool is_folder(const std::string &dir_name) {
  return is_folder(dir_name.data());
}

std::vector<std::string> for_each_file(const std::string &dir_name,
                                       file_filter_type filter, bool sub) {
  std::vector<std::string> v;
  auto dir = opendir(dir_name.data());
  struct dirent *ent;
  if (dir) {
    while ((ent = readdir(dir)) != NULL) {
      auto p =
          std::string(dir_name).append({file_sepator()}).append(ent->d_name);
      if (sub) {
        if (0 == strcmp(ent->d_name, "..") || 0 == strcmp(ent->d_name, ".")) {
          continue;
        } else if (is_folder(p)) {
          auto r = for_each_file(p, filter, sub);
          v.insert(v.end(), r.begin(), r.end());
          continue;
        }
      }
      if (sub || !is_folder(p))
        if (filter(dir_name.data(), ent->d_name)) v.emplace_back(p);
    }
    closedir(dir);
  }
  return v;
}

std::vector<std::string> for_each_folder(const std::string &dir_name,
                                         file_filter_type filter, bool sub) {
  std::vector<std::string> v;
  auto dir = opendir(dir_name.data());
  struct dirent *ent;
  if (dir) {
    while ((ent = readdir(dir)) != NULL) {
      auto p =
          std::string(dir_name).append({file_sepator()}).append(ent->d_name);
      if (sub) {
        if (0 == strcmp(ent->d_name, "..") || 0 == strcmp(ent->d_name, ".")) {
          continue;
        } else if (is_folder(p)) {
          auto r = for_each_folder(p, filter, sub);
          v.insert(v.end(), r.begin(), r.end());
          continue;
        }
      }
      if (sub || is_folder(p))
        if (filter(dir_name.data(), ent->d_name) &&
            0 != strcmp(ent->d_name, "..") && 0 != strcmp(ent->d_name, "."))
          v.emplace_back(ent->d_name);
    }
    closedir(dir);
  }
  return v;
}

file_filter_type default_ls_filter = [](const char *, const char *) {
  return true;
};

std::vector<std::string> ls(const std::string &dir_name, bool sub) {
  return for_each_file(dir_name, default_ls_filter, sub);
}

std::vector<std::string> ls_folder(const std::string &dir_name, bool sub) {
  return for_each_folder(dir_name, default_ls_filter, sub);
}

ssize_t write_n(int fd, const char *buf, ssize_t n_bytes, int retry) {
  ssize_t write_bytes = 0;
  do {
    ssize_t ret = write(fd, (void *)(buf + write_bytes), n_bytes - write_bytes);
    if (ret < 0) {
      return -1;
    }
    write_bytes += ret;
    if (write_bytes >= n_bytes) {
      return n_bytes;
    }
  } while (retry == -1 || retry-- > 0);
  return write_bytes;
}

std::string join(const std::vector<std::string> &strs, char separator) {
  std::stringstream ss;
  ss << "[";
  for (size_t i = 0; i < strs.size(); i++) {
    if (i != 0) {
      ss << separator;
    }
    ss << strs[i];
  }
  ss << "]";
  return ss.str();
}

MEM_PACK *get_memoccupy() {
  FILE *fd;
  double mem_total, mem_used_rate;
  char buff[256];
  MEM_OCCUPY *m = (MEM_OCCUPY *)malloc(sizeof(MEM_OCCUPY));
  MEM_PACK *p = (MEM_PACK *)malloc(sizeof(MEM_PACK));
  fd = fopen("/proc/meminfo", "r");

  fgets(buff, sizeof(buff), fd);
  sscanf(buff, "%s %lu %s\n", m->name, &m->total, m->name2);
  mem_total = m->total;
  fgets(buff, sizeof(buff), fd);
  fgets(buff, sizeof(buff), fd);
  sscanf(buff, "%s %lu %s\n", m->name, &m->total, m->name2);
  mem_used_rate = (1 - m->total / mem_total) * 100;
  mem_total = mem_total / (1024 * 1024);
  p->total = mem_total;
  p->available = (double)m->total / (1024 * 1024);
  p->used_rate = mem_used_rate;
  fclose(fd);
  free(m);
  return p;
}

JsonParser::JsonParser() { content_ = cJSON_CreateObject(); }

JsonParser::~JsonParser() {
  if (content_) {
    cJSON_Delete(content_);
  }
}

int JsonParser::Parse(const char *str) {
  if (content_) {
    cJSON_Delete(content_);
  }
  content_ = cJSON_Parse(str);
  if (content_ == nullptr) {
    return -1;
  }
  return 0;
}

int JsonParser::GetInt(const std::string &name, int &value) const {
  cJSON *jvalue = cJSON_GetObjectItemCaseSensitive(content_, name.c_str());
  if (jvalue == nullptr || !cJSON_IsNumber(jvalue)) return -1;
  value = jvalue->valueint;
  return 0;
}

int JsonParser::GetDouble(const std::string &name, double &value) const {
  cJSON *jvalue = cJSON_GetObjectItemCaseSensitive(content_, name.c_str());
  if (jvalue == nullptr || !cJSON_IsNumber(jvalue)) return -1;
  value = jvalue->valuedouble;
  return 0;
}

int JsonParser::GetString(const std::string &name, std::string &value) const {
  cJSON *jvalue = cJSON_GetObjectItemCaseSensitive(content_, name.c_str());
  if (jvalue == nullptr || !cJSON_IsString(jvalue)) return -1;
  value.assign(jvalue->valuestring);
  return 0;
}

int JsonParser::GetBool(const std::string &name, bool &value) const {
  cJSON *jvalue = cJSON_GetObjectItemCaseSensitive(content_, name.c_str());
  if (jvalue == nullptr || !cJSON_IsBool(jvalue)) return -1;
  value = jvalue->type == cJSON_True;
  return 0;
}

int JsonParser::GetObject(const std::string &name, JsonParser &value) const {
  cJSON *jvalue = cJSON_GetObjectItemCaseSensitive(content_, name.c_str());
  if (jvalue == nullptr || !cJSON_IsObject(jvalue)) return -1;
  cJSON *dup = cJSON_Duplicate(jvalue, cJSON_True);
  if (dup == NULL) return -1;
  value.Reset(dup);
  return 0;
}

bool JsonParser::Contains(const std::string &name) const {
  cJSON *jvalue = cJSON_GetObjectItemCaseSensitive(content_, name.c_str());
  if (jvalue == nullptr) return false;
  return true;
}

int JsonParser::PutString(const std::string &name, const std::string &value) {
  if (!cJSON_AddStringToObject(content_, name.c_str(), value.c_str())) {
    return -1;
  }
  return 0;
}

int JsonParser::PutDouble(const std::string &name, double value) {
  if (!cJSON_AddNumberToObject(content_, name.c_str(), value)) {
    return -1;
  }
  return 0;
}

int JsonParser::PutInt(const std::string &name, int value) {
  return PutDouble(name, static_cast<double>(value));
}

int JsonParser::PutObject(const std::string &name, JsonParser &&jp) {
  int ret = PutObject(name, jp.content_);
  if (ret) return ret;
  jp.content_ = nullptr;
  return 0;
}

int JsonParser::PutObject(const std::string &name, JsonParser &jp) {
  cJSON *dup = cJSON_Duplicate(jp.content_, cJSON_True);
  if (dup == NULL) return -1;
  return PutObject(name, dup);
}

int JsonParser::PutObject(const std::string &name, cJSON *item) {
  cJSON *jvalue = cJSON_GetObjectItemCaseSensitive(content_, name.c_str());
  if (jvalue == nullptr) {
    cJSON_AddItemToObject(content_, name.c_str(), item);
    return 0;
  }
  if (cJSON_ReplaceItemViaPointer(content_, jvalue, item) == cJSON_False) {
    return -1;
  }
  return 0;
}

void JsonParser::Reset(cJSON *content) {
  if (content_) {
    cJSON_Delete(content_);
  }
  content_ = content;
}

void CJsonMergeRight(cJSON *left, cJSON *right) {
  cJSON *r_item = NULL;
  cJSON_ArrayForEach(r_item, right) {
    if (r_item->type == cJSON_Array || r_item->type == cJSON_Object) {
      cJSON *l_item = cJSON_GetObjectItemCaseSensitive(left, r_item->string);
      if (l_item) {
        CJsonMergeRight(l_item, r_item);
      } else {
        cJSON_AddItemToObject(left, r_item->string, r_item);
      }
    } else {
      cJSON_ReplaceItemInObject(left, r_item->string, r_item);
    }
  }
}

void JsonParser::MergeRight(JsonParser &other) {
  CJsonMergeRight(content_, other.content_);
}

std::string JsonParser::ToStr(bool format) const {
  std::string ret;
  char *str = nullptr;
  if (format) {
    str = cJSON_Print(content_);
  } else {
    str = cJSON_PrintUnformatted(content_);
  }
  if (str) {
    ret = str;
    free(str);
  }
  return ret;
}

JsonParser &JsonParser::operator=(const JsonParser &other) {
  cJSON *dup = cJSON_Duplicate(other.content_, cJSON_True);
  if (dup == NULL) throw std::runtime_error("cjson duplicate error");
  content_ = dup;
  return *this;
}

FileIO::FileIO(std::string &file_path) : path(file_path), fp(nullptr) {}
FileIO::~FileIO() {
  if (fp) {
    fclose(fp);
    fp = nullptr;
  }
}

int FileIO::Open(const char *mode) {
  fp = fopen(path.c_str(), mode);
  if (fp == nullptr) {
    return -1;
  }
  return 0;
}

size_t FileIO::Write(const void *data, size_t size, size_t m) {
  return fwrite(data, size, m, fp);
}

size_t FileIO::Read(void *data, size_t size, size_t m) {
  return fread(data, size, m, fp);
}

int64_t StringToInt64(const std::string &src) {
  MD5_CTX ctx;

  std::string md5_string;
  unsigned char md[16] = {0};

  MD5_Init(&ctx);
  MD5_Update(&ctx, src.c_str(), src.size());
  MD5_Final(md, &ctx);

  int64_t a, b;
  memcpy(&a, md, 8);
  memcpy(&b, md + 8, 8);
  return a ^ b;
}

}  // namespace utils
