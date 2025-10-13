/**
 * Copyright 2019 The Gamma Authors.
 *
 * This source code is licensed under the Apache License, Version 2.0 license
 * found in the LICENSE file in the root directory of this source tree.
 */

#include "vector/vector_manager.h"

#include "index/impl/hnswlib/gamma_index_hnswlib.h"
#include "raw_vector_factory.h"
#include "util/utils.h"

namespace vearch {

static bool InnerProductCmp(const VectorDoc *a, const VectorDoc *b) {
  return a->score > b->score;
}

static bool L2Cmp(const VectorDoc *a, const VectorDoc *b) {
  return a->score < b->score;
}

VectorManager::VectorManager(const VectorStorageType &store_type,
                             bitmap::BitmapManager *docids_bitmap,
                             const std::string &root_path, std::string &desc)
    : default_store_type_(store_type),
      docids_bitmap_(docids_bitmap),
      root_path_(root_path) {
  table_created_ = false;
  desc_ = desc + " ";
}

VectorManager::~VectorManager() {
  Close();
  int ret = pthread_rwlock_destroy(&index_rwmutex_);
  if (0 != ret) {
    LOG(ERROR) << "destory read write lock error, ret=" << ret;
  }
}

Status VectorManager::DetermineVectorStorageType(
    std::string index_type, std::string &store_type_str,
    VectorStorageType &store_type) {
  LOG(INFO) << "DetermineVectorStorageType, index_type=" << index_type
            << ", store_type_str=" << store_type_str;
  if (!store_type_str.empty() && store_type_str != "") {
    if (!strcasecmp("MemoryOnly", store_type_str.c_str())) {
      store_type = VectorStorageType::MemoryOnly;
    } else if (!strcasecmp("RocksDB", store_type_str.c_str())) {
      store_type = VectorStorageType::RocksDB;
    } else {
      std::stringstream msg;
      msg << "NO support for store type " << store_type_str;
      LOG(WARNING) << msg.str();
      return Status::NotSupported(msg.str());
    }

    // ivfflat has raw vector data in index, so just use rocksdb to reduce
    // memory footprint
    if (!index_type.empty() && index_type == "IVFFLAT" &&
        strcasecmp("RocksDB", store_type_str.c_str())) {
      std::stringstream msg;
      msg << "IVFFLAT should use RocksDB, now store_type = " << store_type_str;

      LOG(ERROR) << msg.str();
      return Status::ParamError(msg.str());
    }
  } else {
    if (!index_type.empty() && (index_type == "HNSW" || index_type == "FLAT")) {
      store_type = VectorStorageType::MemoryOnly;
      store_type_str = "MemoryOnly";
    } else {
      store_type = VectorStorageType::RocksDB;
      store_type_str = "RocksDB";
    }
  }
  return Status::OK();
}

Status VectorManager::CreateRawVector(struct VectorInfo &vector_info,
                                      std::string index_type, TableInfo &table,
                                      RawVector **vec, int cf_id,
                                      StorageManager *storage_mgr) {
  std::string &vec_name = vector_info.name;
  int dimension = vector_info.dimension;

  std::string &store_type_str = vector_info.store_type;

  VectorStorageType store_type = default_store_type_;
  Status status =
      DetermineVectorStorageType(index_type, store_type_str, store_type);
  if (!status.ok()) {
    LOG(ERROR) << "set vector store type failed, store_type=" << store_type_str
               << ", index_type=" << index_type;
    return status;
  }

  std::string &store_param = vector_info.store_param;

  VectorValueType value_type = VectorValueType::FLOAT;
  if (index_type == "BINARYIVF") {
    value_type = VectorValueType::BINARY;
    dimension /= 8;
  }

  VectorMetaInfo *meta_info =
      new VectorMetaInfo(vec_name, dimension, value_type, 0, desc_);

  StoreParams store_params(meta_info->AbsoluteName());
  if (store_param != "") {
    Status status = store_params.Parse(store_param.c_str());
    if (!status.ok()) {
      delete meta_info;
      return status;
    }
  }

  LOG(INFO) << "store params=" << store_params.ToJsonStr();

  *vec = RawVectorFactory::Create(meta_info, store_type, store_params,
                                  docids_bitmap_, cf_id, storage_mgr);

  if ((*vec) == nullptr) {
    LOG(ERROR) << desc_ << "create raw vector error";
    return Status::IOError(desc_ + "create raw vector error");
  }
  LOG(INFO) << desc_ << "create raw vector success, vec_name[" << vec_name
            << "] store_type[" << store_type_str << "]";
  int ret = (*vec)->Init(vec_name);
  if (ret != 0) {
    std::stringstream msg;
    msg << desc_ << "raw vector " << vec_name << " init error, code [" << ret
        << "]!";
    LOG(ERROR) << msg.str();
    delete (*vec);
    return Status::IOError(msg.str());
  }
  return Status::OK();
}

void VectorManager::DestroyRawVectors() {
  for (const auto &[name, vec] : raw_vectors_) {
    if (vec != nullptr) {
      delete vec;
    }
  }
  raw_vectors_.clear();
  LOG(INFO) << desc_ << "raw vector cleared.";
}

Status VectorManager::CreateVectorIndex(
    std::string &index_type, std::string &index_params, RawVector *vec,
    int training_threshold, bool destroy_vec,
    std::map<std::string, IndexModel *> &vector_indexes) {
  std::string vec_name = vec->MetaInfo()->Name();
  LOG(INFO) << desc_ << "create index model [" << index_type
            << "] for vector: " << vec_name;

  IndexModel *index_model =
      dynamic_cast<IndexModel *>(reflector().GetNewIndex(index_type));
  if (index_model == nullptr) {
    std::stringstream msg;
    msg << desc_ << "cannot get model=" << index_type
        << ", vec_name=" << vec_name;
    LOG(ERROR) << msg.str();
    if (destroy_vec) {
      delete vec;
      raw_vectors_[vec_name] = nullptr;
    }
    return Status::ParamError(msg.str());
  }
  index_model->vector_ = vec;

  Status status = index_model->Init(index_params, training_threshold);
  if (!status.ok()) {
    LOG(ERROR) << desc_ << "gamma index init " << vec_name << " error!";
    if (destroy_vec) {
      delete vec;
      raw_vectors_[vec_name] = nullptr;
    }
    index_model->vector_ = nullptr;
    delete index_model;
    index_model = nullptr;
    return status;
  }
  // init indexed count
  index_model->indexed_count_ = 0;
  vector_indexes[IndexName(vec_name, index_type)] = index_model;

  return Status::OK();
}

void VectorManager::DestroyVectorIndexes() { DestroyVectorIndexes(""); }

void VectorManager::DestroyVectorIndexes(const std::string &index_dir) {
  pthread_rwlock_wrlock(&index_rwmutex_);
  for (const auto &[name, index] : vector_indexes_) {
    if (index != nullptr) {
      delete index;
    }
  }
  vector_indexes_.clear();
  pthread_rwlock_unlock(&index_rwmutex_);

  // Delete index files from disk
  std::string delete_dir = index_dir;
  if (delete_dir.empty()) {
    delete_dir = root_path_ + "/retrieval_model_index";
  }

  if (utils::remove_dir(delete_dir.c_str()) != 0) {
    LOG(WARNING) << desc_ << "failed to remove index directory: " << delete_dir;
  } else {
    LOG(INFO) << desc_ << "removed index directory: " << delete_dir;
  }

  LOG(INFO) << desc_ << "vector indexes cleared.";
}

Status VectorManager::RemoveVectorIndex(const std::string &field_name) {
  pthread_rwlock_wrlock(&index_rwmutex_);

  // Find and remove all indexes related to this field
  std::vector<std::string> indexes_to_remove;
  for (const auto &[name, index] : vector_indexes_) {
    std::string vec_name;
    std::string index_type;
    GetVectorNameAndIndexType(name, vec_name, index_type);

    if (vec_name == field_name) {
      indexes_to_remove.push_back(name);
    }
  }

  // Remove the found indexes
  for (const std::string &index_name : indexes_to_remove) {
    auto it = vector_indexes_.find(index_name);
    if (it != vector_indexes_.end()) {
      if (it->second != nullptr) {
        delete it->second;
      }
      vector_indexes_.erase(it);
      LOG(INFO) << desc_ << "removed vector index: " << index_name;
    }
  }
  index_types_.clear();
  index_params_.clear();

  pthread_rwlock_unlock(&index_rwmutex_);

  // Delete index files from disk
  std::string delete_dir = root_path_ + "/retrieval_model_index";

  if (utils::remove_dir(delete_dir.c_str()) != 0) {
    LOG(WARNING) << desc_ << "failed to remove index directory: " << delete_dir;
  } else {
    LOG(INFO) << desc_ << "removed index directory: " << delete_dir;
  }

  if (indexes_to_remove.empty()) {
    LOG(WARNING) << desc_ << "no vector index found for field: " << field_name;
    return Status::IOError("no vector index found for field: " + field_name);
  }

  LOG(INFO) << desc_ << "successfully removed " << indexes_to_remove.size()
            << " vector index(es) for field: " << field_name;
  return Status::OK();
}

void VectorManager::DescribeVectorIndexes() {
  LOG(INFO) << desc_ << " show vector indexes detail informations";
  pthread_rwlock_rdlock(&index_rwmutex_);
  for (const auto &[name, index] : vector_indexes_) {
    if (index != nullptr) {
      index->Describe();
    }
  }
  pthread_rwlock_unlock(&index_rwmutex_);
}

Status VectorManager::CreateVectorIndexes(
    int training_threshold,
    std::map<std::string, IndexModel *> &vector_indexes) {
  for (const auto &[name, index] : raw_vectors_) {
    if (index != nullptr) {
      std::string &vec_name = index->MetaInfo()->Name();

      for (size_t i = 0; i < index_types_.size(); ++i) {
        Status status =
            CreateVectorIndex(index_types_[i], index_params_[i], index,
                              training_threshold, false, vector_indexes);
        if (!status.ok()) {
          LOG(ERROR) << desc_ << vec_name
                     << " create index failed: " << status.ToString();
          return status;
        }
      }
    }
  }
  return Status::OK();
}

void VectorManager::ResetVectorIndexes(
    std::map<std::string, IndexModel *> &rebuild_vector_indexes) {
  pthread_rwlock_wrlock(&index_rwmutex_);
  for (const auto &[name, index] : vector_indexes_) {
    if (index != nullptr) {
      delete index;
    }
  }
  vector_indexes_.clear();

  for (const auto &[name, index] : rebuild_vector_indexes) {
    if (index != nullptr) {
      vector_indexes_[name] = index;
      LOG(INFO) << desc_ << "set " << name << " index";
    }
  }
  pthread_rwlock_unlock(&index_rwmutex_);
}

Status VectorManager::ReCreateVectorIndexes(int training_threshold) {
  pthread_rwlock_wrlock(&index_rwmutex_);
  for (const auto &[name, index] : vector_indexes_) {
    if (index != nullptr) {
      delete index;
    }
  }
  vector_indexes_.clear();

  Status status = CreateVectorIndexes(training_threshold, vector_indexes_);
  if (!status.ok()) {
    LOG(ERROR) << desc_ << "CreateVectorIndexes failed: " << status.ToString();

    for (const auto &[name, index] : vector_indexes_) {
      if (index != nullptr) {
        delete index;
      }
    }
    vector_indexes_.clear();
  }
  pthread_rwlock_unlock(&index_rwmutex_);
  return status;
}

Status VectorManager::CreateVectorTable(TableInfo &table,
                                        std::vector<int> &vector_cf_ids,
                                        StorageManager *storage_mgr) {
  Status status;
  int ret = pthread_rwlock_init(&index_rwmutex_, NULL);
  if (ret != 0) {
    LOG(ERROR) << desc_ << "init read-write lock error, ret=" << ret;
    return Status::ParamError("create vector read-write lock error");
  }
  if (table_created_) return Status::ParamError("table is created");

  if (table.IndexType() != "") {
    index_types_.push_back(table.IndexType());
    index_params_.push_back(table.IndexParams());
  }

  std::vector<struct VectorInfo> &vectors_infos = table.VectorInfos();

  for (size_t i = 0; i < vectors_infos.size(); i++) {
    Status vec_status;
    RawVector *vec = nullptr;
    struct VectorInfo &vector_info = vectors_infos[i];
    std::string &vec_name = vector_info.name;
    std::string index_type;
    if (index_types_.size() > 0) {
      index_type = index_types_[0];
    }
    vec_status = CreateRawVector(vector_info, index_type, table, &vec,
                                 vector_cf_ids[i], storage_mgr);
    if (!vec_status.ok()) {
      std::stringstream msg;
      msg << desc_ << vec_name
          << " create vector failed:" << vec_status.ToString();
      LOG(ERROR) << msg.str();
      status = Status::ParamError(msg.str());
      return status;
    }

    raw_vectors_[vec_name] = vec;

    if (vector_info.is_index == false) {
      LOG(INFO) << desc_ << vec_name << " need not to indexed!";
      continue;
    }

    for (size_t i = 0; i < index_types_.size(); ++i) {
      status =
          CreateVectorIndex(index_types_[i], index_params_[i], vec,
                            table.TrainingThreshold(), true, vector_indexes_);
      if (!status.ok()) {
        LOG(ERROR) << desc_ << vec_name
                   << " create index failed: " << status.ToString();
        return status;
      }
      // update TrainingThreshold when TrainingThreshold = 0
      if (!table.TrainingThreshold()) {
        IndexModel *index =
            vector_indexes_[IndexName(vec_name, index_types_[i])];
        if (index) {
          table.SetTrainingThreshold(index->training_threshold_);
        }
      }
    }
  }
  table_created_ = true;
  LOG(INFO) << desc_ << "create vectors and indexes success! models="
            << utils::join(index_types_, ',');
  return Status::OK();
}

int VectorManager::AddToStore(
    int docid, std::unordered_map<std::string, struct Field> &fields) {
  int ret = 0;
  if (fields.size() != raw_vectors_.size()) {
    LOG(ERROR) << desc_ << "add to store error: vector fields length ["
               << fields.size() << "] not equal to raw_vectors_ length = "
               << raw_vectors_.size();
    return -1;
  }
  for (auto &[name, field] : fields) {
    if (raw_vectors_.find(name) == raw_vectors_.end()) {
      LOG(ERROR) << desc_ << "cannot find raw vector [" << name << "]";
      continue;
    }
    ret = raw_vectors_[name]->Add(docid, field);
    if (ret != 0) break;
  }
  return ret;
}

int VectorManager::Update(
    int docid, std::unordered_map<std::string, struct Field> &fields) {
  for (auto &[name, field] : fields) {
    auto it = raw_vectors_.find(name);
    if (it == raw_vectors_.end()) {
      continue;
    }
    RawVector *raw_vector = it->second;
    size_t element_size =
        raw_vector->MetaInfo()->DataType() == VectorValueType::BINARY
            ? sizeof(char)
            : sizeof(float);
    if ((size_t)raw_vector->MetaInfo()->Dimension() !=
        field.value.size() / element_size) {
      LOG(ERROR) << desc_ << name
                 << " invalid field value len=" << field.value.size()
                 << ", dimension=" << raw_vector->MetaInfo()->Dimension();
      return -1;
    }

    int ret = raw_vector->Update(docid, field);
    if (ret) return ret;

    pthread_rwlock_rdlock(&index_rwmutex_);
    for (std::string &index_type : index_types_) {
      auto it = vector_indexes_.find(IndexName(name, index_type));
      if (it != vector_indexes_.end()) {
        it->second->updated_vids_.push(docid);
      }
    }
    pthread_rwlock_unlock(&index_rwmutex_);
  }

  return 0;
}

int VectorManager::Delete(int64_t docid) {
  for (const auto &[name, vec] : raw_vectors_) {
    if (0 != vec->Delete(docid)) {
      LOG(ERROR) << desc_ << "delete vector from " << name
                 << " failed! docid=" << docid;
      return -1;
    }
  }
  pthread_rwlock_rdlock(&index_rwmutex_);
  for (const auto &[name, index] : vector_indexes_) {
    std::vector<int64_t> vids;
    vids.resize(1);
    vids[0] = docid;
    if (0 != index->Delete(vids)) {
      LOG(ERROR) << desc_ << "delete index from " << name
                 << " failed! docid=" << docid;
      pthread_rwlock_unlock(&index_rwmutex_);
      return -2;
    }
  }
  pthread_rwlock_unlock(&index_rwmutex_);
  return 0;
}

int VectorManager::TrainIndex(
    std::map<std::string, IndexModel *> &vector_indexes) {
  int ret = 0;
  for (const auto &[name, index] : vector_indexes) {
    if (index->Indexing() != 0) {
      ret = -1;
      LOG(ERROR) << desc_ << "vector table " << name << " indexing failed!";
    }
  }
  return ret;
}

int VectorManager::AddRTVecsToIndex(bool &index_is_dirty) {
  int ret = 0;
  index_is_dirty = false;
  pthread_rwlock_rdlock(&index_rwmutex_);
  for (const auto &[name, index_model] : vector_indexes_) {
    RawVector *raw_vec = dynamic_cast<RawVector *>(index_model->vector_);
    int total_stored_vecs = raw_vec->MetaInfo()->Size();
    int indexed_vec_count = index_model->indexed_count_;

    if (indexed_vec_count > total_stored_vecs) {
      LOG(ERROR) << desc_
                 << "internal error : indexed_vec_count=" << indexed_vec_count
                 << " should not greater than total_stored_vecs="
                 << total_stored_vecs;
      ret = -1;
    } else if (indexed_vec_count == total_stored_vecs) {
#ifdef DEBUG
      LOG(INFO) << "no extra vectors existed for indexing";
#endif
    } else {
      int MAX_NUM_PER_INDEX = 1000;
      int index_count =
          (total_stored_vecs - indexed_vec_count) / MAX_NUM_PER_INDEX + 1;

      for (int i = 0; i < index_count; i++) {
        int64_t start_docid = index_model->indexed_count_;
        size_t count_per_index =
            (i == (index_count - 1) ? total_stored_vecs - start_docid
                                    : MAX_NUM_PER_INDEX);
        if (count_per_index == 0 || count_per_index > MAX_NUM_PER_INDEX || start_docid < indexed_vec_count) break;

        std::vector<int64_t> vids(count_per_index);
        std::iota(vids.begin(), vids.end(), start_docid);
        ScopeVectors vecs;
        int ret = raw_vec->Gets(vids, vecs);
        if (ret) {
          LOG(ERROR) << desc_ << "get vectors from docid " << start_docid
                     << " err: ret=" << ret;
          continue;
        }
        if (vids.size() != count_per_index || vecs.Size() != count_per_index) {
          LOG(ERROR) << desc_ << "get vectors from docid " << start_docid
                     << " err: "
                     << "vids.size()=" << vids.size()
                     << ", vecs.size()=" << vecs.Size();
          continue;
        }
        const uint8_t *add_vec = nullptr;
        utils::ScopeDeleter1<uint8_t> del_vec;

        int raw_d = raw_vec->MetaInfo()->Dimension();
        if (raw_vec->MetaInfo()->DataType() == VectorValueType::BINARY) {
          add_vec = new uint8_t[raw_d * count_per_index];
        } else {
          add_vec = new uint8_t[raw_d * count_per_index * sizeof(float)];
        }
        del_vec.set(add_vec);
        size_t element_size =
            raw_vec->MetaInfo()->DataType() == VectorValueType::BINARY
                ? sizeof(char)
                : sizeof(float);
        for (size_t i = 0; i < vecs.Size(); ++i) {
          if (vecs.Get(i) == nullptr) {
            continue;
          }
          memcpy((void *)(add_vec + i * element_size * raw_d),
                 (void *)vecs.Get(i), element_size * raw_d);
        }

        index_model->start_docid_ = start_docid;
        if (start_docid != index_model->indexed_count_ || !index_model->Add(count_per_index, add_vec)) {
          LOG(ERROR) << desc_ << "add index from docid " << start_docid
                     << " error!";
          ret = -2;
        } else {
          index_model->indexed_count_ += count_per_index;
          index_is_dirty = true;
        }
      }
      if (ret == 0) {
        ret = total_stored_vecs - indexed_vec_count;
      }
    }
    std::vector<int64_t> vids;
    int64_t vid;
    while (index_model->updated_vids_.try_pop(vid)) {
      if (raw_vec->Bitmap()->Test(vid)) continue;
      if (vid >= index_model->indexed_count_) {
        index_model->updated_vids_.push(vid);
        break;
      } else {
        vids.push_back(vid);
      }
      if (vids.size() >= 20000) break;
    }
    if (vids.size() == 0) continue;
    ScopeVectors scope_vecs;
    if (raw_vec->Gets(vids, scope_vecs)) {
      LOG(ERROR) << desc_ << "get update vector error!";
      ret = -3;
      pthread_rwlock_unlock(&index_rwmutex_);
      return ret;
    }
    if (index_model->Update(vids, scope_vecs.Get())) {
      LOG(ERROR) << desc_ << "update index error!";
      ret = -4;
    }
    index_is_dirty = true;
  }
  pthread_rwlock_unlock(&index_rwmutex_);
  return ret;
}

int ParseSearchResult(int n, int k, VectorResult &result, IndexModel *index) {
  RawVector *raw_vec = dynamic_cast<RawVector *>(index->vector_);
  if (raw_vec == nullptr) {
    LOG(ERROR) << "Cannot get raw vector";
    return -1;
  }
  for (int i = 0; i < n; i++) {
    int pos = 0;
    std::map<int, int> docid2count;
    for (int j = 0; j < k; j++) {
      int64_t *docid = result.docids + i * k + j;
      if (docid[0] == -1) continue;
      int vector_id = (int)docid[0];
      if (docid2count.find(vector_id) == docid2count.end()) {
        int real_pos = i * k + pos;
        result.docids[real_pos] = vector_id;
        result.dists[real_pos] = result.dists[i * k + j];
        pos++;
        docid2count[vector_id] = 1;
      }
    }
    if (pos > 0) {
      result.idx[i] = 0;  // init start id of seeking
    }
    for (; pos < k; pos++) {
      result.docids[i * k + pos] = -1;
      result.dists[i * k + pos] = -1;
    }
  }
  return 0;
}

Status VectorManager::Search(GammaQuery &query, GammaResult *results) {
  int n = 0;

  size_t vec_num = query.vec_query.size();
  VectorResult all_vector_results[vec_num];

  query.condition->sort_by_docid = vec_num > 1 ? true : false;
  std::string vec_names[vec_num];
  for (size_t i = 0; i < vec_num; i++) {
    struct VectorQuery &vec_query = query.vec_query[i];

    std::string &name = vec_query.name;
    vec_names[i] = name;

    std::string index_name = name;
    if (index_types_.size() == 0) {
      std::string err = "No index type specified for vector query " + name;
      return Status::InvalidArgument(err);
    }
    std::string index_type = index_types_[0];
    if (index_types_.size() > 1 && vec_query.index_type != "") {
      index_type = vec_query.index_type;
    }
    index_name = IndexName(name, index_type);

    pthread_rwlock_rdlock(&index_rwmutex_);

    std::map<std::string, IndexModel *>::iterator iter =
        vector_indexes_.find(index_name);
    if (iter == vector_indexes_.end()) {
      std::string err = "Query name " + index_name +
                        " not exist in created vector table " + desc_;
      LOG(ERROR) << err;
      pthread_rwlock_unlock(&index_rwmutex_);
      return Status::InvalidArgument(err);
    }

    IndexModel *index = iter->second;
    RawVector *raw_vec = dynamic_cast<RawVector *>(iter->second->vector_);
    int d = raw_vec->MetaInfo()->Dimension();
    if (raw_vec->MetaInfo()->DataType() == VectorValueType::BINARY) {
      n = vec_query.value.size() / d;
    } else {
      n = vec_query.value.size() / (raw_vec->MetaInfo()->DataSize() * d);
    }

    if (n <= 0) {
      std::string err = "Search n shouldn't less than 0!";
      LOG(ERROR) << err;
      pthread_rwlock_unlock(&index_rwmutex_);
      return Status::InvalidArgument(err);
    }

    all_vector_results[i].init(n, query.condition->topn);

    query.condition->Init(vec_query.min_score, vec_query.max_score,
                          docids_bitmap_, raw_vec);
    if (query.condition->retrieval_params_ == nullptr) {
      query.condition->retrieval_params_ =
          index->Parse(query.condition->index_params);
    }
    query.condition->metric_type =
        query.condition->retrieval_params_->GetDistanceComputeType();

    const uint8_t *x =
        reinterpret_cast<const uint8_t *>(vec_query.value.c_str());
    int ret_vec = index->Search(query.condition, n, x, query.condition->topn,
                                all_vector_results[i].dists,
                                all_vector_results[i].docids);

    if (ret_vec != 0) {
      std::string err = desc_ + "faild search of query " + index_name;
      LOG(ERROR) << err;
      pthread_rwlock_unlock(&index_rwmutex_);
      return Status::InvalidArgument(err);
    }
    if (query.condition->sort_by_docid) {
      ParseSearchResult(n, query.condition->topn, all_vector_results[i], index);
      all_vector_results[i].sort_by_docid();
    }
    pthread_rwlock_unlock(&index_rwmutex_);
#ifdef PERFORMANCE_TESTING
    if (query.condition->GetPerfTool()) {
      std::string msg;
      msg += "search " + index_name;
      query.condition->GetPerfTool()->Perf(msg);
    }
#endif
  }

  if (query.condition->sort_by_docid) {
    for (int i = 0; i < n; i++) {
      int start_docid = 0, common_idx = 0;
      size_t common_docid_count = 0;
      double score = 0;
      bool has_common_docid = true;
      results[i].init(query.condition->topn, vec_names, vec_num);

      while (start_docid < INT_MAX) {
        for (size_t j = 0; j < vec_num; j++) {
          float vec_dist = 0;
          int cur_docid = all_vector_results[j].seek(i, start_docid, vec_dist);
          if (cur_docid == start_docid) {
            common_docid_count++;
            // now just support WeightedRanker
            WeightedRanker *ranker =
                dynamic_cast<WeightedRanker *>(query.condition->ranker);
            float weight = 1.0 / vec_num;
            if (ranker != nullptr && ranker->weights.size() == vec_num)
              weight = ranker->weights[j];
            double field_score = vec_dist * weight;
            score += field_score;
            results[i].docs[common_idx]->fields[j].score = field_score;
            if (common_docid_count == vec_num) {
              results[i].docs[common_idx]->docid = start_docid;
              results[i].docs[common_idx++]->score = score;
              results[i].total = all_vector_results[j].total[i] > 0
                                     ? all_vector_results[j].total[i]
                                     : results[i].total;

              start_docid++;
              common_docid_count = 0;
              score = 0;
            }
          } else if (cur_docid > start_docid) {
            common_docid_count = 0;
            start_docid = cur_docid;
            score = 0;
          } else {
            has_common_docid = false;
            break;
          }
        }
        if (!has_common_docid) break;
      }
      results[i].results_count = common_idx;
      if (query.condition->multi_vector_rank) {
        switch (query.condition->metric_type) {
          case DistanceComputeType::INNER_PRODUCT:
            std::sort(results[i].docs, results[i].docs + common_idx,
                      InnerProductCmp);
            break;
          case DistanceComputeType::L2:
            std::sort(results[i].docs, results[i].docs + common_idx, L2Cmp);
            break;
          default:
            LOG(ERROR) << "invalid metric_type="
                       << (int)query.condition->metric_type;
        }
      }
    }
  } else {
    for (int i = 0; i < n; i++) {
      // double score = 0;
      results[i].init(query.condition->topn, vec_names, vec_num);
      results[i].total = all_vector_results[0].total[i] > 0
                             ? all_vector_results[0].total[i]
                             : results[i].total;
      int pos = 0, topn = all_vector_results[0].topn;
      for (int j = 0; j < topn; j++) {
        int real_pos = i * topn + j;
        if (all_vector_results[0].docids[real_pos] == -1) continue;
        results[i].docs[pos]->docid = all_vector_results[0].docids[real_pos];

        double score = all_vector_results[0].dists[real_pos];

        results[i].docs[pos]->fields[0].score = score;
        results[i].docs[pos]->score = score;
        pos++;
      }
      results[i].results_count = pos;
    }
  }

#ifdef PERFORMANCE_TESTING
  if (query.condition->GetPerfTool()) {
    query.condition->GetPerfTool()->Perf("merge result");
  }
#endif
  return Status::OK();
}

int VectorManager::GetVector(
    const std::vector<std::pair<std::string, int>> &fields_ids,
    std::vector<std::string> &vec) {
  for (const auto &pair : fields_ids) {
    const std::string &field = pair.first;
    const int id = pair.second;
    auto iter = raw_vectors_.find(field);
    if (iter == raw_vectors_.end()) {
      continue;
    }
    RawVector *raw_vec = iter->second;
    if (raw_vec == nullptr) {
      LOG(ERROR) << desc_ << "raw_vec is null!";
      return -1;
    }

    ScopeVector scope_vec;
    int ret = raw_vec->GetVector(id, scope_vec);
    if (ret != 0) {
      LOG(ERROR) << desc_ << "get vector err: " << ret;
      return ret;
    }
    int d = raw_vec->MetaInfo()->Dimension();
    int d_byte = d * raw_vec->MetaInfo()->DataSize();
    vec.emplace_back(std::string((const char *)scope_vec.Get(), d_byte));
  }
  return 0;
}

int VectorManager::GetDocVector(int docid, std::string &field_name,
                                std::vector<uint8_t> &vec) {
  auto iter = raw_vectors_.find(field_name);
  if (iter == raw_vectors_.end()) {
    return -1;
  }
  RawVector *raw_vec = iter->second;
  if (raw_vec == nullptr) {
    LOG(ERROR) << desc_ << "raw_vec is null!";
    return -1;
  }

  ScopeVector scope_vec;
  int ret = raw_vec->GetVector(docid, scope_vec);
  if (ret != 0) {
    return ret;
  }
  const float *feature = (const float *)(scope_vec.Get());
  if (feature == nullptr) {
    LOG(ERROR) << desc_ << "vector is null!";
    return -1;
  }

  int d = raw_vec->MetaInfo()->Dimension();
  int d_byte = d * raw_vec->MetaInfo()->DataSize();

  vec.resize(d_byte);

  memcpy((void *)(vec.data()), feature, d_byte);
  return 0;
}

void VectorManager::GetTotalMemBytes(long &index_total_mem_bytes,
                                     long &vector_total_mem_bytes) {
  pthread_rwlock_rdlock(&index_rwmutex_);
  for (const auto &iter : vector_indexes_) {
    index_total_mem_bytes += iter.second->GetTotalMemBytes();
  }
  pthread_rwlock_unlock(&index_rwmutex_);

  for (const auto &iter : raw_vectors_) {
    vector_total_mem_bytes += iter.second->GetTotalMemBytes();
  }
}

int VectorManager::Dump(const std::string &path, int64_t dump_docid,
                        int64_t max_docid) {
  pthread_rwlock_rdlock(&index_rwmutex_);
  std::map<std::string, int64_t> vector_index_counts;
  for (const auto &[name, index] : vector_indexes_) {
    std::string vec_name, index_type;
    GetVectorNameAndIndexType(name, vec_name, index_type);
    vector_index_counts[vec_name] = index->indexed_count_;
    Status status = index->Dump(path);
    if (!status.ok()) {
      LOG(ERROR) << desc_ << "vector " << name << " dump gamma index failed!";
      pthread_rwlock_unlock(&index_rwmutex_);
      return status.code();
    }
    LOG(INFO) << desc_ << "vector " << name << " dump gamma index success!";
  }
  pthread_rwlock_unlock(&index_rwmutex_);

  for (const auto &[name, vec] : raw_vectors_) {
    RawVector *raw_vector = dynamic_cast<RawVector *>(vec);
    if (raw_vector->WithIO()) {
      auto start = dump_docid;
      auto end = max_docid;
      Status status = raw_vector->Dump(start, end + 1);
      if (!status.ok()) {
        LOG(ERROR) << desc_ << "vector " << name << " dump failed!";
        return status.code();
      }
      int64_t vector_index_count = 0;
      if (vector_index_counts.find(name) != vector_index_counts.end()) {
        vector_index_count = vector_index_counts[name];
      }
      status = raw_vector->SetVectorIndexCount(vector_index_count);
      if (!status.ok()) {
        LOG(ERROR) << desc_ << "vector " << name
                   << " set vector index count failed!";
        return status.code();
      }
      LOG(INFO) << desc_ << "vector " << name << " dump success, "
                << "start=" << start << ", end=" << end
                << ", vector_index_count=" << vector_index_count;
    }
  }

  return 0;
}

int VectorManager::Load(const std::vector<std::string> &index_dirs,
                        int64_t &doc_num) {
  auto min_vec_num = doc_num;
  for (const auto &[name, vec] : raw_vectors_) {
    if (vec->WithIO()) {
      auto vector_num = min_vec_num;
      vec->GetDiskVecNum(vector_num);
      LOG(INFO) << desc_ << name << " load vec_num=" << vector_num;
      if (vector_num < min_vec_num) min_vec_num = vector_num;
    }
  }

  LOG(INFO) << desc_ << "vector_mgr load min_vec_num=" << min_vec_num;
  std::unordered_map<std::string, bool> vecs_has_delete;
  std::unordered_map<std::string, int64_t> vec_index_counts;
  for (const auto &[name, vec] : raw_vectors_) {
    if (vec->WithIO()) {
      // TODO: doc num to vector num
      int64_t vec_num = doc_num;
      int64_t real_load_num = 0;
      Status status = vec->Load(vec_num, real_load_num);
      if (!status.ok()) {
        LOG(ERROR) << desc_ << "vector [" << name << "] load failed!";
        return status.code();
      }
      int64_t vector_index_count = 0;
      vec->GetVectorIndexCount(vector_index_count);
      vec_index_counts[name] = vector_index_count;

      LOG(INFO) << desc_ << "vector [" << name
                << "] load success, vec_num=" << vec_num
                << ", real_load_num=" << real_load_num << ", "
                << "vector_index_count=" << vector_index_count;
      if (real_load_num < vec_num) {
        vecs_has_delete[name] = true;
      } else if (real_load_num == vec_num) {
        vecs_has_delete[name] = false;
      } else {
        LOG(ERROR) << desc_ << "vector [" << name
                   << "] load num=" << real_load_num
                   << " >= vec_num=" << vec_num;
        return -1;
      }
    }
  }

  if (index_dirs.size() > 0) {
    for (const auto &[name, index] : vector_indexes_) {
      int64_t load_num = 0;
      std::string vec_name, index_type;
      GetVectorNameAndIndexType(name, vec_name, index_type);

      int64_t vector_index_count = 0;
      if (vec_index_counts.find(vec_name) != vec_index_counts.end()) {
        vector_index_count = vec_index_counts[vec_name];
      } else {
        LOG(ERROR) << desc_ << "vector index [" << name
                   << "] not found in raw_vectors, vec_name=" << vec_name
                   << ", index_type=" << index_type;
        return -1;
      }
      bool has_delete = false;
      if (vecs_has_delete.find(vec_name) != vecs_has_delete.end()) {
        if (vecs_has_delete[vec_name]) {
          has_delete = true;
        }
      } else {
        LOG(ERROR) << desc_ << "vector index [" << name
                   << "] not found in raw_vectors, vec_name=" << vec_name
                   << ", index_type=" << index_type;
        return -1;
      }
      // old version or don't dump vector_index_num
      if (vector_index_count < 0) {
        if (has_delete) {
          LOG(INFO) << desc_ << "vector index [" << name
                    << "] vector_index_count=" << vector_index_count
                    << ", rebuild index";
          continue;
        }
      }
      GammaFLATIndex *flat_index = dynamic_cast<GammaFLATIndex *>(index);
      if (flat_index != nullptr) {
        GammaIndexHNSWLIB *hnsw_index =
            dynamic_cast<GammaIndexHNSWLIB *>(flat_index);
        if (hnsw_index != nullptr) {
          if (has_delete) {
            LOG(INFO) << desc_ << "vector index [" << name
                      << "] has delete, skip load index";
            continue;
          }
        }
      }

      Status status = index->Load(index_dirs[0], load_num);
      if (!status.ok()) {
        LOG(ERROR) << desc_ << "vector [" << name << "] load index "
                   << index_dirs[0] << " failed, num: " << load_num;
        return -1;
      }
      if (load_num > 0 && load_num > doc_num) {
        LOG(ERROR) << desc_ << "load vec_index_num=" << load_num
                   << " > raw_vec_num=" << doc_num;
        return -1;
      }
      if (has_delete) {
        index->indexed_count_ = vector_index_count;
      } else {
        index->indexed_count_ = load_num;
      }
      LOG(INFO) << desc_ << "vector [" << name
                << "] load index success, load_num=" << load_num
                << ", vec_index_count=" << vector_index_count
                << ", has_delete=" << has_delete;
    }
  }
  LOG(INFO) << desc_ << "vector_mgr load vec_num=" << doc_num;
  return 0;
}

bool VectorManager::Contains(std::string &field_name) {
  return raw_vectors_.find(field_name) != raw_vectors_.end();
}

void VectorManager::Close() {
  DestroyRawVectors();

  DestroyVectorIndexes();

  LOG(INFO) << desc_ << "VectorManager closed.";
}

Status VectorManager::CompactVector() {
  for (const auto &[name, vec] : raw_vectors_) {
    RawVector *raw_vector = dynamic_cast<RawVector *>(vec);
    if (raw_vector->CompactIfNeed()) {
      Status status = raw_vector->Compact();
      if (!status.ok()) {
        LOG(ERROR) << desc_ << "vector " << name << " dump failed!";
        return status;
      }
      LOG(INFO) << desc_ << "vector " << name << " dump success!";
    }
  }

  return Status::OK();
}

void VectorManager::ResetIndexTypesAndParams() {
  index_types_.clear();
  index_params_.clear();
  LOG(INFO) << desc_ << "Reset index types and parameters";
}

void VectorManager::AddIndexTypeAndParam(const std::string &index_type,
                                         const std::string &index_param) {
  index_types_.push_back(index_type);
  index_params_.push_back(index_param);
  LOG(INFO) << desc_ << "Added index type: " << index_type
            << ", index param: " << index_param;
}

int VectorManager::MinIndexedNum() {
  int min = 0;
  pthread_rwlock_rdlock(&index_rwmutex_);
  for (const auto &[name, index] : vector_indexes_) {
    if (index != nullptr && (index->indexed_count_ < min || min == 0)) {
      min = index->indexed_count_;
    }
  }
  pthread_rwlock_unlock(&index_rwmutex_);
  return min;
}

}  // namespace vearch
