/**
 * Copyright 2019 The Gamma Authors.
 *
 * This source code is licensed under the Apache License, Version 2.0 license
 * found in the LICENSE file in the root directory of this source tree.
 */

#include "vector/vector_manager.h"

#include "raw_vector_factory.h"
#include "util/utils.h"

namespace tig_gamma {

static bool InnerProductCmp(const VectorDoc *a, const VectorDoc *b) {
  return a->score > b->score;
}

static bool L2Cmp(const VectorDoc *a, const VectorDoc *b) {
  return a->score < b->score;
}

VectorManager::VectorManager(const VectorStorageType &store_type,
                             bitmap::BitmapManager *docids_bitmap,
                             const std::string &root_path)
    : default_store_type_(store_type),
      docids_bitmap_(docids_bitmap),
      root_path_(root_path) {
  table_created_ = false;
}

VectorManager::~VectorManager() { Close(); }

int VectorManager::SetVectorStoreType(std::string &retrieval_type,
                                      std::string &store_type_str,
                                      VectorStorageType &store_type) {
  if (store_type_str != "") {
    if (!strcasecmp("MemoryOnly", store_type_str.c_str())) {
      store_type = VectorStorageType::MemoryOnly;
    } else if (!strcasecmp("RocksDB", store_type_str.c_str())) {
      store_type = VectorStorageType::RocksDB;
    } else {
      LOG(WARNING) << "NO support for store type " << store_type_str;
      return -1;
    }
    // ivfflat has raw vector data in index, so just use rocksdb to reduce
    // memory footprint
    if (retrieval_type == "IVFFLAT" &&
        strcasecmp("RocksDB", store_type_str.c_str())) {
      LOG(ERROR) << "IVFFLAT should use RocksDB, now store_type = "
                 << store_type_str;
      return -1;
    }
  } else {
    if (retrieval_type == "HNSW" || retrieval_type == "FLAT") {
      store_type = VectorStorageType::MemoryOnly;
      store_type_str = "MemoryOnly";
    } else {
      store_type = VectorStorageType::RocksDB;
      store_type_str = "RocksDB";
    }
  }
  return 0;
}

int VectorManager::CreateRawVector(struct VectorInfo &vector_info,
                                   std::string &retrieval_type,
                                   std::map<std::string, int> &vec_dups,
                                   TableInfo &table,
                                   utils::JsonParser &vectors_jp,
                                   RawVector **vec) {
  std::string &vec_name = vector_info.name;
  int dimension = vector_info.dimension;

  std::string &store_type_str = vector_info.store_type;

  VectorStorageType store_type = default_store_type_;
  if (SetVectorStoreType(retrieval_type, store_type_str, store_type)) {
    LOG(ERROR) << "set vector store type failed, store_type=" << store_type_str
               << ", retrieval_type=" << retrieval_type;
    return -1;
  }

  std::string &store_param = vector_info.store_param;

  VectorValueType value_type = VectorValueType::FLOAT;
  if (retrieval_type == "BINARYIVF") {
    value_type = VectorValueType::BINARY;
    dimension /= 8;
  }

  std::string vec_root_path = root_path_ + "/vectors";
  if (utils::make_dir(vec_root_path.c_str())) {
    LOG(ERROR) << "make directory error, path=" << vec_root_path;
    return -2;
  }
  VectorMetaInfo *meta_info =
      new VectorMetaInfo(vec_name, dimension, value_type);

  StoreParams store_params(meta_info->AbsoluteName());
  if (store_param != "" && store_params.Parse(store_param.c_str())) {
    delete meta_info;
    return PARAM_ERR;
  }

  LOG(INFO) << "store params=" << store_params.ToJsonStr();
  if (vectors_jp.Contains(meta_info->AbsoluteName())) {
    utils::JsonParser vec_jp;
    vectors_jp.GetObject(meta_info->AbsoluteName(), vec_jp);
    StoreParams disk_store_params(meta_info->AbsoluteName());
    disk_store_params.Parse(vec_jp);
    store_params.MergeRight(disk_store_params);
    LOG(INFO) << "after merge, store parameters [" << store_params.ToJsonStr()
              << "]";
  }

  *vec = RawVectorFactory::Create(meta_info, store_type, vec_root_path,
                                  store_params, docids_bitmap_);

  if ((*vec) == nullptr) {
    LOG(ERROR) << "create raw vector error";
    return -1;
  }
  LOG(INFO) << "create raw vector success, vec_name[" << vec_name
            << "] store_type[" << store_type_str << "]";
  bool multi_vids = vec_dups[vec_name] > 1 ? true : false;
  int ret = (*vec)->Init(vec_name, multi_vids);
  if (ret != 0) {
    LOG(ERROR) << "Raw vector " << vec_name << " init error, code [" << ret
               << "]!";
    RawVectorIO *rio = (*vec)->GetIO();
    if (rio) {
      delete rio;
      rio = nullptr;
    }
    delete (*vec);
    return -1;
  }
  return 0;
}

void VectorManager::DestroyRawVectors() {
  for (const auto &[name, vec] : raw_vectors_) {
    if (vec != nullptr) {
      RawVectorIO *rio = vec->GetIO();
      if (rio) {
        delete rio;
      }
      delete vec;
    }
  }
  raw_vectors_.clear();
  LOG(INFO) << "Raw vector cleared.";
}

int VectorManager::CreateVectorIndex(
    std::string &retrieval_type, std::string &retrieval_param, RawVector *vec,
    int indexing_size, bool destroy_vec,
    std::map<std::string, RetrievalModel *> &vector_indexes) {
  std::string vec_name = vec->MetaInfo()->Name();
  LOG(INFO) << "Create index model [" << retrieval_type
            << "] for vector: " << vec_name;

  RetrievalModel *retrieval_model =
      dynamic_cast<RetrievalModel *>(reflector().GetNewModel(retrieval_type));
  if (retrieval_model == nullptr) {
    LOG(ERROR) << "Cannot get model=" << retrieval_type
               << ", vec_name=" << vec_name;
    if (destroy_vec) {
      RawVectorIO *rio = vec->GetIO();
      if (rio) {
        delete rio;
        rio = nullptr;
        vec->SetIO(rio);
      }
      delete vec;
      raw_vectors_[vec_name] = nullptr;
    }
    return -1;
  }
  retrieval_model->vector_ = vec;

  if (retrieval_model->Init(retrieval_param, indexing_size) != 0) {
    LOG(ERROR) << "gamma index init " << vec_name << " error!";
    if (destroy_vec) {
      RawVectorIO *rio = vec->GetIO();
      if (rio) {
        delete rio;
        rio = nullptr;
        vec->SetIO(rio);
      }
      delete vec;
      raw_vectors_[vec_name] = nullptr;
    }
    retrieval_model->vector_ = nullptr;
    delete retrieval_model;
    retrieval_model = nullptr;
    return -1;
  }
  // init indexed count
  retrieval_model->indexed_count_ = 0;
  vector_indexes[IndexName(vec_name, retrieval_type)] = retrieval_model;

  return 0;
}

void VectorManager::DestroyVectorIndexes() {
  for (const auto &[name, index] : vector_indexes_) {
    if (index != nullptr) {
      delete index;
    }
  }
  vector_indexes_.clear();
  LOG(INFO) << "Vector indexes cleared.";
}

void VectorManager::DescribeVectorIndexes() {
  LOG(INFO) << " show vector indexes detail informations";
  for (const auto &[name, index] : vector_indexes_) {
    if (index != nullptr) {
      index->Describe();
    }
  }
}

int VectorManager::CreateVectorIndexes(
    int indexing_size,
    std::map<std::string, RetrievalModel *> &vector_indexes) {
  int ret = 0;
  for (const auto &[name, index] : raw_vectors_) {
    if (index != nullptr) {
      std::string &vec_name = index->MetaInfo()->Name();

      for (size_t i = 0; i < retrieval_types_.size(); ++i) {
        ret = CreateVectorIndex(retrieval_types_[i], retrieval_params_[i],
                                index, indexing_size, false, vector_indexes);
        if (ret) {
          LOG(ERROR) << vec_name << " create index failed ret: " << ret;
          return ret;
        }
      }
    }
  }
  return 0;
}

void VectorManager::SetVectorIndexes(
    std::map<std::string, RetrievalModel *> &rebuild_vector_indexes) {
  for (const auto &[name, index] : rebuild_vector_indexes) {
    if (index != nullptr) {
      vector_indexes_[name] = index;
      LOG(INFO) << "Set " << name << " index";
    }
  }
}

int VectorManager::CreateVectorTable(TableInfo &table,
                                     utils::JsonParser *meta_jp) {
  if (table_created_) return -1;

  std::map<std::string, int> vec_dups;

  std::vector<struct VectorInfo> &vectors_infos = table.VectorInfos();

  for (struct VectorInfo &vectors_info : vectors_infos) {
    std::string &name = vectors_info.name;
    auto it = vec_dups.find(name);
    if (it == vec_dups.end()) {
      vec_dups[name] = 1;
    } else {
      ++vec_dups[name];
    }
  }

  utils::JsonParser vectors_jp;
  if (meta_jp) {
    meta_jp->GetObject("vectors", vectors_jp);
  }

  if (table.RetrievalType() != "") {
    retrieval_types_.push_back(table.RetrievalType());
    retrieval_params_.push_back(table.RetrievalParam());
  }

  for (size_t i = 0; i < vectors_infos.size(); i++) {
    int ret = 0;
    RawVector *vec = nullptr;
    struct VectorInfo &vector_info = vectors_infos[i];
    std::string &vec_name = vector_info.name;
    ret = CreateRawVector(vector_info, retrieval_types_[0], vec_dups, table,
                          vectors_jp, &vec);
    if (ret) {
      LOG(ERROR) << vec_name << " create vector failed ret:" << ret;
      return ret;
    }

    raw_vectors_[vec_name] = vec;

    if (vector_info.is_index == false) {
      LOG(INFO) << vec_name << " need not to indexed!";
      continue;
    }

    for (size_t i = 0; i < retrieval_types_.size(); ++i) {
      ret = CreateVectorIndex(retrieval_types_[i], retrieval_params_[i], vec,
                              table.IndexingSize(), true, vector_indexes_);
      if (ret) {
        LOG(ERROR) << vec_name << " create index failed ret: " << ret;
        return ret;
      }
    }
  }
  table_created_ = true;
  LOG(INFO) << "create vectors and indexes success! models="
            << utils::join(retrieval_types_, ',');
  return 0;
}

int VectorManager::AddToStore(
    int docid, std::unordered_map<std::string, struct Field> &fields) {
  int ret = 0;
  for (auto &[name, field] : fields) {
    if (raw_vectors_.find(name) == raw_vectors_.end()) {
      LOG(ERROR) << "Cannot find raw vector [" << name << "]";
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
      LOG(ERROR) << "invalid field value len=" << field.value.size()
                 << ", dimension=" << raw_vector->MetaInfo()->Dimension();
      return -1;
    }

    int ret = raw_vector->Update(docid, field);
    if (ret) return ret;

    int vid = docid;  // TODO docid to vid
    for (std::string &retrieval_type : retrieval_types_) {
      auto it = vector_indexes_.find(IndexName(name, retrieval_type));
      if (it != vector_indexes_.end()) {
        it->second->updated_vids_.push(vid);
      }
    }
    if (raw_vector->GetIO()) {
      ret = raw_vector->GetIO()->Update(vid);
      if (ret) return ret;
    }
  }

  return 0;
}

int VectorManager::Delete(int docid) {
  for (const auto &[name, index] : vector_indexes_) {
    std::vector<int64_t> vids;
    RawVector *vector = dynamic_cast<RawVector *>(index->vector_);
    vector->VidMgr()->DocID2VID(docid, vids);
    if (0 != index->Delete(vids)) {
      LOG(ERROR) << "delete index from " << name << " failed! docid=" << docid;
      return -1;
    }
  }
  return 0;
}

int VectorManager::TrainIndex(
    std::map<std::string, RetrievalModel *> &vector_indexes) {
  int ret = 0;
  for (const auto &[name, index] : vector_indexes) {
    if (index->Indexing() != 0) {
      ret = -1;
      LOG(ERROR) << "vector table " << name << " indexing failed!";
    }
  }
  return ret;
}

int VectorManager::AddRTVecsToIndex(bool &index_is_dirty) {
  int ret = 0;
  index_is_dirty = false;
  for (const auto &[name, retrieval_model] : vector_indexes_) {
    RawVector *raw_vec = dynamic_cast<RawVector *>(retrieval_model->vector_);
    int total_stored_vecs = raw_vec->MetaInfo()->Size();
    int indexed_vec_count = retrieval_model->indexed_count_;

    if (indexed_vec_count > total_stored_vecs) {
      LOG(ERROR) << "internal error : indexed_vec_count=" << indexed_vec_count
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
        int start_docid = retrieval_model->indexed_count_;
        size_t count_per_index =
            (i == (index_count - 1) ? total_stored_vecs - start_docid
                                    : MAX_NUM_PER_INDEX);
        if (count_per_index == 0) break;

        std::vector<int> lens;
        ScopeVectors vector_head;
        raw_vec->GetVectorHeader(start_docid, count_per_index, vector_head,
                                 lens);
        const uint8_t *add_vec = nullptr;
        utils::ScopeDeleter1<uint8_t> del_vec;

        if (lens.size() == 1) {
          add_vec = vector_head.Get(0);
        } else {
          int raw_d = raw_vec->MetaInfo()->Dimension();
          if (raw_vec->MetaInfo()->DataType() == VectorValueType::BINARY) {
            add_vec = new uint8_t[raw_d * count_per_index];
          } else {
            add_vec = new uint8_t[raw_d * count_per_index * sizeof(float)];
          }
          del_vec.set(add_vec);
          size_t offset = 0;
          size_t element_size =
              raw_vec->MetaInfo()->DataType() == VectorValueType::BINARY
                  ? sizeof(char)
                  : sizeof(float);
          for (size_t i = 0; i < vector_head.Size(); ++i) {
            memcpy((void *)(add_vec + offset), (void *)vector_head.Get(i),
                   element_size * raw_d * lens[i]);

            if (raw_vec->MetaInfo()->DataType() == VectorValueType::BINARY) {
              offset += raw_d * lens[i];
            } else {
              offset += sizeof(float) * raw_d * lens[i];
            }
          }
        }
        if (!retrieval_model->Add(count_per_index, add_vec)) {
          LOG(ERROR) << "add index from docid " << start_docid << " error!";
          ret = -2;
        } else {
          retrieval_model->indexed_count_ += count_per_index;
          index_is_dirty = true;
        }
      }
      if (ret == 0) {
        ret = total_stored_vecs - indexed_vec_count;
      }
    }
    std::vector<int64_t> vids;
    int vid;
    while (retrieval_model->updated_vids_.try_pop(vid)) {
      if (raw_vec->Bitmap()->Test(raw_vec->VidMgr()->VID2DocID(vid))) continue;
      if (vid >= retrieval_model->indexed_count_) {
        retrieval_model->updated_vids_.push(vid);
        break;
      } else {
        vids.push_back(vid);
      }
      if (vids.size() >= 20000) break;
    }
    if (vids.size() == 0) continue;
    ScopeVectors scope_vecs;
    if (raw_vec->Gets(vids, scope_vecs)) {
      LOG(ERROR) << "get update vector error!";
      ret = -3;
      return ret;
    }
    if (retrieval_model->Update(vids, scope_vecs.Get())) {
      LOG(ERROR) << "update index error!";
      ret = -4;
    }
    index_is_dirty = true;
  }
  return ret;
}

namespace {
int parse_index_search_result(int n, int k, VectorResult &result,
                              RetrievalModel *index) {
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
      int real_docid = raw_vec->VidMgr()->VID2DocID(vector_id);
      if (docid2count.find(real_docid) == docid2count.end()) {
        int real_pos = i * k + pos;
        result.docids[real_pos] = real_docid;
        result.dists[real_pos] = result.dists[i * k + j];
        pos++;
        docid2count[real_docid] = 1;
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
}  // namespace

int VectorManager::Search(GammaQuery &query, GammaResult *results) {
  int ret = 0, n = 0;

  size_t vec_num = query.vec_query.size();
  VectorResult all_vector_results[vec_num];

  query.condition->sort_by_docid = vec_num > 1 ? true : false;
  std::string vec_names[vec_num];
  for (size_t i = 0; i < vec_num; i++) {
    struct VectorQuery &vec_query = query.vec_query[i];

    std::string &name = vec_query.name;
    vec_names[i] = name;

    std::string index_name = name;
    std::string retrieval_type = retrieval_types_[0];
    if (retrieval_types_.size() > 1 && vec_query.retrieval_type != "") {
      retrieval_type = vec_query.retrieval_type;
    }
    index_name = IndexName(name, retrieval_type);
    std::map<std::string, RetrievalModel *>::iterator iter =
        vector_indexes_.find(index_name);
    if (iter == vector_indexes_.end()) {
      LOG(ERROR) << "Query name " << index_name
                 << " not exist in created vector table";
      return -1;
    }

    RetrievalModel *index = iter->second;
    RawVector *raw_vec = dynamic_cast<RawVector *>(iter->second->vector_);
    int d = raw_vec->MetaInfo()->Dimension();
    if (raw_vec->MetaInfo()->DataType() == VectorValueType::BINARY) {
      n = vec_query.value.size() / d;
    } else {
      n = vec_query.value.size() / (raw_vec->MetaInfo()->DataSize() * d);
    }

    if (n <= 0) {
      LOG(ERROR) << "Search n shouldn't less than 0!";
      return -1;
    }

    if (!all_vector_results[i].init(n, query.condition->topn)) {
      LOG(ERROR) << "Query name " << index_name << "init vector result error";
      return -2;
    }

    query.condition->Init(vec_query.min_score, vec_query.max_score,
                          docids_bitmap_, raw_vec);
    query.condition->retrieval_params_ =
        index->Parse(query.condition->retrieval_parameters);
    query.condition->metric_type =
        query.condition->retrieval_params_->GetDistanceComputeType();

    const uint8_t *x =
        reinterpret_cast<const uint8_t *>(vec_query.value.c_str());
    int ret_vec = index->Search(query.condition, n, x, query.condition->topn,
                                all_vector_results[i].dists,
                                all_vector_results[i].docids);

    if (ret_vec != 0) {
      ret = ret_vec;
      LOG(ERROR) << "faild search of query " << index_name;
      return -3;
    } else {
      if (query.condition->sort_by_docid) {
        parse_index_search_result(n, query.condition->topn, all_vector_results[i], index);
        all_vector_results[i].sort_by_docid();
      }
    }
#ifdef PERFORMANCE_TESTING
    std::string msg;
    msg += "search " + index_name;
    query.condition->GetPerfTool().Perf(msg);
#endif
  }

  if (query.condition->sort_by_docid) {
    for (int i = 0; i < n; i++) {
      int start_docid = 0, common_idx = 0;
      size_t common_docid_count = 0;
      double score = 0;
      bool has_common_docid = true;
      if (!results[i].init(query.condition->topn, vec_names, vec_num)) {
        LOG(ERROR) << "init gamma result(sort by docid) error, topn="
                   << query.condition->topn << ", vector number=" << vec_num;
        return -4;
      }
      while (start_docid < INT_MAX) {
        for (size_t j = 0; j < vec_num; j++) {
          float vec_dist = 0;
          int cur_docid = all_vector_results[j].seek(i, start_docid, vec_dist);
          if (cur_docid == start_docid) {
            common_docid_count++;
            double field_score = query.vec_query[j].has_boost == 1
                                     ? (vec_dist * query.vec_query[j].boost)
                                     : vec_dist;
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
      if (!results[i].init(query.condition->topn, vec_names, vec_num)) {
        LOG(ERROR) << "init gamma result error, topn=" << query.condition->topn
                   << ", vector number=" << vec_num;
        return -5;
      }
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
  query.condition->GetPerfTool().Perf("merge result");
#endif
  return ret;
}

int VectorManager::GetVector(
    const std::vector<std::pair<std::string, int>> &fields_ids,
    std::vector<std::string> &vec, bool is_bytearray) {
  for (const auto &pair : fields_ids) {
    const std::string &field = pair.first;
    const int id = pair.second;
    auto iter = raw_vectors_.find(field);
    if (iter == raw_vectors_.end()) {
      continue;
    }
    RawVector *raw_vec = iter->second;
    if (raw_vec == nullptr) {
      LOG(ERROR) << "raw_vec is null!";
      return -1;
    }
    int vid = raw_vec->VidMgr()->GetFirstVID(id);

    ScopeVector scope_vec;
    raw_vec->GetVector(vid, scope_vec);
    const float *feature = (const float *)(scope_vec.Get());
    std::string str_vec;
    if (is_bytearray) {
      int d = raw_vec->MetaInfo()->Dimension();
      int d_byte = d * raw_vec->MetaInfo()->DataSize();

      char feat_source[sizeof(d) + d_byte];

      memcpy((void *)feat_source, &d_byte, sizeof(int));
      int cur = sizeof(d_byte);

      memcpy((void *)(feat_source + cur), feature, d_byte);
      cur += d_byte;

      str_vec = std::string((char *)feat_source, sizeof(unsigned int) + d_byte);
    } else {
      VectorValueType data_type = raw_vec->MetaInfo()->DataType();
      if (data_type == VectorValueType::FLOAT) {
        const float *feature_float = reinterpret_cast<const float *>(feature);
        for (int i = 0; i < raw_vec->MetaInfo()->Dimension(); ++i) {
          str_vec += std::to_string(feature_float[i]) + ",";
        }
      } else if (data_type == VectorValueType::BINARY) {
        for (int i = 0; i < raw_vec->MetaInfo()->Dimension(); ++i) {
          str_vec += std::to_string(feature[i]) + ",";
        }
      }
      str_vec.pop_back();
    }
    vec.emplace_back(std::move(str_vec));
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
    LOG(ERROR) << "raw_vec is null!";
    return -1;
  }
  int vid = raw_vec->VidMgr()->GetFirstVID(docid);

  ScopeVector scope_vec;
  raw_vec->GetVector(vid, scope_vec);
  const float *feature = (const float *)(scope_vec.Get());

  int d = raw_vec->MetaInfo()->Dimension();
  int d_byte = d * raw_vec->MetaInfo()->DataSize();

  vec.resize(sizeof(d) + d_byte);
  memcpy((void *)vec.data(), &d_byte, sizeof(int));
  int cur = sizeof(d_byte);
  memcpy((void *)(vec.data() + cur), feature, d_byte);
  return 0;
}

void VectorManager::GetTotalMemBytes(long &index_total_mem_bytes,
                                     long &vector_total_mem_bytes) {
  for (const auto &iter : vector_indexes_) {
    index_total_mem_bytes += iter.second->GetTotalMemBytes();
  }

  for (const auto &iter : raw_vectors_) {
    vector_total_mem_bytes += iter.second->GetTotalMemBytes();
  }
}

int VectorManager::Dump(const std::string &path, int dump_docid,
                        int max_docid) {
  for (const auto &[name, index] : vector_indexes_) {
    int ret = index->Dump(path);
    if (ret != 0) {
      LOG(ERROR) << "vector " << name << " dump gamma index failed!";
      return -1;
    }
    LOG(INFO) << "vector " << name << " dump gamma index success!";
  }

  for (const auto &[name, vec] : raw_vectors_) {
    RawVector *raw_vector = dynamic_cast<RawVector *>(vec);
    if (raw_vector->GetIO()) {
      int start = raw_vector->VidMgr()->GetFirstVID(dump_docid);
      int end = raw_vector->VidMgr()->GetLastVID(max_docid);
      int ret = raw_vector->GetIO()->Dump(start, end + 1);
      if (ret != 0) {
        LOG(ERROR) << "vector " << name << " dump failed!";
        return -1;
      }
      LOG(INFO) << "vector " << name << " dump success!";
    }
  }

  return 0;
}

int VectorManager::Load(const std::vector<std::string> &index_dirs,
                        int &doc_num) {
  int min_vec_num = doc_num;
  for (const auto &[name, vec] : raw_vectors_) {
    if (vec->GetIO()) {
      int vector_num = min_vec_num;
      vec->GetIO()->GetDiskVecNum(vector_num);
      if (vector_num < min_vec_num) min_vec_num = vector_num;
    }
  }

  for (const auto &[name, vec] : raw_vectors_) {
    if (vec->GetIO()) {
      // TODO: doc num to vector num
      int vec_num = min_vec_num;
      if (0 != vec->GetIO()->Load(vec_num)) {
        LOG(ERROR) << "vector [" << name << "] load failed!";
        return -1;
      }
      LOG(INFO) << "vector [" << name << "] load success!";
    }
  }

  if (index_dirs.size() > 0) {
    for (const auto &[name, index] : vector_indexes_) {
      int load_num = index->Load(index_dirs[0]);
      if (load_num < 0) {
        LOG(ERROR) << "vector [" << name << "] load gamma index "
                   << index_dirs[0] << " failed, load_num: " << load_num;
        return -1;
      } else {
        if (load_num > min_vec_num) {
          LOG(ERROR) << "load vec_index_num=" << load_num
                     << " > raw_vec_num=" << min_vec_num;
          return -1;
        }
        index->indexed_count_ = load_num;
        LOG(INFO) << "vector [" << name
                  << "] load gamma index success and load_num=" << load_num;
      }
    }
  }
  doc_num = min_vec_num;
  LOG(INFO) << "vector_mgr load vec_num=" << doc_num;
  return 0;
}

bool VectorManager::Contains(std::string &field_name) {
  return raw_vectors_.find(field_name) != raw_vectors_.end();
}

void VectorManager::Close() {
  DestroyRawVectors();

  DestroyVectorIndexes();

  LOG(INFO) << "VectorManager closed.";
}

int VectorManager::MinIndexedNum() {
  int min = 0;
  for (const auto &[name, index] : vector_indexes_) {
    if (index != nullptr && (index->indexed_count_ < min || min == 0)) {
      min = index->indexed_count_;
    }
  }
  return min;
}

int VectorManager::AlterCacheSize(struct CacheInfo &cache_info) {
  auto ite = raw_vectors_.find(cache_info.field_name);
  if (ite != raw_vectors_.end()) {
    RawVector *raw_vec = ite->second;
    int cache_size = cache_info.cache_size;
    int res = raw_vec->AlterCacheSize(cache_size);
    if (res == 0) {
      LOG(INFO) << "vector field[" << cache_info.field_name
                << "] AlterCacheSize success!";
    } else {
      LOG(INFO) << "vector field[" << cache_info.field_name
                << "] AlterCacheSize failure!";
    }
  } else {
    LOG(INFO) << "field_name[" << cache_info.field_name << "] error.";
  }
  return 0;
}

int VectorManager::GetAllCacheSize(Config &conf) {
  for (auto ite = raw_vectors_.begin(); ite != raw_vectors_.end(); ++ite) {
    RawVector *raw_vec = ite->second;
    int cache_size = 0;
    if (0 != raw_vec->GetCacheSize(cache_size)) continue;
    conf.AddCacheInfo(ite->first, (int)cache_size);
  }
  return 0;
}

}  // namespace tig_gamma
