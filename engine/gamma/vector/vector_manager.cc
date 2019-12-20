/**
 * Copyright 2019 The Gamma Authors.
 *
 * This source code is licensed under the Apache License, Version 2.0 license
 * found in the LICENSE file in the root directory of this source tree.
 */

#include "vector_manager.h"
#include "gamma_index_factory.h"
#include "raw_vector_factory.h"
#include "utils.h"

namespace tig_gamma {

static bool InnerProductCmp(const VectorDoc *a, const VectorDoc *b) {
  return a->score > b->score;
}

static bool L2Cmp(const VectorDoc *a, const VectorDoc *b) {
  return a->score < b->score;
}

VectorManager::VectorManager(const RetrievalModel &model,
                             const VectorStorageType &store_type,
                             const char *docids_bitmap, int max_doc_size,
                             const std::string &root_path)
    : default_model_(model),
      default_store_type_(store_type),
      docids_bitmap_(docids_bitmap),
      max_doc_size_(max_doc_size),
      root_path_(root_path) {
  table_created_ = false;
  ivfpq_param_ = nullptr;
}

VectorManager::~VectorManager() { Close(); }

int VectorManager::CreateVectorTable(VectorInfo **vectors_info, int vectors_num,
                                     IVFPQParameters *ivfpq_param) {
  if (table_created_) return -1;

  if (ivfpq_param == nullptr) {
    LOG(ERROR) << "ivf pq parameters is null";
    return -1;
  }

  // copy parameters
  ivfpq_param_ = MakeIVFPQParameters(
      ivfpq_param->metric_type, ivfpq_param->nprobe, ivfpq_param->ncentroids,
      ivfpq_param->nsubvector, ivfpq_param->nbits_per_idx);
  IVFPQParamHelper ivfpq_param_helper(ivfpq_param_);
  ivfpq_param_helper.SetDefaultValue();
  LOG(INFO) << ivfpq_param_helper.ToString();
  if (!ivfpq_param_helper.Validate()) {
    LOG(ERROR) << "validate ivf qp parameters error";
    return -1;
  }

  for (int i = 0; i < vectors_num; i++) {
    std::string vec_name(vectors_info[i]->name->value,
                         vectors_info[i]->name->len);
    int dimension = vectors_info[i]->dimension;

    std::string store_type_str(vectors_info[i]->store_type->value,
                               vectors_info[i]->store_type->len);

    VectorStorageType store_type = default_store_type_;
    if (store_type_str != "") {
      if (!strcasecmp("Mmap", store_type_str.c_str())) {
        store_type = VectorStorageType::Mmap;
#ifdef WITH_ROCKSDB
      } else if (!strcasecmp("RocksDB", store_type_str.c_str())) {
        store_type = VectorStorageType::RocksDB;
#endif  // WITH_ROCKSDB
      } else {
        LOG(WARNING) << "NO support for store type " << store_type_str;
        return -1;
      }
    }

    std::string store_param;
    if (vectors_info[i]->store_param) {
      store_param.assign(vectors_info[i]->store_param->value,
                         vectors_info[i]->store_param->len);
    }

    RawVector *vec =
        RawVectorFactory::Create(store_type, vec_name, dimension, max_doc_size_,
                                 root_path_, store_param);
    if (vec == nullptr) {
      LOG(ERROR) << "create raw vector error";
      return -1;
    }
    int ret = vec->Init();
    if (ret != 0) {
      LOG(ERROR) << "Raw vector " << vec_name << " init error, code [" << ret
                 << "]!";
      return -1;
    }

    StartFlushingIfNeed(vec);

    raw_vectors_[vec_name] = vec;

    if (vectors_info[i]->is_index == FALSE) {
      LOG(INFO) << vec_name << " need not to indexed!";
      continue;
    }

    std::string retrieval_type_str(vectors_info[i]->retrieval_type->value,
                                   vectors_info[i]->retrieval_type->len);

    RetrievalModel model = default_model_;
    if (!strcasecmp("IVFPQ", retrieval_type_str.c_str())) {
      model = RetrievalModel::IVFPQ;
    } else {
      LOG(WARNING) << "NO support for retrieval type " << retrieval_type_str
                   << ", default to " << default_model_;
    }

    GammaIndex *index = GammaIndexFactory::Create(
        model, dimension, docids_bitmap_, vec, ivfpq_param_);
    if (index == nullptr) {
      LOG(ERROR) << "create gamma index " << vec_name << " error!";
      return -1;
    }

    vector_indexes_[vec_name] = index;
  }
  table_created_ = true;
  return 0;
}

int VectorManager::AddToStore(int docid, std::vector<Field *> &fields) {
  for (unsigned int i = 0; i < fields.size(); i++) {
    std::string name =
        std::string(fields[i]->name->value, fields[i]->name->len);
    if (raw_vectors_.find(name) == raw_vectors_.end()) {
      LOG(ERROR) << "Cannot find raw vector [" << name << "]";
      return -1;
    }
    raw_vectors_[name]->Add(docid, fields[i]);
  }
  return 0;
}

int VectorManager::Indexing() {
  int ret = 0;
  for (const auto &iter : vector_indexes_) {
    if (0 != iter.second->Indexing()) {
      ret = -1;
      LOG(ERROR) << "vector table " << iter.first << " indexing failed!";
    }
  }
  return ret;
}

int VectorManager::AddRTVecsToIndex() {
  int ret = 0;
  for (const auto &iter : vector_indexes_) {
    if (0 != iter.second->AddRTVecsToIndex()) {
      ret = -1;
      LOG(ERROR) << "vector table " << iter.first
                 << " add real time vectors failed!";
    }
  }
  return ret;
}

int VectorManager::Search(const GammaQuery &query, GammaResult *results) {
  int ret = 0, n = 0;

  VectorResult all_vector_results[query.vec_num];

  query.condition->sort_by_docid = query.vec_num > 1 ? true : false;
  query.condition->metric_type =
      static_cast<DistanceMetricType>(ivfpq_param_->metric_type);
  std::string vec_names[query.vec_num];
  for (int i = 0; i < query.vec_num; i++) {
    std::string name = std::string(query.vec_query[i]->name->value,
                                   query.vec_query[i]->name->len);
    vec_names[i] = name;
    std::map<std::string, GammaIndex *>::iterator iter =
        vector_indexes_.find(name);
    if (iter == vector_indexes_.end()) {
      LOG(ERROR) << "Query name " << name
                 << " not exist in created vector table";
      return -1;
    }

    int d = iter->second->raw_vec_->GetDimension();
    n = query.vec_query[i]->value->len / (sizeof(float) * d);
    if (!all_vector_results[i].init(n, query.condition->topn)) {
      LOG(ERROR) << "Query name " << name << "init vector result error";
      return -1;
    }

    GammaSearchCondition condition(query.condition);
    condition.min_dist = query.vec_query[i]->min_score;
    condition.max_dist = query.vec_query[i]->max_score;
    int ret_vec = iter->second->Search(query.vec_query[i], &condition,
                                       all_vector_results[i]);
    if (ret_vec != 0) {
      ret = ret_vec;
    }
  }

  if (query.condition->sort_by_docid) {
    for (int i = 0; i < n; i++) {
      int start_docid = 0, common_docid_count = 0, common_idx = 0;
      double score = 0;
      bool has_common_docid = true;
      if (!results[i].init(query.condition->topn, vec_names, query.vec_num)) {
        LOG(ERROR) << "init gamma result(sort by docid) error, topn="
                   << query.condition->topn
                   << ", vector number=" << query.vec_num;
        return -1;
      }
      while (start_docid < INT_MAX) {
        for (int j = 0; j < query.vec_num; j++) {
          float vec_dist = 0;
          char *source = nullptr;
          int source_len = 0;
          int cur_docid = all_vector_results[j].seek(i, start_docid, vec_dist,
                                                     source, source_len);
          if (cur_docid == start_docid) {
            common_docid_count++;
            double field_score = query.vec_query[j]->has_boost == 1
                                     ? (vec_dist * query.vec_query[j]->boost)
                                     : vec_dist;
            score += field_score;
            results[i].docs[common_idx]->fields[j].score = field_score;
            results[i].docs[common_idx]->fields[j].source = source;
            results[i].docs[common_idx]->fields[j].source_len = source_len;
            if (common_docid_count == query.vec_num) {
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
          case InnerProduct:
            std::sort(results[i].docs, results[i].docs + common_idx,
                      InnerProductCmp);
            break;
          case L2:
            std::sort(results[i].docs, results[i].docs + common_idx,
                      L2Cmp);
            break;
          default:
            LOG(ERROR) << "invalid metric_type="
                       << query.condition->metric_type;
        }
      }
    }
  } else {
    for (int i = 0; i < n; i++) {
      // double score = 0;
      if (!results[i].init(query.condition->topn, vec_names, query.vec_num)) {
        LOG(ERROR) << "init gamma result error, topn=" << query.condition->topn
                   << ", vector number=" << query.vec_num;
        return -1;
      }
      results[i].total = all_vector_results[0].total[i] > 0
                             ? all_vector_results[0].total[i]
                             : results[i].total;
      int pos = 0, topn = all_vector_results[0].topn;
      for (int j = 0; j < topn; j++) {
        int real_pos = i * topn + j;
        if (all_vector_results[0].docids[real_pos] == -1) continue;
        results[i].docs[pos]->docid = all_vector_results[0].docids[real_pos];

        results[i].docs[pos]->fields[0].source =
            all_vector_results[0].sources[real_pos];
        results[i].docs[pos]->fields[0].source_len =
            all_vector_results[0].source_lens[real_pos];

        double score = all_vector_results[0].dists[real_pos];

        score = query.vec_query[0]->has_boost == 1
                    ? (score * query.vec_query[0]->boost)
                    : score;

        results[i].docs[pos]->fields[0].score = score;
        results[i].docs[pos]->score = score;
        pos++;
      }
      results[i].results_count = pos;
    }
  }

  return ret;
}

int VectorManager::GetVector(
    const std::vector<std::pair<string, int>> &fields_ids,
    std::vector<string> &vec, bool is_bytearray) {
  for (const auto &pair : fields_ids) {
    const string &field = pair.first;
    const int id = pair.second;
    std::map<std::string, GammaIndex *>::iterator iter =
        vector_indexes_.find(field);
    if (iter == vector_indexes_.end()) {
      continue;
    }
    GammaIndex *gamma_index = iter->second;
    RawVector *raw_vec = gamma_index->raw_vec_;
    if (raw_vec == nullptr) {
      LOG(ERROR) << "raw_vec is null!";
      return -1;
    }
    int *vids_list = raw_vec->docid2vid_[id];
    if (vids_list == nullptr) {
      LOG(ERROR) << "vids_list is null!";
      return -1;
    }
    int vid_num = vids_list[0];
    if (vid_num <= 0) {
      LOG(ERROR) << "vid num [" << vid_num << "]";
      return -1;
    }
    // if (vid_num != fields_ids.size()) {
    //   LOG(ERROR) << "vid num [" << vid_num << "] fields_ids size [" <<
    //   fields_ids.size() << "]";
    //   return -1;
    // }

    int vid = vids_list[1];

    char *source = nullptr;
    int len = -1;
    int ret = raw_vec->GetSource(vid, source, len);

    if (ret != 0 || len < 0) {
      LOG(ERROR) << "Get source failed!";
      return -1;
    }

    ScopeVector scope_vec;
    raw_vec->GetVector(vid, scope_vec);
    const float *feature = scope_vec.Get();
    string str_vec;
    if (is_bytearray) {
      int d = raw_vec->GetDimension();
      int d_byte = d * sizeof(float);

      char feat_source[sizeof(d) + d_byte + len];

      memcpy((void *)feat_source, &d_byte, sizeof(int));
      int cur = sizeof(d_byte);

      memcpy((void *)(feat_source + cur), feature, d_byte);
      cur += d_byte;

      memcpy((void *)(feat_source + cur), source, len);

      str_vec =
          string((char *)feat_source, sizeof(unsigned int) + d_byte + len);
    } else {
      for (int i = 0; i < raw_vec->GetDimension(); ++i) {
        str_vec += std::to_string(feature[i]) + ",";
      }
      str_vec.pop_back();
    }
    vec.emplace_back(std::move(str_vec));
  }
  return 0;
}

int VectorManager::Dump(const string &path, int dump_docid, int max_docid) {
  for (const auto &iter : vector_indexes_) {
    const string &vec_name = iter.first;
    GammaIndex *index = iter.second;

    auto it = raw_vectors_.find(vec_name);
    assert(it != raw_vectors_.end());
    int max_vid = it->second->GetLastVectorID(max_docid);
    int dump_num = index->Dump(path, max_vid);
    if (dump_num < 0) {
      LOG(ERROR) << "vector " << vec_name << " dump gamma index failed!";
      return -1;
    }
    LOG(INFO) << "vector " << vec_name << " dump gamma index success!";
  }

  for (const auto &iter : raw_vectors_) {
    const string &vec_name = iter.first;
    RawVector *raw_vector = iter.second;
    int ret = raw_vector->Dump(path, dump_docid, max_docid);
    if (ret != 0) {
      LOG(ERROR) << "vector " << vec_name << " dump failed!";
      return -1;
    }
    LOG(INFO) << "vector " << vec_name << " dump success!";
  }
  return 0;
}

int VectorManager::Load(const std::vector<std::string> &index_dirs,
                        int doc_num) {
  for (const auto &iter : raw_vectors_) {
    if (0 != iter.second->Load(index_dirs, doc_num)) {
      LOG(ERROR) << "vector [" << iter.first << "] load failed!";
      return -1;
    }
    LOG(INFO) << "vector [" << iter.first << "] load success!";
  }

  if (index_dirs.size() > 0) {
    for (const auto &iter : vector_indexes_) {
      if (iter.second->Load(index_dirs) < 0) {
        LOG(ERROR) << "vector [" << iter.first << "] load gamma index failed!";
        return -1;
      } else {
        LOG(INFO) << "vector [" << iter.first << "] load gamma index success!";
      }
    }
  }

  return 0;
}

void VectorManager::Close() {
  for (const auto &iter : raw_vectors_) {
    if (iter.second != nullptr) {
      StopFlushingIfNeed(iter.second);
      delete iter.second;
    }
  }
  raw_vectors_.clear();

  for (const auto &iter : vector_indexes_) {
    if (iter.second != nullptr) {
      delete iter.second;
    }
  }
  vector_indexes_.clear();

  if (ivfpq_param_ != nullptr) {
    DestroyIVFPQParameters(ivfpq_param_);
    ivfpq_param_ = nullptr;
  }
}
}  // namespace tig_gamma
