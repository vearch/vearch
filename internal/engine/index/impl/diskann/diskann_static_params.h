#pragma once
#include <string>
#include "util/utils.h"
#include "index/index_model.h"

namespace vearch {

struct DiskANNStaticModelParams {
  uint32_t R = 64;                   
  uint32_t L = 100;                  
  float search_dram_budget_gb = 4.0; 
  float build_dram_budget_gb = 16.0;
  uint32_t disk_pq_bytes = 0;        
  uint32_t num_threads = 8;          
  bool use_opq = false;             
  bool append_reorder_data = false;  

  uint32_t beam_width = 4;           
  uint32_t num_nodes_to_cache = 100000; 

  DistanceComputeType metric_type = DistanceComputeType::L2;

  Status ParseFrom(const char *str);
  std::string ToString();
};

struct DiskANNStaticRetrievalParameters : public RetrievalParameters {
  uint64_t l_search = 100;         
  uint32_t beam_width = 4;          
  bool use_reorder_data = false;    

  DiskANNStaticRetrievalParameters() : RetrievalParameters() {}
  DiskANNStaticRetrievalParameters(DistanceComputeType type,
                                   uint64_t l, uint32_t bw, bool reorder)
      : RetrievalParameters(type),
        l_search(l), beam_width(bw), use_reorder_data(reorder) {}
};

}  // namespace vearch