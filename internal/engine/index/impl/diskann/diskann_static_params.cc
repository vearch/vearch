#include "diskann_static_params.h"

#include <sstream>
#include <cstring>

namespace vearch {

Status DiskANNStaticModelParams::ParseFrom(const char *str) {
  utils::JsonParser jp;
  if (jp.Parse(str)) {
    return Status::ParamError("Parse DiskANN static params error");
  }

  int val;

  if (!jp.GetInt("R", val) && val > 0) R = val;
  if (!jp.GetInt("L", val) && val > 0) L = val;
  if (!jp.GetInt("disk_pq_bytes", val) && val >= 0) disk_pq_bytes = val;
  if (!jp.GetInt("num_threads", val) && val > 0) num_threads = val;
  if (!jp.GetInt("beam_width", val) && val > 0) beam_width = val;
  if (!jp.GetInt("num_nodes_to_cache", val) && val >= 0)
    num_nodes_to_cache = val;

  double dval;
  if (!jp.GetDouble("search_dram_budget_gb", dval) && dval > 0)
    search_dram_budget_gb = dval;
  if (!jp.GetDouble("build_dram_budget_gb", dval) && dval > 0)
    build_dram_budget_gb = dval;

  if (build_dram_budget_gb < 0.1) {
    return Status::ParamError(
        "build_dram_budget_gb=" + std::to_string(build_dram_budget_gb) +
        " is too small (minimum 0.1). Vamana graph construction needs "
        "substantial memory beyond raw data storage.");
  }
  if (search_dram_budget_gb <= 0) {
    return Status::ParamError(
        "search_dram_budget_gb must be positive, got " +
        std::to_string(search_dram_budget_gb));
  }

  int bool_val;
  if (!jp.GetInt("use_opq", bool_val)) use_opq = (bool_val != 0);
  if (!jp.GetInt("append_reorder_data", bool_val))
    append_reorder_data = (bool_val != 0);

  std::string mt;
  if (!jp.GetString("metric_type", mt)) {
    if (!strcasecmp("InnerProduct", mt.c_str()))
      metric_type = DistanceComputeType::INNER_PRODUCT;
    else if (!strcasecmp("Cosine", mt.c_str()))
      metric_type = DistanceComputeType::Cosine;
    else
      metric_type = DistanceComputeType::L2;
  }

  return Status::OK();
}

std::string DiskANNStaticModelParams::ToString() {
  std::stringstream ss;
  ss << "DiskANNStatic{R=" << R << ", L=" << L
     << ", search_dram=" << search_dram_budget_gb << "GB"
     << ", build_dram=" << build_dram_budget_gb << "GB"
     << ", disk_pq=" << disk_pq_bytes
     << ", threads=" << num_threads
     << ", beam=" << beam_width
     << ", cache=" << num_nodes_to_cache
     << ", opq=" << use_opq
     << ", reorder=" << append_reorder_data << "}";
  return ss.str();
}

}  // namespace vearch
