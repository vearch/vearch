#ifndef GAMMA_SCANNER_H_
#define GAMMA_SCANNER_H_

#include <stdexcept>
#include "faiss/IndexIVF.h"
#include "index/retrieval_model.h"

namespace tig_gamma {

struct GammaInvertedListScanner : faiss::InvertedListScanner {
  GammaInvertedListScanner() { retrieval_context_ = nullptr; }

  void set_search_context(RetrievalContext *retrieval_context) {
    this->retrieval_context_ = retrieval_context;
  }

  RetrievalContext *retrieval_context_;
};

}  // namespace tig_gamma

#endif  // GAMMA_SCANNER_H_
