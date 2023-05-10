/**
 * Copyright 2019 The Gamma Authors.
 *
 * This source code is licensed under the Apache License, Version 2.0 license
 * found in the LICENSE file in the root directory of this source tree.
 */

#ifndef GAMMA_INDEX_IO_H_
#define GAMMA_INDEX_IO_H_

#include "faiss/IndexIVFPQ.h"
#include "faiss/VectorTransform.h"
#include "faiss/impl/FaissAssert.h"
#include "faiss/impl/HNSW.h"
#include "faiss/impl/io.h"
#include "faiss/index_io.h"
#include "realtime/realtime_invert_index.h"

namespace tig_gamma {
/*************************************************************
 * I/O macros
 *
 * we use macros so that we have a line number to report in abort
 * (). This makes debugging a lot easier. The IOReader or IOWriter is
 * always called f and thus is not passed in as a macro parameter.
 **************************************************************/

#define WRITEANDCHECK(ptr, n)                                                 \
  {                                                                           \
    size_t ret = (*f)(ptr, sizeof(*(ptr)), n);                                \
    FAISS_THROW_IF_NOT_FMT(ret == (n), "write error in %s: %ld != %ld (%s)",  \
                           f->name.c_str(), ret, size_t(n), strerror(errno)); \
  }

#define READANDCHECK(ptr, n)                                                  \
  {                                                                           \
    size_t ret = (*f)(ptr, sizeof(*(ptr)), n);                                \
    FAISS_THROW_IF_NOT_FMT(ret == (n), "read error in %s: %ld != %ld (%s)",   \
                           f->name.c_str(), ret, size_t(n), strerror(errno)); \
  }

#define WRITE1(x) WRITEANDCHECK(&(x), 1)
#define READ1(x) READANDCHECK(&(x), 1)

#define WRITEVECTOR(vec)               \
  {                                    \
    size_t size = (vec).size();        \
    WRITEANDCHECK(&size, 1);           \
    WRITEANDCHECK((vec).data(), size); \
  }

// will fail if we write 256G of data at once...
#define READVECTOR(vec)                                 \
  {                                                     \
    size_t size;                                        \
    READANDCHECK(&size, 1);                             \
    FAISS_THROW_IF_NOT(size >= 0 && size < (1L << 40)); \
    (vec).resize(size);                                 \
    READANDCHECK((vec).data(), size);                   \
  }

/****************************************************************
 * Write
 *****************************************************************/
void write_index_header(const faiss::Index *idx, faiss::IOWriter *f);
void write_direct_map(const faiss::DirectMap *dm, faiss::IOWriter *f);
void write_ivf_header(const faiss::IndexIVF *ivf, faiss::IOWriter *f);
void write_hnsw(const faiss::HNSW *hnsw, faiss::IOWriter *f);
void write_opq(const faiss::VectorTransform *vt, faiss::IOWriter *f);

void read_index_header(faiss::Index *idx, faiss::IOReader *f);
void read_direct_map(faiss::DirectMap *dm, faiss::IOReader *f);
void read_ivf_header(
    faiss::IndexIVF *ivf, faiss::IOReader *f,
    std::vector<std::vector<faiss::Index::idx_t>> *ids = nullptr);
void read_hnsw(faiss::HNSW *hnsw, faiss::IOReader *f);
void read_opq(faiss::VectorTransform *vt, faiss::IOReader *f);

void write_product_quantizer(const faiss::ProductQuantizer *pq,
                             faiss::IOWriter *f);
void read_product_quantizer(faiss::ProductQuantizer *pq, faiss::IOReader *f);

struct FileIOReader : faiss::IOReader {
  FILE *f = nullptr;
  bool need_close = false;

  FileIOReader(FILE *rf) : f(rf) {}

  FileIOReader(const char *fname) {
    name = fname;
    f = fopen(fname, "rb");
    FAISS_THROW_IF_NOT_FMT(f, "could not open %s for reading: %s", fname,
                           strerror(errno));
    need_close = true;
  }

  ~FileIOReader() override {
    if (need_close) {
      int ret = fclose(f);
      if (ret != 0) {  // we cannot raise and exception in the destructor
        fprintf(stderr, "file %s close error: %s", name.c_str(),
                strerror(errno));
      }
    }
  }

  size_t operator()(void *ptr, size_t size, size_t nitems) override {
    return fread(ptr, size, nitems, f);
  }

  int fileno() override { return ::fileno(f); }
};

struct FileIOWriter : faiss::IOWriter {
  FILE *f = nullptr;
  bool need_close = false;

  FileIOWriter(FILE *wf) : f(wf) {}

  FileIOWriter(const char *fname) {
    name = fname;
    f = fopen(fname, "wb");
    FAISS_THROW_IF_NOT_FMT(f, "could not open %s for writing: %s", fname,
                           strerror(errno));
    need_close = true;
  }

  ~FileIOWriter() override {
    if (need_close) {
      int ret = fclose(f);
      if (ret != 0) {
        // we cannot raise and exception in the destructor
        fprintf(stderr, "file %s close error: %s", name.c_str(),
                strerror(errno));
      }
    }
  }

  size_t operator()(const void *ptr, size_t size, size_t nitems) override {
    return fwrite(ptr, size, nitems, f);
  }
  int fileno() override { return ::fileno(f); }
};

int WriteInvertedLists(faiss::IOWriter *f,
                       realtime::RTInvertIndex *rt_invert_index);
int ReadInvertedLists(faiss::IOReader *f,
                      realtime::RTInvertIndex *rt_invert_index, int &indexed_vec_count);
}  // namespace tig_gamma

#endif
