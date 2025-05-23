// automatically generated by the FlatBuffers compiler, do not modify


#ifndef FLATBUFFERS_GENERATED_TABLE_GAMMA_API_H_
#define FLATBUFFERS_GENERATED_TABLE_GAMMA_API_H_

#include "flatbuffers/flatbuffers.h"

#include "types_generated.h"

namespace gamma_api {

struct FieldInfo;

struct VectorInfo;

struct Table;

struct FieldInfo FLATBUFFERS_FINAL_CLASS : private flatbuffers::Table {
  enum FlatBuffersVTableOffset FLATBUFFERS_VTABLE_UNDERLYING_TYPE {
    VT_NAME = 4,
    VT_DATA_TYPE = 6,
    VT_IS_INDEX = 8
  };
  const flatbuffers::String *name() const {
    return GetPointer<const flatbuffers::String *>(VT_NAME);
  }
  DataType data_type() const {
    return static_cast<DataType>(GetField<int8_t>(VT_DATA_TYPE, 0));
  }
  bool is_index() const {
    return GetField<uint8_t>(VT_IS_INDEX, 0) != 0;
  }
  bool Verify(flatbuffers::Verifier &verifier) const {
    return VerifyTableStart(verifier) &&
           VerifyOffset(verifier, VT_NAME) &&
           verifier.VerifyString(name()) &&
           VerifyField<int8_t>(verifier, VT_DATA_TYPE) &&
           VerifyField<uint8_t>(verifier, VT_IS_INDEX) &&
           verifier.EndTable();
  }
};

struct FieldInfoBuilder {
  flatbuffers::FlatBufferBuilder &fbb_;
  flatbuffers::uoffset_t start_;
  void add_name(flatbuffers::Offset<flatbuffers::String> name) {
    fbb_.AddOffset(FieldInfo::VT_NAME, name);
  }
  void add_data_type(DataType data_type) {
    fbb_.AddElement<int8_t>(FieldInfo::VT_DATA_TYPE, static_cast<int8_t>(data_type), 0);
  }
  void add_is_index(bool is_index) {
    fbb_.AddElement<uint8_t>(FieldInfo::VT_IS_INDEX, static_cast<uint8_t>(is_index), 0);
  }
  explicit FieldInfoBuilder(flatbuffers::FlatBufferBuilder &_fbb)
        : fbb_(_fbb) {
    start_ = fbb_.StartTable();
  }
  FieldInfoBuilder &operator=(const FieldInfoBuilder &);
  flatbuffers::Offset<FieldInfo> Finish() {
    const auto end = fbb_.EndTable(start_);
    auto o = flatbuffers::Offset<FieldInfo>(end);
    return o;
  }
};

inline flatbuffers::Offset<FieldInfo> CreateFieldInfo(
    flatbuffers::FlatBufferBuilder &_fbb,
    flatbuffers::Offset<flatbuffers::String> name = 0,
    DataType data_type = INT,
    bool is_index = false) {
  FieldInfoBuilder builder_(_fbb);
  builder_.add_name(name);
  builder_.add_is_index(is_index);
  builder_.add_data_type(data_type);
  return builder_.Finish();
}

inline flatbuffers::Offset<FieldInfo> CreateFieldInfoDirect(
    flatbuffers::FlatBufferBuilder &_fbb,
    const char *name = nullptr,
    DataType data_type = INT,
    bool is_index = false) {
  auto name__ = name ? _fbb.CreateString(name) : 0;
  return gamma_api::CreateFieldInfo(
      _fbb,
      name__,
      data_type,
      is_index);
}

struct VectorInfo FLATBUFFERS_FINAL_CLASS : private flatbuffers::Table {
  enum FlatBuffersVTableOffset FLATBUFFERS_VTABLE_UNDERLYING_TYPE {
    VT_NAME = 4,
    VT_DATA_TYPE = 6,
    VT_IS_INDEX = 8,
    VT_DIMENSION = 10,
    VT_STORE_TYPE = 12,
    VT_STORE_PARAM = 14
  };
  const flatbuffers::String *name() const {
    return GetPointer<const flatbuffers::String *>(VT_NAME);
  }
  DataType data_type() const {
    return static_cast<DataType>(GetField<int8_t>(VT_DATA_TYPE, 0));
  }
  bool is_index() const {
    return GetField<uint8_t>(VT_IS_INDEX, 0) != 0;
  }
  int32_t dimension() const {
    return GetField<int32_t>(VT_DIMENSION, 0);
  }
  const flatbuffers::String *store_type() const {
    return GetPointer<const flatbuffers::String *>(VT_STORE_TYPE);
  }
  const flatbuffers::String *store_param() const {
    return GetPointer<const flatbuffers::String *>(VT_STORE_PARAM);
  }
  bool Verify(flatbuffers::Verifier &verifier) const {
    return VerifyTableStart(verifier) &&
           VerifyOffset(verifier, VT_NAME) &&
           verifier.VerifyString(name()) &&
           VerifyField<int8_t>(verifier, VT_DATA_TYPE) &&
           VerifyField<uint8_t>(verifier, VT_IS_INDEX) &&
           VerifyField<int32_t>(verifier, VT_DIMENSION) &&
           VerifyOffset(verifier, VT_STORE_TYPE) &&
           verifier.VerifyString(store_type()) &&
           VerifyOffset(verifier, VT_STORE_PARAM) &&
           verifier.VerifyString(store_param()) &&
           verifier.EndTable();
  }
};

struct VectorInfoBuilder {
  flatbuffers::FlatBufferBuilder &fbb_;
  flatbuffers::uoffset_t start_;
  void add_name(flatbuffers::Offset<flatbuffers::String> name) {
    fbb_.AddOffset(VectorInfo::VT_NAME, name);
  }
  void add_data_type(DataType data_type) {
    fbb_.AddElement<int8_t>(VectorInfo::VT_DATA_TYPE, static_cast<int8_t>(data_type), 0);
  }
  void add_is_index(bool is_index) {
    fbb_.AddElement<uint8_t>(VectorInfo::VT_IS_INDEX, static_cast<uint8_t>(is_index), 0);
  }
  void add_dimension(int32_t dimension) {
    fbb_.AddElement<int32_t>(VectorInfo::VT_DIMENSION, dimension, 0);
  }
  void add_store_type(flatbuffers::Offset<flatbuffers::String> store_type) {
    fbb_.AddOffset(VectorInfo::VT_STORE_TYPE, store_type);
  }
  void add_store_param(flatbuffers::Offset<flatbuffers::String> store_param) {
    fbb_.AddOffset(VectorInfo::VT_STORE_PARAM, store_param);
  }
  explicit VectorInfoBuilder(flatbuffers::FlatBufferBuilder &_fbb)
        : fbb_(_fbb) {
    start_ = fbb_.StartTable();
  }
  VectorInfoBuilder &operator=(const VectorInfoBuilder &);
  flatbuffers::Offset<VectorInfo> Finish() {
    const auto end = fbb_.EndTable(start_);
    auto o = flatbuffers::Offset<VectorInfo>(end);
    return o;
  }
};

inline flatbuffers::Offset<VectorInfo> CreateVectorInfo(
    flatbuffers::FlatBufferBuilder &_fbb,
    flatbuffers::Offset<flatbuffers::String> name = 0,
    DataType data_type = INT,
    bool is_index = false,
    int32_t dimension = 0,
    flatbuffers::Offset<flatbuffers::String> store_type = 0,
    flatbuffers::Offset<flatbuffers::String> store_param = 0) {
  VectorInfoBuilder builder_(_fbb);
  builder_.add_store_param(store_param);
  builder_.add_store_type(store_type);
  builder_.add_dimension(dimension);
  builder_.add_name(name);
  builder_.add_is_index(is_index);
  builder_.add_data_type(data_type);
  return builder_.Finish();
}

inline flatbuffers::Offset<VectorInfo> CreateVectorInfoDirect(
    flatbuffers::FlatBufferBuilder &_fbb,
    const char *name = nullptr,
    DataType data_type = INT,
    bool is_index = false,
    int32_t dimension = 0,
    const char *store_type = nullptr,
    const char *store_param = nullptr) {
  auto name__ = name ? _fbb.CreateString(name) : 0;
  auto store_type__ = store_type ? _fbb.CreateString(store_type) : 0;
  auto store_param__ = store_param ? _fbb.CreateString(store_param) : 0;
  return gamma_api::CreateVectorInfo(
      _fbb,
      name__,
      data_type,
      is_index,
      dimension,
      store_type__,
      store_param__);
}

struct Table FLATBUFFERS_FINAL_CLASS : private flatbuffers::Table {
  enum FlatBuffersVTableOffset FLATBUFFERS_VTABLE_UNDERLYING_TYPE {
    VT_NAME = 4,
    VT_FIELDS = 6,
    VT_VECTORS_INFO = 8,
    VT_INDEX_TYPE = 10,
    VT_INDEX_PARAMS = 12,
    VT_REFRESH_INTERVAL = 14
  };
  const flatbuffers::String *name() const {
    return GetPointer<const flatbuffers::String *>(VT_NAME);
  }
  const flatbuffers::Vector<flatbuffers::Offset<FieldInfo>> *fields() const {
    return GetPointer<const flatbuffers::Vector<flatbuffers::Offset<FieldInfo>> *>(VT_FIELDS);
  }
  const flatbuffers::Vector<flatbuffers::Offset<VectorInfo>> *vectors_info() const {
    return GetPointer<const flatbuffers::Vector<flatbuffers::Offset<VectorInfo>> *>(VT_VECTORS_INFO);
  }
  const flatbuffers::String *index_type() const {
    return GetPointer<const flatbuffers::String *>(VT_INDEX_TYPE);
  }
  const flatbuffers::String *index_params() const {
    return GetPointer<const flatbuffers::String *>(VT_INDEX_PARAMS);
  }
  int32_t refresh_interval() const {
    return GetField<int32_t>(VT_REFRESH_INTERVAL, 0);
  }
  bool Verify(flatbuffers::Verifier &verifier) const {
    return VerifyTableStart(verifier) &&
           VerifyOffset(verifier, VT_NAME) &&
           verifier.VerifyString(name()) &&
           VerifyOffset(verifier, VT_FIELDS) &&
           verifier.VerifyVector(fields()) &&
           verifier.VerifyVectorOfTables(fields()) &&
           VerifyOffset(verifier, VT_VECTORS_INFO) &&
           verifier.VerifyVector(vectors_info()) &&
           verifier.VerifyVectorOfTables(vectors_info()) &&
           VerifyOffset(verifier, VT_INDEX_TYPE) &&
           verifier.VerifyString(index_type()) &&
           VerifyOffset(verifier, VT_INDEX_PARAMS) &&
           verifier.VerifyString(index_params()) &&
           VerifyField<int32_t>(verifier, VT_REFRESH_INTERVAL) &&
           verifier.EndTable();
  }
};

struct TableBuilder {
  flatbuffers::FlatBufferBuilder &fbb_;
  flatbuffers::uoffset_t start_;
  void add_name(flatbuffers::Offset<flatbuffers::String> name) {
    fbb_.AddOffset(Table::VT_NAME, name);
  }
  void add_fields(flatbuffers::Offset<flatbuffers::Vector<flatbuffers::Offset<FieldInfo>>> fields) {
    fbb_.AddOffset(Table::VT_FIELDS, fields);
  }
  void add_vectors_info(flatbuffers::Offset<flatbuffers::Vector<flatbuffers::Offset<VectorInfo>>> vectors_info) {
    fbb_.AddOffset(Table::VT_VECTORS_INFO, vectors_info);
  }
  void add_index_type(flatbuffers::Offset<flatbuffers::String> index_type) {
    fbb_.AddOffset(Table::VT_INDEX_TYPE, index_type);
  }
  void add_index_params(flatbuffers::Offset<flatbuffers::String> index_params) {
    fbb_.AddOffset(Table::VT_INDEX_PARAMS, index_params);
  }
  void add_refresh_interval(int32_t refresh_interval) {
    fbb_.AddElement<int32_t>(Table::VT_REFRESH_INTERVAL, refresh_interval, 0);
  }
  explicit TableBuilder(flatbuffers::FlatBufferBuilder &_fbb)
        : fbb_(_fbb) {
    start_ = fbb_.StartTable();
  }
  TableBuilder &operator=(const TableBuilder &);
  flatbuffers::Offset<Table> Finish() {
    const auto end = fbb_.EndTable(start_);
    auto o = flatbuffers::Offset<Table>(end);
    return o;
  }
};

inline flatbuffers::Offset<Table> CreateTable(
    flatbuffers::FlatBufferBuilder &_fbb,
    flatbuffers::Offset<flatbuffers::String> name = 0,
    flatbuffers::Offset<flatbuffers::Vector<flatbuffers::Offset<FieldInfo>>> fields = 0,
    flatbuffers::Offset<flatbuffers::Vector<flatbuffers::Offset<VectorInfo>>> vectors_info = 0,
    flatbuffers::Offset<flatbuffers::String> index_type = 0,
    flatbuffers::Offset<flatbuffers::String> index_params = 0,
    int32_t refresh_interval = 0) {
  TableBuilder builder_(_fbb);
  builder_.add_refresh_interval(refresh_interval);
  builder_.add_index_params(index_params);
  builder_.add_index_type(index_type);
  builder_.add_vectors_info(vectors_info);
  builder_.add_fields(fields);
  builder_.add_name(name);
  return builder_.Finish();
}

inline flatbuffers::Offset<Table> CreateTableDirect(
    flatbuffers::FlatBufferBuilder &_fbb,
    const char *name = nullptr,
    const std::vector<flatbuffers::Offset<FieldInfo>> *fields = nullptr,
    const std::vector<flatbuffers::Offset<VectorInfo>> *vectors_info = nullptr,
    const char *index_type = nullptr,
    const char *index_params = nullptr,
    int32_t refresh_interval = 0) {
  auto name__ = name ? _fbb.CreateString(name) : 0;
  auto fields__ = fields ? _fbb.CreateVector<flatbuffers::Offset<FieldInfo>>(*fields) : 0;
  auto vectors_info__ = vectors_info ? _fbb.CreateVector<flatbuffers::Offset<VectorInfo>>(*vectors_info) : 0;
  auto index_type__ = index_type ? _fbb.CreateString(index_type) : 0;
  auto index_params__ = index_params ? _fbb.CreateString(index_params) : 0;
  return gamma_api::CreateTable(
      _fbb,
      name__,
      fields__,
      vectors_info__,
      index_type__,
      index_params__,
      refresh_interval);
}

inline const gamma_api::Table *GetTable(const void *buf) {
  return flatbuffers::GetRoot<gamma_api::Table>(buf);
}

inline const gamma_api::Table *GetSizePrefixedTable(const void *buf) {
  return flatbuffers::GetSizePrefixedRoot<gamma_api::Table>(buf);
}

inline bool VerifyTableBuffer(
    flatbuffers::Verifier &verifier) {
  return verifier.VerifyBuffer<gamma_api::Table>(nullptr);
}

inline bool VerifySizePrefixedTableBuffer(
    flatbuffers::Verifier &verifier) {
  return verifier.VerifySizePrefixedBuffer<gamma_api::Table>(nullptr);
}

inline void FinishTableBuffer(
    flatbuffers::FlatBufferBuilder &fbb,
    flatbuffers::Offset<gamma_api::Table> root) {
  fbb.Finish(root);
}

inline void FinishSizePrefixedTableBuffer(
    flatbuffers::FlatBufferBuilder &fbb,
    flatbuffers::Offset<gamma_api::Table> root) {
  fbb.FinishSizePrefixed(root);
}

}  // namespace gamma_api

#endif  // FLATBUFFERS_GENERATED_TABLE_GAMMA_API_H_
