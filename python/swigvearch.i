%module swigvearch

#define VEARCH_VERSION_MAJOR 0
#define VEARCH_VERSION_MINOR 3
#define VEARCH_VERSION_PATCH 0

%{
#define SWIG_FILE_WITH_INIT
#define NPY_NO_DEPRECATED_API NPY_1_7_API_VERSION
#include <numpy/arrayobject.h>
%}

%include <stdint.i>
typedef int64_t size_t;

#define __restrict

%{
#include "../c_api/gamma_api.h"
%}

%ignore ResponseCode;
%ignore DistanceMetricType;
%ignore DataType;
%ignore IndexStatus;
%ignore SearchResultCode;

%ignore Init;
%ignore Close;

%include "../c_api/gamma_api.h"

%inline %{
struct ResponseCodes {
    enum { SUCCESSED = 0, FAILED = 1 };
};
%}

%inline %{
struct DistanceMetricTypes {
    enum { InnerProduct = 0, L2  };
};
%}

%inline %{
struct DataTypes {
    enum { INT = 0, LONG = 1, FLOAT = 2, DOUBLE = 3, STRING = 4, VECTOR = 5 };
};
%}

%inline %{
struct IndexStatuses {
    enum { UNINDEXED = 0, INDEXING, INDEXED };
};
%}

%inline %{
struct SearchResultCodes {
    enum { SUCCESS = 0, INDEX_NOT_TRAINED, SEARCH_ERROR };
};
%}

%include <std_string.i>
%include <std_vector.i>

%template(IntVector) std::vector<int>;
%template(LongVector) std::vector<long>;
%template(ULongVector) std::vector<unsigned long>;
%template(CharVector) std::vector<char>;
%template(UCharVector) std::vector<unsigned char>;
%template(FloatVector) std::vector<float>;
%template(DoubleVector) std::vector<double>;


const std::string ByteArrayToString(ByteArray *value);

template <typename T>
std::vector<T> ByteArrayToVector(ByteArray *value);

template <typename T>
T ByteArrayToType(ByteArray *value);

template <typename T>
ByteArray * TypeToByteArray(T value);

ByteArray *StringToByteArray(const std::string &str);

template <typename T>
ByteArray *PointerToByteArray(const T *feature, int dimension);

Config *MakeConfig(const std::string &path, int max_doc_size);

VectorInfo *MakeVectorInfo(const std::string &name, int data_type,
                           BOOL is_index, int dimension, 
                           const std::string &model_id,
                           const std::string &retrieval_type, 
                           const std::string &store_type,
                           const std::string &store_param);

Field *MakeField(ByteArray *name, ByteArray *value,
                 enum DataType data_type);

%rename(InitEngine) Init;
extern void *Init(Config *config);

%rename(CloseEngine) Close;
extern enum ResponseCode Close(void *engine);

%{

const std::string ByteArrayToString(ByteArray * value) {
  assert(value != nullptr);
  return std::string(value->value, value->len);
}

template <typename T>
T ByteArrayToType(ByteArray * value) {
  assert(value != nullptr);
  T data = 0;
  memcpy(&data, value->value, value->len);
  return data;
}

template <typename T>
ByteArray * TypeToByteArray(T value) {
  ByteArray *ba = static_cast<ByteArray *>(malloc(sizeof(ByteArray)));
  ba->len = sizeof(T);
  ba->value = static_cast<char *>(malloc(ba->len * sizeof(char)));
  memcpy(ba->value, &value, ba->len);
  return ba;  
}

template <typename T>
std::vector<T> ByteArrayToVector(ByteArray *value) {
  int len = value->len / sizeof(T) - 1;
  std::vector<T> vec(len);
  memcpy(vec.data(), value->value + sizeof(int), value->len);
  return vec;
}

ByteArray *StringToByteArray(const std::string &str) {
  ByteArray *ba = static_cast<ByteArray *>(malloc(sizeof(ByteArray)));
  ba->len = str.length();
  ba->value = static_cast<char *>(malloc((str.length()) * sizeof(char)));
  memcpy(ba->value, str.data(), str.length());
  return ba;
}

template <typename T>
ByteArray *PointerToByteArray(const T *feature, int dimension) {
  ByteArray *ba = static_cast<ByteArray *>(malloc(sizeof(ByteArray)));
  ba->len = dimension * sizeof(T);
  ba->value = static_cast<char *>(malloc(ba->len));
  memcpy((void *)ba->value, (void *)feature, ba->len);
  return ba;
}

Config *MakeConfig(const std::string &path, int max_doc_size) {
    ByteArray *ba = StringToByteArray(path);
    
    Config *config = static_cast<Config *>(malloc(sizeof(Config)));
    memset(config, 0, sizeof(Config));
    config->path = ba;
    config->max_doc_size = max_doc_size;
    return config;
}

VectorInfo *MakeVectorInfo(const std::string &name, int data_type,
                           BOOL is_index, int dimension, const std::string &model_id,
                           const std::string &retrieval_type, const std::string &store_type,
                           const std::string &store_param) {
  ByteArray *ba_name = StringToByteArray(name);
  ByteArray *ba_model_id = StringToByteArray(model_id);
  ByteArray *ba_retrieval_type = StringToByteArray(retrieval_type);
  ByteArray *ba_store_type = StringToByteArray(store_type);
  ByteArray *ba_store_param = StringToByteArray(store_param);

  VectorInfo *vectorInfo =
      static_cast<VectorInfo *>(malloc(sizeof(VectorInfo)));
  memset(vectorInfo, 0, sizeof(VectorInfo));
  vectorInfo->name = ba_name;
  vectorInfo->data_type = static_cast<enum DataType>(data_type);
  vectorInfo->is_index = is_index;
  vectorInfo->dimension = dimension;
  vectorInfo->model_id = ba_model_id;
  vectorInfo->retrieval_type = ba_retrieval_type;
  vectorInfo->store_type = ba_store_type;
  vectorInfo->store_param = ba_store_param;
  return vectorInfo;
}

Field *MakeField(ByteArray *name, ByteArray *value,
                 enum DataType data_type) {
  Field *field_info = static_cast<Field *>(malloc(sizeof(Field)));
  memset(field_info, 0, sizeof(Field));
  field_info->name = name;
  field_info->data_type = data_type;
  field_info->value = value;
  field_info->source = StringToByteArray("");
  return field_info;
}

%}

%template(FloatsToByteArray) PointerToByteArray<float>;
%template(BytesToByteArray) PointerToByteArray<unsigned char>;
%template(CharsToByteArray) PointerToByteArray<char>;
%template(IntsToByteArray) PointerToByteArray<int>;
%template(UInt64sToByteArray) PointerToByteArray<unsigned long>;
%template(LongsToByteArray) PointerToByteArray<long>;
%template(DoublesToByteArray) PointerToByteArray<double>;

%template(FloatToByteArray) TypeToByteArray<float>;
%template(ByteToByteArray) TypeToByteArray<unsigned char>;
%template(CharToByteArray) TypeToByteArray<char>;
%template(IntToByteArray) TypeToByteArray<int>;
%template(UInt64ToByteArray) TypeToByteArray<unsigned long>;
%template(LongToByteArray) TypeToByteArray<long>;
%template(DoubleToByteArray) TypeToByteArray<double>;

%template(ByteArrayToInt) ByteArrayToType<int>;
%template(ByteArrayToLong) ByteArrayToType<long>;
%template(ByteArrayToFloat) ByteArrayToType<float>;
%template(ByteArrayToDouble) ByteArrayToType<double>;

%template(ByteArrayToIntVector) ByteArrayToVector<int>;
%template(ByteArrayToLongVector) ByteArrayToVector<long>;
%template(ByteArrayToULongVector) ByteArrayToVector<unsigned long>;
%template(ByteArrayToCharVector) ByteArrayToVector<char>;
%template(ByteArrayToUCharVector) ByteArrayToVector<unsigned char>;
%template(ByteArrayToFloatVector) ByteArrayToVector<float>;
%template(ByteArrayToDoubleVector) ByteArrayToVector<double>;

/*******************************************************************
 * Python specific: numpy array <-> C++ pointer interface
 *******************************************************************/

%{
PyObject *swig_ptr (PyObject *a)
{
    if(!PyArray_Check(a)) {
        PyErr_SetString(PyExc_ValueError, "input not a numpy array");
        return NULL;
    }
    PyArrayObject *ao = (PyArrayObject *)a;

    if(!PyArray_ISCONTIGUOUS(ao)) {
        PyErr_SetString(PyExc_ValueError, "array is not C-contiguous");
        return NULL;
    }
    void * data = PyArray_DATA(ao);
    if(PyArray_TYPE(ao) == NPY_FLOAT32) {
        return SWIG_NewPointerObj(data, SWIGTYPE_p_float, 0);
    }
    if(PyArray_TYPE(ao) == NPY_FLOAT64) {
        return SWIG_NewPointerObj(data, SWIGTYPE_p_double, 0);
    }
    if(PyArray_TYPE(ao) == NPY_INT32) {
        return SWIG_NewPointerObj(data, SWIGTYPE_p_int, 0);
    }
    if(PyArray_TYPE(ao) == NPY_UINT8) {
        return SWIG_NewPointerObj(data, SWIGTYPE_p_unsigned_char, 0);
    }
    if(PyArray_TYPE(ao) == NPY_INT8) {
        return SWIG_NewPointerObj(data, SWIGTYPE_p_char, 0);
    }
    if(PyArray_TYPE(ao) == NPY_UINT64) {
#ifdef SWIGWORDSIZE64
        return SWIG_NewPointerObj(data, SWIGTYPE_p_unsigned_long, 0);
#else
        return SWIG_NewPointerObj(data, SWIGTYPE_p_unsigned_long_long, 0);
#endif
    }
    if(PyArray_TYPE(ao) == NPY_INT64) {
#ifdef SWIGWORDSIZE64
        return SWIG_NewPointerObj(data, SWIGTYPE_p_long, 0);
#else
        return SWIG_NewPointerObj(data, SWIGTYPE_p_long_long, 0);
#endif
    }
    PyErr_SetString(PyExc_ValueError, "did not recognize array type");
    return NULL;
}

%}

%init %{
    import_array();
%}

// return a pointer usable as input for functions that expect pointers
PyObject *swig_ptr (PyObject *a);

%define REV_SWIG_PTR(ctype, numpytype)

%{
PyObject * rev_swig_ptr(ctype *src, npy_intp size) {
    return PyArray_SimpleNewFromData(1, &size, numpytype, src);
}
%}

PyObject * rev_swig_ptr(ctype *src, size_t size);

%enddef

REV_SWIG_PTR(float, NPY_FLOAT32);
REV_SWIG_PTR(int, NPY_INT32);
REV_SWIG_PTR(unsigned char, NPY_UINT8);
REV_SWIG_PTR(int64_t, NPY_INT64);
REV_SWIG_PTR(uint64_t, NPY_UINT64);

