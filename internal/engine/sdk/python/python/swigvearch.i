%module swigvearch

%{
#define SWIG_FILE_WITH_INIT
#define NPY_NO_DEPRECATED_API NPY_1_7_API_VERSION
#include <numpy/arrayobject.h>
%}

%include<stdint.i> 
%include<std_string.i> 

typedef int64_t size_t;

#define __restrict

%{
#include <stdio.h>
#include <stdlib.h>

#include <iostream>
#include <vector>

#include "c_api/api_data/cpp_api.h"
#include "c_api/api_data/doc.h"
#include "c_api/api_data/raw_data.h"
#include "c_api/api_data/request.h"
#include "c_api/api_data/response.h"
#include "c_api/api_data/table.h"
#include "c_api/gamma_api.h"
#include "common/common_query_data.h"


%}

%inline %{
  void *swigInitEngine(unsigned char *pConfig, int len) {
    char *config_str = (char *)pConfig;
    void *engine = Init(config_str, len);
    return engine;
  }

  int swigClose(void *engine) {
    int res = Close(engine);
    return res;
  }

  PyObject *swigCreateTable(void *engine, unsigned char *pTable, int len) {
    char *table_str = (char *)pTable;
    struct CStatus status = CreateTable(engine, table_str, len);
    PyObject *py_status;

    if (status.msg) {
        py_status = Py_BuildValue("{s:i, s:s}", "code", status.code, "msg", status.msg);
    } else {
        py_status = Py_BuildValue("{s:i, s:s}", "code", status.code, "msg", "");
    }

    return py_status;
  }

  int swigAddOrUpdateDoc(void *engine, unsigned char *pDoc, int len) {
    char *doc_str = (char *)pDoc;
    return AddOrUpdateDoc(engine, doc_str, len);
  }

  int swigDeleteDoc(void *engine, unsigned char *docid, int len) {
    char *doc_id = (char *)docid;
    return DeleteDoc(engine, doc_id, len);
  }

  PyObject *swigGetEngineStatus(void *engine) {
    char *status_str = NULL;
    int len = 0;
    GetEngineStatus(engine, &status_str, &len);
    if (status_str == NULL || len == 0) {
      Py_RETURN_NONE;
    }

    PyObject *py_status = Py_BuildValue("s#", status_str, len);
    free(status_str);
    return py_status;
  }

  PyObject *swigGetDocByID(void *engine, unsigned char *docid, int docid_len) {
    char *doc_str = NULL;
    int len = 0;
    int res = GetDocByID(engine, (char *)docid, docid_len, &doc_str, &len);
    if (res == 0) {
      PyObject *py_doc = Py_BuildValue("y#", doc_str, (Py_ssize_t)len);
      free(doc_str);
      return py_doc;
    } else {
      Py_RETURN_NONE;
    }
  }

  PyObject *swigGetDocByDocID(void *engine, int docid) {
    char *doc_str = NULL;
    int len = 0;
    int res = GetDocByDocID(engine, docid, false, &doc_str, &len);
    if (res == 0) {
      PyObject *py_doc = Py_BuildValue("y#", doc_str, (Py_ssize_t)len);
      free(doc_str);
      return py_doc;
    } else {
      Py_RETURN_NONE;
    }
  }

  int swigDump(void *engine) { return Dump(engine); }

  int swigLoad(void *engine) { return Load(engine); }

  PyObject *swigSearch(void *engine, unsigned char *pRequest, int req_len) {
    char *request_str = (char *)pRequest;
    char *response_str = NULL;
    int res_len = 0;
    struct CStatus status = Search(engine, request_str, req_len, &response_str, &res_len);
    if (status.code == 0) {
      std::vector<unsigned char> vec_res(res_len);
      memcpy(vec_res.data(), response_str, res_len);
      free(response_str);
      response_str = NULL;

      PyObject *py_result = Py_BuildValue("{s:i, s:s, s:O}",
                                          "code", status.code,
                                          "msg", status.msg ? status.msg : "",
                                          "data", Py_BuildValue("y#", vec_res.data(), vec_res.size()));
      return py_result;
    } else {
      PyObject *py_status = Py_BuildValue("{s:i, s:s, s:O}",
                                          "code", status.code,
                                          "msg", status.msg ? status.msg : "",
                                          "data", Py_None);
      return py_status;
    }
  }

  PyObject *swigQuery(void *engine, unsigned char *pRequest, int req_len) {
    char *request_str = (char *)pRequest;
    char *response_str = NULL;
    int res_len = 0;
    struct CStatus status = Query(engine, request_str, req_len, &response_str, &res_len);
    if (status.code == 0) {
      std::vector<unsigned char> vec_res(res_len);
      memcpy(vec_res.data(), response_str, res_len);
      free(response_str);
      response_str = NULL;

      PyObject *py_result = Py_BuildValue("{s:i, s:s, s:O}",
                                          "code", status.code,
                                          "msg", status.msg ? status.msg : "",
                                          "data", Py_BuildValue("y#", vec_res.data(), vec_res.size()));
      return py_result;
    } else {
      PyObject *py_status = Py_BuildValue("{s:i, s:s, s:O}",
                                          "code", status.code,
                                          "msg", status.msg ? status.msg : "",
                                          "data", Py_None);
      return py_status;
    }
  }

  int swigSearchCPP(void *engine, vearch::Request *request,
                    vearch::Response *response) {
    return CPPSearch(engine, request, response);
  }

  int swigAddOrUpdateDocCPP(void *engine, vearch::Doc *doc) {
    return CPPAddOrUpdateDoc(engine, doc);
  }

  unsigned char *swigGetVectorPtr(std::vector<unsigned char> & v) {
    return v.data();
  }
  
  
%}

void *memcpy(void *dest, const void *src, size_t n);

/*******************************************************************
 * Types of vectors we want to manipulate at the scripting language
 * level.
 *******************************************************************/

// simplified interface for vector
namespace std {

template <class T>
class vector {
 public:
  vector();
  void push_back(T);
  void clear();
  T *data();
  size_t size();
  T at(size_t n) const;
  void resize(size_t n);
  void swap(vector<T> &other);
};
};  // namespace std

%template(IntVector) std::vector<int>;
%template(LongVector) std::vector<long>;
%template(ULongVector) std::vector<unsigned long>;
%template(CharVector) std::vector<char>;
%template(UCharVector) std::vector<unsigned char>;
%template(FloatVector) std::vector<float>;
%template(DoubleVector) std::vector<double>;
%template(StringVector) std::vector<std::string>;

/*******************************************************************
 * Python specific: numpy array <-> C++ pointer interface
*******************************************************************/

%{
PyObject * swig_ptr(PyObject * a)
{
  if (!PyArray_Check(a)){
    PyErr_SetString(PyExc_ValueError, "input not a numpy array");
    return NULL;
  }
  PyArrayObject *ao = (PyArrayObject *)a;

  if (!PyArray_ISCONTIGUOUS(ao)) {
    PyErr_SetString(PyExc_ValueError, "array is not C-contiguous");
    return NULL;
  }
  void *data = PyArray_DATA(ao);
  if (PyArray_TYPE(ao) == NPY_FLOAT32) {
    return SWIG_NewPointerObj(data, SWIGTYPE_p_float, 0);
  }
  // if(PyArray_TYPE(ao) == NPY_FLOAT64) {
  //     return SWIG_NewPointerObj(data, SWIGTYPE_p_double, 0);
  // }
  if (PyArray_TYPE(ao) == NPY_INT32) {
    return SWIG_NewPointerObj(data, SWIGTYPE_p_int, 0);
  }
  if (PyArray_TYPE(ao) == NPY_UINT8) {
    return SWIG_NewPointerObj(data, SWIGTYPE_p_unsigned_char, 0);
  }
  if (PyArray_TYPE(ao) == NPY_INT8) {
    return SWIG_NewPointerObj(data, SWIGTYPE_p_char, 0);
  }
  if (PyArray_TYPE(ao) == NPY_UINT64) {
#ifdef SWIGWORDSIZE64
    return SWIG_NewPointerObj(data, SWIGTYPE_p_unsigned_long, 0);
#else
    return SWIG_NewPointerObj(data, SWIGTYPE_p_unsigned_long_long, 0);
#endif
  }
  if (PyArray_TYPE(ao) == NPY_INT64) {
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
PyObject *swig_ptr(PyObject *a);

%define REV_SWIG_PTR(ctype, numpytype)

%{

PyObject * rev_swig_ptr(ctype * src, npy_intp size){
  return PyArray_SimpleNewFromData(1, &size, numpytype, src);
}

%}

PyObject *rev_swig_ptr(ctype *src, size_t size);

%enddef

REV_SWIG_PTR(float, NPY_FLOAT32);
REV_SWIG_PTR(int, NPY_INT32);
REV_SWIG_PTR(unsigned char, NPY_UINT8);
REV_SWIG_PTR(int64_t, NPY_INT64);
REV_SWIG_PTR(uint64_t, NPY_UINT64);
