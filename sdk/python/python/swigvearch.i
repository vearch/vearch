%module swigvearch

#define VEARCH_VERSION_MAJOR 3
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

%include <std_string.i>
%include <std_vector.i>

%{
#include <stdio.h>
#include <vector>
#include <stdlib.h>
#include <iostream>

#include "c_api/gamma_api.h"
#include "c_api/api_data/gamma_cpp_api.h"
#include "c_api/api_data/gamma_raw_data.h"
#include "c_api/api_data/gamma_request.h"
#include "c_api/api_data/gamma_response.h"
#include "c_api/api_data/gamma_table.h"
#include "c_api/api_data/gamma_doc.h"
#include "c_api/api_data/gamma_docs.h"
#include "c_api/api_data/gamma_batch_result.h"
#include "common/common_query_data.h"
%}

%inline%{
    void *swigInitEngine(unsigned char *pConfig, int len){
        char* config_str = (char*)pConfig;
        void* engine = Init(config_str, len);
        return engine;
    }

    int swigClose(void *engine){
        int res = Close(engine);
        return res;
    }

    int swigCreateTable(void *engine, unsigned char *pTable, int len){
        char *table_str = (char*)pTable;
        return CreateTable(engine, table_str, len);
    }

    int swigAddOrUpdateDoc(void *engine, unsigned char *pDoc, int len){
        char *doc_str = (char*)pDoc; 
        return AddOrUpdateDoc(engine, doc_str, len);
    }

    int swigUpdateDoc(void *engine, unsigned char *pDoc, int len){
        char *doc_str = (char*)pDoc;
        return UpdateDoc(engine, doc_str, len);
    }

    int swigDeleteDoc(void *engine,unsigned char *docid, int len){
        char *doc_id = (char*)docid;
        return DeleteDoc(engine, doc_id, len);
    }

    std::vector<unsigned char> swigGetEngineStatus(void *engine){
        char *status_str = NULL;
        int len = 0;
        GetEngineStatus(engine, &status_str, &len);
        std::vector<unsigned char> vec_status(len);
        memcpy(vec_status.data(), status_str, len);
        free(status_str);
        status_str = NULL;
        return vec_status;
    }

    std::vector<unsigned char> swigGetDocByID(void *engine, char *docid, int docid_len){
        char *doc_id = (char*)docid;
        char *doc_str = NULL;
        int len = 0;
        int res = GetDocByID(engine, doc_id, docid_len, &doc_str, &len);
        if(res == 0){
            std::vector<unsigned char> vec_doc(len);
            memcpy(vec_doc.data(), doc_str, len);
            free(doc_str);
            doc_str = NULL;
            return vec_doc;
        }
        else{
            std::vector<unsigned char> vec_doc(1);
            return vec_doc;
        }
    }
    
    std::vector<unsigned char> swigGetDocByDocID(void *engine, int docid){
        char *doc_str = NULL;
        int len = 0;
        int res = GetDocByDocID(engine, docid, &doc_str, &len);
        if(res == 0){
            std::vector<unsigned char> vec_doc(len);
            memcpy(vec_doc.data(), doc_str, len);
            free(doc_str);
            doc_str = NULL;
            return vec_doc;
        }
        else{
            std::vector<unsigned char> vec_doc(1);
            return vec_doc;
        }
    }

    int swigBuildIndex(void* engine){
        return BuildIndex(engine);
    }

    int swigDump(void* engine){
        return Dump(engine);
    }

    int swigLoad(void* engine){
        return Load(engine);
    }
    
    std::vector<unsigned char> swigSearch(void* engine, unsigned char* pRequest, int req_len){
        char* request_str = (char*)pRequest;
        char* response_str = NULL;
        int res_len = 0;
        int code_response = Search(engine, request_str, req_len, &response_str, &res_len);
        if(code_response == 0){    
            std::vector<unsigned char> vec_res(res_len);
            memcpy(vec_res.data(), response_str, res_len);
            free(response_str);
            response_str = NULL;
            return vec_res;
        }else{
            std::vector<unsigned char> vec_res(1);
            return vec_res;
        }
    }
    
    tig_gamma::Request *swigCreateRequest() {
        return new tig_gamma::Request();    
    }

    void swigDeleteRequest(tig_gamma::Request *request) {
        if (request) {
            delete request;
            request = nullptr;
        }    
    }
    
    tig_gamma::Response *swigCreateResponse() {
        return new tig_gamma::Response();    
    }
    
    void swigDeleteResponse(tig_gamma::Response *response) {
        if (response) {
            delete response;
            response = nullptr;
        }    
    }

    template <typename T>
    T GetValueFromStringVector(std::vector<std::string> value, int index, int data_type) {
        T real_value;
        memcpy(&real_value, value[index].c_str(), value[index].size());
        return real_value; 
    }

    template <typename T>
    std::vector<T> GetVectorFromStringVector(std::vector<std::string> value, int index, int is_binary) {
        std::vector<T> real_value;
        if (is_binary) {
            real_value.resize(value[index].size());
        } else {
            real_value.resize(value[index].size() / sizeof(float));
        }
        memcpy(real_value.data(), value[index].c_str(), value[index].size());
        return real_value; 
    }

    template <typename T>
    tig_gamma::Field CreateScalarField(const std::string &name, const T &value, int len, const std::string &source, int data_type) {
        tig_gamma::Field field;
        field.name = name;
        if (data_type == DataType::STRING)
            field.value = value;
        else {
            std::string value_str(len, '0');
            memcpy(const_cast<char *>(value_str.c_str()), &value, len);
            field.value = value_str;
        }
        field.source = source;
        field.datatype = (tig_gamma::DataType)data_type;
        return field; 
    }

    tig_gamma::Field CreateVectorField(const std::string &name, uint8_t *data, int len, const std::string &source, int data_type) {
        tig_gamma::Field field;
        field.name = name;
        field.value = std::string((char *)data, len);
        field.source = source;
        field.datatype = (tig_gamma::DataType)data_type;
        return field; 
    }

    tig_gamma::Doc *swigCreateDoc() {
        return new tig_gamma::Doc();    
    }

    void swigDeleteDoc(tig_gamma::Doc *doc) {
        if (doc) {
            delete doc;
            doc = nullptr;
        }    
    }

    tig_gamma::TermFilter CreateTermFilter(const std::string &name, const std::string &value, int is_union) {
        tig_gamma::TermFilter term_filter;
        term_filter.field = name;
        term_filter.value = value;
        term_filter.is_union = is_union;
        return term_filter; 
    }

    tig_gamma::RangeFilter CreateRangeFilter(const std::string &name, uint8_t *lower_value, int lower_value_len, uint8_t *upper_value, int upper_value_len, int include_lower, int include_upper) {
        tig_gamma::RangeFilter range_filter;
        range_filter.field = name;
        range_filter.lower_value = std::string((char *)lower_value, lower_value_len);
        range_filter.upper_value = std::string((char *)upper_value, upper_value_len);
        range_filter.include_lower = include_lower;
        range_filter.include_upper = include_upper;
        return range_filter; 
    }

    tig_gamma::VectorQuery CreateVectorQuery(const std::string &name, float *data, int len, double min_score, double max_score, double boost, int has_boost, const std::string &retrieval_type) {
        tig_gamma::VectorQuery vector_query;
        vector_query.name = name;
        vector_query.value = std::string((char *)data, len * sizeof(float));
        vector_query.min_score = min_score;
        vector_query.max_score = max_score;
        vector_query.boost = boost;
        vector_query.has_boost = has_boost;
        vector_query.retrieval_type = retrieval_type;
        return vector_query; 
    }

    int swigSearchCPP(void* engine, tig_gamma::Request *request, tig_gamma::Response *response) {
        return CPPSearch(engine, request, response);
    }

    int swigAddOrUpdateDocCPP(void* engine, tig_gamma::Doc *doc) {
        return CPPAddOrUpdateDoc(engine, doc);
    }

    tig_gamma::BatchResult *swigCreateBatchResult(int len) {
        return new tig_gamma::BatchResult(len);    
    }

    void swigDeleteBatchResult(tig_gamma::BatchResult *results) {
        if(results) {
            delete results;
            results = nullptr;
        }    
    }

    int swigAddOrUpdateDocsCPP(void* engine, tig_gamma::Docs *docs, tig_gamma::BatchResult *results) {
        return CPPAddOrUpdateDocs(engine, docs, results);
    }

    // int swigDelDocByQuery(void* engine, unsigned char *pRequest, int len){
    //     char* request_str = (char*)pRequest;
    //     return DelDocByQuery(engine, request_str, len);
    // }
    
    unsigned char* swigGetVectorPtr(std::vector<unsigned char> &v){
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

    template<class T>
    class vector {
    public:
        vector();
        void push_back(T);
        void clear();
        T * data();
        size_t size();
        T at (size_t n) const;
        void resize (size_t n);
        void swap (vector<T> & other);
    };
};

%template(IntVector) std::vector<int>;
%template(LongVector) std::vector<long>;
%template(ULongVector) std::vector<unsigned long>;
%template(CharVector) std::vector<char>;
%template(UCharVector) std::vector<unsigned char>;
%template(FloatVector) std::vector<float>;
%template(DoubleVector) std::vector<double>;
%template(StringVector) std::vector<std::string>;
%template(SearchResultVector) std::vector<tig_gamma::SearchResult>;
%template(ResultItemVector) std::vector<tig_gamma::ResultItem>;
%template(DocVector) std::vector<tig_gamma::Doc>;
%template(FieldVector) std::vector<tig_gamma::Field>;

%template(CreateStringScalarField) CreateScalarField<std::string>;
%template(CreateIntScalarField) CreateScalarField<int>;
%template(CreateLongScalarField) CreateScalarField<long long>;
%template(CreateFloatScalarField) CreateScalarField<float>;
%template(CreateDouleScalarField) CreateScalarField<double>;

%template(GetIntFromStringVector) GetValueFromStringVector<int>;
%template(GetLongFromStringVector) GetValueFromStringVector<long long>;
%template(GetFloatFromStringVector) GetValueFromStringVector<float>;
%template(GetDoubleFromStringVector) GetValueFromStringVector<double>;

%template(GetCharVectorFromStringVector) GetVectorFromStringVector<char>;
%template(GetFloatVectorFromStringVector) GetVectorFromStringVector<float>;

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
    //if(PyArray_TYPE(ao) == NPY_FLOAT64) {
    //    return SWIG_NewPointerObj(data, SWIGTYPE_p_double, 0);
    //}
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
