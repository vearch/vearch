#include "table/table.h"
#include "test.h"
#include "util/bitmap_manager.h"
#include "util/log.h"
#include "util/utils.h"

using namespace std;
using namespace vearch;

namespace Test {

class TableTest : public ::testing::Test {
 protected:
  static void SetUpTestSuite() {}

  static void TearDownTestSuite() {}

  // You can define per-test set-up logic as usual.
  virtual void SetUp() {
    if (utils::isFolderExist("./table")) utils::remove_dir("./table");
    utils::make_dir("./table");

    docids_bitmap = new bitmap::BitmapManager();
    docids_bitmap->SetDumpFilePath("./table/bitmap");
    int init_bitmap_size = 5000 * 10000;
    int file_bytes_size = docids_bitmap->FileBytesSize();
    if (file_bytes_size != 0) {
      init_bitmap_size = file_bytes_size * 8;
    }

    if (docids_bitmap->Init(init_bitmap_size) != 0) {
      LOG(ERROR) << "Cannot create bitmap!";
      return;
    }
    table = new Table("./table", "ts_space");
  }

  // You can define per-test tear-down logic as usual.
  virtual void TearDown() {
    CHECK_DELETE(docids_bitmap);
    CHECK_DELETE(table);
    CHECK_DELETE(table_info);
  }

  // member
  bitmap::BitmapManager *docids_bitmap;
  Table *table;
  TableInfo *table_info;
  std::vector<std::string> field_names;

  // Some expensive resource shared by all tests.
  // static T* shared_resource_;
  TableInfo *CreateTableInfo(struct Options &opt, bool is_index = false,
                             bool is_long = false) {
    TableInfo *table = new TableInfo();
    table->SetName(opt.vector_name);
    table->SetRetrievalType(opt.retrieval_type);
    table->SetRetrievalParam(opt.retrieval_param);
    table->SetIndexingSize(opt.indexing_size);

    struct vearch::FieldInfo field_info;
    field_info.is_index = is_index;

    field_info.name = "int";
    field_names.push_back(field_info.name);
    field_info.data_type = vearch::DataType::INT;
    table->AddField(field_info);

    field_info.name = "long";
    field_names.push_back(field_info.name);
    field_info.data_type = vearch::DataType::LONG;
    table->AddField(field_info);

    field_info.name = "float";
    field_names.push_back(field_info.name);
    field_info.data_type = vearch::DataType::FLOAT;
    table->AddField(field_info);

    field_info.name = "double";
    field_names.push_back(field_info.name);
    field_info.data_type = vearch::DataType::DOUBLE;
    table->AddField(field_info);

    field_info.name = "string";
    field_names.push_back(field_info.name);
    field_info.data_type = vearch::DataType::STRING;
    table->AddField(field_info);

    field_info.name = "_id";
    field_names.push_back(field_info.name);
    if (is_long)
      field_info.data_type = vearch::DataType::LONG;
    else
      field_info.data_type = vearch::DataType::STRING;
    table->AddField(field_info);
    return table;
  }

  int CreateTable(struct Options &opt, bool is_index = false,
                  bool is_long = false) {
    table_info = CreateTableInfo(opt, is_index, is_long);
    TableParams table_params;
    utils::JsonParser *meta_jp = nullptr;
    if (meta_jp) {
      utils::JsonParser table_jp;
      meta_jp->GetObject("table", table_jp);
      table_params.Parse(table_jp);
    }
    return table->CreateTable(*table_info, table_params, docids_bitmap);
  }

  void CreateValue(int i, std::string &value, vearch::DataType dataype) {
    if (dataype == vearch::DataType::INT) {
      value = std::string((char *)(&i), sizeof(int));
    }
    if (dataype == vearch::DataType::LONG) {
      long v = (long)i;
      value = std::string((char *)(&v), sizeof(long));
    }
    if (dataype == vearch::DataType::FLOAT) {
      float v = (float)i;
      value = std::string((char *)(&v), sizeof(float));
    }
    if (dataype == vearch::DataType::DOUBLE) {
      double v = (double)i;
      value = std::string((char *)(&v), sizeof(double));
    }
    if (dataype == vearch::DataType::STRING) {
      value = std::to_string(i);
    }
  }

  int GetValue(int i, std::string &value, vearch::DataType datatype) {
    if (datatype == vearch::DataType::INT) {
      int v;
      memcpy(&v, value.data(), sizeof(int));
      return (v != i ? 1 : 0);
    }
    if (datatype == vearch::DataType::LONG) {
      long v;
      memcpy(&v, value.data(), sizeof(long));
      return (v != i ? 1 : 0);
    }
    if (datatype == vearch::DataType::FLOAT) {
      float v;
      memcpy(&v, value.data(), sizeof(float));
      return (v != i ? 1 : 0);
    }
    if (datatype == vearch::DataType::DOUBLE) {
      double v;
      memcpy(&v, value.data(), sizeof(double));
      return (v != i ? 1 : 0);
    }
    if (datatype == vearch::DataType::STRING) {
      std::string v = std::to_string(i);
      return (v != value ? 1 : 0);
    }
    return -1;
  }

  void CreateFieldValue(
      std::unordered_map<std::string, struct Field> &fields_table, int value,
      bool has_id) {
    for (size_t j = 0; j < field_names.size(); j++) {
      struct Field field;
      if (field_names[j] == "_id" && !has_id) continue;
      field.name = field_names[j];
      table->GetFieldType(field.name, field.datatype);
      CreateValue(value, field.value, field.datatype);
      fields_table[field_names[j]] = field;
    }
  }

  int Add(int add_num = 10000) {
    for (int i = 0; i < add_num; i++) {
      std::string key;
      if (table->IdType() == 1) {
        long v = (long)i;
        key = std::string((char *)(&v), sizeof(long));
      } else {
        key = std::to_string(i);
      }
      std::unordered_map<std::string, struct Field> fields_table;
      CreateFieldValue(fields_table, i, true);
      if (table->Add(key, fields_table, i)) return -1;
    }
    return 0;
  }

  int GetFieldValue(int doc_id, int value) {
    std::string key;
    if (table->IdType() == 1) {
      long v = (long)doc_id;
      key = std::string((char *)(&v), sizeof(long));
    } else {
      key = std::to_string(doc_id);
    }
    std::vector<std::string> fields;
    Doc doc;
    if (table->GetDocInfo(key, doc, fields)) return -1;
    auto &table_fields = doc.TableFields();
    for (auto &f : table_fields) {
      vearch::DataType datatype;
      table->GetFieldType(f.second.name, datatype);
      if (f.second.name == "_id") {
        if (GetValue(doc_id, f.second.value, datatype)) return -2;
      } else {
        if (GetValue(value, f.second.value, datatype)) return -3;
      }
    }
    return 0;
  }

  int Get(int add_num = 10000) {
    int ret = 0;
    for (int i = 0; i < add_num; i++) {
      ret = GetFieldValue(i, i);
      if (ret) return ret;
    }
    return ret;
  }

  int Update() {
    int ret = 0;
    std::vector<int> update_ids = {1, 3, 50, 100, 999};
    int update_value = 555555;
    // update
    for (size_t i = 0; i < update_ids.size(); i++) {
      std::unordered_map<std::string, struct Field> fields;
      CreateFieldValue(fields, update_value, false);
      table->Update(fields, update_ids[i]);
    }
    // check update
    for (size_t i = 0; i < update_ids.size(); i++) {
      ret = GetFieldValue(update_ids[i], update_value);
      if (ret) return ret;
    }
    return ret;
  }

  int Delete() {
    int ret = 0;
    std::vector<int> delete_ids = {1, 3, 50, 100, 999};
    for (size_t i = 0; i < delete_ids.size(); i++) {
      std::string key;
      if (table->IdType() == 1) {
        long v = (long)delete_ids[i];
        key = std::string((char *)(&v), sizeof(long));
      } else {
        key = std::to_string(delete_ids[i]);
      }
      table->Delete(key);
    }
    // check delete
    for (size_t i = 0; i < delete_ids.size(); i++) {
      ret = GetFieldValue(delete_ids[i], i);
    }
    return ret;
  }

  int BatchAdd(int add_num = 10000) { return 0; }
};

TEST_F(TableTest, CreateStringTable) {
  struct Options opt;
  ASSERT_EQ(0, CreateTable(opt));
  ASSERT_EQ(0, Add());
  ASSERT_EQ(9999, table->last_docid_);
  ASSERT_EQ(0, Get());
  ASSERT_EQ(0, Update());
  ASSERT_EQ(-1, Delete());
}

TEST_F(TableTest, CreateStringTableINDEXED) {
  struct Options opt;
  ASSERT_EQ(0, CreateTable(opt, true));
  ASSERT_EQ(0, Add());
  ASSERT_EQ(9999, table->last_docid_);
  ASSERT_EQ(0, Get());
  ASSERT_EQ(0, Update());
  ASSERT_EQ(-1, Delete());
}

TEST_F(TableTest, CreateLongTable) {
  struct Options opt;
  ASSERT_EQ(0, CreateTable(opt, false, true));
  ASSERT_EQ(0, Add());
  ASSERT_EQ(9999, table->last_docid_);
  ASSERT_EQ(0, Get());
  ASSERT_EQ(0, Update());
  ASSERT_EQ(-1, Delete());
}

TEST_F(TableTest, CreateLongTableINDEXED) {
  struct Options opt;
  ASSERT_EQ(0, CreateTable(opt, true, true));
  ASSERT_EQ(0, Add());
  ASSERT_EQ(9999, table->last_docid_);
  ASSERT_EQ(0, Get());
  ASSERT_EQ(0, Update());
  ASSERT_EQ(-1, Delete());
}

}  // namespace Test
