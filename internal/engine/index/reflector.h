/**
 * Copyright 2019 The Gamma Authors.
 *
 * This source code is licensed under the Apache License, Version 2.0 license
 * found in the LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <iostream>
#include <map>
#include <mutex>

class IndexModel;

// Reflector index base class, to create new index
class IndexFactory {
 public:
  IndexFactory() {}

  virtual ~IndexFactory() {}

  virtual IndexModel *NewIndex() = 0;
};

// Index reflector, manage class_name and index factories
class Reflector {
 public:
  Reflector() {}
  ~Reflector() {
    for (auto &factory : index_factories_) {
      delete factory.second;
    }
    index_factories_.clear();
  }

  // Register index factory with index_name
  void RegisterFactory(const std::string &index_name,
                       IndexFactory *index_factory) {
    std::lock_guard<std::mutex> lock(mutex_);
    const auto &it = index_factories_.find(index_name);
    if (it != index_factories_.end()) {
      std::cout << "Duplicated model [" << index_name << "]";
    } else {
      index_factories_[index_name] = index_factory;
    }
  }

  // Create a new index with index_name
  IndexModel *GetNewIndex(const std::string &index_name) {
    const auto &it = index_factories_.find(index_name);
    if (it != index_factories_.end()) {
      IndexFactory *index = it->second;
      return index->NewIndex();
    }
    return nullptr;
  }

 private:
  std::map<std::string, IndexFactory *> index_factories_;
  std::mutex mutex_;
};

// Global var
Reflector &reflector();

// Register index with index_name, example REGISTER_INDEX("INDEX", INDEX_CLASS)
#define REGISTER_INDEX(index_name, class_name)                      \
  class IndexFactory_##class_name : public IndexFactory {           \
   public:                                                          \
    IndexModel *NewIndex() { return new class_name(); }         \
  };                                                                \
  class Register_##class_name {                                     \
   public:                                                          \
    Register_##class_name() {                                       \
      reflector().RegisterFactory(#index_name,                      \
                                  new IndexFactory_##class_name()); \
    }                                                               \
  };                                                                \
  Register_##class_name register_##class_name;
