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

class RetrievalModel;

// Reflector retrieval model base class, to create new retrieval model
class ModelFactory {
 public:
  ModelFactory() {}

  virtual ~ModelFactory() {}

  virtual RetrievalModel *NewModel() = 0;
};

// Model reflector, manage class_name and retrieval model factories
class Reflector {
 public:
  Reflector() {}
  ~Reflector() {
    for (auto &factory : model_factories_) {
      delete factory.second;
    }
    model_factories_.clear();
  }

  // Register retrieval model factory with model_name
  void RegisterFactory(const std::string &model_name,
                       ModelFactory *model_factory) {
    std::lock_guard<std::mutex> lock(mutex_);
    const auto &it = model_factories_.find(model_name);
    if (it != model_factories_.end()) {
      std::cout << "Duplicated model [" << model_name << "]";
    } else {
      model_factories_[model_name] = model_factory;
    }
  }

  // Create a new retrieval model with model_name
  RetrievalModel *GetNewModel(const std::string &model_name) {
    const auto &it = model_factories_.find(model_name);
    if (it != model_factories_.end()) {
      ModelFactory *model = it->second;
      return model->NewModel();
    }
    return nullptr;
  }

 private:
  std::map<std::string, ModelFactory *> model_factories_;
  std::mutex mutex_;
};

// Global var
Reflector &reflector();

// Register model with model_name, example REGISTER_MODEL("MODEL", MODEL_CLASS)
#define REGISTER_MODEL(model_name, class_name)                      \
  class ModelFactory_##class_name : public ModelFactory {           \
   public:                                                          \
    RetrievalModel *NewModel() { return new class_name(); }         \
  };                                                                \
  class Register_##class_name {                                     \
   public:                                                          \
    Register_##class_name() {                                       \
      reflector().RegisterFactory(#model_name,                      \
                                  new ModelFactory_##class_name()); \
    }                                                               \
  };                                                                \
  Register_##class_name register_##class_name;
