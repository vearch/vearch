/**
 * Copyright 2019 The Gamma Authors.
 *
 * This source code is licensed under the Apache License, Version 2.0 license
 * found in the LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <pthread.h>

class ThreadLock {
 private:
  pthread_mutex_t *m_pMutex;
 public:
  ThreadLock(pthread_mutex_t &mutex) : m_pMutex(&mutex) {
    pthread_mutex_lock(m_pMutex);// lock seldom return error. can upto 20000.
  }
  virtual ~ThreadLock() {
    pthread_mutex_unlock(m_pMutex);
  }
};

class ReadThreadLock {
 private:
  pthread_rwlock_t *m_pLock;
 public:
  ReadThreadLock(pthread_rwlock_t &lock) : m_pLock(&lock) {
    pthread_rwlock_rdlock(m_pLock);
  }
  virtual ~ReadThreadLock() {
    pthread_rwlock_unlock(m_pLock);
  }
};

class WriteThreadLock {
 private:
  pthread_rwlock_t *m_pLock;
 public:
  WriteThreadLock(pthread_rwlock_t &lock) : m_pLock(&lock) {
    pthread_rwlock_wrlock(m_pLock);
  }
  virtual ~WriteThreadLock() {
    pthread_rwlock_unlock(m_pLock);
  }
};
