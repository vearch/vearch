#ifndef THREAD_UTIL_H_
#define THREAD_UTIL_H_

#include <pthread.h>
#include <map>

/// 线程锁的定义
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

/// 读写锁的读锁的定义
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

/// 读写锁的写锁的定义
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

#endif
