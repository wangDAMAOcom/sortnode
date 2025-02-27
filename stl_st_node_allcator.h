#ifndef DBSCALE_STL_ST_NODE_ALLCATOR_H
#define DBSCALE_STL_ST_NODE_ALLCATOR_H

/*
 * Copyright (C) 2018 Great OpenSource Inc. All Rights Reserved.
 */

#include <log.h>
#include <option.h>
#include <sql_parser.h>
#include <string.h>

#include <memory>
#include <mutex>

namespace dbscale {
namespace option {}
}  // namespace dbscale

using namespace dbscale;
using namespace dbscale::option;
using namespace std;

template <typename _Tp, typename _Session,
          typename _Alloc = std::allocator<_Tp> >
class AllocList : public list<_Tp, _Alloc> {
  typedef list<_Tp, _Alloc> STLlist;

 public:
  AllocList() {
    can_use_alloc = 0;
    used_alloc = 0;
    session = NULL;
    need_calculate = false;
  }
  ~AllocList() {
    if (!session->get_has_fetch_node()) {
      session->giveback_left_alloc(used_alloc);
    }
  }
  void push_back(_Tp const& tp) {
    if (!session->get_has_fetch_node()) {
      STLlist::push_back(tp);
      return;
    }
    unsigned long size = sizeof(tp);
    if (session->get_alloc(size)) {
      used_alloc += size;
      STLlist::push_back(tp);
    }
  }
  void pop_back() {
    if (!session->get_has_fetch_node()) {
      STLlist::pop_back();
      return;
    }
    _Tp tp = STLlist::back();
    unsigned long size = sizeof(tp);
    session->giveback_left_alloc(size);
    used_alloc -= size;
    STLlist::pop_back();
  }
  void push_front(_Tp const& tp) {
    if (!session->get_has_fetch_node()) {
      STLlist::push_front(tp);
      return;
    }
    unsigned long size = sizeof(tp);
    if (session->get_alloc(size)) {
      used_alloc += size;
      STLlist::push_front(tp);
    }
  }
  void pop_front() {
    if (!session->get_has_fetch_node()) {
      STLlist::pop_front();
      return;
    }
    _Tp tp = STLlist::front();
    unsigned long size = sizeof(tp);
    session->giveback_left_alloc(size);
    used_alloc -= size;
    STLlist::pop_front();
  }
  void clear() {
    if (!session->get_has_fetch_node()) {
      STLlist::clear();
      return;
    }
    session->giveback_left_alloc(used_alloc);
    STLlist::clear();
  }
  void set_session(_Session s) {
    session = s;
    if (session->get_alloc(select_node_once_alloc_size)) {
      can_use_alloc += (select_node_once_alloc_size);
    }
  }

 private:
  unsigned long long can_use_alloc;
  unsigned long long used_alloc;
  _Session session;
  bool need_calculate;
};

template <typename _Session, typename _Alloc>
class AllocList<Packet*, _Session, _Alloc> : public list<Packet*, _Alloc> {
  typedef list<Packet*, _Alloc> STLlist;

 public:
  AllocList() {
    can_use_alloc = 0;
    used_alloc = 0;
    session = NULL;
    need_calculate = false;
  }

  ~AllocList() {
    if (need_calculate) {
      unsigned long long tmp_alloc = used_alloc + can_use_alloc;
      if (tmp_alloc) {
        session->giveback_left_alloc(used_alloc + can_use_alloc);
      }
    }
  }

  void calculate_alloc(Packet* const& tp) {
    if (need_calculate) {
      // pointer item contains 8 bites.
      unsigned long long size = tp->total_capacity() + sizeof(Packet*);
      if (can_use_alloc < size) {
        if (session->get_alloc(select_node_once_alloc_size)) {
          can_use_alloc += (select_node_once_alloc_size);
        } else {
          LOG_INFO("list throw expection : can not get enough memory\n");
          throw ListOutOfMemError("There are no enough memory for the sql.");
        }
      }
      can_use_alloc -= size;
      used_alloc += size;
    }
  }

  void push_back(Packet* const& tp) {
    calculate_alloc(tp);
    STLlist::push_back(tp);
  }

  void pop_back() {
    if (need_calculate) {
      Packet* tp = STLlist::back();
      unsigned long long size = tp->total_capacity() + sizeof(Packet*);

      used_alloc -= size;
      can_use_alloc += size;

      if (can_use_alloc >= select_node_once_alloc_size) {
        session->giveback_left_alloc(can_use_alloc -
                                     select_node_once_alloc_size);
        can_use_alloc = select_node_once_alloc_size;
      }
    }
    STLlist::pop_back();
  }

  void push_front(Packet* const& tp) {
    calculate_alloc(tp);
    STLlist::push_front(tp);
  }

  void pop_front() {
    if (need_calculate) {
      Packet* tp = STLlist::front();
      unsigned long long size = tp->total_capacity() + sizeof(Packet*);

      used_alloc -= size;
      can_use_alloc += size;

      if (can_use_alloc >= select_node_once_alloc_size) {
        session->giveback_left_alloc(can_use_alloc -
                                     select_node_once_alloc_size);
        can_use_alloc = select_node_once_alloc_size;
      }
    }
    STLlist::pop_front();
  }

  void clear() {
    if (need_calculate) {
      unsigned long long tmp_alloc = used_alloc + can_use_alloc;
      if (tmp_alloc) {
        session->giveback_left_alloc(tmp_alloc);
        used_alloc = 0;
        can_use_alloc = 0;
      }
    }
    STLlist::clear();
  }

  void set_session(_Session s) {
    session = s;
    need_calculate =
        enable_calculate_select_total_memory && session->get_has_fetch_node();
  }

 private:
  unsigned long long can_use_alloc;
  unsigned long long used_alloc;
  _Session session;
  bool need_calculate;
};

template <class T>
class StaticAllocator : public std::allocator<T> {};

template <class T>
class Stalloc {
 public:
  typedef T value_type;
  typedef value_type* pointer;
  typedef const value_type* const_pointer;
  typedef value_type& reference;
  typedef const value_type& const_reference;
  typedef std::size_t size_type;
  typedef std::ptrdiff_t difference_type;

  Stalloc() {
    st = new stmt_node();
    init_stmt_node_for_memory(st, pre_memory_pool_size);
    new_stmt = true;
    malloc_count = 0;
  }

  Stalloc(const Stalloc& alloc) throw() {
    st = alloc.st;
    new_stmt = false;
    malloc_count = 0;
  }

  template <class U>
  Stalloc(const Stalloc<U>& alloc) throw() {
    st = alloc.st;
    new_stmt = false;
    malloc_count = 0;
  }
  ~Stalloc() {
    if (new_stmt) {
      dbscale::free_stmt_node_mem(st);
      delete st;
    }
    malloc_count = 0;
  }

  template <class U>
  struct rebind {
    typedef Stalloc<U> other;
  };

  pointer address(reference x) const { return &x; }
  const_pointer address(const_reference x) const { return &x; }

  pointer allocate(size_type n, const_pointer hint = 0) {
    ACE_UNUSED_ARG(hint);
    void* p = NULL;
    p = assign_mem_from_mem_node(&(st->cur_mem), n * sizeof(T));
    if (!p) throw std::bad_alloc();
    ++malloc_count;
    return static_cast<pointer>(p);
  }

  void deallocate(pointer p, size_type n) {
    ACE_UNUSED_ARG(p);
    ACE_UNUSED_ARG(n);
    if (malloc_count > 0) malloc_count--;
    if (malloc_count == 0) {
      memory_node* mem = st->mem_head;
      int i = 0;
      if (!mem->next)
        reset_stmt_node_mem(st);
      else {
        while (mem) {
          ++i;
          if (i == 4) {
            reset_stmt_node_mem(st);
            break;
          }
          mem = mem->next;
        }
      }
    }
  }

  size_type max_size() const throw() {
    return static_cast<size_type>(-1) / sizeof(value_type);
  }

  void construct(pointer p, const_reference val) { new (p) value_type(val); }

  void destroy(pointer p) { p->~value_type(); }

 public:
  uint64_t malloc_count;
  stmt_node* st;
  bool new_stmt;
};

template <class T>
inline bool operator==(const Stalloc<T>& left, const Stalloc<T>& right) {
  if (left.st == right.st) return true;
  return false;
}

template <class T>
inline bool operator!=(const Stalloc<T>& left, const Stalloc<T>& right) {
  if (left.st != right.st) return true;
  return false;
}

template <typename T>
class ConcurrentSet {
 public:
  ConcurrentSet() {}
  ~ConcurrentSet() {}

  void insert(T& value) {
    std::lock_guard<std::mutex> lock(mutex_);
    set_.insert(value);
  }

  bool contains(const T& value) {
    std::lock_guard<std::mutex> lock(mutex_);
    return set_.count(value) > 0;
  }
  void clear() {
    std::lock_guard<std::mutex> lock(mutex_);
    set_.clear();
  }

 private:
  std::set<T> set_;
  mutable std::mutex mutex_;
};

template <typename K, typename V>
class ConcurrentMap {
 public:
  ConcurrentMap() {}
  ~ConcurrentMap() {}
  V& operator[](const K& key) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto iter = map_.find(key);
    if (iter == map_.end()) {
      iter = map_.insert(std::make_pair(key, V())).first;
    }
    return iter->second;
  }

  void AddKeyValue(const K& key, const V& value) {
    std::lock_guard<std::mutex> lock(mutex_);
    map_[key] += value;
  }

  bool CheckAndAppend(const K& key, const V& value) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!map_.count(key)) {
      map_[key] = value;
      return true;
    }
    return false;
  }

  void CheckAndAppendMore(const K& key, const V& value2) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (map_.count(key)) {
      string s = ":";
      s += value2;
      map_[key] += s;
    } else {
      map_[key] += value2;
    }
  }

  V GetValue(const K& key) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = map_.find(key);
    if (it != map_.end()) {
      return it->second;
    } else {
      return V();
    }
  }

  bool get(const K& key, V& value) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = map_.find(key);
    if (it != map_.end()) {
      value = it->second;
      return true;
    }
    return false;
  }

  void clear() {
    std::lock_guard<std::mutex> lock(mutex_);
    map_.clear();
  }

  bool count(const K& key) const {
    std::lock_guard<std::mutex> lock(mutex_);
    return map_.count(key);
  }

  typename std::map<K, V>::iterator begin() {
    std::lock_guard<std::mutex> lock(mutex_);
    return map_.begin();
  }

  typename std::map<K, V>::iterator end() {
    std::lock_guard<std::mutex> lock(mutex_);
    return map_.end();
  }

 private:
  std::map<K, V> map_;
  mutable std::mutex mutex_;
};
#endif  // DBSCALE_STL_ST_NODE_ALLCATOR_H
