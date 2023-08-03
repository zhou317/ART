#pragma once

#include <atomic>
#include <iostream>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "macros.h"

struct ObjectPoolInfo {
  size_t local_pool_num;
  size_t block_group_num;
  size_t block_num;
  size_t item_num;
  size_t block_item_num;
  size_t free_chunk_item_num;
  size_t total_size;
};

namespace detail {

template <typename T, size_t NITEM>
struct ObjectPoolFreeChunk {
  size_t nfree;
  T *ptrs[NITEM];
};

template <typename T>
struct ObjectPoolFreeChunk<T, 0> {
  size_t nfree;
  T *ptrs[0];
};

static const size_t OP_MAX_BLOCK_NGROUP = 65536;
static const size_t OP_GROUP_NBLOCK_NBIT = 16;
static const size_t OP_GROUP_NBLOCK = (1UL << OP_GROUP_NBLOCK_NBIT);
static const size_t OP_INITIAL_FREE_LIST_SIZE = 1024;

// Memory is allocated in blocks, memory size of a block will not exceed:
//   min(ObjectPoolBlockMaxSize<T>::value,
//       ObjectPoolBlockMaxItem<T>::value * sizeof(T))
template <typename T>
struct ObjectPoolBlockMaxSize {
  static const size_t value = 64 * 1024;  // bytes
};
template <typename T>
struct ObjectPoolBlockMaxItem {
  static const size_t value = 256;
};

template <typename T>
class ObjectPoolBlockItemNum {
  static const size_t N1 = ObjectPoolBlockMaxSize<T>::value / sizeof(T);
  static const size_t N2 = (N1 < 1 ? 1 : N1);

 public:
  static const size_t value =
      (N2 > ObjectPoolBlockMaxItem<T>::value ? ObjectPoolBlockMaxItem<T>::value
                                             : N2);
};

// Free objects of each thread are grouped into a chunk before they are merged
// to the global list. Memory size of objects in one free chunk will not exceed:
//   min(ObjectPoolFreeChunkMaxItem<T>::value() * sizeof(T),
//       ObjectPoolBlockMaxSize<T>::value,
//       ObjectPoolBlockMaxItem<T>::value * sizeof(T))
template <typename T>
struct ObjectPoolFreeChunkMaxItem {
  static size_t value() { return 256; }
};

// ObjectPool calls this function on newly constructed objects. If this
// function returns false, the object is destructed immediately and
// get_object() shall return NULL. This is useful when the constructor
// failed internally(namely ENOMEM).
template <typename T>
struct ObjectPoolValidator {
  static bool validate(const T *) { return true; }
};

template <typename T>
class alignas(64) ObjectPool {
 public:
  static const size_t BLOCK_NITEM = ObjectPoolBlockItemNum<T>::value;
  static const size_t FREE_CHUNK_NITEM = BLOCK_NITEM;

  // Free objects are batched in a FreeChunk before they're added to
  // global list(_free_chunks).
  typedef ObjectPoolFreeChunk<T, FREE_CHUNK_NITEM> FreeChunk;
  typedef ObjectPoolFreeChunk<T, 0> DynamicFreeChunk;

  // When a thread needs memory, it allocates a Block. To improve locality,
  // items in the Block are only used by the thread.
  // To support cache-aligned objects, align Block.items by cacheline.
  struct alignas(64) Block {
    char items[sizeof(T) * BLOCK_NITEM];
    size_t nitem;

    Block() : nitem(0) {}
  };

  // An Object addresses at most OP_MAX_BLOCK_NGROUP BlockGroups,
  // each BlockGroup addresses at most OP_GROUP_NBLOCK blocks. So an
  // object addresses at most OP_MAX_BLOCK_NGROUP * OP_GROUP_NBLOCK Blocks.
  struct BlockGroup {
    std::atomic<size_t> nblock = {0};
    std::atomic<Block *> blocks[OP_GROUP_NBLOCK];
  };

  // Each thread has an instance of this class.
  class alignas(64) LocalPool {
   public:
    explicit LocalPool(ObjectPool *pool)
        : _pool(pool), _cur_block(NULL), _cur_block_index(0) {
      _cur_free.nfree = 0;
    }

    ~LocalPool() {
      // Add to global _free if there're some free objects
      if (_cur_free.nfree) {
        _pool->push_free_chunk(_cur_free);
      }

      _pool->clear_from_destructor_of_local_pool();
    }

    static void delete_local_pool(void *arg) { delete (LocalPool *)arg; }

    inline T *get() {
      if (_cur_free.nfree) {
        return _cur_free.ptrs[--_cur_free.nfree];
      }
      if (_pool->pop_free_chunk(_cur_free)) {
        return _cur_free.ptrs[--_cur_free.nfree];
      }
      if (_cur_block && _cur_block->nitem < BLOCK_NITEM) {
        T *obj = new ((T *)_cur_block->items + _cur_block->nitem) T;
        if (!ObjectPoolValidator<T>::validate(obj)) {
          obj->~T();
          return nullptr;
        }
        ++_cur_block->nitem;
        return obj;
      }
      _cur_block = _pool->add_block(&_cur_block_index);
      if (_cur_block != __null) {
        T *obj = new ((T *)_cur_block->items + _cur_block->nitem) T;
        if (!ObjectPoolValidator<T>::validate(obj)) {
          obj->~T();
          return nullptr;
        }
        ++_cur_block->nitem;
        return obj;
      }
      return nullptr;
    }

    inline int return_object(T *ptr) {
      // Return to local free list
      if (_cur_free.nfree < ObjectPool::free_chunk_nitem()) {
        _cur_free.ptrs[_cur_free.nfree++] = ptr;
        return 0;
      }
      // Local free list is full, return it to global.
      // For copying issue, check comment in upper get()
      if (_pool->push_free_chunk(_cur_free)) {
        _cur_free.nfree = 1;
        _cur_free.ptrs[0] = ptr;
        return 0;
      }
      return -1;
    }

   private:
    ObjectPool *_pool;
    Block *_cur_block;
    size_t _cur_block_index;
    FreeChunk _cur_free;
  };

  struct ThreadExiter {
    void (*callback)(void *arg) = {nullptr};
    void *arg = nullptr;

    ThreadExiter() = default;

    ~ThreadExiter() {
      if (callback) callback(arg);
    }
  };

  inline T *get_object() {
    LocalPool *lp = get_or_new_local_pool();
    if (likely(lp != NULL)) {
      return lp->get();
    }
    return NULL;
  }

  inline int return_object(T *ptr) {
    LocalPool *lp = get_or_new_local_pool();
    if (likely(lp != NULL)) {
      return lp->return_object(ptr);
    }
    return -1;
  }

  void clear_objects() {
    LocalPool *lp = _local_pool;
    if (lp) {
      _local_pool = NULL;
      thread_atexit_cancel();
      delete lp;
    }
  }

  inline static size_t free_chunk_nitem() {
    const size_t n = ObjectPoolFreeChunkMaxItem<T>::value();
    return (n < FREE_CHUNK_NITEM ? n : FREE_CHUNK_NITEM);
  }

  // Number of all allocated objects, including being used and free.
  ObjectPoolInfo describe_objects() const {
    ObjectPoolInfo info;
    info.local_pool_num = _nlocal.load(std::memory_order_relaxed);
    info.block_group_num = _ngroup.load(std::memory_order_acquire);
    info.block_num = 0;
    info.item_num = 0;
    info.free_chunk_item_num = free_chunk_nitem();
    info.block_item_num = BLOCK_NITEM;

    for (size_t i = 0; i < info.block_group_num; ++i) {
      BlockGroup *bg = _block_groups[i].load(std::memory_order_consume);
      if (NULL == bg) {
        break;
      }
      size_t nblock =
          std::min(bg->nblock.load(std::memory_order_relaxed), OP_GROUP_NBLOCK);
      info.block_num += nblock;
      for (size_t j = 0; j < nblock; ++j) {
        Block *b = bg->blocks[j].load(std::memory_order_consume);
        if (NULL != b) {
          info.item_num += b->nitem;
        }
      }
    }
    info.total_size = info.block_num * info.block_item_num * sizeof(T);
    return info;
  }

  static inline ObjectPool *singleton() {
    static ObjectPool _p;
    return &_p;
  }

 private:
  ObjectPool() { _free_chunks.reserve(OP_INITIAL_FREE_LIST_SIZE); }

  // Create a Block and append it to right-most BlockGroup.
  Block *add_block(size_t *index) {
    Block *const new_block = new (std::nothrow) Block;
    if (NULL == new_block) {
      return NULL;
    }
    size_t ngroup;
    do {
      ngroup = _ngroup.load(std::memory_order_acquire);
      if (ngroup >= 1) {
        BlockGroup *g =
            _block_groups[ngroup - 1].load(std::memory_order_consume);
        const size_t block_index =
            g->nblock.fetch_add(1, std::memory_order_relaxed);
        if (block_index < OP_GROUP_NBLOCK) {
          g->blocks[block_index].store(new_block, std::memory_order_release);
          *index = (ngroup - 1) * OP_GROUP_NBLOCK + block_index;
          return new_block;
        }
        g->nblock.fetch_sub(1, std::memory_order_relaxed);
      }
    } while (add_block_group(ngroup));

    // Fail to add_block_group.
    delete new_block;
    return NULL;
  }

  // Create a BlockGroup and append it to _block_groups.
  // Shall be called infrequently because a BlockGroup is pretty big.
  bool add_block_group(size_t old_ngroup) {
    BlockGroup *bg = NULL;
    std::lock_guard<std::mutex> guard(_block_group_mutex);

    const size_t ngroup = _ngroup.load(std::memory_order_acquire);
    if (ngroup != old_ngroup) {
      // Other thread got lock and added group before this thread.
      return true;
    }
    if (ngroup < OP_MAX_BLOCK_NGROUP) {
      bg = new (std::nothrow) BlockGroup;
      if (NULL != bg) {
        // Release fence is paired with consume fence in add_block()
        // to avoid un-constructed bg to be seen by other threads.
        _block_groups[ngroup].store(bg, std::memory_order_release);
        _ngroup.store(ngroup + 1, std::memory_order_release);
      }
    }
    return bg != NULL;
  }

  inline LocalPool *get_or_new_local_pool() {
    LocalPool *lp = _local_pool;
    if (likely(lp != NULL)) {
      return lp;
    }
    lp = new (std::nothrow) LocalPool(this);
    if (NULL == lp) {
      return NULL;
    }

    // avoid race with clear()
    std::lock_guard<std::mutex> guard(_change_thread_mutex);
    _local_pool = lp;

    on_thread_exit(LocalPool::delete_local_pool, lp);

    _local_pool = lp;
    _nlocal.fetch_add(1, std::memory_order_relaxed);
    return lp;
  }

  void clear_from_destructor_of_local_pool() {
    // Remove tls
    _local_pool = NULL;

    // Do nothing if there're active threads.
    if (_nlocal.fetch_sub(1, std::memory_order_relaxed) != 1) {
      return;
    }
    // Todo(xxx). Free global?
  }

 private:
  void on_thread_exit(void (*callback)(void *arg), void *arg) {
    _exiter.callback = callback;
    _exiter.arg = arg;
  }

  void thread_atexit_cancel() {
    _exiter.callback = nullptr;
    _exiter.arg = nullptr;
  }

  bool pop_free_chunk(FreeChunk &c) {
    // Critical for the case that most return_object are called in
    // different threads of get_object.
    if (_free_chunks.empty()) {
      return false;
    }
    std::unique_lock<std::mutex> uniqueLock(_free_chunks_mutex);
    if (_free_chunks.empty()) {
      return false;
    }
    DynamicFreeChunk *p = _free_chunks.back();
    _free_chunks.pop_back();
    uniqueLock.unlock();

    c.nfree = p->nfree;
    memcpy(c.ptrs, p->ptrs, sizeof(*p->ptrs) * p->nfree);
    free(p);
    return true;
  }

  bool push_free_chunk(const FreeChunk &c) {
    DynamicFreeChunk *p = (DynamicFreeChunk *)malloc(
        offsetof(DynamicFreeChunk, ptrs) + sizeof(*c.ptrs) * c.nfree);
    if (!p) {
      return false;
    }
    p->nfree = c.nfree;
    memcpy(p->ptrs, c.ptrs, sizeof(*c.ptrs) * c.nfree);
    std::unique_lock<std::mutex> uniqueLock(_free_chunks_mutex);
    _free_chunks.push_back(p);
    return true;
  }

  static thread_local LocalPool *_local_pool;

  static thread_local ThreadExiter _exiter;

  std::atomic<long> _nlocal{0};
  std::atomic<size_t> _ngroup{0};

  std::mutex _block_group_mutex;
  std::mutex _change_thread_mutex;
  std::atomic<BlockGroup *> _block_groups[OP_MAX_BLOCK_NGROUP];

  std::vector<DynamicFreeChunk *> _free_chunks;
  std::mutex _free_chunks_mutex;
};

template <typename T>
thread_local typename ObjectPool<T>::ThreadExiter ObjectPool<T>::_exiter{};

template <typename T>
thread_local typename ObjectPool<T>::LocalPool *ObjectPool<T>::_local_pool =
    NULL;

}  // namespace detail

inline std::ostream &operator<<(std::ostream &os, ObjectPoolInfo const &info) {
  return os << "local_pool_num: " << info.local_pool_num
            << "\nblock_group_num: " << info.block_group_num
            << "\nblock_num: " << info.block_num
            << "\nitem_num: " << info.item_num
            << "\nblock_item_num: " << info.block_item_num
            << "\nfree_chunk_item_num: " << info.free_chunk_item_num
            << "\ntotal_size: " << info.total_size;
}

// Get an object typed |T|. The object should be cleared before usage.
// NOTE: T must be default-constructible.
template <typename T>
inline T *get_object() {
  return detail::ObjectPool<T>::singleton()->get_object();
}

// Return the object |ptr| back. The object is NOT destructed and will be
// returned by later get_object<T>. Similar with free/delete, validity of
// the object is not checked, user shall not return a not-yet-allocated or
// already-returned object otherwise behavior is undefined.
// Returns 0 when successful, -1 otherwise.
template <typename T>
inline int return_object(T *ptr) {
  return detail::ObjectPool<T>::singleton()->return_object(ptr);
}

// Reclaim all allocated objects typed T if caller is the last thread called
// this function, otherwise do nothing. You rarely need to call this function
// manually because it's called automatically when each thread quits.
template <typename T>
inline void clear_objects() {
  detail::ObjectPool<T>::singleton()->clear_objects();
}

// Get description of objects typed T.
// This function is possibly slow because it iterates internal structures.
// Don't use it frequently like a "getter" function.
template <typename T>
ObjectPoolInfo describe_objects() {
  return detail::ObjectPool<T>::singleton()->describe_objects();
}
