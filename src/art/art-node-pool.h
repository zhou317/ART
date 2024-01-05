#pragma once

#include "art/art-node.h"
#include "common/object-pool.h"

namespace art {
namespace detail {

template <class T, bool all_new = false>
static T *get_new_art_node() {
  T *p = nullptr;
  if constexpr (!all_new) {
    p = get_object<T>();
  }

  if (p) {
    memset(p, 0, sizeof(T));
    p->type = ArtNodeTrait<T>::NODE_TYPE;
  } else {
    p = new T;
    p->set_from_new();
  }

  return p;
}

template <class T, bool all_new = false>
static ArtLeaf<T> *get_new_leaf_node(const char *k, uint32_t l, T v) {
  ArtLeaf<T> *p = nullptr;
  if constexpr (!all_new) {
    p = get_object<ArtLeaf<T>>();
  }

  if (p) {
    p->type = ArtNodeTrait<ArtLeaf<T>>::NODE_TYPE;
    p->set_leaf_key_val(k, l, v);
  } else {
    p = new ArtLeaf<T>;
    p->set_from_new();
    p->set_leaf_key_val(k, l, v);
  }

  return p;
}

template <class T>
static void return_art_node(T *ptr) {
  assert(ptr);
  if (ptr->is_from_new()) {
    delete ptr;
  } else {
    ptr->reset();
    return_object<T>(ptr);
  }
}

}  // namespace detail
}  // namespace art
