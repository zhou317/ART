#pragma once

#include "art/art-node.h"
#include "common/object-pool.h"

namespace art {
namespace detail {

template <class T>
struct ArtNodeTypeTrait {
  static ArtNodeType get_node_type();
};

template <>
struct ArtNodeTypeTrait<ArtNode4> {
  static ArtNodeType get_node_type() { return ArtNodeType::ART_NODE_4; }
};

template <>
struct ArtNodeTypeTrait<ArtNode16> {
  static ArtNodeType get_node_type() { return ArtNodeType::ART_NODE_16; }
};

template <>
struct ArtNodeTypeTrait<ArtNode48> {
  static ArtNodeType get_node_type() { return ArtNodeType::ART_NODE_48; }
};

template <>
struct ArtNodeTypeTrait<ArtNode256> {
  static ArtNodeType get_node_type() { return ArtNodeType::ART_NODE_256; }
};

template <class T>
struct ArtNodeTypeTrait<ArtLeaf<T>> {
  static ArtNodeType get_node_type() { return ArtNodeType::ART_NODE_LEAF; }
};

template <class T>
static T *get_new_node() {
  auto p = get_object<T>();
  if (p) {
    p->type = ArtNodeTypeTrait<T>::get_node_type();
  } else {
    p = new T;
    p->set_from_new();
  }

  return p;
}

template <class T>
static ArtLeaf<T> *get_new_leaf_node(const char *k, uint32_t l, T v) {
  auto p = get_object<ArtLeaf<T>>();
  if (p) {
    p->set_leaf_key_val(k, l, v);
    p->type = ArtNodeTypeTrait<T>::get_node_type();
  } else {
    p = new ArtLeaf<T>;
    p->set_from_new();
    p->set_leaf_key_val(k, l, v);
  }

  return p;
}

template <class T>
static void return_new_node(T *ptr) {
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
