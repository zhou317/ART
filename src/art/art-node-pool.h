#pragma once

#include "art/art-node.h"
#include "common/object-pool.h"

namespace art {
namespace detail {

template <class T>
struct ArtNodeTrait {
  static constexpr ArtNodeType NODE_TYPE = ArtNodeType::ART_NODE_INVALID;
  static constexpr uint32_t NODE_CAPASITY = 0;
};

template <>
struct ArtNodeTrait<ArtNode4> {
  static constexpr ArtNodeType NODE_TYPE = ArtNodeType::ART_NODE_4;
  static constexpr uint32_t NODE_CAPASITY = 4;
};

template <>
struct ArtNodeTrait<ArtNode16> {
  static constexpr ArtNodeType NODE_TYPE = ArtNodeType::ART_NODE_16;
  static constexpr uint32_t NODE_CAPASITY = 16;
};

template <>
struct ArtNodeTrait<ArtNode48> {
  static constexpr ArtNodeType NODE_TYPE = ArtNodeType::ART_NODE_48;
  static constexpr uint32_t NODE_CAPASITY = 48;
};

template <>
struct ArtNodeTrait<ArtNode256> {
  static constexpr ArtNodeType NODE_TYPE = ArtNodeType::ART_NODE_256;
  static constexpr uint32_t NODE_CAPASITY = 256;
};

template <class T>
struct ArtNodeTrait<ArtLeaf<T>> {
  static constexpr ArtNodeType NODE_TYPE = ArtNodeType::ART_NODE_LEAF;
  static constexpr uint32_t NODE_CAPASITY = 0;
};

static_assert(ArtNodeTrait<ArtNode4>::NODE_TYPE == ArtNodeType::ART_NODE_4,
              "should be equal");
static_assert(ArtNodeTrait<ArtNode16>::NODE_TYPE == ArtNodeType::ART_NODE_16,
              "should be equal");
static_assert(ArtNodeTrait<ArtNode48>::NODE_TYPE == ArtNodeType::ART_NODE_48,
              "should be equal");
static_assert(ArtNodeTrait<ArtNode256>::NODE_TYPE == ArtNodeType::ART_NODE_256,
              "should be equal");
static_assert(ArtNodeTrait<ArtLeaf<int>>::NODE_TYPE ==
                  ArtNodeType::ART_NODE_LEAF,
              "should be equal");

static_assert(ArtNodeTrait<ArtNode4>::NODE_CAPASITY == 4, "should be equal");
static_assert(ArtNodeTrait<ArtNode16>::NODE_CAPASITY == 16, "should be equal");
static_assert(ArtNodeTrait<ArtNode48>::NODE_CAPASITY == 48, "should be equal");
static_assert(ArtNodeTrait<ArtNode256>::NODE_CAPASITY == 256,
              "should be equal");
static_assert(ArtNodeTrait<ArtLeaf<int>>::NODE_CAPASITY == 0,
              "should be equal");

template <class T, bool all_new = true>
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
    memset(p, 0, sizeof(ArtLeaf<T>));
    p->set_leaf_key_val(k, l, v);
    p->type = ArtNodeTrait<ArtLeaf<T>>::NODE_TYPE;
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
