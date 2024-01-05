#pragma once

#include <algorithm>
#include <cassert>
#include <cstdint>
#include <cstring>
#include <string>

#include "common/logger.h"
#include "common/macros.h"

#if defined(__i386__) || defined(__amd64__)
#include <emmintrin.h>
#endif

namespace art {

enum ArtNodeType : uint8_t {
  ART_NODE_INVALID,
  ART_NODE_4,
  ART_NODE_16,
  ART_NODE_48,
  ART_NODE_256,
  ART_NODE_LEAF
};

union ArtNodeKey {
  char shortKey[ART_MAX_PREFIX_LEN];
  char *keyPtr;
};

template <class T>
struct ArtNodeTrait {
  static constexpr ArtNodeType NODE_TYPE = ArtNodeType::ART_NODE_INVALID;
  static constexpr uint32_t NODE_CAPASITY = 0;
};

struct alignas(void *) ArtNodeCommon {
  ArtNodeKey key = {.keyPtr = nullptr};
  uint32_t keyLen = 0;
  uint8_t flag = 0;
  ArtNodeType type = ART_NODE_INVALID;
  uint16_t childNum = 0;

  void set_from_new() { flag = 1; }

  bool is_from_new() const { return flag & 1; }

  void reset() {
    reset_key();
    flag = 0;
    type = ART_NODE_INVALID;
    childNum = 0;
  }

  void get_key_from_another(ArtNodeCommon *node) {
    key.keyPtr = node->key.keyPtr;
    keyLen = node->keyLen;
    node->key.keyPtr = nullptr;
    node->keyLen = 0;
  }

  void copy_key_to(char *buf) {
    const char *ptr = get_key();
    std::memcpy(buf, ptr, keyLen);
  }

  void set_key(const char *k, uint32_t l) {
    reset_key();
    if (type == ArtNodeType::ART_NODE_LEAF) {
      if (l <= ART_MAX_PREFIX_LEN) {
        std::memcpy(key.shortKey, k, std::min((uint32_t)ART_MAX_PREFIX_LEN, l));
      } else {
        char *tmp = (char *)malloc(l);
        std::memcpy(tmp, k, l);
        key.keyPtr = tmp;
      }
    } else {
      std::memcpy(key.shortKey, k, std::min((uint32_t)ART_MAX_PREFIX_LEN, l));
    }
    keyLen = l;
  }

  void reset_key() {
    if (type == ArtNodeType::ART_NODE_LEAF) {
      if (keyLen > ART_MAX_PREFIX_LEN) {
        free(key.keyPtr);
      }
    }

    key.keyPtr = nullptr;
    keyLen = 0;
  }

  void reset_prefix(ArtNodeKey new_prefix, int32_t remove_len) {
    assert(remove_len <= keyLen);
    keyLen -= remove_len;
    key = new_prefix;
  }

  std::string to_string() const {
    const char *ptr = get_key();
    uint32_t len = 0;
    if (type == ArtNodeType::ART_NODE_LEAF) {
      len = keyLen;
    } else {
      len = std::min(keyLen, (uint32_t)ART_MAX_PREFIX_LEN);
    }

    return {ptr, len};
  }

  inline const char *get_key() const {
    const char *ptr = nullptr;
    if (type == ArtNodeType::ART_NODE_LEAF && keyLen > ART_MAX_PREFIX_LEN) {
      ptr = key.keyPtr;
    } else {
      ptr = key.shortKey;
    }

    return ptr;
  }

  void set_prefix_key(const char *k, uint32_t l) {
    keyLen = l;
    if (l)
      std::memcpy(key.shortKey, k, std::min(l, (uint32_t)ART_MAX_PREFIX_LEN));
  }
};

struct ArtNode4 : public ArtNodeCommon {
  uint8_t keys[4] = {0};
  uint32_t alignNoUse = 0;
  ArtNodeCommon *children[4] = {nullptr};

  ArtNode4() { type = ArtNodeType::ART_NODE_4; }

  ArtNodeCommon **find_child(uint8_t keyByte) {
    ArtNodeCommon **ret = nullptr;
    for (int32_t i = 0; i < childNum; i++) {
      if (keys[i] == keyByte) {
        ret = &children[i];
        break;
      }
    }
    return ret;
  }

  void init_with_leaf(uint8_t c1, ArtNodeCommon *l1, uint8_t c2,
                      ArtNodeCommon *l2) {
    assert(c1 != c2);
    childNum = 2;
    if (c1 > c2) {
      std::swap(c1, c2);
      std::swap(l1, l2);
    }

    keys[0] = c1;
    keys[1] = c2;
    children[0] = l1;
    children[1] = l2;
  }
};

struct ArtNode16 : public ArtNodeCommon {
  uint8_t keys[16] = {0};
  ArtNodeCommon *children[16] = {nullptr};

  ArtNode16() { type = ArtNodeType::ART_NODE_16; }

  ArtNodeCommon **find_child(uint8_t keyByte) {
#if defined(__i386__) || defined(__amd64__)
    int bitfield =
        _mm_movemask_epi8(_mm_cmpeq_epi8(_mm_set1_epi8(keyByte),
                                         _mm_loadu_si128((__m128i *)keys))) &
        ((1 << childNum) - 1);
    return (bool)bitfield ? &children[__builtin_ctz(bitfield)] : nullptr;
#else
    int lo, mid, hi;
    lo = 0;
    hi = childNum;
    while (lo < hi) {
      mid = (lo + hi) / 2;
      if (keyByte < keys[mid]) {
        hi = mid;
      } else if (keyByte > keys[mid]) {
        lo = mid + 1;
      } else {
        return &children[mid];
      }
    }
    return nullptr;
#endif
  }
};

struct ArtNode48 : public ArtNodeCommon {
  uint8_t index[256] = {0};
  ArtNodeCommon *children[48] = {nullptr};

  ArtNode48() { type = ArtNodeType::ART_NODE_48; }

  ArtNodeCommon **find_child(uint8_t keyByte) {
    ArtNodeCommon **ret = nullptr;
    if (index[keyByte]) {
      ret = &children[index[keyByte] - 1];
    }
    return ret;
  }
};

struct ArtNode256 : public ArtNodeCommon {
  ArtNodeCommon *children[256] = {nullptr};

  ArtNode256() { type = ArtNodeType::ART_NODE_256; }

  ArtNodeCommon **find_child(uint8_t keyByte) {
    return children[keyByte] != nullptr ? &children[keyByte] : nullptr;
  }
};

template <class T>
struct ArtLeaf : public ArtNodeCommon {
  ArtLeaf() { type = ArtNodeType::ART_NODE_LEAF; }

  void set_leaf_key_val(const char *k, uint32_t l, T v) {
    set_key(k, l);
    value = v;
  }

  bool leaf_matches(const char *k, uint32_t l, uint32_t d) const {
    if (keyLen != l) return false;
    if (d >= l) return true;  // when str2 = str1 + xxx
    auto leaf_key = get_key();
    if (leaf_key[d] != k[d]) return false;

    bool ret = std::memcmp(leaf_key, k, l) == 0;
    return ret;
  }

  T value;
};

static_assert(sizeof(ArtNode256) == 16 + 256 * 8, "Wrong memory layout");
static_assert(sizeof(ArtNode48) == 16 + 256 + 48 * 8, "Wrong memory layout");
static_assert(sizeof(ArtNode16) == 16 + 16 + 16 * 8, "Wrong memory layout");
// node4 keys is 8-align
static_assert(sizeof(ArtNode4) == 16 + 8 + 4 * 8, "Wrong memory layout");

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

namespace detail {

struct PrefixDiffResult {
  int32_t prefix_len;
  uint8_t c1;
  uint8_t c2;
};

static PrefixDiffResult get_prefix_len_and_diff_char(const ArtNodeCommon *leaf1,
                                                     const ArtNodeCommon *leaf2,
                                                     int32_t depth) {
  assert(leaf1->type == ArtNodeType::ART_NODE_LEAF);
  assert(leaf2->type == ArtNodeType::ART_NODE_LEAF);

  int32_t maxCmp = std::min(leaf1->keyLen, leaf2->keyLen) - depth;
  int idx = 0;
  uint8_t c1 = 0;
  uint8_t c2 = 0;

  auto key1 = leaf1->get_key() + depth;
  auto key2 = leaf2->get_key() + depth;

  for (; idx < maxCmp; idx++) {
    if (key1[idx] != key2[idx]) {
      c1 = key1[idx];
      c2 = key2[idx];
      break;
    }
  }

  // It a problem that how to support predix contain: str2 = str1 + xxx
  if (unlikely(idx == maxCmp)) {
    if (leaf1->keyLen > leaf2->keyLen) {
      c1 = key1[idx];
    } else {
      c2 = key2[idx];
    }
  }

  return {idx, c1, c2};
}

static const ArtNodeCommon *art_get_minimum_node(const ArtNodeCommon *node) {
  auto ret = node;

  while (ret) {
    if (ret->type == ArtNodeType::ART_NODE_LEAF) {
      break;
    }

    switch (ret->type) {
      case ArtNodeType::ART_NODE_4: {
        ret = reinterpret_cast<const ArtNode4 *>(ret)->children[0];
      } break;
      case ArtNodeType::ART_NODE_16: {
        ret = reinterpret_cast<const ArtNode16 *>(ret)->children[0];
      } break;
      case ArtNodeType::ART_NODE_48: {
        int32_t idx = 0;
        auto tmp = reinterpret_cast<const ArtNode48 *>(ret);
        while (!tmp->index[idx]) idx++;
        idx = tmp->index[idx] - 1;
        ret = tmp->children[idx];
      } break;
      case ArtNodeType::ART_NODE_256: {
        int32_t idx = 0;
        auto tmp = reinterpret_cast<const ArtNode256 *>(ret);
        while (!tmp->children[idx]) idx++;
        ret = tmp->children[idx];
      } break;
      default: {
        LOG_ERROR("unknown node type");
      }
    }
  }

  return ret;
}

static inline ArtNodeKey funGetNewPrefix(const char *ptr, uint32_t depth,
                                         uint32_t prefix_len,
                                         uint32_t prefix_diff) {
  ArtNodeKey ret = {.keyPtr = nullptr};
  uint32_t remain_len = prefix_len - prefix_diff - 1;
  assert(prefix_len >= prefix_diff + 1);
  if (remain_len > 0) {
    std::memcpy(ret.shortKey, ptr + depth + prefix_diff + 1,
                std::min((uint32_t)ART_MAX_PREFIX_LEN, remain_len));
  }

  return ret;
};

struct InnerPrefixDiffResult {
  int32_t prefix_len;
  uint8_t c1;
  uint8_t c2;
  ArtNodeKey new_inner;
};

static InnerPrefixDiffResult art_check_inner_prefix(const ArtNodeCommon *node,
                                                    const char *key,
                                                    uint32_t len,
                                                    uint32_t depth) {
  assert(node->type != ArtNodeType::ART_NODE_LEAF);
  int32_t ret = 0;
  uint8_t c1 = 0;
  uint8_t c2 = 0;
  ArtNodeKey new_prefix = {.keyPtr = nullptr};

  auto max_cmp = std::min(node->keyLen, len - depth);
  if (node->keyLen <= ART_MAX_PREFIX_LEN) {
    auto prefix = node->get_key();
    for (; ret < max_cmp; ret++) {
      if (prefix[ret] != key[depth + ret]) {
        c1 = prefix[ret];
        c2 = key[depth + ret];
        new_prefix = funGetNewPrefix(prefix, (uint32_t)0, node->keyLen, ret);
        return {ret, c1, c2, new_prefix};
      }
    }

    if (len - depth < node->keyLen) {
      c1 = prefix[ret];
      new_prefix = funGetNewPrefix(prefix, (uint32_t)0, node->keyLen, ret);
    }

    return {ret, c1, c2, new_prefix};
  } else {
    // We need get a leaf for entire prefix.
    // All child have same prefix, wo get leftmost.
    auto l = art_get_minimum_node(node);

    for (; ret < max_cmp; ret++) {
      if (l->get_key()[depth + ret] != key[depth + ret]) {
        c1 = l->get_key()[depth + ret];
        c2 = key[depth + ret];
        new_prefix = funGetNewPrefix(l->get_key(), depth, node->keyLen, ret);
        return {ret, c1, c2, new_prefix};
      }
    }

    if (len - depth < node->keyLen) {
      c1 = l->get_key()[depth + ret];
      new_prefix = funGetNewPrefix(l->get_key(), depth, node->keyLen, ret);
    }

    return {ret, c1, c2, new_prefix};
  }
}

static bool art_inner_prefix_match(const ArtNodeCommon *node, const char *key,
                                   uint32_t len, uint32_t depth) {
  auto store_inner_prefix_len =
      std::min(node->keyLen, (uint32_t)ART_MAX_PREFIX_LEN);
  uint32_t remain_key_len = len - depth;

  bool ret = false;
  // if remain key len less than inner prefix, mismatch
  if (remain_key_len >= store_inner_prefix_len) {
    ret = true;

    int32_t idx = 0;
    auto prefix = node->get_key();
    for (; idx < store_inner_prefix_len; idx++) {
      if (prefix[idx] != key[depth + idx]) {
        ret = false;
        break;
      }
    }
  }

  return ret;
}

static inline ArtNodeCommon **art_find_child(ArtNodeCommon *node,
                                             uint8_t keyByte) {
  ArtNodeCommon **ret;
  switch (node->type) {
    case ART_NODE_4: {
      ret = reinterpret_cast<ArtNode4 *>(node)->find_child(keyByte);
    } break;
    case ART_NODE_16: {
      ret = reinterpret_cast<ArtNode16 *>(node)->find_child(keyByte);
    } break;
    case ART_NODE_48: {
      ret = reinterpret_cast<ArtNode48 *>(node)->find_child(keyByte);
    } break;
    case ART_NODE_256: {
      ret = reinterpret_cast<ArtNode256 *>(node)->find_child(keyByte);
    } break;
    default: {
      LOG_ERROR("unknown node type");
    } break;
  }
  return ret;
}

static inline std::string node_type_string(const ArtNodeCommon *node) {
  std::string ret;
  switch (node->type) {
    case ART_NODE_4: {
      ret = "Node4";
    } break;
    case ART_NODE_16: {
      ret = "Node16";
    } break;
    case ART_NODE_48: {
      ret = "Node48";
    } break;
    case ART_NODE_256: {
      ret = "Node256";
    } break;
    case ART_NODE_LEAF: {
      ret = "Leaf";
    } break;
    default: {
      LOG_ERROR("unknown node type");
    } break;
  }

  return ret;
}

}  // namespace detail
}  // namespace art
