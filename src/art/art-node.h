#pragma once

#include <algorithm>
#include <cassert>
#include <cstdint>
#include <cstring>

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
    const char *ptr = get_key_in_depth(0);
    std::memcpy(buf, ptr, keyLen);
  }

  void reset_key() {
    if (keyLen > ART_MAX_PREFIX_LEN) {
      free(key.keyPtr);
    }
    key.keyPtr = nullptr;
    keyLen = 0;
  }

  void remove_prefix(int32_t l) {
    assert(l <= keyLen);
    if (l == keyLen) {
      reset_key();
    } else {
      if (keyLen <= ART_MAX_PREFIX_LEN) {
        keyLen -= l;
        std::memmove(key.shortKey, key.shortKey + l, keyLen);
      } else {
        uint32_t after_len = keyLen - l;
        if (after_len <= ART_MAX_PREFIX_LEN) {
          std::memmove(key.shortKey, key.keyPtr + l, after_len);
        } else {
          auto tmp = static_cast<char *>(malloc(keyLen - l));
          std::memcpy(tmp, key.keyPtr + l, after_len);
          reset_key();
          key.keyPtr = tmp;
          keyLen = after_len;
        }
      }
    }
  }

  void set_key(const char *k, uint32_t l) {
    keyLen = l;
    if (l <= ART_MAX_PREFIX_LEN) {
      std::memcpy(key.shortKey, k, l);
    } else {
      key.keyPtr = static_cast<char *>(malloc(l));
      std::memcpy(key.keyPtr, k, l);
    }
  }

  std::string to_string() {
    if (keyLen > ART_MAX_PREFIX_LEN) {
      return std::string(key.keyPtr, keyLen);
    } else {
      return std::string(key.shortKey, keyLen);
    }
  }

  inline const char *get_key_in_depth(uint32_t d) const {
    const char *ptr = nullptr;
    if (keyLen > ART_MAX_PREFIX_LEN) {
      ptr = key.keyPtr;
    } else {
      ptr = key.shortKey;
    }
    return ptr + d;
  }

  inline uint8_t get_char_in_depth(uint32_t d) const {
    return get_key_in_depth(d)[0];
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
    char *leafKeyCmpStart = nullptr;
    if (l > ART_MAX_PREFIX_LEN) {
      leafKeyCmpStart = key.keyPtr;
    } else {
      leafKeyCmpStart = key.shortKey;
    }
    leafKeyCmpStart += d;

    return std::memcmp(leafKeyCmpStart, k + d, l - d) == 0;
  }

  T value;
};

static_assert(sizeof(ArtNode256) == 16 + 256 * 8, "Wrong memory layout");
static_assert(sizeof(ArtNode48) == 16 + 256 + 48 * 8, "Wrong memory layout");
static_assert(sizeof(ArtNode16) == 16 + 16 + 16 * 8, "Wrong memory layout");
// node4 keys is 8-align
static_assert(sizeof(ArtNode4) == 16 + 8 + 4 * 8, "Wrong memory layout");

namespace detail {

std::tuple<int32_t, uint8_t, uint8_t> get_prefix_len_and_diff_char(
    const ArtNodeCommon *leaf1, const ArtNodeCommon *leaf2, int32_t depth) {
  assert(leaf1->type == ArtNodeType::ART_NODE_LEAF);
  assert(leaf2->type == ArtNodeType::ART_NODE_LEAF);

  int32_t maxCmp = std::min(leaf1->keyLen, leaf2->keyLen) - depth;
  int idx = 0;
  uint8_t c1 = 0;
  uint8_t c2 = 0;

  auto key1 = leaf1->get_key_in_depth(depth);
  auto key2 = leaf2->get_key_in_depth(depth);

  for (; idx < maxCmp; idx++) {
    if (key1[idx] != key2[idx]) {
      c1 = key1[idx];
      c2 = key2[idx];
      break;
    }
  }
  assert(idx < maxCmp);

  return {idx, c1, c2};
}

std::tuple<int32_t, uint8_t, uint8_t> art_check_inner_prefix(
    const ArtNodeCommon *node, const char *key, uint32_t len, uint32_t depth) {
  assert(node->type != ArtNodeType::ART_NODE_LEAF);
  auto prefix = node->get_key_in_depth(0);
  auto minLen = std::min(node->keyLen, len - depth);

  int32_t ret = 0;
  uint8_t c1 = 0;
  uint8_t c2 = 0;

  for (; ret < minLen; ret++) {
    if (prefix[ret] != key[depth + ret]) {
      c1 = prefix[ret];
      c2 = key[depth + ret];
      break;
    }
  }

  return {ret, c1, c2};
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
      ret = nullptr;
      assert(false);
    } break;
  }
  return ret;
}

}  // namespace detail
}  // namespace art
