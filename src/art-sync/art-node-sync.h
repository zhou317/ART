#pragma once

#include <algorithm>
#include <atomic>
#include <cassert>
#include <cstdint>
#include <cstring>

#include "common/macros.h"

#if defined(__i386__) || defined(__amd64__)
#include <emmintrin.h>
#endif

namespace ArtSync {

enum ArtNodeType : uint16_t {
  ART_NODE_INVALID,
  ART_NODE_4,
  ART_NODE_16,
  ART_NODE_48,
  ART_NODE_256,
  ART_NODE_LEAF
};

struct ArtNodeCommon {
  std::atomic<uint64_t> version{0};

  union {
    char innerPrefix[ART_MAX_PREFIX_LEN];
    char* leafKeyPtr;
  } key;

  union {
    uint32_t innerPrefixLen;
    uint32_t leafKeyLen;
  } len;

  ArtNodeCommon() {
    len.innerPrefixLen = 0;
    key.leafKeyPtr = nullptr;
  }

  ArtNodeType type = ART_NODE_INVALID;
};

struct ArtNode4 : public ArtNodeCommon {
  uint16_t childNum = 0;
  uint8_t keys[4] = {0};
  ArtNodeCommon* children[4] = {nullptr};

  ArtNode4() { type = ArtNodeType::ART_NODE_4; }

  ArtNodeCommon** findChild(uint8_t keyByte) {
    ArtNodeCommon** ret = nullptr;
    for (int32_t i = 0; i < childNum; i++) {
      if (keys[i] == keyByte) {
        ret = &children[i];
        break;
      }
    }
    return ret;
  }
};

struct ArtNode16 : public ArtNodeCommon {
  uint16_t childNum = 0;
  uint8_t keys[16] = {0};
  ArtNodeCommon* children[16] = {nullptr};

  ArtNode16() { type = ArtNodeType::ART_NODE_16; }

  ArtNodeCommon** findChild(uint8_t keyByte) {
#if defined(__i386__) || defined(__amd64__)
    int bitfield =
        _mm_movemask_epi8(_mm_cmpeq_epi8(_mm_set1_epi8(keyByte),
                                         _mm_loadu_si128((__m128i*)keys))) &
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
        return &children_[mid];
      }
    }
    return nullptr;
#endif
  }
};

struct ArtNode48 : public ArtNodeCommon {
  uint16_t childNum = 0;
  uint8_t index[256] = {0};
  ArtNodeCommon* children[48] = {nullptr};

  ArtNode48() { type = ArtNodeType::ART_NODE_48; }

  ArtNodeCommon** findChild(uint8_t keyByte) {
    ArtNodeCommon** ret = nullptr;
    if (index[keyByte]) {
      ret = &children[index[keyByte] - 1];
    }
    return ret;
  }
};

struct ArtNode256 : public ArtNodeCommon {
  uint16_t childNum = 0;
  ArtNodeCommon* children[256] = {nullptr};

  ArtNode256() { type = ArtNodeType::ART_NODE_256; }

  ArtNodeCommon** findChild(uint8_t keyByte) {
    return children[keyByte] != nullptr ? &children[keyByte] : nullptr;
  }
};

template <class T>
struct ArtLeaf : public ArtNodeCommon {
  ArtLeaf(const char* k, uint32_t l, T v) {
    key.leafKeyPtr = static_cast<char*>(malloc(l));
    len.leafKeyLen = l;
    type = ArtNodeType::ART_NODE_LEAF;
    std::memcpy(key.leafKeyPtr, k, l);
    value = v;
  }

  ~ArtLeaf() { free(key.leafKeyPtr); }

  T value;
};

static inline void ArtDeleteChild4(ArtNodeCommon** node, uint8_t keyByte) {
  assert(*node != nullptr);
  auto nodePtr = reinterpret_cast<ArtNode4*>(*node);
  int32_t idx = 0;
  for (; idx < nodePtr->childNum; idx++) {
    if (keyByte == nodePtr->keys[idx]) break;
  }
  assert(idx < nodePtr->childNum);
  for (int32_t i = idx; i < nodePtr->childNum - 1; i++) {
    nodePtr->keys[i] = nodePtr->keys[i + 1];
    nodePtr->children[i] = nodePtr->children[i + 1];
  }
  nodePtr->childNum--;
  nodePtr->keys[nodePtr->childNum] = 0;
  nodePtr->children[nodePtr->childNum] = nullptr;
  if (nodePtr->childNum < 2) {
    assert(nodePtr->childNum == 1);
    ArtNodeCommon* child = nodePtr->children[0];
    if (child->type != ArtNodeType::ART_NODE_LEAF) {
      // path compression
      char childPrefix[ART_MAX_PREFIX_LEN];
      std::memcpy(childPrefix, child->key.innerPrefix, ART_MAX_PREFIX_LEN);
      uint32_t childPrefixLen = child->len.innerPrefixLen;

      std::memcpy(child->key.innerPrefix, nodePtr->key.innerPrefix,
                  ART_MAX_PREFIX_LEN);
      child->len.innerPrefixLen = nodePtr->len.innerPrefixLen;

      if (child->len.innerPrefixLen < ART_MAX_PREFIX_LEN) {
        child->key.innerPrefix[child->len.innerPrefixLen] = nodePtr->keys[0];
        child->len.innerPrefixLen++;
        if (child->len.innerPrefixLen < ART_MAX_PREFIX_LEN &&
            childPrefixLen > 0) {
          uint32_t size = std::min(
              ART_MAX_PREFIX_LEN - child->len.innerPrefixLen, childPrefixLen);
          std::memcpy(&(child->key.innerPrefix[child->len.innerPrefixLen]),
                      childPrefix, size);
          child->len.innerPrefixLen += size;
        }
      }
    }
    *node = child;
    delete nodePtr;
  }
}

static inline void ArtDeleteChild16(ArtNodeCommon** node, uint8_t keyByte) {
  assert(*node != nullptr);
  auto nodePtr = reinterpret_cast<ArtNode16*>(*node);
  int32_t idx = 0;
#if defined(__i386__) || defined(__amd64__)
  int bitfield =
      _mm_movemask_epi8(_mm_cmpeq_epi8(
          _mm_set1_epi8(keyByte), _mm_loadu_si128((__m128i*)nodePtr->keys))) &
      ((1 << nodePtr->childNum) - 1);
  assert((bool)bitfield);
  idx = __builtin_ctz(bitfield);
#else
  for (; idx < nodePtr->childNum; idx++) {
    if (keyByte == nodePtr->keys[idx]) break;
  }
#endif

  assert(idx < nodePtr->childNum);

  // move keys and child pointers
  size_t diff = nodePtr->childNum - idx - 1;
  if (diff) {
    std::memmove(&(nodePtr->keys[idx]), &(nodePtr->keys[idx + 1]), diff);
    std::memmove(&(nodePtr->children[idx]), &(nodePtr->children[idx + 1]),
                 diff * sizeof(ArtNodeCommon*));
  }

  nodePtr->childNum--;
  nodePtr->keys[nodePtr->childNum] = 0;
  nodePtr->children[nodePtr->childNum] = nullptr;
  assert(nodePtr->childNum >= 4);
  if (nodePtr->childNum == 4) {
    auto newNode4 = new ArtNode4();
    std::memcpy(newNode4->keys, nodePtr->keys, 4 * sizeof(uint8_t));
    std::memcpy(newNode4->children, nodePtr->children,
                4 * sizeof(ArtNodeCommon*));
    newNode4->childNum = nodePtr->childNum;
    newNode4->len.innerPrefixLen = nodePtr->len.innerPrefixLen;
    newNode4->key.leafKeyPtr = nodePtr->key.leafKeyPtr;
    *node = reinterpret_cast<ArtNodeCommon*>(newNode4);
    delete nodePtr;
  }
}

static inline void ArtDeleteChild48(ArtNodeCommon** node, uint8_t keyByte) {
  assert(*node != nullptr);
  auto nodePtr = reinterpret_cast<ArtNode48*>(*node);
  assert(nodePtr->index[keyByte] != 0);
  uint8_t childIdx = nodePtr->index[keyByte] - 1;
  nodePtr->children[childIdx] = nullptr;
  nodePtr->index[keyByte] = 0;
  nodePtr->childNum--;
  if (nodePtr->childNum == 16) {
    auto newNode16 = new ArtNode16();
    int32_t idx = 0;
    for (uint16_t i = 0; i <= 255; i++) {
      if (nodePtr->index[i]) {
        newNode16->keys[idx] = i;
        newNode16->children[idx] = nodePtr->children[nodePtr->index[i] - 1];
        idx++;
      }
    }
    assert(idx == 16);
    newNode16->childNum = nodePtr->childNum;
    newNode16->len.innerPrefixLen = nodePtr->len.innerPrefixLen;
    newNode16->key.leafKeyPtr = nodePtr->key.leafKeyPtr;
    *node = reinterpret_cast<ArtNodeCommon*>(newNode16);
    delete nodePtr;
  }
}

static inline void ArtDeleteChild256(ArtNodeCommon** node, uint8_t keyByte) {
  assert(*node != nullptr);
  auto nodePtr = reinterpret_cast<ArtNode256*>(*node);
  assert(nodePtr->children[keyByte] != nullptr);
  nodePtr->children[keyByte] = nullptr;
  nodePtr->childNum--;
  if (nodePtr->childNum == 48) {
    auto newNode48 = new ArtNode48();
    int32_t idx = 0;
    for (uint16_t i = 0; i <= 255; i++) {
      if (nodePtr->children[i]) {
        newNode48->children[idx] = nodePtr->children[i];
        newNode48->index[i] = idx + 1;
        idx++;
      }
    }
    assert(idx == 48);
    newNode48->childNum = nodePtr->childNum;
    newNode48->len.innerPrefixLen = nodePtr->len.innerPrefixLen;
    newNode48->key.leafKeyPtr = nodePtr->key.leafKeyPtr;
    *node = reinterpret_cast<ArtNodeCommon*>(newNode48);
    delete nodePtr;
  }
}

template <class T>
static void destroyNode(ArtNodeCommon* node) {
  switch (node->type) {
    case ART_NODE_4: {
      auto delNode = reinterpret_cast<ArtNode4*>(node);
      for (uint8_t i = 0; i < delNode->childNum; i++) {
        destroyNode<T>(delNode->children[i]);
      }
      delete delNode;
    } break;
    case ART_NODE_16: {
      auto delNode = reinterpret_cast<ArtNode16*>(node);
      for (uint8_t i = 0; i < delNode->childNum; i++) {
        destroyNode<T>(delNode->children[i]);
      }
      delete delNode;
    } break;
    case ART_NODE_48: {
      auto delNode = reinterpret_cast<ArtNode48*>(node);
      for (uint8_t i = 0; i < 255; i++) {
        if (delNode->index[i])
          destroyNode<T>(delNode->children[delNode->index[i] - 1]);
      }
      delete delNode;
    } break;
    case ART_NODE_256: {
      auto delNode = reinterpret_cast<ArtNode256*>(node);
      for (uint8_t i = 0; i < 255; i++) {
        if (delNode->children[i]) destroyNode<T>(delNode->children[i]);
      }
      delete delNode;
    } break;
    case ART_NODE_LEAF: {
      auto delNode = reinterpret_cast<ArtLeaf<T>*>(node);
      delete delNode;
    } break;
    default: {
      assert(false);
    } break;
  }
}

static inline int32_t ArtCheckPrefix(const ArtNodeCommon* node, const char* key,
                                     uint32_t len, uint32_t depth) {
  assert(node->type != ArtNodeType::ART_NODE_LEAF);
  auto prefix = node->key.innerPrefix;
  auto minLen = std::min(node->len.innerPrefixLen, len - depth);
  int32_t ret = 0;
  for (; ret < minLen; ret++) {
    if (prefix[ret] != key[depth + ret]) break;
  }

  return ret;
}

static inline ArtNodeCommon** ArtFindChild(ArtNodeCommon* node,
                                           uint8_t keyByte) {
  ArtNodeCommon** ret;
  switch (node->type) {
    case ART_NODE_4: {
      ret = reinterpret_cast<ArtNode4*>(node)->findChild(keyByte);
    } break;
    case ART_NODE_16: {
      ret = reinterpret_cast<ArtNode16*>(node)->findChild(keyByte);
    } break;
    case ART_NODE_48: {
      ret = reinterpret_cast<ArtNode48*>(node)->findChild(keyByte);
    } break;
    case ART_NODE_256: {
      ret = reinterpret_cast<ArtNode256*>(node)->findChild(keyByte);
    } break;
    default: {
      ret = nullptr;
      assert(false);
    } break;
  }
  return ret;
}

static_assert(sizeof(ArtNode256) == 8 + 16 + 256 * 8, "Wrong memory layout");
static_assert(sizeof(ArtNode48) == 8 + 16 + 256 + 48 * 8,
              "Wrong memory layout");
static_assert(sizeof(ArtNode16) == 8 + 16 + 16 + 16 * 8, "Wrong memory layout");
// node4 keys is 8-align
static_assert(sizeof(ArtNode4) == 8 + 16 + 8 + 4 * 8, "Wrong memory layout");

}  // namespace ArtSync
