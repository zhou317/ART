#pragma once

#include <cstdint>
#include <string>

#include "art/art-node.h"

namespace ArtSingle {

template <class T>
class ArtTree {
 public:
  ArtTree() = default;

  ~ArtTree() {
    if (likely(root_)) destroyNode<T>(root_);
  }

  T set(const char* key, uint32_t len, T value) {
    return insertInt(&root_, key, len, value, 0);
  }

  T get(const char* key, uint32_t len) const {
    auto leafNode = findInt(root_, key, len, 0);
    if (leafNode) {
      return reinterpret_cast<const ArtLeaf<T>*>(leafNode)->value;
    } else {
      return T{};
    }
  }

  T del(const char* key, uint32_t len) {
    return deleteInt(nullptr, &root_, key, len, 0);
  }

  int32_t count(const char* key, uint32_t len) const {
    auto leafNode = findInt(root_, key, len, 0);
    if (leafNode) {
      return 1;
    } else {
      return 0;
    }
  }

  // for debug
  ArtNodeCommon* getRoot() { return root_; }

  uint64_t size() const { return size_; }

 private:
  bool leafMatches(const ArtNodeCommon* leaf1, const char* key, uint32_t len,
                   uint32_t depth) const {
    assert(leaf1->type == ArtNodeType::ART_NODE_LEAF);
    if (leaf1->len.leafKeyLen != len) return false;
    auto leafKey = leaf1->key.leafKeyPtr + depth;
    return std::memcmp(leafKey, key + depth, len - depth) == 0;
  }

  int32_t longestPrefixLen(const ArtNodeCommon* leaf1,
                           const ArtNodeCommon* leaf2, int32_t depth) const {
    assert(leaf1->type == ArtNodeType::ART_NODE_LEAF);
    assert(leaf2->type == ArtNodeType::ART_NODE_LEAF);

    int32_t maxCmp =
        std::min(leaf1->len.leafKeyLen, leaf2->len.leafKeyLen) - depth;
    maxCmp = std::min(maxCmp, ART_MAX_PREFIX_LEN);
    int idx = 0;
    auto key1 = leaf1->key.leafKeyPtr + depth;
    auto key2 = leaf2->key.leafKeyPtr + depth;

    for (; idx < maxCmp; idx++) {
      if (key1[idx] != key2[idx]) return idx;
    }
    return idx;
  }

  void adjustNodeAfterDel(ArtNodeCommon** parNode, ArtNodeCommon** node,
                          const char* key, uint32_t len, uint32_t depth) {
    assert(depth < len);
    if (unlikely(parNode == nullptr)) {  // root is delete.
      *node = nullptr;
      return;
    }
    auto partNodePtr = *parNode;
    assert(partNodePtr != nullptr);
    switch (partNodePtr->type) {
      case ART_NODE_4: {
        ArtDeleteChild4(parNode, key[depth - 1]);
      } break;
      case ART_NODE_16: {
        ArtDeleteChild16(parNode, key[depth - 1]);
      } break;
      case ART_NODE_48: {
        ArtDeleteChild48(parNode, key[depth - 1]);
      } break;
      case ART_NODE_256: {
        ArtDeleteChild256(parNode, key[depth - 1]);
      } break;
      default: {
        assert(false);
      } break;
    }
  }

  T deleteInt(ArtNodeCommon** parNode, ArtNodeCommon** node, const char* key,
              uint32_t len, uint32_t depth) {
    if (unlikely(*node == nullptr)) {  // empty root
      return T{};
    }

    auto nodePtr = *node;
    if (nodePtr->type == ArtNodeType::ART_NODE_LEAF) {
      if (leafMatches(nodePtr, key, len, depth)) {
        auto leaf = reinterpret_cast<ArtLeaf<T>*>(nodePtr);
        T v = leaf->value;
        adjustNodeAfterDel(parNode, node, key, len, depth);
        size_--;
        delete leaf;
        return v;
      } else {
        return T{};
      }
    }

    int32_t p = ArtCheckPrefix(nodePtr, key, len, depth);
    if (p != nodePtr->len.innerPrefixLen) {
      return T{};
    }

    depth += nodePtr->len.innerPrefixLen;
    auto next = ArtFindChild(nodePtr, key[depth]);
    if (next) {
      return deleteInt(node, next, key, len, depth + 1);
    } else {
      return T{};
    }
  }

  T insertInt(ArtNodeCommon** node, const char* key, uint32_t len, T value,
              uint32_t depth) {
    if (unlikely(*node == nullptr)) {
      *node = new ArtLeaf<T>(key, len, value);
      size_++;
      return T{};
    }

    auto nodePtr = *node;
    if (nodePtr->type == ArtNodeType::ART_NODE_LEAF) {
      if (leafMatches(nodePtr, key, len, depth)) {
        auto leaf = reinterpret_cast<ArtLeaf<T>*>(nodePtr);
        auto OldV = leaf->value;
        leaf->value = value;
        return OldV;
      }

      auto newInner4 = new ArtNode4();
      auto newLeaf = new ArtLeaf<T>(key, len, value);
      int32_t prefixLen = longestPrefixLen(nodePtr, newLeaf, depth);
      newInner4->len.innerPrefixLen = prefixLen;
      std::memcpy(newInner4->key.innerPrefix, key + depth,
                  std::min(ART_MAX_PREFIX_LEN, prefixLen));

      ArtAddChild4(reinterpret_cast<ArtNodeCommon**>(&newInner4),
                   nodePtr->key.leafKeyPtr[depth + prefixLen], nodePtr);
      ArtAddChild4(reinterpret_cast<ArtNodeCommon**>(&newInner4),
                   newLeaf->key.leafKeyPtr[depth + prefixLen], newLeaf);

      *node = newInner4;
      size_++;
      return T{};
    }

    int32_t p = ArtCheckPrefix(nodePtr, key, len, depth);
    if (p != nodePtr->len.innerPrefixLen) {
      auto newInner4 = new ArtNode4();
      auto newLeaf = new ArtLeaf<T>(key, len, value);

      uint8_t nodeChar = nodePtr->key.innerPrefix[p];
      uint8_t leafChar = newLeaf->key.leafKeyPtr[depth + p];

      newInner4->len.innerPrefixLen = p;
      if (p) std::memmove(newInner4->key.innerPrefix, key + depth, p);

      nodePtr->len.innerPrefixLen -= (p + 1);
      if (nodePtr->len.innerPrefixLen)
        std::memmove(nodePtr->key.innerPrefix, nodePtr->key.innerPrefix + p + 1,
                     nodePtr->len.innerPrefixLen);

      ArtAddChild4(reinterpret_cast<ArtNodeCommon**>(&newInner4), nodeChar,
                   nodePtr);
      ArtAddChild4(reinterpret_cast<ArtNodeCommon**>(&newInner4), leafChar,
                   newLeaf);

      *node = newInner4;
      size_++;
      return T{};
    }

    depth += nodePtr->len.innerPrefixLen;
    auto next = ArtFindChild(nodePtr, key[depth]);
    if (next) {
      return insertInt(next, key, len, value, depth + 1);
    } else {
      auto newLeaf = new ArtLeaf<T>(key, len, value);
      switch (nodePtr->type) {
        case ART_NODE_4: {
          ArtAddChild4(node, newLeaf->key.leafKeyPtr[depth], newLeaf);
        } break;
        case ART_NODE_16: {
          ArtAddChild16(node, newLeaf->key.leafKeyPtr[depth], newLeaf);
        } break;
        case ART_NODE_48: {
          ArtAddChild48(node, newLeaf->key.leafKeyPtr[depth], newLeaf);
        } break;
        case ART_NODE_256: {
          ArtAddChild256(node, newLeaf->key.leafKeyPtr[depth], newLeaf);
        } break;
        default:
          assert(false);
      }

      size_++;
      return T{};
    }
  }

  const ArtNodeCommon* findInt(const ArtNodeCommon* node, const char* key,
                               uint32_t len, uint32_t depth) const {
    if (unlikely(node == nullptr)) {
      return nullptr;
    }

    if (node->type == ArtNodeType::ART_NODE_LEAF) {
      if (leafMatches(node, key, len, depth)) {
        return node;
      } else {
        return nullptr;
      }
    }

    if (ArtCheckPrefix(node, key, len, depth) != node->len.innerPrefixLen) {
      return nullptr;
    }

    depth += node->len.innerPrefixLen;
    auto next = ArtFindChild(const_cast<ArtNodeCommon*>(node), key[depth]);
    if (!next) return nullptr;
    return findInt(*next, key, len, depth + 1);
  }

 private:
  ArtNodeCommon* root_ = nullptr;
  uint64_t size_ = 0;
};

}  // namespace ArtSingle
