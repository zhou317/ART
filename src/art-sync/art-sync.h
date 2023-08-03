#pragma once

#include <setjmp.h>

#include <cstdint>
#include <string>

#include "art-sync/art-node-sync.h"

namespace ArtSync {

static inline void ArtAddChild256(ArtNodeCommon** node, uint8_t keyByte,
                                  ArtNodeCommon* child);

static inline void ArtAddChild48(ArtNodeCommon** node, uint8_t keyByte,
                                 ArtNodeCommon* child);

static inline void ArtAddChild16(ArtNodeCommon** node, uint8_t keyByte,
                                 ArtNodeCommon* child);

static inline void ArtAddChild4(ArtNodeCommon** node, uint8_t keyByte,
                                ArtNodeCommon* child);

#define ART_SET_RESTART_POINT setjmp(_thdArtRestartEnv)
#define ART_RESTART longjmp(ArtTreeSync::_thdArtRestartEnv, 1)

#define ART_SET_LOCK_BIT(v) ((v) + 2)
#define ART_IS_OBSOLETE(v) ((v) & 1)
#define ART_IS_LOCKED(v) ((v) & 2)

template <class T>
class ArtTreeSync {
 public:
  ArtTreeSync() = default;

  ~ArtTreeSync() {
    if (likely(root_.childNum > 0)) destroyNode<T>(root_.children[0]);
  }

  bool set(const char* key, uint32_t len, T value) {
    ART_SET_RESTART_POINT;
    uint64_t version = ArtReadLockOrRestart(&root_);
    bool ret = insertInt(&root_.children[0], key, len, value, 0, &root_, version);
    return ret;
  }

  T get(const char* key, uint32_t len) const {
    T ret{};

    ART_SET_RESTART_POINT;
    uint64_t version = ArtReadLockOrRestart(&root_);
    findInt(root_.children[0], key, len, 0, nullptr, 0, &ret);
    ArtReadUnlockOrRestart(&root_, version);

    return ret;
  }

  T del(const char* key, uint32_t len) {
    ART_SET_RESTART_POINT;
    return deleteInt(nullptr, &root_, key, len, 0);
  }

  int32_t count(const char* key, uint32_t len) const {
    ART_SET_RESTART_POINT;
    T ret{};
    findInt(root_, key, len, 0, nullptr, 0, &ret);
    return ret != T{};
  }

  // for debug
  ArtNodeCommon* getRoot() { return &root_; }

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

  bool insertInt(ArtNodeCommon** node, const char* key, uint32_t len, T value,
                 uint32_t depth, ArtNodeCommon* pNode, uint64_t pv) {
    if (unlikely(*node == nullptr)) {
      // Avoid concurrently initialization of root.
      ArtUpgradeToWriteOrRestart(pNode, pv);
      *node = new ArtLeaf<T>(key, len, value);
      size_++;
      ArtWriteUnlock(pNode);
      return true;
    }

    auto nodePtr = *node;
    uint64_t nv = ArtReadLockOrRestart(nodePtr);
    if (nodePtr->type == ArtNodeType::ART_NODE_LEAF) {
      if (leafMatches(nodePtr, key, len, depth)) {
        return false;
      }
      ArtUpgradeToWriteOrRestart(pNode, pv);
      ArtUpgradeToWriteOrRestart(nodePtr, nv, pNode);

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
      ArtWriteUnlock(nodePtr);
      ArtWriteUnlock(pNode);
      return true;
    }

    int32_t p = ArtCheckPrefix(nodePtr, key, len, depth);
    if (p != nodePtr->len.innerPrefixLen) {
      ArtUpgradeToWriteOrRestart(pNode, pv);
      ArtUpgradeToWriteOrRestart(nodePtr, nv, pNode);

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

      ArtWriteUnlock(nodePtr);
      ArtWriteUnlock(pNode);
      return true;
    }

    depth += nodePtr->len.innerPrefixLen;
    auto next = ArtFindChild(nodePtr, key[depth]);
    // checking if we get the right pointer
    ArtReadUnlockOrRestart(nodePtr, nv);
    if (next) {
      return insertInt(next, key, len, value, depth + 1, nodePtr, nv);
    } else {
      ArtUpgradeToWriteOrRestart(pNode, pv);
      ArtUpgradeToWriteOrRestart(nodePtr, nv, pNode);
      auto newLeaf = new ArtLeaf<T>(key, len, value);
      bool full = false;
      switch (nodePtr->type) {
        case ART_NODE_4: {
          full = reinterpret_cast<ArtNode4*>(nodePtr)->childNum == 16;
          ArtAddChild4(node, newLeaf->key.leafKeyPtr[depth], newLeaf);
        } break;
        case ART_NODE_16: {
          full = reinterpret_cast<ArtNode16*>(nodePtr)->childNum == 16;
          ArtAddChild16(node, newLeaf->key.leafKeyPtr[depth], newLeaf);
        } break;
        case ART_NODE_48: {
          full = reinterpret_cast<ArtNode48*>(nodePtr)->childNum == 48;
          ArtAddChild48(node, newLeaf->key.leafKeyPtr[depth], newLeaf);
        } break;
        case ART_NODE_256: {
          ArtAddChild256(node, newLeaf->key.leafKeyPtr[depth], newLeaf);
        } break;
        default:
          assert(false);
      }
      if (full) {
        ArtWriteUnlockObsolete(nodePtr);
      } else {
        ArtWriteUnlock(nodePtr);
      }
      ArtWriteUnlock(pNode);
      size_++;
      return true;
    }
  }

  void findInt(const ArtNodeCommon* node, const char* key,
                               uint32_t len, uint32_t depth,
                               const ArtNodeCommon* pNode, uint64_t pv,
                               T* out) const {
    if (unlikely(node == nullptr)) {
      return;
    }
    uint64_t nv = ArtReadLockOrRestart(node);
    if (pNode) {
      // TODO(zzh). Do we really need to restart here?
      ArtReadUnlockOrRestart(pNode, pv);
    }

    if (node->type == ArtNodeType::ART_NODE_LEAF) {
      if (leafMatches(node, key, len, depth)) {
        *out = reinterpret_cast<const ArtLeaf<T>*>(node)->value;
        ArtReadUnlockOrRestart(node, nv);
      }
      return;
    }

    if (ArtCheckPrefix(node, key, len, depth) != node->len.innerPrefixLen) {
      ArtReadUnlockOrRestart(node, nv);
      return;
    }

    depth += node->len.innerPrefixLen;
    auto next = ArtFindChild(const_cast<ArtNodeCommon*>(node), key[depth]);
    // In case when we search, someone change node
    ArtReadUnlockOrRestart(node, nv);
    if (!next) return;
    findInt(*next, key, len, depth + 1, node, nv, out);
  }

  static uint64_t ArtAwaitNodeUnlocked(const ArtNodeCommon* node) {
    uint64_t version = node->version.load();
    while (ART_IS_LOCKED(version)) {
      __builtin_ia32_pause();
      version = node->version.load();
    }
    return version;
  }

  static uint64_t ArtReadLockOrRestart(const ArtNodeCommon* node) {
    uint64_t version = ArtAwaitNodeUnlocked(node);
    if (ART_IS_OBSOLETE(version)) ART_RESTART;  // node may be logically delete
    return version;
  }

  static void ArtReadUnlockOrRestart(const ArtNodeCommon* node, uint64_t oldV) {
    if (oldV != node->version.load()) ART_RESTART;
  }

  static void ArtUpgradeToWriteOrRestart(ArtNodeCommon* node, uint64_t oldV) {
    if (!node->version.compare_exchange_strong(oldV, ART_SET_LOCK_BIT(oldV)))
      ART_RESTART;
  }

  static void ArtWriteUnlock(ArtNodeCommon* node) {
    node->version.fetch_add(2);
  }

  static void ArtWriteUnlockObsolete(ArtNodeCommon* node) {
    node->version.fetch_add(3);
  }

  static void ArtUpgradeToWriteOrRestart(ArtNodeCommon* node, uint64_t oldV,
                                         ArtNodeCommon* lockNode) {
    if (!node->version.compare_exchange_strong(oldV, ART_SET_LOCK_BIT(oldV))) {
      ArtWriteUnlock(lockNode);
      ART_RESTART;
    }
  }

 private:
  thread_local static jmp_buf _thdArtRestartEnv;
  ArtNode4 root_;
  std::atomic<uint64_t> size_{0};
};

template <class T>
thread_local jmp_buf ArtTreeSync<T>::_thdArtRestartEnv;

static inline void ArtAddChild256(ArtNodeCommon** node, uint8_t keyByte,
                                  ArtNodeCommon* child) {
  assert(*node);
  auto nodePtr = reinterpret_cast<ArtNode256*>(*node);
  nodePtr->children[keyByte] = child;
  nodePtr->childNum++;
}

static inline void ArtAddChild48(ArtNodeCommon** node, uint8_t keyByte,
                                 ArtNodeCommon* child) {
  assert(*node);
  auto nodePtr = reinterpret_cast<ArtNode48*>(*node);

  if (nodePtr->childNum < 48) {
    int32_t pos = nodePtr->childNum;
    while (nodePtr->children[pos]) {
      pos++;
      pos = pos % 48;
    }
    nodePtr->index[keyByte] = pos + 1;
    nodePtr->children[pos] = child;
    nodePtr->childNum++;
  } else {
    auto newNode256 = new ArtNode256();

    for (int32_t i = 0; i < 256; i++) {
      if (nodePtr->index[i]) {
        newNode256->children[i] = nodePtr->children[nodePtr->index[i] - 1];
      }
    }
    newNode256->childNum = nodePtr->childNum;
    newNode256->len.innerPrefixLen = nodePtr->len.innerPrefixLen;
    newNode256->key.leafKeyPtr = nodePtr->key.leafKeyPtr;
    *node = reinterpret_cast<ArtNodeCommon*>(newNode256);
    ArtAddChild256(node, keyByte, child);
  }
}

static inline void ArtAddChild16(ArtNodeCommon** node, uint8_t keyByte,
                                 ArtNodeCommon* child) {
  assert(*node);
  auto nodePtr = reinterpret_cast<ArtNode16*>(*node);

  if (nodePtr->childNum < 16) {
    int32_t idx = 0;
    for (; idx < nodePtr->childNum; idx++) {
      if (keyByte < nodePtr->keys[idx]) break;
    }

    // move keys and child pointers
    size_t diff = nodePtr->childNum - idx;
    if (diff) {
      std::memmove(&(nodePtr->keys[idx + 1]), &(nodePtr->keys[idx]), diff);
      std::memmove(&(nodePtr->children[idx + 1]), &(nodePtr->children[idx]),
                   diff * sizeof(ArtNodeCommon*));
    }

    nodePtr->keys[idx] = keyByte;
    nodePtr->children[idx] = child;
    nodePtr->childNum++;
  } else {
    auto newNode48 = new ArtNode48();

    std::memcpy(newNode48->children, nodePtr->children,
                16 * sizeof(ArtNodeCommon*));
    for (int32_t i = 0; i < 16; i++) {
      newNode48->index[nodePtr->keys[i]] = i + 1;
    }

    newNode48->childNum = nodePtr->childNum;
    newNode48->len.innerPrefixLen = nodePtr->len.innerPrefixLen;
    newNode48->key.leafKeyPtr = nodePtr->key.leafKeyPtr;
    *node = reinterpret_cast<ArtNodeCommon*>(newNode48);
    ArtAddChild48(node, keyByte, child);
  }
}

static inline void ArtAddChild4(ArtNodeCommon** node, uint8_t keyByte,
                                ArtNodeCommon* child) {
  assert(*node != nullptr);
  auto nodePtr = reinterpret_cast<ArtNode4*>(*node);

  if (nodePtr->childNum < 4) {
    int32_t idx = 0;
    for (; idx < nodePtr->childNum; idx++) {
      if (keyByte < nodePtr->keys[idx]) break;
    }

    // move keys and child pointers
    for (int32_t i = nodePtr->childNum; i > idx; i--) {
      nodePtr->keys[i] = nodePtr->keys[i - 1];
      nodePtr->children[i] = nodePtr->children[i - 1];
    }

    nodePtr->keys[idx] = keyByte;
    nodePtr->children[idx] = child;
    nodePtr->childNum++;
  } else {
    auto newNode16 = new ArtNode16();
    std::memcpy(newNode16->keys, nodePtr->keys, 4 * sizeof(uint8_t));
    std::memcpy(newNode16->children, nodePtr->children,
                4 * sizeof(ArtNodeCommon*));
    newNode16->childNum = nodePtr->childNum;
    newNode16->len.innerPrefixLen = nodePtr->len.innerPrefixLen;
    // use assign instead memcpy()
    newNode16->key.leafKeyPtr = nodePtr->key.leafKeyPtr;
    *node = reinterpret_cast<ArtNodeCommon*>(newNode16);
    ArtAddChild16(node, keyByte, child);
  }
}


}  // namespace ArtSync
