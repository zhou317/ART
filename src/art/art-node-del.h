#pragma once

#include "art/art-node-pool.h"

namespace art {
namespace detail {

static inline void art_delete_from_n4(ArtNodeCommon** node, uint8_t keyByte) {
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

      ArtNodeKey key = nodePtr->key;
      uint32_t new_key_len = nodePtr->keyLen;
      if (nodePtr->keyLen < ART_MAX_PREFIX_LEN) {
        key.shortKey[new_key_len++] = nodePtr->keys[0];
      }

      if (new_key_len < ART_MAX_PREFIX_LEN) {
        uint32_t sub_prefix =
            std::min(child->keyLen, (uint32_t)ART_MAX_PREFIX_LEN - new_key_len);
        memcpy(key.shortKey + new_key_len, child->key.shortKey, sub_prefix);
        new_key_len += sub_prefix;
      }

      child->key = key;
      child->keyLen += nodePtr->keyLen + 1;
    }
    *node = child;
    return_art_node(nodePtr);
  }
}

static inline void art_delete_from_n16(ArtNodeCommon** node, uint8_t keyByte) {
  assert(*node != nullptr);
  auto nodePtr = reinterpret_cast<ArtNode16*>(*node);
  int32_t idx = 0;
#if defined(__i386__) || defined(__amd64__)
  int bitfield =
      _mm_movemask_epi8(_mm_cmpeq_epi8(
          _mm_set1_epi8(keyByte), _mm_loadu_si128((__m128i*)nodePtr->keys))) &
      ((1 << nodePtr->childNum) - 1);
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
    auto newNode4 = get_new_art_node<ArtNode4>();
    std::memcpy(newNode4->keys, nodePtr->keys, 4 * sizeof(uint8_t));
    std::memcpy(newNode4->children, nodePtr->children,
                4 * sizeof(ArtNodeCommon*));
    newNode4->childNum = nodePtr->childNum;
    std::swap(newNode4->keyLen, nodePtr->keyLen);
    std::swap(newNode4->key.keyPtr, nodePtr->key.keyPtr);
    *node = reinterpret_cast<ArtNodeCommon*>(newNode4);
    return_art_node(nodePtr);
  }
}

static inline void art_delete_from_n48(ArtNodeCommon** node, uint8_t keyByte) {
  assert(*node != nullptr);
  auto nodePtr = reinterpret_cast<ArtNode48*>(*node);
  assert(nodePtr->index[keyByte] != 0);
  uint8_t childIdx = nodePtr->index[keyByte] - 1;
  nodePtr->children[childIdx] = nullptr;
  nodePtr->index[keyByte] = 0;
  nodePtr->childNum--;
  if (nodePtr->childNum == 16) {
    auto newNode16 = get_new_art_node<ArtNode16>();
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
    std::swap(newNode16->keyLen, nodePtr->keyLen);
    std::swap(newNode16->key.keyPtr, nodePtr->key.keyPtr);
    *node = reinterpret_cast<ArtNodeCommon*>(newNode16);
    return_art_node(nodePtr);
  }
}

static inline void art_delete_from_n256(ArtNodeCommon** node, uint8_t keyByte) {
  assert(*node != nullptr);
  auto nodePtr = reinterpret_cast<ArtNode256*>(*node);
  assert(nodePtr->children[keyByte] != nullptr);
  nodePtr->children[keyByte] = nullptr;
  nodePtr->childNum--;
  if (nodePtr->childNum == 48) {
    auto newNode48 = get_new_art_node<ArtNode48>();
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
    std::swap(newNode48->keyLen, nodePtr->keyLen);
    std::swap(newNode48->key.keyPtr, nodePtr->key.keyPtr);
    *node = reinterpret_cast<ArtNodeCommon*>(newNode48);
    return_art_node(nodePtr);
  }
}

static void art_delete_from_node(ArtNodeCommon** node, uint8_t key_byte) {
  auto node_p = *node;
  assert(node_p != nullptr);
  switch (node_p->type) {
    case ART_NODE_4: {
      art_delete_from_n4(node, key_byte);
    } break;
    case ART_NODE_16: {
      art_delete_from_n16(node, key_byte);
    } break;
    case ART_NODE_48: {
      art_delete_from_n48(node, key_byte);
    } break;
    case ART_NODE_256: {
      art_delete_from_n256(node, key_byte);
    } break;
    default: {
      LOG_ERROR("unknown node type");
    } break;
  }
}

static void art_delete_from_node(ArtNodeCommon** parNode, ArtNodeCommon** node,
                                 const char* key, uint32_t len,
                                 uint32_t depth) {
  if (unlikely(parNode == nullptr)) {  // root is delete.
    *node = nullptr;
    return;
  }
  art_delete_from_node(parNode, key[depth - 1]);
}

template <class T>
static void destroy_node(ArtNodeCommon* node) {
  switch (node->type) {
    case ART_NODE_4: {
      auto delNode = reinterpret_cast<ArtNode4*>(node);
      for (uint16_t i = 0; i < delNode->childNum; i++) {
        destroy_node<T>(delNode->children[i]);
      }
      return_art_node(delNode);
    } break;
    case ART_NODE_16: {
      auto delNode = reinterpret_cast<ArtNode16*>(node);
      for (uint16_t i = 0; i < delNode->childNum; i++) {
        destroy_node<T>(delNode->children[i]);
      }
      return_art_node(delNode);
    } break;
    case ART_NODE_48: {
      auto delNode = reinterpret_cast<ArtNode48*>(node);
      for (uint16_t i = 0; i < 255; i++) {
        if (delNode->index[i])
          destroy_node<T>(delNode->children[delNode->index[i] - 1]);
      }
      return_art_node(delNode);
    } break;
    case ART_NODE_256: {
      auto delNode = reinterpret_cast<ArtNode256*>(node);
      for (uint16_t i = 0; i < 255; i++) {
        if (delNode->children[i]) destroy_node<T>(delNode->children[i]);
      }
      return_art_node(delNode);
    } break;
    case ART_NODE_LEAF: {
      auto delNode = reinterpret_cast<ArtLeaf<T>*>(node);
      return_art_node(delNode);
    } break;
    default: {
      LOG_ERROR("unknown node type");
    } break;
  }
}

}  // namespace detail
}  // namespace art
