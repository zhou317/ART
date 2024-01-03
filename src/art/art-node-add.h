#pragma once

#include "art/art-node-pool.h"

namespace art {
namespace detail {

static inline void art_add_child_to_n256(ArtNodeCommon **node, uint8_t keyByte,
                                         ArtNodeCommon *child) {
  assert(*node);
  auto nodePtr = reinterpret_cast<ArtNode256 *>(*node);
  assert(nodePtr->type = ArtNodeTypeTrait<ArtNode256>::get_node_type());

  nodePtr->children[keyByte] = child;
  nodePtr->childNum++;
}

static inline void art_add_child_to_n48(ArtNodeCommon **node, uint8_t keyByte,
                                        ArtNodeCommon *child) {
  assert(*node);
  auto nodePtr = reinterpret_cast<ArtNode48 *>(*node);
  assert(nodePtr->type = ArtNodeTypeTrait<ArtNode48>::get_node_type());

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
    auto newNode256 = get_new_node<ArtNode256>();

    for (int32_t i = 0; i < 256; i++) {
      if (nodePtr->index[i]) {
        newNode256->children[i] = nodePtr->children[nodePtr->index[i] - 1];
      }
    }
    newNode256->childNum = nodePtr->childNum;
    newNode256->get_key_from_another(nodePtr);
    return_new_node(nodePtr);
    *node = reinterpret_cast<ArtNodeCommon *>(newNode256);
    art_add_child_to_n256(node, keyByte, child);
  }
}

static inline void art_add_child_to_n16(ArtNodeCommon **node, uint8_t keyByte,
                                        ArtNodeCommon *child) {
  assert(*node);
  auto nodePtr = reinterpret_cast<ArtNode16 *>(*node);
  assert(nodePtr->type = ArtNodeTypeTrait<ArtNode16>::get_node_type());

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
                   diff * sizeof(ArtNodeCommon *));
    }

    nodePtr->keys[idx] = keyByte;
    nodePtr->children[idx] = child;
    nodePtr->childNum++;
  } else {
    auto newNode48 = get_new_node<ArtNode48>();

    std::memcpy(newNode48->children, nodePtr->children,
                16 * sizeof(ArtNodeCommon *));
    for (int32_t i = 0; i < 16; i++) {
      newNode48->index[nodePtr->keys[i]] = i + 1;
    }

    newNode48->childNum = nodePtr->childNum;
    newNode48->get_key_from_another(nodePtr);
    return_new_node(nodePtr);

    *node = reinterpret_cast<ArtNodeCommon *>(newNode48);
    art_add_child_to_n48(node, keyByte, child);
  }
}

static inline void art_add_child_to_n4(ArtNodeCommon **node, uint8_t keyByte,
                                       ArtNodeCommon *child) {
  assert(*node != nullptr);
  auto nodePtr = reinterpret_cast<ArtNode4 *>(*node);
  assert(nodePtr->type = ArtNodeTypeTrait<ArtNode4>::get_node_type());

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
    auto newNode16 = get_new_node<ArtNode16>();
    std::memcpy(newNode16->keys, nodePtr->keys, 4 * sizeof(uint8_t));
    std::memcpy(newNode16->children, nodePtr->children,
                4 * sizeof(ArtNodeCommon *));
    newNode16->childNum = nodePtr->childNum;
    newNode16->get_key_from_another(nodePtr);
    return_new_node(nodePtr);

    *node = reinterpret_cast<ArtNodeCommon *>(newNode16);
    art_add_child_to_n16(node, keyByte, child);
  }
}

static inline void art_add_child_to_node(ArtNodeCommon **node, uint8_t keyByte,
                                         ArtNodeCommon *child) {
  assert(node);
  assert(*node);
  auto nodePtr = *node;

  switch (nodePtr->type) {
    case ART_NODE_4: {
      art_add_child_to_n4(node, keyByte, child);
    } break;
    case ART_NODE_16: {
      art_add_child_to_n16(node, keyByte, child);
    } break;
    case ART_NODE_48: {
      art_add_child_to_n48(node, keyByte, child);
    } break;
    case ART_NODE_256: {
      art_add_child_to_n256(node, keyByte, child);
    } break;
    default:
      assert(false);
  }
}

}  // namespace detail
}  // namespace art
