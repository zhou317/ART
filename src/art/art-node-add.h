#pragma once

#include "art/art-node-pool.h"
#include "art/art-printer.h"
#include "common/logger.h"

namespace art {
namespace detail {

static inline void art_add_child_to_n256(ArtNodeCommon **node, uint8_t keyByte,
                                         ArtNodeCommon *child) {
  assert(*node);
  auto nodePtr = reinterpret_cast<ArtNode256 *>(*node);
  assert(nodePtr->type = ArtNodeTrait<ArtNode256>::NODE_TYPE);
  assert(nodePtr->childNum <= ArtNodeTrait<ArtNode256>::NODE_CAPASITY);

  nodePtr->children[keyByte] = child;
  nodePtr->childNum++;
}

static inline void art_add_child_to_n48(ArtNodeCommon **node, uint8_t keyByte,
                                        ArtNodeCommon *child) {
  assert(*node);
  auto nodePtr = reinterpret_cast<ArtNode48 *>(*node);
  assert(nodePtr->type = ArtNodeTrait<ArtNode48>::NODE_TYPE);
  assert(nodePtr->childNum <= ArtNodeTrait<ArtNode48>::NODE_CAPASITY);

  if (nodePtr->childNum < ArtNodeTrait<ArtNode48>::NODE_CAPASITY) {
    int32_t pos = nodePtr->childNum;
    while (nodePtr->children[pos]) {
      pos++;
      pos = pos % ArtNodeTrait<ArtNode48>::NODE_CAPASITY;
    }
    nodePtr->index[keyByte] = pos + 1;
    nodePtr->children[pos] = child;
    nodePtr->childNum++;
  } else {
    auto newNode256 = get_new_art_node<ArtNode256>();

    for (int32_t i = 0; i < 256; i++) {
      if (nodePtr->index[i]) {
        newNode256->children[i] = nodePtr->children[nodePtr->index[i] - 1];
      }
    }
    newNode256->childNum = nodePtr->childNum;
    newNode256->get_key_from_another(nodePtr);
    return_art_node(nodePtr);
    *node = reinterpret_cast<ArtNodeCommon *>(newNode256);
    art_add_child_to_n256(node, keyByte, child);
  }
}

static inline void art_add_child_to_n16(ArtNodeCommon **node, uint8_t keyByte,
                                        ArtNodeCommon *child) {
  assert(*node);
  auto nodePtr = reinterpret_cast<ArtNode16 *>(*node);
  assert(nodePtr->type = ArtNodeTrait<ArtNode16>::NODE_TYPE);
  assert(nodePtr->childNum <= ArtNodeTrait<ArtNode16>::NODE_CAPASITY);

  if (nodePtr->childNum < ArtNodeTrait<ArtNode16>::NODE_CAPASITY) {
    int32_t idx = 0;
    for (; idx < nodePtr->childNum; idx++) {
      if (keyByte < nodePtr->keys[idx]) break;
    }

    // move keys and child pointers
    for (int32_t j = nodePtr->childNum; j > idx; j--) {
      nodePtr->keys[j] = nodePtr->keys[j - 1];
      nodePtr->children[j] = nodePtr->children[j - 1];
    }

    nodePtr->keys[idx] = keyByte;
    nodePtr->children[idx] = child;
    nodePtr->childNum++;
  } else {
    auto newNode48 = get_new_art_node<ArtNode48>();

    std::memcpy(
        newNode48->children, nodePtr->children,
        ArtNodeTrait<ArtNode16>::NODE_CAPASITY * sizeof(ArtNodeCommon *));
    for (int32_t i = 0; i < ArtNodeTrait<ArtNode16>::NODE_CAPASITY; i++) {
      newNode48->index[nodePtr->keys[i]] = i + 1;
    }

    newNode48->childNum = nodePtr->childNum;
    newNode48->get_key_from_another(nodePtr);
    return_art_node(nodePtr);

    *node = reinterpret_cast<ArtNodeCommon *>(newNode48);
    art_add_child_to_n48(node, keyByte, child);
  }
}

static inline void art_add_child_to_n4(ArtNodeCommon **node, uint8_t keyByte,
                                       ArtNodeCommon *child) {
  assert(*node != nullptr);
  auto nodePtr = reinterpret_cast<ArtNode4 *>(*node);
  assert(nodePtr->type = ArtNodeTrait<ArtNode4>::NODE_TYPE);
  assert(nodePtr->childNum <= ArtNodeTrait<ArtNode4>::NODE_CAPASITY);

  if (nodePtr->childNum < ArtNodeTrait<ArtNode4>::NODE_CAPASITY) {
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
    auto newNode16 = get_new_art_node<ArtNode16>();
    assert(newNode16->type == ArtNodeTrait<ArtNode16>::NODE_TYPE);
    std::memcpy(newNode16->keys, nodePtr->keys,
                ArtNodeTrait<ArtNode4>::NODE_CAPASITY * sizeof(uint8_t));
    std::memcpy(
        newNode16->children, nodePtr->children,
        ArtNodeTrait<ArtNode4>::NODE_CAPASITY * sizeof(ArtNodeCommon *));
    newNode16->childNum = nodePtr->childNum;
    newNode16->get_key_from_another(nodePtr);
    return_art_node(nodePtr);

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
