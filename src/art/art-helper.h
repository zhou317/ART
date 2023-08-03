#pragma once

#include <string>

#include "art/art-node.h"

namespace ArtSingle {

static void ArtNodeToString(ArtNodeCommon *node, std::string &out,
                            uint32_t depth) {
  switch (node->type) {
    case ART_NODE_4: {
      std::string nodePre;
      if (node->len.innerPrefixLen)
        nodePre = std::string(node->key.innerPrefix, node->len.innerPrefixLen);
      auto node4 = reinterpret_cast<ArtNode4 *>(node);
      out += std::string(depth, '-') + "node4, " + nodePre + " child cnt" +
             std::to_string(node4->childNum) + "\n";

      for (uint8_t i = 0; i < node4->childNum; i++) {
        ArtNodeToString(node4->children[i], out,
                        depth + node->len.innerPrefixLen + 1);
      }
    } break;
    case ART_NODE_16: {
      std::string nodePre;
      if (node->len.innerPrefixLen)
        nodePre = std::string(node->key.innerPrefix, node->len.innerPrefixLen);
      auto node16 = reinterpret_cast<ArtNode16 *>(node);
      out += std::string(depth, '-') + "node16, " + nodePre + " child cnt" +
             std::to_string(node16->childNum) + "\n";

      for (uint8_t i = 0; i < node16->childNum; i++) {
        ArtNodeToString(node16->children[i], out,
                        depth + node->len.innerPrefixLen + 1);
      }
    } break;
    case ART_NODE_48: {
      std::string nodePre;
      if (node->len.innerPrefixLen)
        nodePre = std::string(node->key.innerPrefix, node->len.innerPrefixLen);
      auto node48 = reinterpret_cast<ArtNode48 *>(node);
      out += std::string(depth, '-') + "node48, " + nodePre + " child cnt" +
             std::to_string(node48->childNum) + "\n";

      for (uint16_t i = 0; i <= 255; i++) {
        if (node48->index[i]) {
          ArtNodeToString(node48->children[node48->index[i] - 1], out,
                          depth + node->len.innerPrefixLen + 1);
        }
      }
    } break;
    case ART_NODE_256: {
      std::string nodePre;
      if (node->len.innerPrefixLen)
        nodePre = std::string(node->key.innerPrefix, node->len.innerPrefixLen);
      auto node256 = reinterpret_cast<ArtNode256 *>(node);
      out += std::string(depth, '-') + "node256, " + nodePre + " child cnt" +
             std::to_string(node256->childNum) + "\n";

      for (uint16_t i = 0; i <= 255; i++) {
        if (node256->children[i]) {
          ArtNodeToString(node256->children[i], out,
                          depth + node->len.innerPrefixLen + 1);
        }
      }
    } break;
    case ART_NODE_LEAF: {
      auto leaf = reinterpret_cast<ArtLeaf<int64_t> *>(node);
      out += std::string(depth, '-') +
             std::string(node->key.leafKeyPtr, node->len.leafKeyLen) + "\n";
    } break;
    default: {
      assert(false);
    } break;
  }
}

static std::string ArtNodeToString(ArtNodeCommon *node) {
  std::string ret;
  ArtNodeToString(node, ret, 0);
  return ret;
}

template <class T>
static std::string ArtTreeToString(T *tree) {
  ArtNodeCommon *node = tree->getRoot();
  std::string ret;
  ArtNodeToString(node, ret, 0);
  return ret;
}

}  // namespace ArtSingle
