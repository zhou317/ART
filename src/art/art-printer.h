#pragma once

#include <string>

#include "art/art-node.h"

namespace art {
namespace detail {

static void art_node_to_string(std::string &out, const ArtNodeCommon *node,
                               uint32_t depth, uint8_t key) {
  if (depth > 0) {
    out += std::string(depth - 1, '-') + "key char:";
    if (std::isdigit(key) || std::isalpha(key)) {
      out += static_cast<char>(key);
      out += ", ";
    } else {
      out += std::to_string(key) + ", ";
    }
  }

  out += "type:" + node_type_string(node);
  if (node->childNum) {
    out += ", prefix:" + node->to_string() +
           ", ccnt:" + std::to_string(node->childNum) + "\n";
  } else {
    out += ", key:" + node->to_string() + "\n";
  }

  switch (node->type) {
    case ART_NODE_4: {
      auto node4 = reinterpret_cast<const ArtNode4 *>(node);
      for (int32_t i = 0; i < node->childNum; i++) {
        art_node_to_string(out, node4->children[i], depth + node->keyLen + 1,
                           node4->keys[i]);
      }
    } break;
    case ART_NODE_16: {
      auto node16 = reinterpret_cast<const ArtNode16 *>(node);
      for (int32_t i = 0; i < node16->childNum; i++) {
        art_node_to_string(out, node16->children[i], depth + node->keyLen + 1,
                           node16->keys[i]);
      }
    } break;
    case ART_NODE_48: {
      auto node48 = reinterpret_cast<const ArtNode48 *>(node);
      for (uint16_t i = 0; i <= 255; i++) {
        if (node48->index[i]) {
          art_node_to_string(out, node48->children[node48->index[i] - 1],
                             depth + node->keyLen + 1, node48->index[i]);
        }
      }
    } break;
    case ART_NODE_256: {
      auto node256 = reinterpret_cast<const ArtNode256 *>(node);
      for (uint16_t i = 0; i <= 255; i++) {
        if (node256->children[i]) {
          art_node_to_string(out, node256->children[i],
                             depth + node->keyLen + 1, i);
        }
      }
    } break;
    default: {
    } break;
  }
}

}  // namespace detail

static std::string art_node_to_string(const ArtNodeCommon *node) {
  std::string ret;
  detail::art_node_to_string(ret, node, 0, 0);
  return ret;
}

inline std::ostream &operator<<(std::ostream &os, const ArtNodeCommon *node) {
  return os << art_node_to_string(node);
}

inline std::ostream &operator<<(std::ostream &os, const ArtNodeCommon &node) {
  return os << art_node_to_string(&node);
}

}  // namespace art
