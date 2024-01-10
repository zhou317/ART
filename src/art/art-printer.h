#pragma once

#include <sstream>
#include <string>

#include "art/art-node-pool.h"
#include "art/art-node.h"

namespace art {
namespace detail {

static void art_node_to_string(std::string &out, const ArtNodeCommon *node,
                               uint32_t depth, uint8_t key, bool recusive) {
  if (!node && depth == 0) {
    out += "Empty";
    return;
  }

  if (depth > 0) {
    out += std::string(depth - 1, '-') + "key char:";
    if (std::isdigit(key) || std::isalpha(key)) {
      out += static_cast<char>(key);
      out += ", ";
    } else {
      out += std::to_string(key) + ", ";
    }

    if (!recusive) return;
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
                           node4->keys[i], recusive);
      }
    } break;
    case ART_NODE_16: {
      auto node16 = reinterpret_cast<const ArtNode16 *>(node);
      for (int32_t i = 0; i < node16->childNum; i++) {
        art_node_to_string(out, node16->children[i], depth + node->keyLen + 1,
                           node16->keys[i], recusive);
      }
    } break;
    case ART_NODE_48: {
      auto node48 = reinterpret_cast<const ArtNode48 *>(node);
      for (uint16_t i = 0; i <= 255; i++) {
        if (node48->index[i]) {
          art_node_to_string(out, node48->children[node48->index[i] - 1],
                             depth + node->keyLen + 1, node48->index[i],
                             recusive);
        }
      }
    } break;
    case ART_NODE_256: {
      auto node256 = reinterpret_cast<const ArtNode256 *>(node);
      for (uint16_t i = 0; i <= 255; i++) {
        if (node256->children[i]) {
          art_node_to_string(out, node256->children[i],
                             depth + node->keyLen + 1, i, recusive);
        }
      }
    } break;
    default: {
    } break;
  }
}

}  // namespace detail

static std::string art_node_to_string_unsafe(const ArtNodeCommon *node,
                                             bool recursive = true) {
  std::string ret;
  detail::art_node_to_string(ret, node, 0, 0, recursive);
  return ret;
}

inline std::ostream &operator<<(std::ostream &os, const ArtNodeCommon *node) {
  return os << art_node_to_string_unsafe(node);
}

inline std::ostream &operator<<(std::ostream &os, const ArtNodeCommon &node) {
  return os << art_node_to_string_unsafe(&node);
}

template <class T>
std::string get_pool_usage() {
  std::stringstream os;

  os << "Node4:\n"
     << describe_objects<ArtNode4>() << "\nNode16:\n"
     << describe_objects<ArtNode16>() << "\nNode48:\n"
     << describe_objects<ArtNode48>() << "\nNode256:\n"
     << describe_objects<ArtNode256>() << "\nLeaf:\n"
     << describe_objects<ArtLeaf<T>>() << "\n";

  return os.str();
}

}  // namespace art
