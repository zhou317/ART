#pragma once

#include <cstdint>
#include <string>

#include "art/art-node-add.h"
#include "art/art-node-del.h"

namespace art {

template <class T>
class ArtTree {
 public:
  ArtTree() = default;

  ~ArtTree() {
    if (likely(root_)) {
      detail::destroy_node<T>(root_);
      root_ = nullptr;
    }
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
  T deleteInt(ArtNodeCommon** parNode, ArtNodeCommon** node, const char* key,
              uint32_t len, uint32_t depth) {
    if (unlikely(*node == nullptr)) {  // empty root
      return T{};
    }

    auto nodePtr = *node;
    if (nodePtr->type == ArtNodeType::ART_NODE_LEAF) {
      auto leaf = reinterpret_cast<ArtLeaf<T>*>(nodePtr);
      if (leaf->leaf_matches(key, len, depth)) {
        T v = leaf->value;
        detail::art_delete_from_node(parNode, node, key, len, depth);
        size_--;
        return_new_node(leaf);
        return v;
      } else {
        return T{};
      }
    }

    auto [p, c1, c2] = detail::art_check_inner_prefix(nodePtr, key, len, depth);
    if (p != nodePtr->keyLen) {
      return T{};
    }

    depth += p;
    uint8_t child_key = key[depth];
    auto next = detail::art_find_child(nodePtr, child_key);
    if (next) {
      return deleteInt(node, next, key, len, depth + 1);
    } else {
      return T{};
    }
  }

  T insertInt(ArtNodeCommon** node, const char* key, uint32_t len, T value,
              uint32_t depth) {
    if (unlikely(*node == nullptr)) {
      *node = detail::get_new_leaf_node<T>(key, len, value);
      size_++;
      return T{};
    }

    auto nodePtr = *node;
    if (nodePtr->type == ArtNodeType::ART_NODE_LEAF) {
      auto leaf = reinterpret_cast<ArtLeaf<T>*>(nodePtr);
      if (leaf->leaf_matches(key, len, depth)) {
        auto OldV = leaf->value;
        leaf->value = value;
        return OldV;
      }

      auto newInner4 = detail::get_new_node<ArtNode4>();
      auto newLeaf = detail::get_new_leaf_node<T>(key, len, value);
      auto [prefixLen, c1, c2] =
          detail::get_prefix_len_and_diff_char(nodePtr, newLeaf, depth);
      newInner4->set_key(nodePtr->get_key_in_depth(depth), prefixLen);
      newInner4->init_with_leaf(c1, leaf, c2, newLeaf);

      *node = newInner4;
      size_++;
      return T{};
    }

    auto [p, c1, c2] = detail::art_check_inner_prefix(nodePtr, key, len, depth);
    if (p != nodePtr->keyLen) {  // new leaf at this node
      auto newInner4 = detail::get_new_node<ArtNode4>();
      auto newLeaf = detail::get_new_leaf_node<T>(key, len, value);
      if (p) newInner4->set_key(key + depth, p);

      nodePtr->remove_prefix(p + 1);
      newInner4->init_with_leaf(c1, nodePtr, c2, newLeaf);
      *node = newInner4;
      size_++;
      return T{};
    }

    depth += p;
    uint8_t child_key = key[depth];
    auto next = detail::art_find_child(nodePtr, child_key);
    if (next) {
      return insertInt(next, key, len, value, depth + 1);
    } else {
      auto newLeaf = detail::get_new_leaf_node<T>(key, len, value);
      detail::art_add_child_to_node(node, child_key, newLeaf);
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
      auto leaf = reinterpret_cast<const ArtLeaf<T>*>(node);
      if (leaf->leaf_matches(key, len, depth)) {
        return node;
      } else {
        return nullptr;
      }
    }

    auto [p, c1, c2] = detail::art_check_inner_prefix(node, key, len, depth);
    if (p != node->keyLen) {
      return nullptr;
    }

    depth += p;
    auto next =
        detail::art_find_child(const_cast<ArtNodeCommon*>(node), key[depth]);
    if (!next) return nullptr;
    return findInt(*next, key, len, depth + 1);
  }

 private:
  ArtNodeCommon* root_ = nullptr;
  uint64_t size_ = 0;
};

}  // namespace art
