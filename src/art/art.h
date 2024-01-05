#pragma once

#include <csetjmp>
#include <cstdint>
#include <string>

#include "art/art-node-add.h"
#include "art/art-node-del.h"

namespace art {

#define ART_SET_RESTART_POINT setjmp(ArtTree::restart_jmp_buf_)
#define ART_RESTART longjmp(ArtTree::restart_jmp_buf_, 1)

template <class T>
class ArtTree {
 public:
  ArtTree() = default;

  ~ArtTree() {
    if (likely(meta_to_root_.children[0])) {
      detail::destroy_node<T>(meta_to_root_.children[0]);
      meta_to_root_.children[0] = nullptr;
    }
  }

  T set(const char* key, uint32_t len, T value) {
    return insertInt(&root_, key, len, value, 0);
  }

  T get(const char* key, uint32_t len) const {
    ART_SET_RESTART_POINT;

    uint64_t save_v = art_read_lock_or_restart(&meta_to_root_);
    auto root_ptr = meta_to_root_.children[0];
    art_read_unlock_or_restart(&meta_to_root_, save_v);
    auto ret = findInt(root_ptr, key, len, 0);
    return ret;
  }

  T del(const char* key, uint32_t len) {
    return deleteInt(nullptr, &root_, key, len, 0);
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
        detail::return_art_node(leaf);
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

      auto newInner4 = detail::get_new_art_node<ArtNode4>();
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
      auto newInner4 = detail::get_new_art_node<ArtNode4>();
      auto newLeaf = detail::get_new_leaf_node<T>(key, len, value);
      if (p) newInner4->set_key(key + depth, p);

      nodePtr->remove_prefix(p + 1);
      newInner4->init_with_leaf(c1, nodePtr, c2, newLeaf);
      *node = newInner4;
      size_++;
      return T{};
    }

    depth += p;
    uint8_t child_key = depth < len ? key[depth] : 0;
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

  T findInt(const ArtNodeCommon* node, const char* key, uint32_t len,
            uint32_t depth) const {
    if (unlikely(node == nullptr)) {
      return T{};
    }

    auto save_v = art_read_lock_or_restart(node);

    if (node->type == ArtNodeType::ART_NODE_LEAF) {
      // no one will modify leaf
      auto leaf = reinterpret_cast<const ArtLeaf<T>*>(node);
      if (leaf->leaf_matches(key, len, depth)) {
        return node;
      } else {
        return nullptr;
      }
    }

    auto [p, c1, c2] = detail::art_check_inner_prefix(node, key, len, depth);
    if (p != node->keyLen) {
      art_read_unlock_or_restart(node, save_v);
      return nullptr;
    }

    depth += p;
    auto next =
        detail::art_find_child(const_cast<ArtNodeCommon*>(node), key[depth]);
    art_read_unlock_or_restart(node, save_v);

    if (!next) return nullptr;
    return findInt(*next, key, len, depth + 1);
  }

 private:
#define ART_SET_LOCK_BIT(v) ((v) + 2)
#define ART_IS_OBSOLETE(v) ((v)&1)
#define ART_IS_LOCKED(v) ((v)&2)

  static uint64_t _art_spin_on_locked_node(const ArtNodeCommon* node) {
    uint64_t version = node->version.load();
    while (ART_IS_LOCKED(version)) {
      __builtin_ia32_pause();
      version = node->version.load();
    }
    return version;
  }

  static uint64_t art_read_lock_or_restart(const ArtNodeCommon* node) {
    uint64_t version = _art_spin_on_locked_node(node);
    if (ART_IS_OBSOLETE(version)) ART_RESTART;  // node may be logically delete
    return version;
  }

  static void art_read_unlock_or_restart(const ArtNodeCommon* node,
                                         uint64_t oldV) {
    if (oldV != node->version.load()) ART_RESTART;
  }

  static void art_upgrade_to_write_or_restart(ArtNodeCommon* node,
                                              uint64_t oldV) {
    if (!node->version.compare_exchange_strong(oldV, ART_SET_LOCK_BIT(oldV)))
      ART_RESTART;
  }

  static void art_write_unlock(ArtNodeCommon* node) {
    node->version.fetch_add(2);
  }

  static void art_write_unlock_obsolete(ArtNodeCommon* node) {
    node->version.fetch_add(3);
  }

  static void art_upgrade_to_write_or_restart_and_release(
      ArtNodeCommon* node, uint64_t oldV, ArtNodeCommon* lockNode) {
    if (!node->version.compare_exchange_strong(oldV, ART_SET_LOCK_BIT(oldV))) {
      art_write_unlock(lockNode);
      ART_RESTART;
    }
  }

 private:
  static thread_local jmp_buf restart_jmp_buf_;
  ArtNode4 meta_to_root_;
  uint64_t size_ = 0;
};

}  // namespace art
