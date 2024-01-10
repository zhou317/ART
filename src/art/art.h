#pragma once

#include <atomic>
#include <cstdint>
#include <string>

#include "art/art-node-add.h"
#include "art/art-node-del.h"
#include "art/art-printer.h"

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

  T set(const std::string& k, T v) { return insertInt(k.data(), k.size(), v); }

  T set(const char* key, uint32_t len, T value) {
    return insertInt(key, len, value);
  }

  T get(const std::string& k) const { return findInt(k.data(), k.size()); }
  T get(const char* key, uint32_t len) const { return findInt(key, len); }

  T del(const std::string& k) const { return deleteInt(k.data(), k.size()); }
  T del(const char* key, uint32_t len) { return deleteInt(key, len); }

  // for debug
  ArtNodeCommon* get_root_unsafe() const { return root_; }

  uint64_t size() const { return size_; }

 private:
  T deleteInt(const char* key, uint32_t len) {
    ArtNodeCommon** parent_pp = nullptr;
    ArtNodeCommon** current_pp = &root_;
    uint32_t depth = 0;

    if (root_ == nullptr) return T{};

    while (current_pp) {
      auto current_p = *current_pp;
      if (current_p->type == ArtNodeType::ART_NODE_LEAF) {
        auto leaf = reinterpret_cast<ArtLeaf<T>*>(current_p);
        if (leaf->leaf_matches(key, len, depth)) {
          T v = leaf->value;
          detail::art_delete_from_node(parent_pp, current_pp, key, len, depth);
          size_--;
          detail::return_art_node(leaf);
          return v;
        } else {
          return T{};
        }
      }

      bool inner_match =
          detail::art_inner_prefix_match(current_p, key, len, depth);
      if (!inner_match) {
        return T{};
      }

      depth += current_p->keyLen;
      uint8_t child_key = depth < len ? key[depth] : 0;
      auto next = detail::art_find_child(current_p, child_key);
      depth++;
      parent_pp = current_pp;
      current_pp = next;
    }

    return T{};
  }

  T insertInt(const char* key, uint32_t len, T value) {
    ArtNodeCommon** current_pp = &root_;
    uint32_t depth = 0;

    if (root_ == nullptr) {
      root_ = detail::get_new_leaf_node<T>(key, len, value);
      size_++;
      return T{};
    }

    while (current_pp) {
      auto current_p = *current_pp;
      if (current_p->type == ArtNodeType::ART_NODE_LEAF) {
        auto leaf = reinterpret_cast<ArtLeaf<T>*>(current_p);
        if (leaf->leaf_matches(key, len, depth)) {
          auto OldV = leaf->value;
          leaf->value = value;
          return OldV;
        }

        auto newInner4 = detail::get_new_art_node<ArtNode4>();
        auto newLeaf = detail::get_new_leaf_node<T>(key, len, value);
        auto [prefixLen, c1, c2] =
            detail::get_prefix_len_and_diff_char(current_p, newLeaf, depth);
        newInner4->set_prefix_key(key + depth, prefixLen);
        newInner4->init_with_leaf(c1, leaf, c2, newLeaf);

        *current_pp = newInner4;
        size_++;
        return T{};
      }

      auto [p, c1, c2, new_prefix] =
          detail::art_check_inner_prefix(current_p, key, len, depth);
      if (p < current_p->keyLen) {  // new leaf at this node
        auto newInner4 = detail::get_new_art_node<ArtNode4>();
        auto newLeaf = detail::get_new_leaf_node<T>(key, len, value);
        newInner4->set_prefix_key(key + depth, p);

        current_p->reset_prefix(new_prefix, p + 1);

        newInner4->init_with_leaf(c1, current_p, c2, newLeaf);
        *current_pp = newInner4;
        size_++;
        return T{};
      }

      depth += p;
      uint8_t child_key = depth < len ? key[depth] : 0;
      auto next = detail::art_find_child(current_p, child_key);

      if (!next) {
        auto newLeaf = detail::get_new_leaf_node<T>(key, len, value);
        detail::art_add_child_to_node(current_pp, child_key, newLeaf);
        size_++;
        return T{};
      }

      depth++;
      current_pp = next;
    }

    LOG_ERROR("Should not go here");
    return T{};
  }

  T findInt(const char* key, uint32_t len) const {
    ArtNodeCommon** child;
    ArtNodeCommon* cur = root_;
    uint32_t depth = 0;

    while (cur) {
      if (cur->type == ArtNodeType::ART_NODE_LEAF) {
        auto leaf = reinterpret_cast<ArtLeaf<T>*>(cur);
        if (leaf->leaf_matches(key, len, depth)) {
          return reinterpret_cast<ArtLeaf<T>*>(cur)->value;
        }
        return T{};
      }

      bool inner_match = detail::art_inner_prefix_match(cur, key, len, depth);
      if (!inner_match) {
        return T{};
      } else {
        depth += cur->keyLen;
      }
      // search child
      uint8_t child_key = depth < len ? key[depth] : 0;
      child =
          detail::art_find_child(const_cast<ArtNodeCommon*>(cur), child_key);
      cur = (child) ? *child : nullptr;
      depth++;
    }

    return T{};
  }

 private:
  ArtNodeCommon* root_ = nullptr;
  std::atomic<uint64_t> size_ = 0;
};

}  // namespace art

template <class T>
inline std::ostream& operator<<(std::ostream& os, const art::ArtTree<T>& tree) {
  return os << art::art_node_to_string_unsafe(tree.get_root_unsafe());
}
