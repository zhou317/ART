#pragma once

#include <atomic>
#include <cstdint>
#include <string>

#include "art/art-node-add.h"
#include "art/art-node-del.h"
#include "art/art-printer.h"
#include "art/art-sync-macros.h"

namespace art {

template <class T>
class ArtTree {
 public:
  ArtTree() = default;

  ~ArtTree() {
    auto root = this->get_root_unsafe();
    if (likely(root)) {
      detail::destroy_node<T>(root);
      root = nullptr;
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
  ArtNodeCommon* get_root_unsafe() const { return meta_to_root_.children[0]; }

  uint64_t size() const { return size_; }

 private:
  T deleteInt(const char* key, uint32_t len) {
    uint64_t version_parent_parent = 0;
    uint64_t version_parent = 0;
    uint64_t version_current = 0;
    /*
     * When deleting a leaf, we may adjust parent to another node type. So three
     * write latch will be hold.
     *            |
     *    [parent_parent]
     *            \
     *          (parent)
     *              \
     *              leaf
     */

  label_delete_retry:
    ArtNodeCommon* parent_parent_p = nullptr;
    ArtNodeCommon** parent_pp = nullptr;
    ArtNodeCommon* parent_p = nullptr;
    ArtNodeCommon** current_pp = nullptr;
    ArtNodeCommon* current_p = nullptr;
    uint32_t depth = 0;

    ART_MACRO_READ_LOCK_OR_RESTART(&meta_to_root_, version_parent,
                                   label_delete_retry);
    current_pp = &(meta_to_root_.children[0]);
    current_p = *current_pp;
    parent_p = &meta_to_root_;
    ART_MACRO_READ_UNLOCK_OR_RESTART(&meta_to_root_, version_parent,
                                     label_delete_retry);

    if (current_p == nullptr) return T{};
    if (current_p->type == ArtNodeType::ART_NODE_LEAF) {
      auto leaf = reinterpret_cast<ArtLeaf<T>*>(current_p);
      T v = leaf->value;
      bool b_leaf_match = leaf->leaf_matches(key, len, depth);
      ART_MACRO_READ_UNLOCK_OR_RESTART(current_p, version_current,
                                       label_delete_retry);
      if (!b_leaf_match) {
        return T{};
      } else {
        ART_MACRO_UPGRADE_TO_WRITE_OR_RESTART(parent_p, version_parent,
                                              label_delete_retry);
        ART_MACRO_UPGRADE_TO_WRITE_OR_RESTART_AND_RELEASE(
            current_p, version_current, parent_p, label_delete_retry);

        detail::return_art_node(leaf);
        *current_pp = nullptr;

        ART_MACRO_WRITE_UNLOCK(parent_p);
        ART_MACRO_WRITE_UNLOCK_OBSOLETE(current_p);

        return v;
      }
    }

    while (current_pp) {
      ART_MACRO_READ_LOCK_OR_RESTART(current_p, version_current,
                                     label_delete_retry);
      if (current_p->type == ArtNodeType::ART_NODE_LEAF) {
        auto leaf = reinterpret_cast<ArtLeaf<T>*>(current_p);
        T v = leaf->value;
        bool b_leaf_match = leaf->leaf_matches(key, len, depth);
        ART_MACRO_READ_UNLOCK_OR_RESTART(current_p, version_current,
                                         label_delete_retry);

        if (!b_leaf_match) {
          return T{};
        } else {
          if (parent_p->need_adjust_after_delete()) {
            ART_MACRO_UPGRADE_TO_WRITE_OR_RESTART(
                parent_parent_p, version_parent_parent, label_delete_retry);
            ART_MACRO_UPGRADE_TO_WRITE_OR_RESTART_AND_RELEASE(
                parent_p, version_parent, parent_parent_p, label_delete_retry);
            ART_MACRO_UPGRADE_TO_WRITE_OR_RESTART_AND_RELEASE_TWO(
                current_p, version_current, parent_p, parent_parent_p,
                label_delete_retry);

            detail::art_delete_from_node(parent_pp, key[depth - 1]);

            ART_MACRO_WRITE_UNLOCK(parent_parent_p);
            ART_MACRO_WRITE_UNLOCK_OBSOLETE(parent_p);
            ART_MACRO_WRITE_UNLOCK_OBSOLETE(current_p);
          } else {
            ART_MACRO_UPGRADE_TO_WRITE_OR_RESTART(parent_p, version_parent,
                                                  label_delete_retry);
            ART_MACRO_UPGRADE_TO_WRITE_OR_RESTART_AND_RELEASE(
                current_p, version_current, parent_p, label_delete_retry);

            detail::art_delete_from_node(parent_pp, key[depth - 1]);

            ART_MACRO_WRITE_UNLOCK(parent_p);
            ART_MACRO_WRITE_UNLOCK_OBSOLETE(current_p);
          }
          size_--;
          detail::return_art_node(leaf);
          return v;
        }
      }

      bool inner_match =
          detail::art_inner_prefix_match(current_p, key, len, depth);
      if (!inner_match) {
        ART_MACRO_READ_UNLOCK_OR_RESTART(current_p, version_current,
                                         label_delete_retry);
        return T{};
      }

      depth += current_p->keyLen;
      uint8_t child_key = depth < len ? key[depth] : 0;
      auto child = detail::art_find_child(current_p, child_key);
      auto child_ptr_tmp = (child) ? *child : nullptr;
      ART_MACRO_READ_UNLOCK_OR_RESTART(current_p, version_current,
                                       label_delete_retry);
      depth++;

      version_parent_parent = version_parent;
      version_parent = version_current;

      parent_parent_p = parent_p;

      parent_pp = current_pp;
      parent_p = current_p;

      current_pp = child;
      current_p = child_ptr_tmp;
    }

    return T{};
  }

  T insertInt(const char* key, uint32_t len, T value) {
    uint64_t version_parent = 0;
    uint64_t version_current = 0;

  label_insert_retry:
    ArtNodeCommon** current_pp = nullptr;
    ArtNodeCommon* parent_p = nullptr;
    ArtNodeCommon* current_p = nullptr;
    uint32_t depth = 0;

    ART_MACRO_READ_LOCK_OR_RESTART(&meta_to_root_, version_parent,
                                   label_insert_retry);
    current_pp = &(meta_to_root_.children[0]);
    current_p = *current_pp;
    parent_p = &meta_to_root_;
    ART_MACRO_READ_UNLOCK_OR_RESTART(&meta_to_root_, version_parent,
                                     label_insert_retry);

    // empty
    if (current_p == nullptr) {
      ART_MACRO_UPGRADE_TO_WRITE_OR_RESTART(&meta_to_root_, version_parent,
                                            label_insert_retry);
      *current_pp = detail::get_new_leaf_node<T>(key, len, value);
      size_++;
      ART_MACRO_WRITE_UNLOCK(&meta_to_root_);
      return T{};
    }

    while (current_pp) {
      ART_MACRO_READ_LOCK_OR_RESTART(current_p, version_current,
                                     label_insert_retry);
      if (current_p->type == ArtNodeType::ART_NODE_LEAF) {
        auto leaf = reinterpret_cast<ArtLeaf<T>*>(current_p);
        if (leaf->leaf_matches(key, len, depth)) {
          ART_MACRO_UPGRADE_TO_WRITE_OR_RESTART(current_p, version_current,
                                                label_insert_retry);
          auto OldV = leaf->value;
          leaf->value = value;
          ART_MACRO_WRITE_UNLOCK(current_p);
          return OldV;
        }

        // since we will change parent's pointer to a new ArtNode4
        ART_MACRO_UPGRADE_TO_WRITE_OR_RESTART(parent_p, version_parent,
                                              label_insert_retry);
        ART_MACRO_UPGRADE_TO_WRITE_OR_RESTART_AND_RELEASE(
            current_p, version_current, parent_p, label_insert_retry);
        auto newInner4 = detail::get_new_art_node<ArtNode4>();
        auto newLeaf = detail::get_new_leaf_node<T>(key, len, value);
        auto [prefixLen, c1, c2] =
            detail::get_prefix_len_and_diff_char(current_p, newLeaf, depth);
        newInner4->set_prefix_key(key + depth, prefixLen);
        newInner4->init_with_leaf(c1, leaf, c2, newLeaf);

        *current_pp = newInner4;
        size_++;

        ART_MACRO_WRITE_UNLOCK(parent_p);
        ART_MACRO_WRITE_UNLOCK(current_p);

        return T{};
      }

      int32_t p = 0;
      uint8_t c1 = 0;
      uint8_t c2 = 0;
      ArtNodeKey new_prefix = {.keyPtr = nullptr};
      auto max_cmp = std::min(current_p->keyLen, len - depth);
      if (current_p->keyLen <= ART_MAX_PREFIX_LEN) {
        auto prefix = current_p->get_key();
        for (; p < max_cmp; p++) {
          if (prefix[p] != key[depth + p]) {
            c1 = prefix[p];
            c2 = key[depth + p];
            new_prefix = detail::funGetNewPrefix(prefix, (uint32_t)0,
                                                 current_p->keyLen, p);
            break;
          }
        }

        if (len - depth < current_p->keyLen) {
          c1 = prefix[p];
          new_prefix = detail::funGetNewPrefix(prefix, (uint32_t)0,
                                               current_p->keyLen, p);
        }
      } else {
        // We need get a leaf for entire prefix.
        // All child have same prefix, wo get leftmost.
        uint64_t find_leaf_v;
        auto l = current_p;
        while (l->type != ArtNodeType::ART_NODE_LEAF) {
          ART_MACRO_READ_LOCK_OR_RESTART(l, find_leaf_v, label_insert_retry);
          ArtNodeCommon* find_leaf_tmp = nullptr;
          switch (l->type) {
            case ArtNodeType::ART_NODE_4: {
              find_leaf_tmp = reinterpret_cast<const ArtNode4*>(l)->children[0];
            } break;
            case ArtNodeType::ART_NODE_16: {
              find_leaf_tmp =
                  reinterpret_cast<const ArtNode16*>(l)->children[0];
            } break;
            case ArtNodeType::ART_NODE_48: {
              int32_t idx = 0;
              auto tmp = reinterpret_cast<const ArtNode48*>(l);
              while (!tmp->index[idx]) idx++;
              idx = tmp->index[idx] - 1;
              find_leaf_tmp = tmp->children[idx];
            } break;
            case ArtNodeType::ART_NODE_256: {
              int32_t idx = 0;
              auto tmp = reinterpret_cast<const ArtNode256*>(l);
              while (!tmp->children[idx]) idx++;
              find_leaf_tmp = tmp->children[idx];
            } break;
          }
          // to make sure pointer is valid
          ART_MACRO_READ_UNLOCK_OR_RESTART(l, find_leaf_v, label_insert_retry);
          l = find_leaf_tmp;
        }

        for (; p < max_cmp; p++) {
          if (l->get_key()[depth + p] != key[depth + p]) {
            c1 = l->get_key()[depth + p];
            c2 = key[depth + p];
            new_prefix = detail::funGetNewPrefix(l->get_key(), depth,
                                                 current_p->keyLen, p);
            break;
          }
        }

        if (len - depth < current_p->keyLen) {
          c1 = l->get_key()[depth + p];
          new_prefix = detail::funGetNewPrefix(l->get_key(), depth,
                                               current_p->keyLen, p);
        }
      }

      if (p < current_p->keyLen) {  // new leaf at this node
        auto newInner4 = detail::get_new_art_node<ArtNode4>();
        auto newLeaf = detail::get_new_leaf_node<T>(key, len, value);
        newInner4->set_prefix_key(key + depth, p);

        ART_MACRO_UPGRADE_TO_WRITE_OR_RESTART(parent_p, version_parent,
                                              label_insert_retry);
        ART_MACRO_UPGRADE_TO_WRITE_OR_RESTART_AND_RELEASE(
            current_p, version_current, parent_p, label_insert_retry);

        current_p->reset_prefix(new_prefix, p + 1);
        newInner4->init_with_leaf(c1, current_p, c2, newLeaf);
        *current_pp = newInner4;
        size_++;

        ART_MACRO_WRITE_UNLOCK(parent_p);
        ART_MACRO_WRITE_UNLOCK(current_p);

        return T{};
      }

      depth += p;
      uint8_t child_key = depth < len ? key[depth] : 0;
      auto next = detail::art_find_child(current_p, child_key);
      auto child_ptr_tmp = next ? *next : nullptr;
      ART_MACRO_READ_LOCK_OR_RESTART(current_p, version_current,
                                     label_insert_retry);

      if (child_ptr_tmp == nullptr) {
        auto newLeaf = detail::get_new_leaf_node<T>(key, len, value);

        if (current_p->is_full()) {
          ART_MACRO_UPGRADE_TO_WRITE_OR_RESTART(parent_p, version_parent,
                                                label_insert_retry);
          ART_MACRO_UPGRADE_TO_WRITE_OR_RESTART_AND_RELEASE(
              current_p, version_current, parent_p, label_insert_retry);
          detail::art_add_child_to_node(current_pp, child_key, newLeaf);

          ART_MACRO_WRITE_UNLOCK(parent_p);
          ART_MACRO_WRITE_UNLOCK_OBSOLETE(current_p);
        } else {
          ART_MACRO_UPGRADE_TO_WRITE_OR_RESTART_AND_RELEASE(
              current_p, version_current, parent_p, label_insert_retry);
          detail::art_add_child_to_node(current_pp, child_key, newLeaf);
          ART_MACRO_WRITE_UNLOCK(current_p);
        }
        size_++;
        return T{};
      } else {
        version_parent = version_current;
        parent_p = current_p;
        current_pp = next;
        current_p = child_ptr_tmp;
      }

      depth++;
    }

    LOG_ERROR("Should not go here");
    return T{};
  }

  T findInt(const char* key, uint32_t len) const {
    uint64_t version = 0;

  label_find_retry:
    ART_MACRO_READ_LOCK_OR_RESTART(&meta_to_root_, version, label_find_retry);
    auto root_ptr = this->get_root_unsafe();
    ART_MACRO_READ_UNLOCK_OR_RESTART(&meta_to_root_, version, label_find_retry);

    ArtNodeCommon** child;
    const ArtNodeCommon* cur = root_ptr;
    uint32_t depth = 0;

    while (cur) {
      ART_MACRO_READ_LOCK_OR_RESTART(cur, version, label_find_retry);
      if (cur->type == ArtNodeType::ART_NODE_LEAF) {
        auto leaf = reinterpret_cast<const ArtLeaf<T>*>(cur);
        auto ret = leaf->value;
        bool b_leaf_match = leaf->leaf_matches(key, len, depth);
        ART_MACRO_READ_UNLOCK_OR_RESTART(cur, version, label_find_retry);

        if (b_leaf_match) {
          return ret;
        } else {
          return T{};
        }
      }

      bool inner_match = detail::art_inner_prefix_match(cur, key, len, depth);
      if (!inner_match) {
        ART_MACRO_READ_UNLOCK_OR_RESTART(cur, version, label_find_retry);
        return T{};
      }

      // search child
      depth += cur->keyLen;
      uint8_t child_key = depth < len ? key[depth] : 0;
      child =
          detail::art_find_child(const_cast<ArtNodeCommon*>(cur), child_key);
      auto child_ptr_tmp = (child) ? *child : nullptr;
      ART_MACRO_READ_UNLOCK_OR_RESTART(cur, version, label_find_retry);
      cur = child_ptr_tmp;
      depth++;
    }

    return T{};
  }

 private:
  ArtNode4 meta_to_root_;
  std::atomic<uint64_t> size_ = 0;
};

}  // namespace art

template <class T>
inline std::ostream& operator<<(std::ostream& os, const art::ArtTree<T>& tree) {
  return os << art::art_node_to_string_unsafe(tree.get_root_unsafe());
}
