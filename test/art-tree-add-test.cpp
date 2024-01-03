#include <random>
#include <unordered_map>

#include "art/art-printer.h"
#include "art/art.h"
#include "common/logger.h"
#include "gtest/gtest.h"

namespace art {

class ArtTreeAddTest : public ::testing::Test {
 public:
  void TearDown() override {
    for (auto& iter : verify_map) {
      EXPECT_EQ(tree.get(iter.first.c_str(), iter.first.size()), iter.second);
    }
  }

  int64_t set(const std::string& key, int64_t val) {
    verify_map[key] = val;
    return tree.set(key.c_str(), key.size(), val);
  }

  std::unordered_map<std::string, int64_t> verify_map;
  ArtTree<int64_t> tree;
};

/*
 *            (dd)
 *        a /     \ b
 *         /       \
 *       (aaa)      "ddbddb"
 *   a /      \ b
 *    /        \
 * "ddaaaaac"   "ddaaaabcd"
 */

/*
 *               (dd)
 *           a /     \ b
 *            /       \
 *           (a)      "ddbddb"
 *        a/   \ e
 *        /     \
 *       (a)    "ddaaeabcd"
 *  a /       \ b
 *   /         \
 * "ddaaaaac" "ddaaaabcd"
 */
TEST_F(ArtTreeAddTest, diff_with_inner) {
  set("ddaaaaac", 1);
  set("ddbddb", 2);
  set("ddaaaabcd", 3);
  LOG_INFO("\n%s", art_node_to_string(tree.getRoot()).c_str());

  set("ddaaeabcd", 4);
  LOG_INFO("\n%s", art_node_to_string(tree.getRoot()).c_str());

  auto node = tree.getRoot();
  EXPECT_EQ(node->type, ArtNodeType::ART_NODE_4);
  auto node4 = reinterpret_cast<ArtNode4*>(node);
  EXPECT_EQ(node4->keys[0], 'a');
  auto inner_l1_a = node4->children[0];
  EXPECT_EQ(inner_l1_a->keyLen, 1);
  EXPECT_EQ(inner_l1_a->to_string(), "a");

  auto inner_l2_a = reinterpret_cast<ArtNode4*>(inner_l1_a)->children[0];
  EXPECT_EQ(inner_l1_a->keyLen, 1);
  EXPECT_EQ(inner_l1_a->to_string(), "a");
}

/*
 *               (dd)
 *          a /        \ b
 *          /           \
 *       (aaa)           (b)
 *   a /      \ b    d /      \ e
 *    /        \     "ddbddb"  "ddbdee"
 * "ddaaaaac"   "ddaaaabcd"
 */
TEST_F(ArtTreeAddTest, diff_with_leaf) {
  set("ddaaaaac", 1);
  set("ddbddb", 2);
  set("ddaaaabcd", 3);
  LOG_INFO("\n%s", art_node_to_string(tree.getRoot()).c_str());

  set("ddbdee", 4);
  LOG_INFO("\n%s", art_node_to_string(tree.getRoot()).c_str());

  auto node = tree.getRoot();
  EXPECT_EQ(node->type, ArtNodeType::ART_NODE_4);
  auto node4 = reinterpret_cast<ArtNode4*>(node);
  EXPECT_EQ(node4->keys[1], 'b');
  auto inner_l1_b = node4->children[1];
  EXPECT_EQ(inner_l1_b->keyLen, 1);
  EXPECT_EQ(inner_l1_b->to_string(), "d");

  auto inner_l2_e = reinterpret_cast<ArtNode4*>(inner_l1_b)->children[1];
  EXPECT_EQ(inner_l2_e->type, ArtNodeType::ART_NODE_LEAF);
  EXPECT_EQ(inner_l2_e->keyLen, 6);
  EXPECT_EQ(inner_l2_e->to_string(), "ddbdee");
}

/*
 *                      (dd)
 *                a /          \ b
 *                /             \
 *           (aaa)              "ddbddb"
 *   a /       | b       \ g
 *    /        |          \
 * "ddaaaaac" "ddaaaabcd"  "ddaaaagxxx"
 */
TEST_F(ArtTreeAddTest, diff_with_none) {
  set("ddaaaaac", 1);
  set("ddbddb", 2);
  set("ddaaaabcd", 3);
  LOG_INFO("\n%s", art_node_to_string(tree.getRoot()).c_str());

  set("ddaaaagxxx", 4);
  LOG_INFO("\n%s", art_node_to_string(tree.getRoot()).c_str());

  auto node = tree.getRoot();
  EXPECT_EQ(node->type, ArtNodeType::ART_NODE_4);
  auto node4 = reinterpret_cast<ArtNode4*>(node);
  EXPECT_EQ(node4->keys[0], 'a');
  auto inner_l1_a = node4->children[0];
  EXPECT_EQ(inner_l1_a->keyLen, 3);
  EXPECT_EQ(inner_l1_a->to_string(), "aaa");

  EXPECT_EQ(reinterpret_cast<ArtNode4*>(inner_l1_a)->keys[2], 'g');
  auto inner_l2_g = reinterpret_cast<ArtNode4*>(inner_l1_a)->children[2];
  EXPECT_EQ(inner_l2_g->type, ArtNodeType::ART_NODE_LEAF);
  EXPECT_EQ(inner_l2_g->keyLen, 10);
  EXPECT_EQ(inner_l2_g->to_string(), "ddaaaagxxx");
}

TEST_F(ArtTreeAddTest, same_value_replace) {
  EXPECT_EQ(0, set("same key", 1));
  EXPECT_EQ(1, set("same key", 2));
  EXPECT_EQ(2, set("same key", 3));
  EXPECT_EQ(3, set("same key", 4));

  EXPECT_EQ(1, tree.size());
}

TEST_F(ArtTreeAddTest, random_test) {
  int64_t seed = time(nullptr);
  LOG_INFO("Seed is %lld", seed);
  // error when seed = 1704274660, 1704274885
  std::mt19937_64 rng(seed);
  for (int32_t i = 0; i < 10000; i++) {
    int64_t val = rng();
    int64_t cnt = rng() % 4 + 1;
    std::string key;
    for (int32_t j = 0; j < cnt; j++) {
      key += std::to_string(rng());
    }
    set(key, val);
  }
  LOG_INFO("size %llu.", tree.size());
}

}  // namespace art
