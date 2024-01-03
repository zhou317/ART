#include "art/art-node-add.h"

#include <map>
#include <random>

#include "art/art-printer.h"
#include "common/logger.h"
#include "gtest/gtest.h"

namespace art {
namespace detail {

class ArtNodeAddTest : public ::testing::Test {};

template <class T>
void verify_node(ArtNodeCommon*,
                 const std::map<uint64_t, ArtLeaf<int64_t>*>& mp);

template <>
void verify_node<ArtNode4>(ArtNodeCommon* node,
                           const std::map<uint64_t, ArtLeaf<int64_t>*>& mp) {
  EXPECT_EQ(node->type, ArtNodeTrait<ArtNode4>::NODE_TYPE);
  auto node4 = reinterpret_cast<ArtNode4*>(node);
  int32_t idx = 0;
  for (auto& iter : mp) {
    EXPECT_EQ(iter.first, node4->keys[idx]);
    EXPECT_EQ(iter.second, node4->children[idx++]);
  }
}

template <>
void verify_node<ArtNode16>(ArtNodeCommon* node,
                            const std::map<uint64_t, ArtLeaf<int64_t>*>& mp) {
  EXPECT_EQ(node->type, ArtNodeTrait<ArtNode16>::NODE_TYPE);
  auto node16 = reinterpret_cast<ArtNode16*>(node);
  int32_t idx = 0;
  for (auto& iter : mp) {
    EXPECT_EQ(iter.first, node16->keys[idx]);
    EXPECT_EQ(iter.second, node16->children[idx++]);
  }
}

template <>
void verify_node<ArtNode48>(ArtNodeCommon* node,
                            const std::map<uint64_t, ArtLeaf<int64_t>*>& mp) {
  EXPECT_EQ(node->type, ArtNodeTrait<ArtNode48>::NODE_TYPE);
  auto node48 = reinterpret_cast<ArtNode48*>(node);
  for (auto& iter : mp) {
    EXPECT_NE(node48->index[iter.first], 0);
    EXPECT_EQ(node48->children[node48->index[iter.first] - 1], iter.second);
  }
}

template <>
void verify_node<ArtNode256>(ArtNodeCommon* node,
                             const std::map<uint64_t, ArtLeaf<int64_t>*>& mp) {
  EXPECT_EQ(node->type, ArtNodeTrait<ArtNode256>::NODE_TYPE);
  auto node256 = reinterpret_cast<ArtNode256*>(node);
  for (auto& iter : mp) {
    EXPECT_EQ(node256->children[iter.first], iter.second);
  }
}

template <class BeforeT, class OverFlowT>
void test_fun() {
  std::vector<char> v;
  for (int32_t i = 0; i <= 255; i++) {
    if (isalpha(i) || isdigit(i)) {
      v.push_back(i);
    }
  }
  std::random_device rd;
  int64_t seed = rd();
  LOG_INFO("Seed is %lld", seed);
  std::mt19937 g(seed);
  std::shuffle(v.begin(), v.end(), g);
  std::map<uint64_t, ArtLeaf<int64_t>*> child_map;

  auto node = get_new_art_node<BeforeT>();
  auto common_n = reinterpret_cast<ArtNodeCommon*>(node);

  int32_t i = 0;
  for (; i < ArtNodeTrait<BeforeT>::NODE_CAPASITY && i < v.size(); i++) {
    std::string key = "test" + std::to_string(i);
    auto new_leaf = get_new_leaf_node<int64_t>(key.data(), key.size(), i);
    art_add_child_to_node(&common_n, v[i], new_leaf);
    child_map[v[i]] = new_leaf;
  }

  LOG_INFO("===== node full ====\n%s", art_node_to_string(common_n).c_str());
  verify_node<BeforeT>(common_n, child_map);
  if (i < v.size()) {
    std::string key = "test" + std::to_string(i);
    auto new_leaf = get_new_leaf_node<int64_t>(key.data(), key.size(), i);
    art_add_child_to_node(&common_n, v[i], new_leaf);
    child_map[v[i]] = new_leaf;
    LOG_INFO("===== node overflow ====\n%s",
             art_node_to_string(common_n).c_str());
    verify_node<OverFlowT>(common_n, child_map);
  }

  return_art_node(node);
  for (auto& iter : child_map) {
    return_art_node(iter.second);
  }
}

TEST_F(ArtNodeAddTest, test_add_to_n4) { test_fun<ArtNode4, ArtNode16>(); }
TEST_F(ArtNodeAddTest, test_add_to_n16) { test_fun<ArtNode16, ArtNode48>(); }
TEST_F(ArtNodeAddTest, test_add_to_n48) { test_fun<ArtNode48, ArtNode256>(); }
TEST_F(ArtNodeAddTest, test_add_to_n256) { test_fun<ArtNode256, ArtNode256>(); }

}  // namespace detail
}  // namespace art
