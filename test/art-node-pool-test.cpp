#include "art/art-node-pool.h"

#include "common/logger.h"
#include "gtest/gtest.h"

namespace art {
namespace detail {

class ArtNodePoolTest : public ::testing::Test {};

template <bool allNew>
void get_and_return() {
  TempLogLevelSetter setter(LogLevel::LOG_LEVEL_DEBUG);
  auto node4 = get_new_art_node<ArtNode4, allNew>();
  EXPECT_EQ(node4->is_from_new(), allNew);
  EXPECT_EQ(node4->type, ArtNodeType::ART_NODE_4);
  EXPECT_EQ(node4->childNum, 0);
  for (int32_t i = 0; i < 4; i++) {
    EXPECT_EQ(node4->keys[i], 0);
    EXPECT_EQ(node4->children[i], nullptr);
  }

  EXPECT_EQ(node4->key.keyPtr, nullptr);
  EXPECT_EQ(node4->keyLen, 0);

  node4->set_key("hello", 5);
  EXPECT_EQ(node4->to_string(), "hello");

  LOG_DEBUG("%s", node4->to_string().c_str());

  return_art_node(node4);

  node4 = get_new_art_node<ArtNode4, allNew>();
  EXPECT_EQ(node4->is_from_new(), allNew);
  EXPECT_EQ(node4->type, ArtNodeType::ART_NODE_4);
  EXPECT_EQ(node4->childNum, 0);
  for (int32_t i = 0; i < 4; i++) {
    EXPECT_EQ(node4->keys[i], 0);
    EXPECT_EQ(node4->children[i], nullptr);
  }

  EXPECT_EQ(node4->key.keyPtr, nullptr);
  EXPECT_EQ(node4->keyLen, 0);

  node4->set_key("hello long key", 14);

  EXPECT_EQ(node4->to_string(), "hello long key");
  LOG_DEBUG("%s", node4->to_string().c_str());

  auto nodel = get_new_leaf_node<int, allNew>("hello", 5, 2023);
  EXPECT_EQ(nodel->is_from_new(), allNew);
  EXPECT_EQ(nodel->value, 2023);
  EXPECT_EQ(nodel->to_string(), "hello");
  return_art_node(nodel);

  nodel = get_new_leaf_node<int, allNew>("hello long key", 14, 2024);
  EXPECT_EQ(nodel->is_from_new(), allNew);
  EXPECT_EQ(nodel->value, 2024);
  EXPECT_EQ(nodel->to_string(), "hello long key");
  return_art_node(nodel);
}

TEST_F(ArtNodePoolTest, get_and_return_pool) { get_and_return<false>(); }

TEST_F(ArtNodePoolTest, get_and_return_direct) { get_and_return<true>(); }

}  // namespace detail
}  // namespace art
