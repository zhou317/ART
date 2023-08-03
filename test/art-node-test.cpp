#include "art/art-node-pool.h"
#include "common/logger.h"
#include "gtest/gtest.h"

namespace art {

class ArtNodeBasicTest : public ::testing::Test {};

TEST_F(ArtNodeBasicTest, prefix) {
  std::string short_key = "hello";
  ArtNodeCommon n;
  n.set_key(short_key.data(), short_key.size());
  n.remove_prefix(2);
  EXPECT_EQ(n.to_string(), "llo");
  EXPECT_EQ(n.keyLen, 3);
  n.remove_prefix(3);
  EXPECT_EQ(n.to_string(), "");
  EXPECT_EQ(n.keyLen, 0);

  std::string log_key = "hello123456789";
  ArtNodeCommon n2;
  n2.set_key(log_key.data(), log_key.size());
  n2.remove_prefix(5);
  EXPECT_EQ(n2.to_string(), "123456789");
  EXPECT_EQ(n2.keyLen, log_key.size() - 5);
  n2.remove_prefix(2);
  EXPECT_EQ(n2.to_string(), "3456789");
  EXPECT_EQ(n2.keyLen, log_key.size() - 5 - 2);
  n2.remove_prefix(7);
  EXPECT_EQ(n2.keyLen, 0);
}

TEST_F(ArtNodeBasicTest, merge_prefix) {
  std::string short_key = "hello";
  ArtNodeCommon n;
  n.set_key(short_key.data(), short_key.size());

  std::string short_key2 = "s";
  ArtNodeCommon n2;
  n2.set_key(short_key2.data(), short_key2.size());

  n2.merge_prefix(&n, ' ');
  EXPECT_EQ(n2.to_string(), "hello s");

  std::string l_key = "long key";
  ArtNodeCommon n3;
  n3.set_key(l_key.data(), l_key.size());

  std::string l_key2 = "hello long key";
  ArtNodeCommon n4;
  n4.set_key(l_key2.data(), l_key2.size());

  n4.merge_prefix(&n3, ' ');
  EXPECT_EQ(n4.to_string(), "long key hello long key");
}

}  // namespace art
