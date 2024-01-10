#include <random>
#include <unordered_map>

#include "art/art-printer.h"
#include "art/art.h"
#include "common/logger.h"
#include "gtest/gtest.h"

namespace art {

class ArtTreeBasicTest : public ::testing::Test {
 public:
  void TearDown() override {
    for (auto &iter : verify_map) {
      EXPECT_EQ(tree.get(iter.first.c_str(), iter.first.size()), iter.second);
    }
  }

  int64_t set(const std::string &key, int64_t val) {
    verify_map[key] = val;
    return tree.set(key.c_str(), key.size(), val);
  }

  void del(const std::string &key) {
    auto tmp = tree.del(key.data(), key.size());
    EXPECT_EQ(tmp, verify_map[key]);
    verify_map.erase(key);
  }

  int64_t del_not_exist_key(const std::string &key) {
    return tree.del(key.data(), key.size());
  }

  int64_t get(const std::string &key) {
    return tree.get(key.data(), key.size());
  }

  std::unordered_map<std::string, int64_t> verify_map;
  ArtTree<int64_t> tree;
};

TEST_F(ArtTreeBasicTest, basic) {
  for (int32_t i = 0; i < 10000; i++) {
    set(std::to_string(i), i);
  }
  for (int32_t i = 0; i < 10000; i += 2) {
    del(std::to_string(i));
  }
}

TEST_F(ArtTreeBasicTest, find_and_delete_not_exist) {
  EXPECT_EQ(del_not_exist_key("abc"), 0);
  EXPECT_EQ(get("abc"), 0);

  set("abc", 1);
  set("adc", 2);
  // leaf not match
  EXPECT_EQ(get("adcb"), 0);
  EXPECT_EQ(del_not_exist_key("adcb"), 0);
  // prefix not match
  EXPECT_EQ(get("ccc"), 0);
  EXPECT_EQ(del_not_exist_key("ccc"), 0);
  // no child at 'c'
  EXPECT_EQ(get("acc"), 0);
  EXPECT_EQ(del_not_exist_key("acc"), 0);

  std::cout << tree.get_root_unsafe() << "\n";
  del("abc");
  del("adc");
}

TEST_F(ArtTreeBasicTest, delete_a_prefix) {
  set("abcd", 1);
  set("accd", 2);
  set("acddfgh", 3);
  set("acddfghij", 4);

  del("acddfgh");
}

}  // namespace art
