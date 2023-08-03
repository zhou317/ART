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

}  // namespace art
