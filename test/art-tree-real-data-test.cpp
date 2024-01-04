#include <random>
#include <unordered_map>

#include "art/art.h"
#include "common/logger.h"
#include "gtest/gtest.h"

const char *words_data[] = {
#include "data/words.txt"
};

const char *uuid_data[] = {
#include "data/uuid.txt"
};

namespace art {

class ArtTreeRealDataTest : public ::testing::Test {
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

TEST_F(ArtTreeRealDataTest, words_data) {
  int len = sizeof(words_data) / sizeof(const char *);
  for (int32_t i = 0; i < len; i++) {
    set(words_data[i], i);
  }

  for (int32_t i = 0; i < len; i += 2) {
    del(words_data[i]);
  }
}

TEST_F(ArtTreeRealDataTest, uuid_data) {
  int len = sizeof(uuid_data) / sizeof(const char *);
  for (int32_t i = 0; i < len; i++) {
    set(uuid_data[i], i);
  }

  for (int32_t i = 0; i < len; i += 2) {
    del(uuid_data[i]);
  }
}

}  // namespace art
