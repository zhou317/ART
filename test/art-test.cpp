#include "art/art.h"

#include <cassert>
#include <map>
#include <memory>
#include <random>
#include <string>
#include <unordered_map>

#include "common/utils.h"
#include "gtest/gtest.h"

TEST(ArtTest, monteCarloTest) {
  const int n = 1000;
  std::string keys[n];
  int32_t *values[n];

  std::mt19937_64 rng(0);
  for (int32_t experiment = 0; experiment < 10; experiment += 1) {
    for (int32_t i = 0; i < n; i += 1) {
      keys[i] = std::to_string(rng());
      values[i] = new int32_t();
    }

    ArtSingle::ArtTree<int32_t *> tree;
    for (int32_t i = 0; i < n; i += 1) {
      tree.set(keys[i].c_str(), keys[i].size(), values[i]);

      for (int32_t j = 0; j < i; j += 1) {
        auto val = tree.get(keys[j].c_str(), keys[j].size());
        EXPECT_EQ(values[j], val);
      }
    }

    for (int32_t i = 0; i < n; i += 1) {
      auto delV = tree.del(keys[i].c_str(), keys[i].size());
      EXPECT_EQ(values[i], delV);
      for (int32_t j = i + 1; j < n; j += 1) {
        auto val = tree.get(keys[j].c_str(), keys[j].size());
        EXPECT_EQ(values[j], val);
      }
    }

    for (int32_t i = 0; i < n; i += 1) {
      delete values[i];
    }
    EXPECT_EQ(tree.size(), 0);
  }
}

TEST(ArtTest, monteCarloTest2) {
  const int n = 1000;
  std::string keys[n];
  int32_t *values[n];
  char buf[sizeof(uint64_t)];
  std::mt19937_64 rng(0);
  for (int32_t experiment = 0; experiment < 10; experiment += 1) {
    for (int32_t i = 0; i < n; i += 1) {
      *reinterpret_cast<uint64_t *>(buf) = rng();
      keys[i] = std::string{buf, sizeof(uint64_t)};
      values[i] = new int32_t();
    }

    ArtSingle::ArtTree<int32_t *> tree;
    for (int32_t i = 0; i < n; i += 1) {
      tree.set(keys[i].c_str(), keys[i].size(), values[i]);

      for (int32_t j = 0; j < i; j += 1) {
        auto val = tree.get(keys[j].c_str(), keys[j].size());
        EXPECT_EQ(values[j], val);
      }
    }

    for (int32_t i = 0; i < n; i += 1) {
      auto delV = tree.del(keys[i].c_str(), keys[i].size());
      EXPECT_EQ(values[i], delV);
      for (int32_t j = i + 1; j < n; j += 1) {
        auto val = tree.get(keys[j].c_str(), keys[j].size());
        EXPECT_EQ(values[j], val);
      }
    }

    for (int32_t i = 0; i < n; i += 1) {
      delete values[i];
    }
  }
}
