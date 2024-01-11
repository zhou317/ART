#include <cassert>
#include <map>
#include <memory>
#include <random>
#include <string>
#include <unordered_map>

#include "art/art.h"
#include "common/utils.h"
#include "gtest/gtest.h"

namespace art {

const uint64_t n = 10000 * 1 * 16;

template <class T>
static void insertSparseStl() {
  T mp;
  std::mt19937_64 rng(0);
  {
    TIMER_START(t, "Insert and look up %llu", n);
    for (uint64_t i = 0; i < n; i++) {
      auto t = std::to_string(rng());
      mp[t] = i;
    }
  }
}

TEST(ArtBench, insertSparse) {
  std::mt19937_64 rng(0);
  {
    ArtTree<int64_t> tree;
    {
      TIMER_START(t, "Insert and look up %llu", n);
      for (uint64_t i = 0; i < n; i++) {
        auto t = std::to_string(rng());
        tree.set(t.c_str(), t.size(), i);
      }
    }
  }
  //  insertSparseStl<std::map<std::string, int64_t>>();
  //  insertSparseStl<std::unordered_map<std::string, int64_t>>();
}

template <class T>
static void insertSparseStl2() {
  T mp;
  {
    std::mt19937_64 rng(0);
    char buf[sizeof(uint64_t)];
    TIMER_START(t, "Insert and look up %llu", n);
    for (uint64_t i = 0; i < n; i++) {
      *reinterpret_cast<uint64_t *>(buf) = rng();
      std::string tmp{buf, sizeof(uint64_t)};
      mp[tmp] = i;
    }
    std::cout << get_pool_usage<int64_t>() << std::endl;
  }
}

TEST(ArtBench, insertSparse2) {
  std::mt19937_64 rng(0);
  char buf[sizeof(uint64_t)];
  {
    ArtTree<int64_t> tree;
    {
      TIMER_START(t, "Insert int64, key count %llu", n);
      for (uint64_t i = 0; i < n; i++) {
        *reinterpret_cast<uint64_t *>(buf) = rng();
        tree.set(buf, sizeof(uint64_t), i);
      }
    }
    std::cout << get_pool_usage<int64_t>() << std::endl;
  }

  {
    ArtTree<int64_t> tree;
    {
      TIMER_START(t, "Insert int32, key count %llu", n);
      for (uint64_t i = 0; i < n; i++) {
        *reinterpret_cast<uint64_t *>(buf) = rng();
        tree.set(buf, sizeof(uint32_t), i);
      }
    }
  }
  //  insertSparseStl2<std::map<std::string, int64_t>>();
  //  insertSparseStl2<std::unordered_map<std::string, int64_t>>();
}

}  // namespace art
