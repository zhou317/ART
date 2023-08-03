#include <cassert>
#include <map>
#include <memory>
#include <random>
#include <string>
#include <unordered_map>

#include "art/art.h"
#include "common/utils.h"
#include "gtest/gtest.h"

const uint64_t n = 10000 * 160;

template <class T>
static void insertSparseStl() {
  int v = 1;
  T mp;
  std::mt19937_64 rng(0);
  TIMER_START(t, "Insert and look up %llu", n);
  for (uint64_t i = 0; i < n; i++) {
    auto t = std::to_string(rng());
    mp[t] = &v;
  }
}

TEST(ArtBench, insertSparse) {
  int v = 1;
  std::mt19937_64 rng(0);
  {
    ArtSingle::ArtTree<int *> tree;
    {
      TIMER_START(t, "Insert and look up %llu", n);
      for (uint64_t i = 0; i < n; i++) {
        auto t = std::to_string(rng());
        tree.set(t.c_str(), t.size(), &v);
      }
    }
  }
  //  insertSparseStl<std::map<std::string, int *>>();
  //  insertSparseStl<std::unordered_map<std::string, int *>>();
}

template <class T>
static void insertSparseStl2() {
  int v = 1;
  T mp;
  std::mt19937_64 rng(0);
  char buf[sizeof(uint64_t)];
  TIMER_START(t, "Insert and look up %llu", n);
  for (uint64_t i = 0; i < n; i++) {
    *reinterpret_cast<uint64_t *>(buf) = rng();
    std::string tmp{buf, sizeof(uint64_t)};
    mp[tmp] = &v;
  }
}

TEST(ArtBench, insertSparse2) {
  int v = 1;
  std::mt19937_64 rng(0);
  char buf[sizeof(uint64_t)];
  {
    ArtSingle::ArtTree<int *> tree;
    {
      TIMER_START(t, "Insert %llu", n);
      for (uint64_t i = 0; i < n; i++) {
        *reinterpret_cast<uint64_t *>(buf) = rng();
        tree.set(buf, sizeof(uint64_t), &v);
      }
    }
  }
}