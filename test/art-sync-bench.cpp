#include <cassert>
#include <map>
#include <memory>
#include <random>
#include <string>
#include <unordered_map>

#include "art-sync/art-sync.h"
#include "common/utils.h"
#include "gtest/gtest.h"

const uint64_t n = 10000 * 160;

TEST(ArtSyncBench, insertSparse) {
  int v = 1;
  std::vector<int32_t> thdCnt{1, 2, 4, 8};
  for (auto t : thdCnt) {
    ArtSync::ArtTreeSync<int *> tree;
    std::vector<std::thread> worker;
    TIMER_START(tmp, "======= ThdCnt %d =======", t);
    for (int i = 0; i < t; i++) {
      worker.emplace_back([&tree, i, &v](){
        {
          TIMER_START(tmp, "ThdCnt %d, Insert %llu", i, n);
          std::mt19937_64 rng(0);
          char buf[sizeof(uint64_t)];
          for (uint64_t j = 0; j < n; j++) {
            *reinterpret_cast<uint64_t *>(buf) = rng();
            buf[7] = i;
            tree.set(buf, sizeof(uint64_t), &v);
          }
        }
      });
    }
    for (int i = 0; i < t; i++) worker[i].join();
  }
}