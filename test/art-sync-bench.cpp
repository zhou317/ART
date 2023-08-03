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
  std::vector<int32_t> thdCnt{1, 2, 4, 8, 16, 24, 32};
  for (auto t : thdCnt) {
    ArtSync::ArtTreeSync<int *> tree;
    std::vector<std::thread> worker;
    TIMER_START(tmp, "======= ThdCnt %d =======", t);
    for (int i = 0; i < t; i++) {
      worker.emplace_back([&tree, i, t, &v](){
        {
          std::mt19937_64 rng(0);
          for (uint64_t j = 0; j < n; j++) {
            std::string tmp = std::to_string(rng());
	    tree.set(tmp.data(), tmp.size(), &v);
          }
        }
      });
    }
    for (int i = 0; i < t; i++) worker[i].join();
  }
}
