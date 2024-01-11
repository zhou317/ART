#include <random>
#include <unordered_map>

#include "art/art.h"
#include "common/utils.h"
#include "gtest/gtest.h"

namespace art {

class ArtTreeThreadSafeTest : public ::testing::Test {};

TEST_F(ArtTreeThreadSafeTest, multi_insert) {
  auto insert_fun = [](ArtTree<int64_t>* tree, int32_t cnt, char idx) {
    std::mt19937_64 rng(0);
    char buf[sizeof(uint64_t)];
    for (int i = 0; i < cnt; i++) {
      *reinterpret_cast<uint64_t*>(buf) = rng();
      buf[sizeof(uint64_t) - 1] = idx;
      int64_t val = *reinterpret_cast<int64_t*>(buf);
      tree->set(buf, sizeof(uint64_t), val);
      if (i % 1000 == 0) LOG_INFO("thread %d cur %d", idx, i);
    }
  };

  const int each_cnt = 1 * 10000;
  std::vector<int32_t> thd_cnt{1, 2, 4, 8, 16, 32, 64};
  for (auto c : thd_cnt) {
    ArtTree<int64_t> tree;
    std::vector<std::thread> worker;
    {
      TIMER_START(t, "thread count %d", c);
      for (int i = 0; i < c; i++)
        worker.emplace_back(std::thread(insert_fun, &tree, each_cnt, i));
      for (int i = 0; i < c; i++) worker[i].join();
    }
  }
}

}  // namespace art
