#include "gtest/gtest.h"

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  signal(SIGPIPE, SIG_IGN);

  return RUN_ALL_TESTS();
}
