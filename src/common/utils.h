#pragma once

#include <string>

#include "common/logger.h"

class TimeUtils {
 public:
  static uint64_t currentTime() {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return tv.tv_sec * 1000000LL + tv.tv_usec;
  }
};

class Timer {
 public:
  Timer(const std::string& event) : event_(event) {
    start_ = TimeUtils::currentTime();
  }

  ~Timer() {
    uint64_t dur = TimeUtils::currentTime() - start_;
    if (dur > 1000 * 1000) {
      Logger::getLogger().out("%s takes %f s", event_.c_str(),
                              (dur / 1000000.0));
    } else if (dur > 1000) {
      Logger::getLogger().out("%s takes %f ms", event_.c_str(), (dur / 1000.0));
    } else {
      Logger::getLogger().out("%s takes %f us", event_.c_str(), (dur / 1.0));
    }
  }

  std::string event_;
  uint64_t start_;
};

#define TIMER_START(t, ...)                                                 \
  std::string _str = Logger::getLogger().buildOut(__SHORT_FILE__, __LINE__, \
                                                  __func__, ##__VA_ARGS__); \
  Timer _t##t(_str);