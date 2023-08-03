#pragma once

#include <sys/stat.h>
#include <sys/time.h>
#include <time.h>

#include <sstream>
#include <thread>

#include "common/macros.h"

// https://blog.galowicz.de/2016/02/20/short_file_macro/
using cstr = const char *;

static constexpr cstr past_last_slash(cstr str, cstr last_slash) {
  return *str == '\0'  ? last_slash
         : *str == '/' ? past_last_slash(str + 1, str + 1)
                       : past_last_slash(str + 1, last_slash);
}

static constexpr cstr PastLastSlash(cstr a) { return past_last_slash(a, a); }

#define __SHORT_FILE__                            \
  ({                                              \
    constexpr cstr sf__{PastLastSlash(__FILE__)}; \
    sf__;                                         \
  })

enum LogLevel {
  LOG_LEVEL_DEBUG,
  LOG_LEVEL_INFO,
  LOG_LEVEL_WARNING,
  LOG_LEVEL_ERROR
};

class Logger {
  Logger() = default;

  static constexpr int kLogMaxSize = 1024 * 8;

 public:
  static Logger &getLogger() {
    static Logger _logger;
    return _logger;
  }

  void setLogLevel(LogLevel level) { curLogLeve_ = level; }

  template <typename... Args>
  std::string buildOut(const char *file, int line, const char *func,
                       const std::string &format, Args... args) {
    std::ostringstream oss;
    oss << std::this_thread::get_id();
    struct timeval tv {
      0, 0
    };
    gettimeofday(&tv, nullptr);
    struct tm *tm = localtime(&tv.tv_sec);
    char buf[64] = {0};
    strftime(buf, sizeof(buf), "%F-%X", tm);
    char logBuf[kLogMaxSize] = {0};
    std::string totalFormat = "[(%s):%s:%s:%d:%s]" + format;
    int n = std::snprintf(logBuf, kLogMaxSize, totalFormat.c_str(), buf,
                          oss.str().c_str(), file, line, func, args...);
    return {logBuf, logBuf + n};
  }

  template <typename... Args>
  void out(LogLevel level, const char *file, int line, const char *func,
           const std::string &format, Args... args) {
    __builtin_assume(level <= LOG_LEVEL_ERROR);
    if (level >= curLogLeve_) {
      std::string msg = buildOut(file, line, func, format, args...);
      std::lock_guard<std::mutex> lockGuard(m_);
      fprintf(stdout, "%s\n", msg.c_str());
      ::fflush(stdout);
      if (level == LOG_LEVEL_ERROR) {
        ::abort();
      }
    }
  };

  template <typename... Args>
  void out(const std::string &format, Args... args) {
    char logBuf[kLogMaxSize] = {0};
    std::snprintf(logBuf, kLogMaxSize, format.c_str(), args...);
    std::lock_guard<std::mutex> lockGuard(m_);
    fprintf(stdout, "%s\n", logBuf);
    ::fflush(stdout);
  };

 private:
  std::mutex m_;
  uint32_t curLogLeve_ = LogLevel::LOG_LEVEL_INFO;
};

#ifndef NDEBUG
#define LOG_DEBUG(...)                                                         \
  Logger::getLogger().out(LOG_LEVEL_DEBUG, __SHORT_FILE__, __LINE__, __func__, \
                          ##__VA_ARGS__)
#else
#define LOG_DEBUG(...) \
  {}
#endif

#define LOG_INFO(...)                                                         \
  Logger::getLogger().out(LOG_LEVEL_INFO, __SHORT_FILE__, __LINE__, __func__, \
                          ##__VA_ARGS__)

#define LOG_WARNING(...)                                               \
  Logger::getLogger().out(LOG_LEVEL_WARNING, __SHORT_FILE__, __LINE__, \
                          __func__, ##__VA_ARGS__)

#define LOG_ERROR(...)                                                         \
  Logger::getLogger().out(LOG_LEVEL_ERROR, __SHORT_FILE__, __LINE__, __func__, \
                          ##__VA_ARGS__)

#define SET_LOG_LEVEL(l) Logger::getLogger().setLogLevel(l);

#ifndef NDEBUG
#define ASSERT(cond)                          \
  do {                                        \
    if (unlikely(!(cond))) {                  \
      LOG_WARNING("ASSERT %s FAILED", #cond); \
      ::abort();                              \
    }                                         \
  } while (0)
#else
#define ASSERT(cond) \
  {}
#endif

#define INSIST(cond)                          \
  do {                                        \
    if (unlikely(!(cond))) {                  \
      LOG_WARNING("INSIST %s FAILED", #cond); \
      ::abort();                              \
    }                                         \
  } while (0)
