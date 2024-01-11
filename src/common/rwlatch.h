#pragma once

#include <condition_variable>
#include <mutex>
#include <thread>

class ReaderWriterLatch {
  using mutex_t = std::mutex;
  using cond_t = std::condition_variable;
  static const uint32_t max_readers_ = UINT32_MAX;

 public:
  ReaderWriterLatch() = default;
  ~ReaderWriterLatch() { std::lock_guard<mutex_t> guard(mutex_); }

  /**
   * Acquire a write latch.
   */
  void WLock() {
    std::unique_lock<mutex_t> latch(mutex_);
    while (writer_entered_) {
      reader_.wait(latch);
    }
    writer_entered_ = true;
    while (reader_count_ > 0) {
      writer_.wait(latch);
    }
  }

  /**
   * Release a write latch.
   */
  void WUnlock() {
    std::lock_guard<mutex_t> guard(mutex_);
    writer_entered_ = false;
    reader_.notify_all();
  }

  /**
   * Acquire a read latch.
   */
  void RLock() {
    std::unique_lock<mutex_t> latch(mutex_);
    while (writer_entered_ || reader_count_ == max_readers_) {
      reader_.wait(latch);
    }
    reader_count_++;
  }

  /**
   * Release a read latch.
   */
  void RUnlock() {
    std::lock_guard<mutex_t> guard(mutex_);
    reader_count_--;
    if (writer_entered_) {
      if (reader_count_ == 0) {
        writer_.notify_one();
      }
    } else {
      if (reader_count_ == max_readers_ - 1) {
        reader_.notify_one();
      }
    }
  }

  void RUpgradeToW() {
    std::unique_lock<mutex_t> latch(mutex_);
    while (writer_entered_) {
      reader_.wait(latch);
    }
    writer_entered_ = true;
    reader_count_--;
    while (reader_count_ > 0) {
      writer_.wait(latch);
    }
    owner_ = std::this_thread::get_id();
  }

 private:
  mutex_t mutex_;
  cond_t writer_;
  cond_t reader_;
  uint32_t reader_count_{0};
  bool writer_entered_{false};
  std::thread::id owner_;
};
