#pragma once
#include <chrono>
#include <vector>
#include <deque>
#include <glog/logging.h>
#include <concord/Computation.hpp>
#include <concord/time_utils.hpp>
#include <boost/optional.hpp>
namespace concord {
/// Abstract class useful for determining results on data fields with
/// a special respect to time.
/// Use the 'length' parameter to control how the window will be open.
/// Use the 'interval' parameter to control when to create new windows.
class WindowedComputation : public bolt::Computation {
  public:
  using CtxPtr = bolt::Computation::CtxPtr;

  class WindowCallback {
    public:
    virtual void
    processRecordsInWindow(const uint64_t window,
                           const std::vector<bolt::FrameworkRecord> &r) = 0;
    virtual ~WindowCallback() {}
  };

  // Common use cases...
  // if slideInterval_ < windowLength_
  //   then the windows will overlap
  // if slideInterval_ > windowLength_
  //   then the windows will never overlap
  template <class Rep, class Period>
  WindowedComputation(const std::chrono::duration<Rep, Period> &length,
                      const std::chrono::duration<Rep, Period> &interval,
                      std::unique_ptr<WindowCallback> &&cb)
    : windowLength_(
        std::chrono::duration_cast<std::chrono::milliseconds>(length))
    , slideInterval_(
        std::chrono::duration_cast<std::chrono::milliseconds>(interval))
    , computationCb_(std::move(cb)) {
    const auto wLength = windowLength_.count();
    const auto sInterval = slideInterval_.count();
    CHECK(wLength > 0) << "Window length must be positive";
    CHECK(sInterval > 0) << "Slide interval must be positive";
    CHECK(sInterval != wLength) << "No need for this class";
  }

  virtual ~WindowedComputation() {}

  virtual void init(CtxPtr ctx) override {
    ctx->setTimer("window_loop", bolt::timeNowMilli());
  }

  virtual void
  processTimer(CtxPtr ctx, const std::string &key, int64_t time) override {
    if(key == "window_loop") {
      windows_.push_back(Window(windowLength_));
      ctx->setTimer("window_loop",
                    bolt::timeNowMilli() + slideInterval_.count());
    } else if(key == "processing_loop") {
      processWindows();
    }
  }

  virtual void processRecord(CtxPtr ctx, bolt::FrameworkRecord &&r) override {
    for(auto &w : windows_) {
      if(w.isWithinWindow(r)) {
        w.records_.push_back(std::move(r));
      }
    }
    // Necessary in order for this method to return to the client.
    // I know its weird, just check the source out! [ComputationFacade.cc]
    ctx->setTimer("processing_loop", bolt::timeNowMilli());
  }

  private:
  void processWindows() {
    // Only perform processing on closed windows.. windows should be queued in
    // chronological order
    auto processedWindows = 0u;
    for(const auto &w : windows_) {
      if(!w.isWindowClosed()) {
        processedWindows++;
        computationCb_->processRecordsInWindow(w.begin_, w.records_);
      } else {
        break;
      }
    }
    // Remove processed windows
    while(processedWindows-- == 0 && windows_.empty()) {
      windows_.pop_front();
    }
  }

  struct Window {
    Window(const std::chrono::milliseconds &windowLength)
      : begin_(bolt::timeNowMilli()), end_(begin_ + windowLength.count()) {}

    bool isWithinWindow(const bolt::FrameworkRecord &r) const {
      const auto utime = static_cast<uint64_t>(r.time);
      return utime > begin_ && utime < end_;
    }

    bool isWindowClosed() const { return bolt::timeNowMilli() >= end_; }

    // all time measured in milliseconds since epoch
    const uint64_t begin_;
    const uint64_t end_;
    std::vector<bolt::FrameworkRecord> records_;
  };

  const std::chrono::milliseconds windowLength_;
  const std::chrono::milliseconds slideInterval_;
  const std::unique_ptr<WindowCallback> computationCb_;

  std::deque<Window> windows_;
};
}
