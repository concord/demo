#pragma once
#include <chrono>
#include <vector>
#include <deque>
#include <algorithm>
#include <concord/Computation.hpp>
#include <concord/time_utils.hpp>
#include <glog/logging.h>
namespace concord {
/// Abstract class useful for determining results on data fields with
/// a special respect to time.
/// Use the 'length' parameter to control how the window will be open.
/// Use the 'interval' parameter to control when to create new windows.
template <class ReducerType>
class WindowedComputation : public bolt::Computation {
  public:
  using CtxPtr = bolt::Computation::CtxPtr;

  static WindowedComputation *create() {
    // const auto wLength = windowLength_.count();
    // const auto sInterval = slideInterval_.count();
    // CHECK(wLength > 0) << "Window length must be positive";
    // CHECK(sInterval > 0) << "Slide interval must be positive";
    return new WindowedComputation<ReducerType>();
  }

  template <class Rep, class Period>
  WindowedComputation *
  setWindowLength(const std::chrono::duration<Rep, Period> &length) {
    windowLength_ =
      std::chrono::duration_cast<std::chrono::milliseconds>(length);
    return this;
  }

  template <class Rep, class Period>
  WindowedComputation *
  setSlideInterval(const std::chrono::duration<Rep, Period> &interval) {
    slideInterval_ =
      std::chrono::duration_cast<std::chrono::milliseconds>(interval);
    return this;
  }

  template <class Reducer>
  WindowedComputation *setReducerFunction(Reducer reducer) {
    reducerFunction_ = reducer;
    return this;
  }

  template <class WindowerResult>
  WindowedComputation *setWindowerResultFunction(WindowerResult result) {
    resultFunction_ = result;
    return this;
  }

  WindowedComputation *setComputationMetadata(const bolt::Metadata &md) {
    metadata_ = md;
    return this;
  }

  virtual ~WindowedComputation() {}
  virtual void destroy() override {}

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
    auto recordPtr = std::make_shared<bolt::FrameworkRecord>(r);
    recordPtr->key = std::move(r.key);
    recordPtr->value = std::move(r.value);
    recordPtr->time = bolt::timeNowMilli();
    for(auto &w : windows_) {
      if(w.isWithinWindow(recordPtr.get())) {
        w.records_.push_back(recordPtr);
      }
    }

    processWindows();
    // BUG: This should only happen once. We are setting a ton of timers
    // // I know its weird, just check the source out! [ComputationFacade.cc]
    // ctx->setTimer("processing_loop", bolt::timeNowMilli());
  }

  virtual bolt::Metadata metadata() override { return metadata_; }

  private:
  void processWindows() {
    // Only perform processing on closed windows.. windows should be queued in
    // chronological order
    auto processedWindows = 0u;
    for(const auto &w : windows_) {
      if(!w.isWindowClosed()) {
        ReducerType acc = std::accumulate(
          w.records_.begin(), w.records_.end(), ReducerType(),
          [this](ReducerType &a, std::shared_ptr<bolt::FrameworkRecord> rec) {
            return reducerFunction_(a, rec.get());
          });
        resultFunction_(w.begin_, acc);
        // TODO(rob): if ostreams is set
        // ctx->produceRecord(acc);
        processedWindows++;
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

    bool isWithinWindow(const bolt::FrameworkRecord *r) const {
      const auto utime = static_cast<uint64_t>(r->time);
      return utime > begin_ && utime < end_;
    }

    bool isWindowClosed() const { return bolt::timeNowMilli() >= end_; }

    // all time measured in milliseconds since epoch
    const uint64_t begin_;
    const uint64_t end_;
    std::vector<std::shared_ptr<bolt::FrameworkRecord>> records_;
  };

  private:
  WindowedComputation() {}

  std::chrono::milliseconds windowLength_;
  std::chrono::milliseconds slideInterval_;
  std::function<ReducerType(ReducerType &, const bolt::FrameworkRecord *)>
    reducerFunction_;
  std::function<void(const uint64_t, const ReducerType &)> resultFunction_;
  bolt::Metadata metadata_;

  std::deque<Window> windows_;
};
}
