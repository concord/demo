#pragma once
#include <chrono>
#include <set>
#include <concord/Computation.hpp>
#include <concord/time_utils.hpp>
#include <boost/optional.hpp>
namespace concord {
class LessThanFrameworkRecord {
  public:
  bool operator()(const std::shared_ptr<bolt::FrameworkRecord> lhs,
                  const std::shared_ptr<bolt::FrameworkRecord> rhs) {
    return lhs->time() < rhs->time();
  }
};

class Window {
  public:
  using RecordPtr = std::shared_ptr<bolt::FrameworkRecord>;

  Window(const std::chrono::milliseconds &windowLength)
    : begin_(bolt::timeNowMilli()), end_(begin_ + windowLength.count()) {}

  void addRecord(RecordPtr r) {
    if(isWithinWindow(r)) {
      records_.insert(r);
    }
  }

  private:
  bool isWithinWindow(const RecordPtr r) const {
    const auto utime = static_cast<uint64_t>(r->time());
    return utime > begin_ && utime < end_;
  }

  // all time measured in milliseconds since epoch
  const uint64_t begin_;
  const uint64_t end_;
  std::set<RecordPtr, LessThanFrameworkRecord> records_;
};

/// Abstract class useful for determining results on data fields with
/// a special respect to time.
/// Use the 'length' parameter to control how the window will be open.
/// Use the 'interval' parameter to control when to create new windows.
class WindowedComputation : public bolt::Computation {
  public:
  using CtxPtr = bolt::Computation::CtxPtr;

  template <class Rep, class Period>
  WindowedComputation(const std::chrono::duration<Rep, Period> &length,
                      const std::chrono::duration<Rep, Period> &interval)
    : windowLength_(
        std::chrono::duration_cast<std::chrono::milliseconds>(length))
    , slideInterval_(
        std::chrono::duration_cast<std::chrono::milliseconds>(interval)) {
    // CHECK(windowLength_ > 0) << "Window length must be positive";
    // CHECK(slideInterval_ > 0) << "Slide interval must be positive";
    // CHECK(slideInterval_ != windowLength_) << "No need for this class";

    // Common use cases...
    // if slideInterval_ < windowLength_
    //   then the windows will overlap (COOL)
    // if slideInterval_ > windowLength_
    //   then the windows will never overlap (COOL)
  }
  virtual ~WindowedComputation() {}

  virtual void init(CtxPtr ctx) override {
    ctx->setTimer("loop", bolt::timeNowMilli());
  }

  virtual void
  processTimer(CtxPtr ctx, const std::string &key, int64_t time) override {
    windows_.push_back(Window(windowLength_));
    ctx->setTimer("loop", bolt::timeNowMilli() + slideInterval_.count());
  }

  virtual void processRecord(CtxPtr ctx, bolt::FrameworkRecord &&r) override {
    // 'Window' class will conditionally addRecord to collection
    auto record = r.clone();
    for(auto &w : windows_) {
      w.addRecord(record);
    }
    
  }

  // virtual void processRecordWindowed(CtxPtr ctx, bolt::FrameworkRecord &&r) =
  // 0;

  private:
  const std::chrono::milliseconds windowLength_;
  const std::chrono::milliseconds slideInterval_;

  std::vector<Window> windows_;
};
}
