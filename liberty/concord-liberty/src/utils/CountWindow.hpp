#pragma once
#include <vector>
#include <deque>
#include <concord/Computation.hpp>
#include "WindowOptions.hpp"
#include <glog/logging.h>
namespace concord {
template <class ReducerType> class CountWindow : public bolt::Computation {
  public:
  using CtxPtr = bolt::Computation::CtxPtr;

  CountWindow(const CountWindowOptions<ReducerType> &opts) : opts_(opts) {
    CHECK(!opts_.metadata.istreams.empty()) << "Must contain istreams";
  }
  virtual ~CountWindow() {}
  void destroy() override {}
  void init(CtxPtr ctx) override {}
  void processTimer(CtxPtr ctx, const std::string &key, int64_t time) override {
  }
  bolt::Metadata metadata() override { return opts_.metadata; }

  void processRecord(CtxPtr ctx, bolt::FrameworkRecord &&r) override {
    // Close any existing full windows
    while(!windows_.empty() && isWindowFull(windows_.front())) {
      processWindow(windows_.front(), ctx);
      windows_.pop_front();
    }

    // Create new window if count is within slide
    if(isNextWindowReady()) {
      windows_.emplace_back(windowCount_++);
    }

    // In the case of tumbling windows there may be spaces of time where
    // we aggregate into no buckets.
    if(!windows_.empty()) {
      auto recordPtr = std::make_shared<bolt::FrameworkRecord>(r);
      for(auto &w : windows_) {
        w.records_.push_back(recordPtr);
      }
    }
    recordCount_++;
  }

  private:
  struct Window {
    std::vector<std::shared_ptr<bolt::FrameworkRecord>> records_;
    uint64_t bucketNum_;
    Window(const uint64_t num) : bucketNum_(num) {}
  };

  bool isWindowFull(const Window &w) const {
    return w.records_.size() >= opts_.windowLength;
  }

  bool isNextWindowReady() const {
    if(recordCount_ == 0) {
      return true;
    }
    return recordCount_ % opts_.slideInterval == 0;
  }

  // Code duplication
  void processWindow(const Window &window, CtxPtr ctx) const {
    ReducerType acc;
    for(auto r : window.records_) {
      opts_.reducerFn(acc, r.get());
    }
    // When the result has finished calculating, call a user supplied callback
    // and produce the result onto any downstream subscribers. The key being
    // the bucket num and the value being the calculated result
    opts_.resultFn(window.bucketNum_, acc);
    for(const auto stream : opts_.metadata.ostreams) {
      // TODO: Add support for serializerKeyFn_ and serializeValueFn_
      ctx->produceRecord(stream, std::to_string(window.bucketNum_),
                         opts_.serializerFn(acc));
    }
  }

  const CountWindowOptions<ReducerType> opts_;
  uint64_t recordCount_{0};
  uint64_t windowCount_{0};
  std::deque<Window> windows_;
};
}
