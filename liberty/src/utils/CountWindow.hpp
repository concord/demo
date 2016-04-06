#pragma once
#include <deque>
#include <concord/Computation.hpp>
#include "WindowOptions.hpp"
template <class ReducerType> class CountWindow : public bolt::Computation {
  public:
  using CtxPtr = bolt::Computation::CtxPtr;
  using Window = std::vector<std::shared_ptr<bolt::FrameworkRecord>>;

  CountWindow(const WindowOptions<ReducerType> &opts) : opts_(opts) {
    CHECK(!opts_.metadata.istreams.empty()) << "Must contain istreams";
  }
  virtual ~CountWindow() {}
  void destroy() override {}
  void init(CtxPtr ctx) override {}
  void processTimer(CtxPtr ctx, const std::string &key, int64_t time) override {
  }

  void processRecord(CtxPtr ctx, bolt::FrameworkRecord &&r) override {
    // Close any existing full windows
    while(isWindowFull(windows_.front())) {
      processWindow(windows_.front());
      windows_.pop_front();
    }

    // Create new window if count is within slide
    if(isNextWindowReady()) {
      windows_.push_back(Window());
    }

    // In the case of tumbling windows there may be spaces of time where
    // we aggregate into no buckets.
    if(!windows_.empty()) {
      auto recordPtr = std::make_shared<bolt::FrameworkRecord>(r);
      recordPtr->key = std::move(r.key);
      recordPtr->value = std::move(r.value);
      recordPtr->time = bolt::timeNowMilli();
      for(auto &w : windows_) {
        w.push_back(recordPtr);
      }
    }
    recordCount++;
  }


  private:
  bool isWindowFull(const Window &w) const {
    return w.size() >= opts_.windowLength.count();
  }

  bool isNextWindowReady() const {
    return recordCount_ % opts_.slideInterval.count() == 0;
  }

  // Code duplication
  void processWindow(const Window &w) const {
    ReducerType acc = std::accumulate(
      w.begin(), w.end(), ReducerType(),
      [this](ReducerType &a, std::shared_ptr<bolt::FrameworkRecord> rec) {
        return opts_.reducerFn(a, rec.get());
      });
    // When the result has finished calculating, call a user supplied callback
    // and produce the result onto any downstream subscribers. The key being
    // the windowID and the value being the calculated result
    opts_.resultFn(w.begin_, acc);
    for(const auto stream : opts_.metadata.ostreams) {
      ctx->produceRecord(stream, std::to_string(w.begin_),
                         opts_.serializerFn(acc));
    }
  }

  const WindowOptions<ReducerType> opts_;
  uint64_t recordCount_{0};
  std::deque<Window> windows_;
};
