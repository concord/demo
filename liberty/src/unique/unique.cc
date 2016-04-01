#include <memory>
#include <unordered_map>
#include <algorithm>
#include <sstream>
#include <bloom.h>

#include <concord/glog_init.hpp>
#include <concord/Computation.hpp>
#include <concord/time_utils.hpp>


class Unique final : public bolt::Computation {
  public:
  using CtxPtr = bolt::Computation::CtxPtr;
  virtual void init(CtxPtr ctx) override {
    bloom_init(&bloom_, 1000000, 0.01);
    bloom_print(&bloom_);
    LOG(INFO) << "Initializing unique";
  }

  virtual void destroy() override {
    bloom_print(&bloom_);
    bloom_free(&bloom_);
    LOG(INFO) << "Destructing word count sink [cpp]";
  }

  virtual void processRecord(CtxPtr ctx, bolt::FrameworkRecord &&r) override {
  }

  virtual void
  processTimer(CtxPtr ctx, const std::string &key, int64_t time) override {}

  virtual bolt::Metadata metadata() override {
    bolt::Metadata m;
    m.name = "unique";
    m.istreams.insert({"words", bolt::Grouping::GROUP_BY});
    return m;
  }


  private:
  struct bloom bloom_;
};

int main(int argc, char *argv[]) {
  bolt::logging::glog_init(argv[0]);
  bolt::client::serveComputation(std::make_shared<Unique>(), argc, argv);
  return 0;
}
