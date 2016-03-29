#include <hyperloglog/hyperloglog.hpp>
#include <concord/glog_init.hpp>
#include <concord/Computation.hpp>
#include <concord/time_utils.hpp>

class LogCounter final : public bolt::Computation {
  public:
  using CtxPtr = bolt::Computation::CtxPtr;

  LogCounter() : hll_(10) {}

  virtual void init(CtxPtr ctx) override {
    LOG(INFO) << "Log Counter initialized.. ready for events";
    ctx->setTimer("loop", bolt::timeNowMilli());
  }

  virtual void destroy() override {}

  virtual void
  processTimer(CtxPtr ctx, const std::string &key, int64_t time) override {
    LOG(INFO) << "Cardinality: " << hll_.estimate();
    ctx->setTimer("loop", bolt::timeNowMilli());
  }

  virtual void processRecord(CtxPtr ctx, bolt::FrameworkRecord &&r) override {}

  virtual bolt::Metadata metadata() override {
    bolt::Metadata m;
    m.name = "log-counter";
    m.istreams.insert({"logs", bolt::Grouping::GROUP_BY});
    return m;
  }

  private:
  hll::HyperLogLog hll_;
};

int main(int argc, char *argv[]) {
  bolt::logging::glog_init(argv[0]);
  bolt::client::serveComputation(std::make_shared<LogCounter>(), argc, argv);
  return 0;
}
