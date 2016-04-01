#include <gflags/gflags.h>
#include <hyperloglog/hyperloglog.hpp>
#include <concord/glog_init.hpp>
#include <concord/time_utils.hpp>
#include <concord/Computation.hpp>

DEFINE_string(kafka_topic,
              "default_topic",
              "Kafka topic that consumer is reading from");

class LogCounter final : public bolt::Computation {
  public:
  using CtxPtr = bolt::Computation::CtxPtr;

  LogCounter(const std::string &topic) : hll_(10), kafkaTopic_(topic) {}

  virtual void init(CtxPtr ctx) override {
    LOG(INFO) << "Log Counter initialized.. ready for events";
    LOG(INFO) << "Source kafka topic: " << kafkaTopic_;
    ctx->setTimer("loop", bolt::timeNowMilli() * 2000);
  }

  virtual void destroy() override {}

  virtual void
  processTimer(CtxPtr ctx, const std::string &key, int64_t time) override {
    LOG(INFO) << "Cardinality: " << hll_.estimate();
    ctx->setTimer("loop", bolt::timeNowMilli() * 2000);
  }

  virtual void processRecord(CtxPtr ctx, bolt::FrameworkRecord &&r) override {}

  virtual bolt::Metadata metadata() override {
    bolt::Metadata m;
    m.name = "log-counter";
    m.istreams.insert({kafkaTopic_, bolt::Grouping::GROUP_BY});
    return m;
  }

  private:
  hll::HyperLogLog hll_;
  std::string kafkaTopic_;
};

int main(int argc, char *argv[]) {
  bolt::logging::glog_init(argv[0]);
  google::SetUsageMessage("Start LogCounter operator\n"
                          "Usage:\n"
                          "\tlog_counter\t--kafka_topic topic_name \\\n"
                          "\n");
  google::ParseCommandLineFlags(&argc, &argv, true);
  if(FLAGS_kafka_topic == "default_topic") {
    LOG(FATAL) << "Topic not found";
  }
  bolt::client::serveComputation(
    std::make_shared<LogCounter>(FLAGS_kafka_topic), argc, argv);
  return 0;
}
