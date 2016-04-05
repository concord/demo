#include <thread>
#include <gflags/gflags.h>
#include <memory>
#include <folly/ProducerConsumerQueue.h>

#include <concord/glog_init.hpp>
#include <concord/Computation.hpp>
#include <concord/time_utils.hpp>

#include "kafka_utils/HighLevelKafkaConsumer.hpp"


DEFINE_string(kafka_brokers, "localhost:9092", "seed kafka brokers");
DEFINE_string(kafka_topics, "", "coma delimited list of topics");
DEFINE_bool(kafka_topics_consume_from_beginning,
            false,
            "should the driver consume from the begining");

class KafkaSource final : public bolt::Computation {
  public:
  using CtxPtr = bolt::Computation::CtxPtr;
  KafkaSource() {
    std::vector<std::string> brokers;
    folly::split(",", FLAGS_kafka_brokers, brokers);
    folly::split(",", FLAGS_kafka_topics, ostreams_);
    std::vector<concord::KafkaConsumerTopicMetadata> topics;
    for(auto &s : ostreams_) {
      topics.emplace_back(s, FLAGS_kafka_topics_consume_from_beginning);
    }
    kafkaConsumer_.reset(new concord::HighLevelKafkaConsumer(brokers, topics));
  }
  virtual void init(CtxPtr ctx) override {
    ctx->setTimer("print_loop", bolt::timeNowMilli());
    LOG_IF(FATAL, FLAGS_kafka_topics.empty()) << "Empty --kafka-topics flag";
    std::thread([this]() mutable {
      kafkaConsumer_->consume([this](std::unique_ptr<RdKafka::Message> msg) {
        while(!queue_.write(std::move(msg))) {
          // this thread's job is just to read
        }
        return kafkaPoll_;
      });
    }).detach();
  }

  virtual void destroy() override {
    kafkaPoll_ = false;
    kafkaConsumer_ = nullptr;
  }

  virtual void processRecord(CtxPtr ctx, bolt::FrameworkRecord &&r) override {}


  virtual void
  processTimer(CtxPtr ctx, const std::string &key, int64_t time) override {
    ctx->setTimer(key, time); // do it now again :)
    auto maxRecords = 10240;
    std::unique_ptr<RdKafka::Message> msg{nullptr};
    while(maxRecords-- > 0 && queue_.read(msg)) {
      ctx->produceRecord(msg->topic_name(), *msg->key(),
                         std::string((char *)msg->payload(), msg->len()));
    }
  }

  virtual bolt::Metadata metadata() override {
    bolt::Metadata m;
    m.name = "kafka_source_" + folly::join(".", ostreams_);
    m.ostreams = std::set<std::string>(ostreams_.begin(), ostreams_.end());
    return m;
  }

  private:
  bool kafkaPoll_{true};
  std::vector<std::string> ostreams_{};
  std::unique_ptr<concord::HighLevelKafkaConsumer> kafkaConsumer_{nullptr};
  folly::ProducerConsumerQueue<std::unique_ptr<RdKafka::Message>> queue_{20480};
};

int main(int argc, char *argv[]) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  bolt::logging::glog_init(argv[0]);
  bolt::client::serveComputation(std::make_shared<KafkaSource>(), argc, argv);
  /*
   * Wait for RdKafka to decommission.
   * This is not strictly needed (when check outq_len() above), but
   * allows RdKafka to clean up all its resources before the application
   * exits so that memory profilers such as valgrind wont complain about
   * memory leaks.
   */
  RdKafka::wait_destroyed(5000);
  return 0;
}
