#include <memory>
#include <unordered_map>
#include <algorithm>
#include <sstream>
#include <bloom.h>
#include <re2/re2.h>
#include <concord/glog_init.hpp>
#include <concord/Computation.hpp>
#include <concord/time_utils.hpp>
#include "kafka_utils/KafkaProducer.hpp"


DEFINE_string(kafka_brokers, "localhost:9092", "seed kafka brokers");
DEFINE_string(kafka_unique_topic_out,
              "liberty_unique",
              "topic to output uniques");

std::ostream &operator<<(std::ostream &o, const struct bloom &bloom) {
  o << "Bloom filter: { ->entries = " << bloom.entries << ","
    << " ->error = " << bloom.error << ", ->bits = " << bloom.bits
    << ", ->bits per elem = " << bloom.bpe << ", ->bytes = " << bloom.bytes
    << ", ->buckets = " << bloom.buckets
    << ", ->bucket_bytes = " << bloom.bucket_bytes
    << ", ->bucket_bytes_exponent = " << bloom.bucket_bytes_exponent
    << " ->bucket_bits_fast_mod_operand = "
    << bloom.bucket_bits_fast_mod_operand
    << ", ->hash functions = " << bloom.hashes << "}";
  return o;
}


class Unique final : public bolt::Computation {
  public:
  using CtxPtr = bolt::Computation::CtxPtr;
  // 10 Million unique entries
  Unique() {
    bloom_init(&bloom_, 265'569'231llu, 0.08);
    std::vector<std::string> brokers;
    folly::split(",", FLAGS_kafka_brokers, brokers);
    std::vector<std::string> topics = {FLAGS_kafka_unique_topic_out};
    kafkaProducer_.reset(new concord::KafkaProducer(brokers, topics));
  }
  virtual void init(CtxPtr ctx) override {
    ctx->setTimer("print_loop", bolt::timeNowMilli());
    LOG(INFO) << "Initializing unique with bloom_: " << bloom_;
  }

  virtual void destroy() override {
    LOG(INFO) << "Destructing unique with bloom_: " << bloom_;
    bloom_free(&bloom_);
  }

  virtual void processRecord(CtxPtr ctx, bolt::FrameworkRecord &&r) override {
    static RE2 regex("-\\s(\\d+)\\s\\d+\\.\\d+\\.\\d+\\s\\w+\\s\\w+\\s\\d+"
                     "\\s\\d+:\\d+:\\d+\\s\\w+@\\w+(.*)$");
    ++recordCount_;
    long date = 0;
    std::string log = "";

    if(RE2::FullMatch(r.value, regex, &date, &log)) {
      date *= 1000; // millis
      if(bloom_check(&bloom_, (void *)log.c_str(), log.length()) == 0) {
        bloom_add(&bloom_, (void *)log.c_str(), log.length());
        ++uniqueRecords_;
        produceToKafka(std::to_string(date), r.value);
      }
    }
  }


  virtual void
  processTimer(CtxPtr ctx, const std::string &key, int64_t time) override {
    ctx->setTimer(key, bolt::timeNowMilli() + 10000);
    LOG(INFO) << "unique records: " << uniqueRecords_
              << ", total records: " << recordCount_ << ", bloom_: " << bloom_;
  }

  virtual bolt::Metadata metadata() override {
    bolt::Metadata m;
    m.name = "unique";
    m.istreams.insert({"liberty", bolt::Grouping::GROUP_BY});
    return m;
  }

  private:
  void produceToKafka(const std::string &key, const std::string &value) {
    kafkaProducer_->produce(FLAGS_kafka_unique_topic_out, key, value);
  }
  struct bloom bloom_;
  uint64_t recordCount_{0};
  uint64_t uniqueRecords_{0};
  std::unique_ptr<concord::KafkaProducer> kafkaProducer_{nullptr};
};

int main(int argc, char *argv[]) {
  bolt::logging::glog_init(argv[0]);
  bolt::client::serveComputation(std::make_shared<Unique>(), argc, argv);
  return 0;
}
