#include <map>
#include <gflags/gflags.h>
#include <hyperloglog/hyperloglog.hpp>
#include <concord/glog_init.hpp>
#include <concord/time_utils.hpp>
#include <concord/Computation.hpp>
#include <re2/re2.h>

DEFINE_string(kafka_topic,
              "default_topic",
              "Kafka topic that consumer is reading from");

// Must be between [4, 30] otherwise std::invalid_argument is thrown from HLL
static const uint8_t kHllRegisterSize = 10;

class LogCounter final : public bolt::Computation {
  public:
  using CtxPtr = bolt::Computation::CtxPtr;

  LogCounter(const std::string &topic) : kafkaTopic_(topic) {}

  virtual void init(CtxPtr ctx) override {
    LOG(INFO) << "Log Counter initialized.. ready for events";
    LOG(INFO) << "Source kafka topic: " << kafkaTopic_;
    ctx->setTimer("loop", bolt::timeNowMilli());
  }

  virtual void destroy() override { LOG(INFO) << "Shutting down"; }

  virtual void
  processTimer(CtxPtr ctx, const std::string &key, int64_t time) override {
    for(const auto &month : data_) {
      LOG(INFO) << "Data for the month of: " << month.first;
      for(const auto &year : month.second) {
        LOG(INFO) << "Cardinality within year: " << year.first
                  << ".... : " << year.second.estimate();
      }
    }
    LOG(INFO) << "Map size: " << data_.size();
    ctx->setTimer("loop", bolt::timeNowMilli() + 2000);
  }

  virtual void processRecord(CtxPtr ctx, bolt::FrameworkRecord &&r) override {
    static RE2 simpleDateRegex("-\\s\\d+\\s(\\d+)\\.\\d+\\.(\\d+)\\s\\w+\\s\\w+"
                               "\\s\\d+\\s\\d+:\\d+:\\d+\\s\\S+\\s(.*)$");
    int year, month;
    std::string log;
    if(RE2::FullMatch(r.key, simpleDateRegex, &year, &month, &log)) {
      if(data_.find(month) == data_.end()) {
        data_[month].emplace(year, kHllRegisterSize);
      } else {
        auto &yearMap = data_[month];
        if(yearMap.find(year) == yearMap.end()) {
          yearMap.emplace(year, kHllRegisterSize);
        } else {
          data_[month][year].add(r.key.c_str(), r.key.size());
        }
      }
    }
  }

  virtual bolt::Metadata metadata() override {
    bolt::Metadata m;
    m.name = "log-counter";
    m.istreams.insert({kafkaTopic_, bolt::Grouping::GROUP_BY});
    return m;
  }

  private:
  // Group by month, then year
  std::map<int, std::map<int, hll::HyperLogLog>> data_;
  const std::string kafkaTopic_;
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
