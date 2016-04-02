#pragma once
#include <city.h>
#include <librdkafka/rdkafkacpp.h>
#include <folly/String.h>
#include <glog/logging.h>

namespace concord {

class KafkaProducer {
  public:
  class Topic {
    public:
    Topic(RdKafka::Producer *producer, std::string topicName)
      : producer(CHECK_NOTNULL(producer)), topicName(topicName) {
      std::map<std::string, std::string> opts{
        {"queue.buffering.max.messages", "10000000"},
        {"num.partitions", "144"}};

      topicConfig.reset(RdKafka::Conf::create(RdKafka::Conf::CONF_TOPIC));
      LOG_IF(FATAL, !topicConfig)
        << "Could not create kafka topic configuration";
      for(const auto &t : opts) {
        std::string err;
        LOG_IF(FATAL, topicConfig->set(t.first, t.second, err)
                        != RdKafka::Conf::CONF_OK)
          << "Could not set variable: " << t.first << " -> " << t.second << err;
      }
      // clunky librdkafka api
      std::string err;
      topic.reset(
        RdKafka::Topic::create(producer, topicName, topicConfig.get(), err));
    }

    RdKafka::Producer *producer;
    const std::string topicName;
    std::unique_ptr<RdKafka::Conf> topicConfig{nullptr};
    std::unique_ptr<RdKafka::Topic> topic{nullptr};
  };


  // The librdkafka producer API is awkward
  // This is a thin wrapper around it, so one can actually use it
  KafkaProducer(std::vector<std::string> &brokers,
                std::vector<std::string> &topics,
                // take this by value
                std::map<std::string, std::string> opts = {})
    : clusterConfig_(RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL)) {

    opts.insert({"metadata.broker.list", folly::join(", ", brokers)});
    opts.insert({"queue.buffering.max.messages", "10000000"});
    opts.insert({"num.partitions", "144"});
    std::string err;
    for(const auto &t : opts) {
      LOG_IF(FATAL, clusterConfig_->set(t.first, t.second, err)
                      != RdKafka::Conf::CONF_OK)
        << "Could not set variable: " << t.first << " -> " << t.second << err;
    }
    producer_.reset(RdKafka::Producer::create(clusterConfig_.get(), err));
    LOG_IF(FATAL, !producer_) << "Could not create producer: " << err;
    for(auto &t : topics) {
      auto ptr = std::make_unique<Topic>(producer_.get(), t);
      topicConfigs_.emplace(t, std::move(ptr));
    }
  }

  void produce(const std::string &topic,
               const std::string &key,
               const std::string &value,
               int64_t partition = -1) {
    assert(topicConfigs_.find(topic) != topicConfigs_.end());

    if(partition < 0) {
      auto tmp = CityHash64(key.c_str(), key.length());
      partition = reinterpret_cast<int64_t &>(tmp) % 144;
    }
    auto &t = topicConfigs_[topic]; // reference to the unique ptr

    auto maxTries = 10;
    RdKafka::ErrorCode resp;
    while(maxTries-- > 0
          && (resp = t->producer->produce(
                t->topic.get(), partition, RdKafka::Producer::RK_MSG_COPY,
                (char *)value.c_str(), value.length(), &key, NULL))
          && resp != RdKafka::ERR_NO_ERROR) {
      LOG(ERROR) << "Issue when producing: " << RdKafka::err2str(resp);
    }
    bytesSent_ += value.length() + key.length();
    ++msgsSent_;
    if(msgsSent_ % 1'000'000llu == 0) {
      t->producer->poll(5);
      LOG(INFO) << "Total msgs sent: " << msgsSent_
                << ", total bytes sent: " << bytesSent_;
    }
  }

  private:
  uint64_t bytesSent_{0};
  uint64_t msgsSent_{0};
  std::unique_ptr<RdKafka::Producer> producer_{nullptr};
  std::unique_ptr<RdKafka::Conf> clusterConfig_{nullptr};
  std::unordered_map<std::string, std::unique_ptr<Topic>> topicConfigs_{};
};
}
