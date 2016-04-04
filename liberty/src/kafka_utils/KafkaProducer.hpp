#pragma once
#include <librdkafka/rdkafkacpp.h>
#include <folly/String.h>
#include <glog/logging.h>

namespace concord {

class KafkaProducer : public RdKafka::EventCb,
                      public RdKafka::DeliveryReportCb {
  public:
  class Topic {
    public:
    Topic(RdKafka::Producer *producer, std::string topicName)
      : producer(CHECK_NOTNULL(producer)), topicName(topicName) {
      topicConfig.reset(RdKafka::Conf::create(RdKafka::Conf::CONF_TOPIC));
      LOG_IF(FATAL, !topicConfig)
        << "Could not create kafka topic configuration";
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
                const std::map<std::string, std::string> &opts = {})
    : clusterConfig_(RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL)) {

    std::map<std::string, std::string> defaultOpts{
      {"metadata.broker.list", folly::join(", ", brokers)},
      {"queue.buffering.max.messages", "10000000"},
      {"statistics.interval.ms", "1000"}};

    for(auto &t : opts) {
      if(defaultOpts.find(t.first) == defaultOpts.end()) {
        defaultOpts.insert(t);
      }
    }

    std::string err;
    LOG_IF(FATAL,
           clusterConfig_->set("dr_cb", (RdKafka::DeliveryReportCb *)this, err)
             != RdKafka::Conf::CONF_OK)
      << err;
    LOG_IF(FATAL, clusterConfig_->set("event_cb", (RdKafka::EventCb *)this, err)
                    != RdKafka::Conf::CONF_OK)
      << err;

    // set the automatic topic creation and make sure you set these partitions
    // on the kafka broker itself. On the server.properties.
    // num.partitions=144;
    for(const auto &t : defaultOpts) {
      LOG_IF(ERROR, clusterConfig_->set(t.first, t.second, err)
                      != RdKafka::Conf::CONF_OK)
        << "Could not set variable: " << t.first << " -> " << t.second << err;
    }
    producer_.reset(RdKafka::Producer::create(clusterConfig_.get(), err));
    LOG_IF(FATAL, !producer_) << "Could not create producer: " << err;
    for(auto &t : topics) {
      auto ptr = std::make_unique<Topic>(producer_.get(), t);
      topicConfigs_.emplace(t, std::move(ptr));
    }
    LOG(INFO) << "Configuration: " << folly::join(" ", *clusterConfig_->dump());
  }

  // RdKafka::DeliveryReportCb methods
  void dr_cb(RdKafka::Message &message) override {
    bytesKafkaReceived_ += message.len();
    ++msgsKafkaReceived_;
  }

  // RdKafka::EventCb methods
  // messages from librdkafka, not from the brokers.
  void event_cb(RdKafka::Event &event) override {
    switch(event.type()) {
    case RdKafka::Event::EVENT_ERROR:
      LOG(ERROR) << "Librdkafka error: " << RdKafka::err2str(event.err());
      if(event.err() == RdKafka::ERR__ALL_BROKERS_DOWN) {
        LOG(FATAL) << "All brokers are down. Cannot communicate w/ kafka: "
                   << event.str();
      }
      break;

    case RdKafka::Event::EVENT_STATS:
      LOG(INFO) << "Librdkafka stats: " << event.str();
      break;
    case RdKafka::Event::EVENT_LOG:
      LOG(INFO) << "Librdkafka log: severity: " << event.severity()
                << ", fac: " << event.fac() << ", event: " << event.str();
      break;

    case RdKafka::Event::EVENT_THROTTLE:
      std::cerr << "THROTTLED: " << event.throttle_time() << "ms by "
                << event.broker_name() << " id " << event.broker_id()
                << std::endl;
      break;

    default:
      LOG(ERROR) << "Librdkafka unknown event: type: " << event.type()
                 << ", str: " << RdKafka::err2str(event.err());
      break;
    }
  }


  void produce(const std::string &topic,
               const std::string &key,
               const std::string &value,
               int64_t partition = RdKafka::Topic::PARTITION_UA) {
    assert(topicConfigs_.find(topic) != topicConfigs_.end());
    auto &t = topicConfigs_[topic]; // reference to the unique ptr
    auto maxTries = 10;
    RdKafka::ErrorCode resp;
    while(maxTries-- > 0
          && (resp = t->producer->produce(
                t->topic.get(), partition, RdKafka::Producer::RK_MSG_COPY,
                (char *)value.c_str(), value.length(), &key, NULL))
          && resp != RdKafka::ERR_NO_ERROR) {
      t->producer->poll(1);
      LOG(ERROR) << "Issue when producing: " << RdKafka::err2str(resp);
    }
    bytesSent_ += value.length() + key.length();
    ++msgsSent_;
    if(msgsSent_ % 1'000'000llu == 0) {
      t->producer->poll(5);
      LOG(INFO) << "Total msgs sent: " << msgsSent_
                << ", total bytes sent: " << bytesSent_
                << ", bytes received by the broker: " << bytesKafkaReceived_
                << ", msgs received by broker: " << msgsKafkaReceived_;
    }
  }

  private:
  uint64_t bytesSent_{0};
  uint64_t msgsSent_{0};
  uint64_t bytesKafkaReceived_{0};
  uint64_t msgsKafkaReceived_{0};
  std::unique_ptr<RdKafka::Producer> producer_{nullptr};
  std::unique_ptr<RdKafka::Conf> clusterConfig_{nullptr};
  std::unordered_map<std::string, std::unique_ptr<Topic>> topicConfigs_{};
};
}
