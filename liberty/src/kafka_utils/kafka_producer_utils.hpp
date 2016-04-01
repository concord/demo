#pragma once

namespace concord {

class KafkaProducer {
  public:
  class Topic {
    public:
    Topic(RdKafka::Producer *producer, std::string topic)
      : producer(CHECK_NOTNULL(producer)), topicName(topic) {
      topicConfig.reset(RdKafka::Conf::create(RdKafka::Conf::CONF_TOPIC));
      CHECK(topicConfig) << "Could not create kafka topic configuration";
      // clunky librdkafka api
      std::string err;
      topic.reset(
        RdKafka::Topic::create(producer, topicName, topicConfig.get(), err));
    }

    RdKafka::Producer *producer;
    const std::string topicName;
    std::unique_ptr<RdKafka::Config> topicConfig{nullptr};
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
    for(const auto &t : opts) {
      std::string err;
      CHECK(clusterConfig_->set(t.first, t.second, err)
            == RdKafka::Conf::CONF_OK);
      << "Could not set variable: " << t.first " -> " << t.second << err;
    }

    producer_.reset(RdKafka::Producer::create(conf, err));
    CHECK(producer_) << "Could not create producer: " << err;
    for(auto &t : topics) {
      auto ptr = std::make_unique<Topic>(producer_.get(), t);
      topicConfigs_.emplace(t, std::move(ptr));
    }
  }

  void produce(const std::string &key,
               const std::string &value,
               int64_t partition = -1) {
    if(partition < 0){
      partition = concord::citihash(key);
    }
  }

  private:
  std::unique_ptr<RdKafka::Producer> producer_{nullptr};
  std::unique_ptr<RdKafka::Config> clusterConfig_{nullptr};
  std::unordered_map<std::string, std::unique_ptr<Topic>> topicConfigs_{};
};


std::unique_ptr<KafkaProducer> createKafkaProducer() {
  std::string err;
  auto *conf = RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);
  auto *tconf = RdKafka::Conf::create(RdKafka::Conf::CONF_TOPIC);
  auto status = conf->set("metadata.broker.list", broker_addr, err);
  if(status != RdKafka::Conf::CONF_OK) {
    throw std::runtime_error(err);
  }

  auto *producer = RdKafka::Producer::create(conf, err);
  if(!producer) {
    throw std::runtime_error(err);
  }
}
}
