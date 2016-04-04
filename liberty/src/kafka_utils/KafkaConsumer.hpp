#pragma once
#include <librdkafka/rdkafkacpp.h>
#include <folly/String.h>
#include <glog/logging.h>

namespace concord {

// TODO(agallego) - add ability to read a properties file
//
// THE ONLY IMPORTANT callback is RdKafka::RebalanceCb. This callback is
// necessary  for setting:
//
// *  (librdkafka's CONFIGURATION.md) group.id, session.timeout.ms,
// *      partition.assignment.strategy, etc.
//
class KafkaConsumer : public RdKafka::EventCb,
                      public RdKafka::OffsetCommitCb,
                      public RdKafka::RebalanceCb {
  public:
  class KafkaConsumerTopicMetadata {
    public:
    Topic(const std::string topicName, const bool fromBegining = false)
      : topicName(topicName)
      , startOffset(fromBegining ? RdKafka::Topic::OFFSET_BEGINNING :
                                   RdKafka::Topic::OFFSET_STORED) {}
    const std::string topicName;
    const int64_t startOffset;
  };

  struct KafkaConsumerTopicMetrics {
    int64_t currentOffset{0};
    uint64_t bytesSent{0};
    uint64_t msgsSent{0};
    std::ostream &operator<<(std::ostream &o, Topic &rhs) {
      o << "Topic: " << topicName << ", offset: " << currentOffset
        << ", bytesSent: " << bytesSent << ", msgsSent: " << msgsSent;
      return o;
    }
    void updateMetrics(RdKafka::Message *msg) {}
  };

  KakfaConsumer(std::vector<std::string> &brokers,
                std::vector<KafkaConsumerTopicMetadata> &topics,
                const std::map<std::string, std::string> &opts = {})
    : clusterConfig_(RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL))
    , topicMetadata_(tpics) {
    defaultTopicConf_.reset(RdKafka::Conf::create(RdKafka::Conf::CONF_TOPIC));
    std::string err;
    LOG_IF(FATAL, clusterConfig_->set("event_cb", (RdKafka::EventCb *)this, err)
                    != RdKafka::Conf::CONF_OK)
      << err;
    LOG_IF(FATAL, clusterConfig_->set("offset_commit_cb",
                                      (RdKafka::OffsetCommitCb *)this, err)
                    != RdKafka::Conf::CONF_OK)
      << err;
    LOG_IF(FATAL,
           clusterConfig_->set("rebalance_cb", (RdKafka::RebalanceCb *)this,
                               err) != RdKafka::Conf::CONF_OK)
      << err;
    // TOPIC!
    LOG_IF(FATAL, tconf->set("auto.offset.reset", "smallest", err)
                    != RdKafka::Conf::CONF_OK)
      << err;

    std::map<std::string, std::string> defaultOpts{
      {"metadata.broker.list", folly::join(", ", brokers)},
      {"group.id", "concord_group_id_" + folly::join(".", topics)},
      {"client.id", "concord_client_id_" + std::to_string(rand_.nextRand())},
      {"statistics.interval.ms", "60000"}, // every minute
      {"queued.min.messages", "10240"},    // needed for queues of msgs
      {"default_topic_conf", defaultTopicConf_.get()}};

    for(const auto &t : defaultOpts) {
      if(opts.find(t.fist) == opts.end()) {
        opts.insert(t);
      }
    }

    LOG_IF(INFO, opts.find("compression.codec") == opts.end())
      << "No kafka codec selected. Consider using compression.codec:snappy "
         "when producing and consuming";

    for(const auto &t : opts) {
      LOG(INFO) << "Kafka " << RdKafka::version_str() << ". " << t.first << ":"
                << t.second;
      LOG_IF(ERROR, clusterConfig_->set(t.first, t.second, err)
                      != RdKafka::Conf::CONF_OK)
        << "Could not set variable: " << t.first << " -> " << t.second << err;
    }
    consumer_.reset(RdKafka::KafkaConsumer::create(clusterConfig_.get(), err));
    LOG_IF(FATAL, !consumer_) << err;
    std::vector<std::string> topicNames{};
    for(auto &t : topicMetadata_) {
      topicNames.push_back(t.topicName);
    }
    RdKafka::ErrorCode err = consumer->subscribe(topicNames);
    LOG_IF(FATAL, err != RdKafka::ErrorCode::ERR_NO_ERROR)
      << "Could not subscribe consumers to: " << folly::join(", ", topics)
      << ". Error: " << RdKafka::err2str(err);
    LOG(INFO) << "Configuration: " << folly::join(" ", *clusterConfig_->dump());
  }
  ~KafkaConsumer() {
    // FIXME(agallego)for loop and print
    // all the stats per partition
    consumer->commitSync();
    consumer->close();
  }
  std::string name() { return consumer_->name(); }
  // blocking call
  void consume(std::function<bool(RdKafka::Message *message)> fn) {
    bool run = true;
    while(run) {
      auto m = unique_ptr<RdKafka::Message>(consumer_->poll(1));
      switch(m->err()) {
      case RdKafka::ERR__TIMED_OUT:
        break;
      case RdKafka::ERR_NO_ERROR:
        updateMetrics(m.get());
        run = fn(m.get());
        break;
      case RdKafka::ERR__PARTITION_EOF:
        LOG(INFO) << "Reached end of parition for topic: " << m->topic();
        break;
      case RdKafka::ERR__UNKNOWN_TOPIC:
      case RdKafka::ERR__UNKNOWN_PARTITION:
        LOG(FATAL) << "Consume failed. Unknown parition|topic: "
                   << message.errstr();
        break;
      default:
        LOG(FATAL) << "Consume failed. Uknown reason: " << message.errstr();
        break;
      }
    }
    LOG(INFO) << "Exiting consume loop";
  }
  void commit() { consumer_->commitAsync(); }

  public:
  // callbacks
  // RdKafka::OffsetCommitCb methods
  void
  offset_commit_cb(RdKafka::ErrorCode err,
                   std::vector<RdKafka::TopicPartition *> &offsets) override {
    if(err == RdKafka::ERR__NO_OFFSET) {
      /* No offsets to commit, dont report anything. */
      return;
    }
    for(const auto &o : offsets) {
      LOG(INFO) << "Commited offset for: " << o->topic()
                << ", offset: " << o->offset()
                << ", partition: " << o->partition();
    }
  }
  // RdKafka::RebalanceCb
  void rebalance_cb(RdKafka::KafkaConsumer *consumer,
                    RdKafka::ErrorCode err,
                    std::vector<RdKafka::TopicPartition *> &partitions) {
    const auto assignment =
      (err == RdKafka::ERR__ASSIGN_PARTITIONS ? "Assigned" : "Revoked");

    if(err == RdKafka::ERR__ASSIGN_PARTITIONS) {
      for(auto &p : partitions) {
        for(auto &m : topicMetadata_) {
          if(m.topicName == p->topic()) {
            LOG(INFO) << "Setting the offset for: " << m.topicName
                      << ", to: " << m.startOffset << ", from: " << p->offset()
                      << ", on partition: " << p->partition();
            p->set_offset(m.startOffset);
            topicConfigs_[p->topic()][p->partition()]();
          }
        }
      }
    }
    if(err == RdKafka::ERR__REVOKE_PARTITIONS) {
      LOG(INFO) << "Unassigning all partitions";
      consumer->unassign();
    } else {
      LOG(INFO) << "Assigning partitions";
      consumer->assign(partitions);
    }
  }

  // RdKafka::EventCb methods
  // messages from librdkafka, not from the brokers.
  void event_cb(RdKafka::Event &event) {
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

  private:
  void updateMetrics(RdKafka::Message *msg) {

  }

  std::unique_ptr<RdKafka::Conf> clusterConfig_{nullptr};
  std::unique_ptr<RdKafka::Conf> defaultTopicConf_{nullptr};
  std::unique_ptr<RdKafka::KafkaConsumer> consumer_{nullptr};
  std::vector<KafkaConsumerTopicMetadata> topicMetadata_{};
  std::unordered_map<std::string,
                     std::unordered_map<int64_t, KafkaConsumerTopicMetrics>>
    topicConfigs_{};
  Random rand_;
};
}
