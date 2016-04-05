#pragma once
#include <gflags/gflags.h>
#include <map>
#include <librdkafka/rdkafkacpp.h>
#include <folly/String.h>
#include <glog/logging.h>
#include "utils/Random.hpp"


DEFINE_bool(enable_kafka_consumer_debug,
            false,
            "enable debugging hooks _VERY VERBOSE_");


namespace concord {

// TODO(agalleog) - clean up
class KafkaConsumerTopicMetadata {
  public:
  KafkaConsumerTopicMetadata(const std::string &topicName,
                             const bool fromBegining = false)
    : topicName(topicName)
    , startOffset(fromBegining ? RdKafka::Topic::OFFSET_BEGINNING :
                                 RdKafka::Topic::OFFSET_STORED) {}
  std::string topicName;
  int64_t startOffset;
};

class KafkaConsumerTopicMetrics {
  public:
  int64_t currentOffset{0};
  uint64_t bytesReceived{0};
  uint64_t msgsReceived{0};
  std::ostream &operator<<(std::ostream &o) {
    o << "Offset: " << currentOffset << ", bytesReceived: " << bytesReceived
      << ", msgsReceived: " << msgsReceived;
    return o;
  }
  void updateMetrics(const RdKafka::Message *msg) {
    currentOffset = msg->offset();
    bytesReceived += msg->len();
    ++msgsReceived;
  }
};

// TODO(agallego) - add ability to read a properties file
//
// THE ONLY IMPORTANT callback is RdKafka::RebalanceCb. This callback is
// necessary  for setting:
//
// *  (librdkafka's CONFIGURATION.md) group.id, session.timeout.ms,
// *      partition.assignment.strategy, etc.
//
class HighLevelKafkaConsumer : public RdKafka::EventCb,
                               public RdKafka::OffsetCommitCb,
                               public RdKafka::RebalanceCb {
  public:
  HighLevelKafkaConsumer(const std::vector<std::string> &brokers,
                         const std::vector<KafkaConsumerTopicMetadata> &topics,
                         const std::map<std::string, std::string> &opts = {}) {
    clusterConfig_.reset(RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL));
    topicMetadata_ = topics;
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
    LOG_IF(FATAL,
           clusterConfig_->set("default_topic_conf", defaultTopicConf_.get(),
                               err) != RdKafka::Conf::CONF_OK)
      << err;
    // TOPIC!
    LOG_IF(FATAL, defaultTopicConf_->set("auto.offset.reset", "smallest", err)
                    != RdKafka::Conf::CONF_OK)
      << err;

    std::vector<std::string> topicNames{};
    for(auto &t : topicMetadata_) {
      topicNames.push_back(t.topicName);
    }

    std::map<std::string, std::string> defaultOpts{
      {"metadata.broker.list", folly::join(", ", brokers)},
      {"group.id", "concord_group_id_" + folly::join(".", topicNames)},
      {"client.id", "concord_client_id_" + std::to_string(rand_.nextRand())},
      {"receive.message.max.bytes", "100000000"}, // Max receive buff or 100MB
      {"fetch.message.max.bytes", "20000"},       // Some smmalller default
      {"statistics.interval.ms", "60000"},        // every minute
    };
    if(FLAGS_enable_kafka_consumer_debug) {
      defaultOpts.insert({"debug",
                          "all,generic,broker,topic,metadata,producer,"
                          "queue,msg,protocol,cgrp,security,fetch"});
    }
    for(auto &t : opts) {
      if(defaultOpts.find(t.first) == defaultOpts.end()) {
        defaultOpts.insert(t);
      }
    }

    LOG_IF(INFO, defaultOpts.find("compression.codec") == defaultOpts.end())
      << "No kafka codec selected. Consider using compression.codec:snappy "
         "when producing and consuming";

    for(const auto &t : defaultOpts) {
      LOG(INFO) << "Kafka " << RdKafka::version_str() << ". " << t.first << ":"
                << t.second;
      LOG_IF(ERROR, clusterConfig_->set(t.first, t.second, err)
                      != RdKafka::Conf::CONF_OK)
        << "Could not set variable: " << t.first << " -> " << t.second << err;
    }
    consumer_.reset(RdKafka::KafkaConsumer::create(clusterConfig_.get(), err));
    LOG_IF(FATAL, !consumer_) << err;
    LOG_IF(FATAL,
           consumer_->subscribe(topicNames) != RdKafka::ErrorCode::ERR_NO_ERROR)
      << "Could not subscribe consumers to: " << folly::join(", ", topicNames);
    LOG(INFO) << "Configuration: " << folly::join(" ", *clusterConfig_->dump());
  }
  ~HighLevelKafkaConsumer() {
    // FIXME(agallego)for loop and print
    // all the stats per partition
    consumer_->commitSync();
    consumer_->close();
  }
  std::string name() { return consumer_->name(); }
  // blocking call
  void
  consume(std::function<bool(std::unique_ptr<RdKafka::Message> message)> fn) {
    bool run = true;
    while(run) {
      auto m = std::unique_ptr<RdKafka::Message>(consumer_->consume(10));
      switch(m->err()) {
      case RdKafka::ERR__TIMED_OUT:
        break;
      case RdKafka::ERR_NO_ERROR:
        updateMetrics(m.get());
        run = fn(std::move(m));
        break;
      case RdKafka::ERR__PARTITION_EOF:
        break;
      case RdKafka::ERR__UNKNOWN_TOPIC:
      case RdKafka::ERR__UNKNOWN_PARTITION:
        LOG(FATAL) << "Consume failed. Unknown parition|topic: " << m->errstr();
        break;
      default:
        LOG(ERROR) << "Consume failed. Uknown reason: " << m->errstr();
        break;
      }
    }
    LOG(INFO) << "Exiting consume loop";
  }
  void commit() { consumer_->commitAsync(); }

  public:
  // callbacks
  // RdKafka::OffsetCommitCb methods
  virtual void
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
  virtual void
  rebalance_cb(RdKafka::KafkaConsumer *consumer,
               RdKafka::ErrorCode err,
               std::vector<RdKafka::TopicPartition *> &partitions) override {

    LOG(INFO) << "RdKafka::RebalanceCb. partitions: " << partitions.size();

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
  virtual void event_cb(RdKafka::Event &event) override {
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
    topicMetrics_[msg->topic_name()][msg->partition()].updateMetrics(msg);
  }

  std::unique_ptr<RdKafka::Conf> clusterConfig_{nullptr};
  std::unique_ptr<RdKafka::Conf> defaultTopicConf_{nullptr};
  std::unique_ptr<RdKafka::KafkaConsumer> consumer_{nullptr};
  std::vector<KafkaConsumerTopicMetadata> topicMetadata_{};
  std::unordered_map<std::string,
                     std::unordered_map<int32_t, KafkaConsumerTopicMetrics>>
    topicMetrics_{};
  Random rand_{};
};
}
