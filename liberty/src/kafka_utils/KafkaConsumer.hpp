#pragma once
#include <librdkafka/rdkafkacpp.h>
#include <folly/String.h>
#include <glog/logging.h>

namespace concord {
  // consumecb is not really needed, I wanna bring my own callback
  // need hooks just for commit of offsets.
  // suggest fot the user to have a codec
  class KafkaConsumer: public RdKafka::EventCb,  public RdKafka::ConsumeCb{
  public:
    KakfaConsumer(
                std::vector<std::string> &brokers,
                std::vector<std::string> &topics,
                const std::map<std::string, std::string> &opts = {})
    : clusterConfig_(RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL)) {
      std::string err;
  LOG_IF(FATAL, clusterConfig_->set("event_cb", this, err)
         != RdKafka::Conf::CONF_OK) << ". " << err;

  auto tconf = std::unique_ptr<RdKafka::Conf>(RdKafka::Conf::create(RdKafka::Conf::CONF_TOPIC));
  std::map<std::string, std::string> defaultOpts {
    {"metadata.broker.list", folly::join(", ", brokers)},
      {"group.id", "concord_group_id_" + folly::join(".", topics)},
        {"statistics.interval.ms", "1000"},
          ("default_topic_conf", tconf.get());
  };

  for(const auto &t : defaultOpts) {
    if(opts.find(t.fist) == opts.end()){
      opts.insert(t);
    }
  }

  LOG_IF(INFO, opts.find("compression.codec") == opts.end()) << "No kafka codec selected. Consider using compression.codec:snappy when producing and consuming";

    for(const auto &t : opts) {
      LOG(INFO) << "Kafka " << RdKafka::version_str() << ". " << t.first << ":" <<
        t.second;
      LOG_IF(ERROR, clusterConfig_->set(t.first, t.second, err)
                      != RdKafka::Conf::CONF_OK)
        << "Could not set variable: " << t.first << " -> " << t.second << err;
    }

    RdKafka::KafkaConsumer *consumer = RdKafka::KafkaConsumer::create(conf, errstr);
    RdKafka::ErrorCode err = consumer->subscribe(topics);
    LOG_IF(FATAL) << "Could not subscribe consumers to: " << folly::join(", ", topics) << ". Error: " <<  RdKafka::err2str(err);
  }
    ~KafkaConsumer(){
  std::cerr << "% Consumed " << msg_cnt << " messages ("
            << msg_bytes << " bytes)" << std::endl;
      consumer->close();
    }
    std::string name() {
      return consumer_->name();
    }
    // blocking call
    void consume(std::function<bool(RdKafka::Message* message)> fn){

    }
    // callbacks
  public:

    // RdKafka::ConsumeCb methods
    // main method for consuming messages
  void consume_cb (RdKafka::Message &msg, void *opaque) {
  switch (message->err()) {
    case RdKafka::ERR__TIMED_OUT:
      break;

    case RdKafka::ERR_NO_ERROR:
      /* Real message */
      msg_cnt++;
      msg_bytes += message->len();
      if (verbosity >= 3)
        std::cerr << "Read msg at offset " << message->offset() << std::endl;
      if (verbosity >= 2 && message->key()) {
        std::cout << "Key: " << *message->key() << std::endl;
      }
      if (verbosity >= 1) {
        printf("%.*s\n",
               static_cast<int>(message->len()),
               static_cast<const char *>(message->payload()));
      }
      break;

    case RdKafka::ERR__PARTITION_EOF:
      /* Last message */
      if (exit_eof && ++eof_cnt == partition_cnt) {
        std::cerr << "%% EOF reached for all " << partition_cnt <<
            " partition(s)" << std::endl;
        run = false;
      }
      break;

    case RdKafka::ERR__UNKNOWN_TOPIC:
    case RdKafka::ERR__UNKNOWN_PARTITION:
      std::cerr << "Consume failed: " << message->errstr() << std::endl;
      run = false;
      break;

    default:
      /* Errors */
      std::cerr << "Consume failed: " << message->errstr() << std::endl;
      run = false;
  }
  }
    // RdKafka::EventCb methods
    // messages from librdkafka, not from the brokers.
  void event_cb (RdKafka::Event &event) {
    switch (event.type())
    {
      case RdKafka::Event::EVENT_ERROR:
        LOG(ERROR) << "Librdkafka error: " << RdKafka::err2str(event.err());
        if (event.err() == RdKafka::ERR__ALL_BROKERS_DOWN) {
          // TODO(agallego)
        }
        break;

      case RdKafka::Event::EVENT_STATS:
        LOG(INFO) << "Librdkafka stats: " << event.str();
        break;
      case RdKafka::Event::EVENT_LOG:
        LOG(INFO) << "Librdkafka log: severity: " <<
          event.severity() << ", fac: " << event.fac()
                  << ", event: " << event.str();
        break;

      case RdKafka::Event::EVENT_THROTTLE:
        std::cerr << "THROTTLED: " << event.throttle_time() << "ms by " <<
          event.broker_name() << " id " << event.broker_id() << std::endl;
        break;

      default:
        LOG(ERROR) << "Librdkafka unknown event: type: " << event.type() <<
          ", str: " << RdKafka::err2str(event.err());
        break;
    }
  }
  private:
    uint64_t bytesConsumed_{0};
    uint64_t msgsSent_{0};
    std::unique_ptr<RdKafka::Conf> clusterConfig_{nullptr};
    std::unique_ptr<RdKafka::KafkaConsumer> consumer_{nullptr};
  };
}




  /*
   * Wait for RdKafka to decommission.
   * This is not strictly needed (with check outq_len() above), but
   * allows RdKafka to clean up all its resources before the application
   * exits so that memory profilers such as valgrind wont complain about
   * memory leaks.
   */
  // RdKafka::wait_destroyed(5000);
  // return 0;
}
