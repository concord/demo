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


int main(int argc, char *argv[]) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  google::InitGoogleLogging(argv[0]);
  google::InstallFailureSignalHandler();


  LOG(INFO) << "Kafka --borkers: " << FLAGS_kafka_brokers;
  LOG(INFO) << "Kafka --topics: " << FLAGS_kafka_topics;
  LOG(INFO) << "Kafka --kafka_topics_consume_from_beginning: "
            << FLAGS_kafka_topics_consume_from_beginning;

  std::unique_ptr<concord::HighLevelKafkaConsumer> kafkaConsumer_;
  std::vector<std::string> brokers;
  std::vector<std::string> ostreams;
  folly::split(",", FLAGS_kafka_brokers, brokers);
  folly::split(",", FLAGS_kafka_topics, ostreams);
  std::vector<concord::KafkaConsumerTopicMetadata> topics;
  for(auto &s : ostreams) {
    topics.emplace_back(s, FLAGS_kafka_topics_consume_from_beginning);
  }
  kafkaConsumer_.reset(new concord::HighLevelKafkaConsumer(brokers, topics));
  kafkaConsumer_->consume([](std::unique_ptr<RdKafka::Message> msg) {
    LOG_EVERY_N(INFO, 100000)
      << "Got msg: " << google::COUNTER << ", topic: " << msg->topic_name()
      << ", partition: " << msg->partition() << ", key: " << *msg->key();
    return true;
  });
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
