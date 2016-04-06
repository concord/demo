#include <string>
#include <queue>
#include <concord/Computation.hpp>
#include <concord/glog_init.hpp>
#include <concord/time_utils.hpp>
#include <gflags/gflags.h>
#include <boost/algorithm/searching/boyer_moore.hpp>
#include "cassandra/CassandraClient.hpp"
#include "utils/parse_utils.hpp"

DEFINE_string(kafka_topic, "", "Kafka topic that consumer is reading from");
DEFINE_string(cassandra_nodes, "127.0.0.1", "Cassandra endpoints");
DEFINE_string(cassandra_keyspace, "", "Cassandra keyspace");
DEFINE_string(cassandra_table, "irq", "Cassandra table name");

namespace concord {
class PatternMatcher : public bolt::Computation {
  public:
  using CtxPtr = bolt::Computation::CtxPtr;

  PatternMatcher(const std::string &cassandraNodes,
                 const std::string &cassandraKeyspace,
                 const std::string &cassandraTable,
                 const std::string &kafkaTopicName)
    : cassandraTable_(cassandraTable)
    , kafkaTopicName_(kafkaTopicName)
    , cassClient_(new CassandraClient(cassandraNodes, cassandraKeyspace)) {
    LOG_IF(FATAL, !cassClient_->isConnected())
      << "Cassandra Client could not establish a connection to cassandra";
  }

  void init(CtxPtr ctx) override {
    LOG(INFO) << "Pattern Matcher initialized...";
    ctx->setTimer("loop", bolt::timeNowMilli() + 1000);
  }
  void destroy() override {
    LOG(INFO) << "Pattern Matcher closing connection to cassandra";
    cassClient_ = nullptr; // Call destructor, gracefully shutdown
  }

  void processTimer(CtxPtr ctx, const std::string &key, int64_t time) override {
    cassClient_->wait();
    ctx->setTimer("loop", bolt::timeNowMilli() + 1000);
  }

  void processRecord(CtxPtr ctx, bolt::FrameworkRecord &&r) override {
    static const std::string kLogPattern("IRQ");
    const auto &log = r.value;

    // Use boyer moore search algorithm to quickly find pattern in log.
    // If this record contains a match, then write a unique key to cassandra.
    if(log.end() != boost::algorithm::boyer_moore_search(log.begin(), log.end(),
                                                         kLogPattern.begin(),
                                                         kLogPattern.end())) {
      const auto kv(buildKeyAndValue(log));
      const auto &key = std::get<0>(kv);
      const auto &value = std::get<1>(kv);
      if(!key.empty() && !value.empty()) {
        cassClient_->asyncInsert(cassandraTable_, key, value);
      }
    }
  }

  bolt::Metadata metadata() override {
    std::set<bolt::Metadata::StreamGrouping> istreams{
      {kafkaTopicName_, bolt::Grouping::GROUP_BY}};
    return bolt::Metadata("match", istreams);
  }

  private:
  const std::string cassandraTable_;
  const std::string kafkaTopicName_;
  std::unique_ptr<CassandraClient> cassClient_;
};
}

int main(int argc, char *argv[]) {
  bolt::logging::glog_init(argv[0]);
  google::SetUsageMessage(
    "Start PatternMatch operator\n"
    "Usage:\n"
    "\tmatch\t--kafka_topic topic_name"
    "\t--cassandra_nodes cass_nodes \tcassandra_table table"
    "\t--kafka_topic topic_name\\\n"
    "\n");
  google::ParseCommandLineFlags(&argc, &argv, true);
  bolt::client::serveComputation(std::make_shared<concord::PatternMatcher>(
                                   FLAGS_cassandra_nodes,
                                   FLAGS_cassandra_keyspace,
                                   FLAGS_cassandra_table, FLAGS_kafka_topic),
                                 argc, argv);
  return 0;
}
