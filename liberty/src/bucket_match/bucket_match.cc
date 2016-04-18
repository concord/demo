#include <string>
#include <map>
#include <gflags/gflags.h>
#include <concord/glog_init.hpp>
#include <boost/algorithm/searching/boyer_moore.hpp>
#include "cassandra/CassandraClient.hpp"
#include "utils/CountWindow.hpp"
#include "utils/parse_utils.hpp"

DEFINE_string(kafka_topic, "", "Kafka topic that consumer is reading from");
DEFINE_string(cassandra_nodes, "127.0.0.1", "Cassandra endpoints");
DEFINE_string(cassandra_keyspace, "", "Cassandra keyspace");
DEFINE_string(cassandra_table, "irq", "Cassandra table name");
DEFINE_int64(window_length, 100000, "# of records to aggregate records");
DEFINE_int64(slide_interval, 100000, "# of records between new windows");

namespace concord {
using ReducerType = std::map<int64_t, std::string>;

/// SAME code as WindowedPatternMatcher, with the exception of the supertype.
/// TODO: establish a way to abstract the similarities.
class BucketedPatternMatcher : public CountWindow<ReducerType> {
  public:
  BucketedPatternMatcher(const std::string &cassandraNodes,
                         const std::string &cassandraKeyspace,
                         const std::string &cassandraTable,
                         const CountWindowOptions<ReducerType> &opts)
    : CountWindow<ReducerType>(addOptionsCallbacks(opts))
    , cassandraTable_(cassandraTable)
    , cassClient_(new CassandraClient(cassandraNodes, cassandraKeyspace)) {
    LOG_IF(FATAL, !cassClient_->isConnected())
      << "Cassandra Client could not establish a connection to cassandra";
  }

  void destroy() override { cassClient_ = nullptr; }

  void windowerFinished(const uint64_t window, const ReducerType &results) {
    for(const auto &entry : results) {
      cassClient_->asyncInsert(cassandraTable_, entry.first, entry.second);
    }
    LOG(INFO) << "Window: " << window << " produced " << results.size()
              << " unique results out of: " << totalRecords << " total records";
    totalRecords = 0;
  }

  void reduceEvents(ReducerType &a, const bolt::FrameworkRecord *b) {
    totalRecords++;
    static const std::string kLogPattern("IRQ");
    const auto &log = b->value;
    if(log.end() != boost::algorithm::boyer_moore_search(log.begin(), log.end(),
                                                         kLogPattern.begin(),
                                                         kLogPattern.end())) {
      const auto kv(buildKeyAndValue(log));
      const auto &key = std::get<0>(kv);
      const auto &value = std::get<1>(kv);
      if(!value.empty()) {
        a[key] = value;
      }
    }
  }

  private:
  CountWindowOptions<ReducerType>
  addOptionsCallbacks(const CountWindowOptions<ReducerType> &opts) {
    using namespace std::placeholders;
    return CountWindowOptions<ReducerType>(opts)
      .setReducerFunction(
         std::bind(&BucketedPatternMatcher::reduceEvents, this, _1, _2))
      .setWindowerResultFunction(
        std::bind(&BucketedPatternMatcher::windowerFinished, this, _1, _2));
  }

  uint64_t totalRecords{0};
  const std::string cassandraTable_;
  std::unique_ptr<CassandraClient> cassClient_;
};


std::shared_ptr<BucketedPatternMatcher> bucketMatchFactory() {
  std::set<bolt::Metadata::StreamGrouping> istreams{
    {FLAGS_kafka_topic, bolt::Grouping::ROUND_ROBIN}};
  const auto baseOpts =
    CountWindowOptions<ReducerType>()
      .setWindowLength(FLAGS_window_length)
      .setSlideInterval(FLAGS_slide_interval)
      .setComputationMetadata(bolt::Metadata("time-match", istreams));
  return std::make_shared<BucketedPatternMatcher>(
    FLAGS_cassandra_nodes, FLAGS_cassandra_keyspace, FLAGS_cassandra_table,
    baseOpts);
}
}

int main(int argc, char *argv[]) {
  bolt::logging::glog_init(argv[0]);
  google::ParseCommandLineFlags(&argc, &argv, true);
  google::SetUsageMessage("Start BucketedPatternMatcher operator\n"
                          "Usage:\n"
                          "\tkafka topic\t--kafka_topic topic_name \\\n"
                          "\twindow length\t--window_length time_sec \\\n"
                          "\tslide interval\t--slide_interval time_sec \\\n"
                          "\n");
  bolt::client::serveComputation(concord::bucketMatchFactory(), argc, argv);
  return 0;
}
