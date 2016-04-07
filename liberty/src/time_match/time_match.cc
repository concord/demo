#include <string>
#include <map>
#include <gflags/gflags.h>
#include <concord/glog_init.hpp>
#include <boost/algorithm/searching/boyer_moore.hpp>
#include "cassandra/CassandraClient.hpp"
#include "utils/TimeWindow.hpp"
#include "utils/parse_utils.hpp"

DEFINE_string(kafka_topic, "", "Kafka topic that consumer is reading from");
DEFINE_string(cassandra_nodes, "127.0.0.1", "Cassandra endpoints");
DEFINE_string(cassandra_keyspace, "", "Cassandra keyspace");
DEFINE_string(cassandra_table, "irq", "Cassandra table name");
DEFINE_int64(window_length, 10, "Amount of time(s) to aggregate records");
DEFINE_int64(slide_interval, 10, "Amount of time(s) between new windows");

namespace concord {
using ReducerType = std::map<std::string, std::string>;

class WindowedPatternMatcher : public TimeWindow<ReducerType> {
  public:
  WindowedPatternMatcher(const std::string &cassandraNodes,
                         const std::string &cassandraKeyspace,
                         const std::string &cassandraTable,
                         const TimeWindowOptions<ReducerType> &opts)
    : TimeWindow<ReducerType>(addOptionsCallbacks(opts))
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
              << " unique results";
  }

  void reduceEvents(ReducerType &a, const bolt::FrameworkRecord *b) {
    static const std::string kLogPattern("IRQ");
    const auto &log = b->value;
    if(log.end() != boost::algorithm::boyer_moore_search(log.begin(), log.end(),
                                                         kLogPattern.begin(),
                                                         kLogPattern.end())) {
      const auto kv(buildKeyAndValue(log));
      const auto &key = std::get<0>(kv);
      const auto &value = std::get<1>(kv);
      if(!key.empty() && !value.empty()) {
        a[key] = value;
      }
    }
  }

  private:
  TimeWindowOptions<ReducerType>
  addOptionsCallbacks(const TimeWindowOptions<ReducerType> &opts) {
    using namespace std::placeholders;
    return TimeWindowOptions<ReducerType>(opts)
      .setReducerFunction(
         std::bind(&WindowedPatternMatcher::reduceEvents, this, _1, _2))
      .setWindowerResultFunction(
        std::bind(&WindowedPatternMatcher::windowerFinished, this, _1, _2));
  }

  const std::string cassandraTable_;
  std::unique_ptr<CassandraClient> cassClient_;
};


std::shared_ptr<WindowedPatternMatcher> timeMatchFactory() {
  std::set<bolt::Metadata::StreamGrouping> istreams{
    {FLAGS_kafka_topic, bolt::Grouping::GROUP_BY}};
  const auto baseOpts =
    TimeWindowOptions<ReducerType>()
      .setWindowLength(std::chrono::seconds(FLAGS_window_length))
      .setSlideInterval(std::chrono::seconds(FLAGS_slide_interval))
      .setComputationMetadata(bolt::Metadata("time-match", istreams));
  return std::make_shared<WindowedPatternMatcher>(
    FLAGS_cassandra_nodes, FLAGS_cassandra_keyspace, FLAGS_cassandra_table,
    baseOpts);
}
}

int main(int argc, char *argv[]) {
  bolt::logging::glog_init(argv[0]);
  google::SetUsageMessage("Start WindowedPatternMatcher operator\n"
                          "Usage:\n"
                          "\tkafka topic\t--kafka_topic topic_name \\\n"
                          "\twindow length\t--window_length time_sec \\\n"
                          "\tslide interval\t--slide_interval time_sec \\\n"
                          "\n");
  google::ParseCommandLineFlags(&argc, &argv, true);
  bolt::client::serveComputation(concord::timeMatchFactory(), argc, argv);
  return 0;
}
