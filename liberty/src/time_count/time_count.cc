#include <gflags/gflags.h>
#include <concord/glog_init.hpp>
#include "utils/TimeWindow.hpp"

DEFINE_string(kafka_topic, "", "Kafka topic that consumer is reading from");
DEFINE_int64(window_length, 1, "Amount of time(s) to aggregate records");
DEFINE_int64(slide_interval, 2, "Amount of time(s) between new windows");

namespace concord {
using ReducerType = std::set<std::string>;
size_t gTotalRecords = 0;

void windowerFinished(const uint64_t bucket, const ReducerType &results) {
  LOG(INFO) << "Bucket#: " << bucket << " produced " << results.size()
            << " unique results and " << gTotalRecords << " total results";
  gTotalRecords = 0;
}

std::shared_ptr<TimeWindow<ReducerType>> windowedLogCounter() {
  using namespace std::placeholders;
  std::set<bolt::Metadata::StreamGrouping> istreams{
    {FLAGS_kafka_topic, bolt::Grouping::GROUP_BY}};
  const auto opts =
    TimeWindowOptions<ReducerType>()
      .setWindowLength(std::chrono::seconds(FLAGS_window_length))
      .setSlideInterval(std::chrono::seconds(FLAGS_slide_interval))
      .setComputationMetadata(bolt::Metadata("time-count", istreams))
      .setWindowerResultFunction(windowerFinished)
      .setReducerFunction([](ReducerType &a, const bolt::FrameworkRecord *b) {
        gTotalRecords++;
        a.insert(b->key);
      });
  return std::make_shared<TimeWindow<ReducerType>>(opts);
}
}

int main(int argc, char *argv[]) {
  bolt::logging::glog_init(argv[0]);
  google::SetUsageMessage("Start WindowedLogCounter operator\n"
                          "Usage:\n"
                          "\tkafka topic\t--kafka_topic topic_name \\\n"
                          "\twindow length\t--window_length time_sec \\\n"
                          "\tslide interval\t--slide_interval time_sec \\\n"
                          "\n");
  google::ParseCommandLineFlags(&argc, &argv, true);
  bolt::client::serveComputation(concord::windowedLogCounter(), argc, argv);
  return 0;
}
