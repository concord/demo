#include <string>
#include <gflags/gflags.h>
#include <concord/glog_init.hpp>
#include "utils/TimeWindow.hpp"

DEFINE_string(kafka_topic, "", "Kafka topic that consumer is reading from");
DEFINE_int64(window_length, 1, "Amount of time(s) to aggregate records");
DEFINE_int64(slide_interval, 2, "Amount of time(s) between new windows");

namespace concord {
int reduceEvents(int &a, const bolt::FrameworkRecord *rec) { return a + 1; }

std::string streamSerializer(const int &a) { return std::to_string(a); }

void windowerFinished(const uint64_t window, const int &results) {
  const auto time = bolt::timeInMillisAsIso8601(window);
  LOG(INFO) << "Window: " << time << " produced " << results << " results";
}

std::shared_ptr<TimeWindow<int>> windowedLogCounter() {
  std::set<bolt::Metadata::StreamGrouping> istreams{
    {FLAGS_kafka_topic, bolt::Grouping::ROUND_ROBIN}};
  const auto opts =
    WindowOptions<int>()
      .setWindowLength(std::chrono::seconds(FLAGS_window_length))
      .setSlideInterval(std::chrono::seconds(FLAGS_slide_interval))
      .setComputationMetadata(bolt::Metadata("time-count", istreams))
      .setReducerFunction(reduceEvents)
      .setDownstreamSerializer(streamSerializer)
      .setWindowerResultFunction(windowerFinished);
  return std::make_shared<TimeWindow<int>>(opts);
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
