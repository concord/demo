#include "WindowedComputation.hpp"
#include <string>
#include <gflags/gflags.h>
#include <concord/glog_init.hpp>

DEFINE_string(kafka_topic,
              "default_topic",
              "Kafka topic that consumer is reading from");
DEFINE_int64(window_length,
             1,
             "The duration of the records onto which a reducer is applied to");
DEFINE_int64(slide_interval,
             2,
             "Interval at which the window operation is performed");

namespace concord {
int reduceEvents(int &a, const bolt::FrameworkRecord *rec) { return a + 1; }

void windowerFinished(const uint64_t window, const int &results) {
  const auto time = bolt::timeInMillisAsIso8601(window);
  LOG(INFO) << "Window: " << time << " produced " << results << " results";
}

std::shared_ptr<WindowedComputation<int>> windowedLogCounter() {
  bolt::Metadata md;
  md.name = "windowed-log-counter";
  md.istreams.insert({FLAGS_kafka_topic, bolt::Grouping::ROUND_ROBIN});
  auto wLogCounter =
    WindowedComputation<int>::create()
      ->setWindowLength(std::chrono::seconds(FLAGS_window_length))
      ->setSlideInterval(std::chrono::seconds(FLAGS_slide_interval))
      ->setComputationMetadata(md)
      ->setReducerFunction(reduceEvents)
      ->setWindowerResultFunction(windowerFinished);
  return std::shared_ptr<WindowedComputation<int>>(wLogCounter);
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
