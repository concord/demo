#include <string>
#include <map>
#include <gflags/gflags.h>
#include <concord/glog_init.hpp>
#include "LogParser.hpp"
#include "WindowedComputation.hpp"

DEFINE_string(kafka_topic,
              "default_topic",
              "Kafka topic that consumer is reading from");
DEFINE_int64(window_length,
             10,
             "The duration of the records onto which a reducer is applied to");
DEFINE_int64(slide_interval,
             10,
             "Interval at which the window operation is performed");

namespace concord {
using ReducerType = std::map<uint64_t, LogParser>;

/// After fold is performed on window, there will be a maximum of one log
/// containing the logId within each window.
ReducerType reduceEvents(ReducerType &a, const bolt::FrameworkRecord *b) {
  const auto parser = LogParser(b.key);
  if(!parser.parseSuccess()) {
    return a;
  }
  a.insert({parser.getTag(), parser});
  return a;
}

void windowerFinished(const uint64_t window, const ReducerType &results) {
  LOG(INFO) << "Window: " << window << " produced " << results.size()
            << " unique results";
}

std::shared_ptr<WindowedComputation<ReducerType>> windowedPatternMatcher() {
  // Define istreams and ostreams..
  bolt::Metadata md;
  // set md.name
  md.istreams.insert({FLAGS_kafka_topic + "logs", bolt::Grouping::ROUND_ROBIN});

  auto wPatternMatcher =
    WindowedComputation<ReducerType>::create()
      ->setWindowLength(std::chrono::seconds(FLAGS_window_length))
      ->setSlideInterval(std::chrono::seconds(FLAGS_slide_interval))
      ->setComputationMetadata(md)
      ->setReducerFunction(reduceEvents)
      ->setWindowerResultFunction(windowerFinished);
  return std::shared_ptr<WindowedComputation<ReducerType>>(wPatternMatcher);
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
  bolt::client::serveComputation(concord::windowedPatternMatcher(), argc, argv);
  return 0;
}
