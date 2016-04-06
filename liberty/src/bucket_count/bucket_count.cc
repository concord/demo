#include <gflags/gflags.h>
#include <concord/glog_init.hpp>
#include "utils/CountWindow.hpp"

DEFINE_string(kafka_topic, "", "Kafka topic that consumer is reading from");
DEFINE_int64(window_length, 1000000, "Amount of items until window is closed");
DEFINE_int64(slide_interval, 100000, "Amount of items until next window");

namespace concord {
using ReducerType = std::set<std::string>;
size_t gTotalRecords = 0;

void windowerFinished(const uint64_t bucket, const ReducerType &results) {
  LOG(INFO) << "Bucket#: " << bucket << " produced " << results.size()
            << " unique results";
  gTotalRecords = 0;
}

std::shared_ptr<CountWindow<ReducerType>> bucketCountFactory() {
  using namespace std::placeholders;
  std::set<bolt::Metadata::StreamGrouping> istreams{
    {FLAGS_kafka_topic, bolt::Grouping::GROUP_BY}};
  const auto opts =
    CountWindowOptions<ReducerType>()
      .setWindowLength(FLAGS_window_length)
      .setSlideInterval(FLAGS_slide_interval)
      .setComputationMetadata(bolt::Metadata("bucket-count", istreams))
      .setWindowerResultFunction(windowerFinished)
      .setReducerFunction([](ReducerType &a, const bolt::FrameworkRecord *b) {
        gTotalRecords++;
        a.insert(b->key);
      });
  return std::make_shared<CountWindow<ReducerType>>(opts);
}
}

int main(int argc, char *argv[]) {
  bolt::logging::glog_init(argv[0]);
  google::SetUsageMessage("Start BucketedLogCounter operator\n"
                          "Usage:\n"
                          "\tkafka topic\t--kafka_topic topic_name \\\n"
                          "\twindow length\t--window_length time_sec \\\n"
                          "\tslide interval\t--slide_interval time_sec \\\n"
                          "\n");
  google::ParseCommandLineFlags(&argc, &argv, true);
  bolt::client::serveComputation(concord::bucketCountFactory(), argc, argv);
  return 0;
}
