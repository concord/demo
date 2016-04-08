#include <memory>
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <map>
#include <list>
#include <ctime>
#include <fstream>
#include <algorithm>
#include <re2/re2.h>

DEFINE_string(throughput_file, "", "concat all files and put them here first");
DEFINE_string(output_csv, "", "csv output file");

class ThroughputLine {
  public:
  uint64_t qps{0};
  uint64_t duration{0};
  uint64_t time{0};
};

class ThroughputBucket {
  public:
  enum Bucket { SECONDS };
  Bucket bucket;
  std::list<ThroughputLine> latencies{};
};

void addThroughputLine(std::map<uint64_t, ThroughputBucket> &buckets,
                       ThroughputLine &&line) {
  // auto bucket = line.time -= line.time % 1000;
  auto bucket = line.time;
  buckets[bucket].latencies.push_front(std::move(line));
}

void produceCSVLines(std::ofstream &ofl,
                     const std::map<uint64_t, ThroughputBucket> &buckets) {

  ofl << "time,qps" << std::endl;
  for(const auto &t : buckets) {
    auto qps = std::accumulate(
      t.second.latencies.begin(), t.second.latencies.end(), uint64_t(0),
      [](const uint64_t &acc, const ThroughputLine &l) { return acc + l.qps; });
    ofl << t.first << "," << qps << std::endl;
  }
}


int main(int argc, char *argv[]) {
  google::ParseCommandLineFlags(&argc, &argv, true);
  google::InitGoogleLogging(argv[0]);
  google::InstallFailureSignalHandler();

  CHECK(!FLAGS_throughput_file.empty()) << "File cannot be empty";
  if(FLAGS_output_csv.empty()) {
    FLAGS_output_csv = FLAGS_throughput_file + "_out.csv";
  }

  static RE2 kThroughputRegex(
    "\\[(\\d{4})-(\\d{2})-(\\d{2})\\s(\\d{2}):(\\d{2})"
    ":(\\d{2})\\.\\d{3}\\](?:\\s\\[\\w+\\])+\\s("
    "\\d+)\\s\\w+\\s\\w+\\s(\\d+).*");

  LOG(INFO) << "Using regex" << kThroughputRegex.pattern();

  std::map<uint64_t, ThroughputBucket> buckets{};

  std::ifstream ifl(FLAGS_throughput_file);
  CHECK(ifl) << "Couldn't open input file";
  std::ofstream ofl(FLAGS_output_csv);
  CHECK(ofl) << "Couldn't open output file";
  std::string line = "";

  while(std::getline(ifl, line)) {
    // clang-format off
    // [2016-04-06 21:10:58.196] [incoming_throughput] [info] 43240 req in 1001098us. total: 2068663 req
    // clang-format on
    // 2016
    // 04
    // 06
    // 21
    // 10
    // 58
    // 43240
    // 1001098
    struct ThroughputLine l {};
    std::tm time{};
    if(RE2::FullMatch(line, kThroughputRegex, &time.tm_year, &time.tm_mon,
                      &time.tm_mday, &time.tm_hour, &time.tm_min, &time.tm_sec,
                      &l.qps, &l.duration)) {
      time.tm_year -= 1900;
      time.tm_mon -= 1;
      time.tm_hour -= 1;
      time_t t = std::mktime(&time);
      l.time = t * 1000;
      addThroughputLine(buckets, std::move(l));
    }
  }
  produceCSVLines(ofl, buckets);


  return 0;
}
