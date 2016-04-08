#include <memory>
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <map>
#include <list>
#include <ctime>
#include <fstream>
#include <algorithm>
#include <re2/re2.h>
#include "utils/time_utils.hpp"

DEFINE_string(latency_file, "", "concat all files and put them here first");
DEFINE_string(output_csv, "", "csv output file");


class LatencyLine {
  public:
  uint64_t p50{0};
  uint64_t p999{0};
  uint64_t time{0};
};

class LatencyBucket {
  public:
  enum Bucket { SECONDS };
  Bucket bucket;
  std::list<LatencyLine> latencies{};
};

void addLatencyLine(std::map<uint64_t, LatencyBucket> &buckets,
                    LatencyLine &&line) {
  // auto bucket = line.time -= line.time % 1000;
  auto bucket = line.time;
  buckets[bucket].latencies.push_front(std::move(line));
}

void produceCSVLines(std::ofstream &ofl,
                     const std::map<uint64_t, LatencyBucket> &buckets) {

  ofl << "date,time,p50,p999" << std::endl;
  for(const auto &t : buckets) {
    double p50 = std::accumulate(
      t.second.latencies.begin(), t.second.latencies.end(), uint64_t(0),
      [](const uint64_t &acc, const LatencyLine &l) { return acc + l.p50; });
    double p999 = std::accumulate(
      t.second.latencies.begin(), t.second.latencies.end(), uint64_t(0),
      [](const uint64_t &acc, const LatencyLine &l) { return acc + l.p999; });
    p50 /= t.second.latencies.size();
    p999 /= t.second.latencies.size();

    ofl << concord::excelTime(t.first) << "," << uint64_t(p50) << ","
        << uint64_t(p999) << std::endl;
  }
}


int main(int argc, char *argv[]) {
  google::ParseCommandLineFlags(&argc, &argv, true);
  google::InitGoogleLogging(argv[0]);
  google::InstallFailureSignalHandler();

  CHECK(!FLAGS_latency_file.empty()) << "File cannot be empty";
  if(FLAGS_output_csv.empty()) {
    FLAGS_output_csv = FLAGS_latency_file + "_out.csv";
  }

  static RE2 kLatencyRegex("\\[(\\d{4})-(\\d{2})-(\\d{2})\\s(\\d{2}):(\\d{2}):("
                           "\\d{2})\\.\\d+\\].*p50:\\s(\\d+).*p999:\\s(\\d+)us."
                           "*");

  LOG(INFO) << "Using regex" << kLatencyRegex.pattern();

  std::map<uint64_t, LatencyBucket> buckets{};

  std::ifstream ifl(FLAGS_latency_file);
  CHECK(ifl) << "Couldn't open input file";
  std::ofstream ofl(FLAGS_output_csv);
  CHECK(ofl) << "Couldn't open output file";
  std::string line = "";

  while(std::getline(ifl, line)) {
    // clang-format off
    // [2016-04-06 21:19:21.549] [principal_latencies] [info] 127.0.0.1:31002: traceId: -2823056735374592900, parentId: 4410988282745891113, id: -307255297444731547, p50: 483us, p95: 963us, p99: 1094us, p999: 1896us
    // clang-format on

    struct LatencyLine l {};
    std::tm time{};
    if(RE2::FullMatch(line, kLatencyRegex, &time.tm_year, &time.tm_mon,
                      &time.tm_mday, &time.tm_hour, &time.tm_min, &time.tm_sec,
                      &l.p50, &l.p999)) {
      time.tm_year -= 1900;
      time.tm_mon -= 1;
      time.tm_hour -= 1;
      time_t t = std::mktime(&time);
      l.time = t * 1000;
      addLatencyLine(buckets, std::move(l));
    }
  }
  produceCSVLines(ofl, buckets);


  return 0;
}
