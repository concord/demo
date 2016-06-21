#include <memory>
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <map>
#include <list>
#include <ctime>
#include <fstream>
#include <algorithm>
#include <re2/re2.h>
#include <iomanip>
#include "utils/time_utils.hpp"

DEFINE_string(hardware_file, "", "concat all files and put them here first");
DEFINE_string(output_csv, "", "csv output file");


class HardwareLine {
  public:
  double cpu1min{0};
  double cpu5min{0};
  double freeMem{0};
  double totalMem{0};
  uint64_t time{0};
};

class HardwareBucket {
  public:
  enum Bucket { SECONDS };
  Bucket bucket;
  std::list<HardwareLine> latencies{};
};

double asBytes(const double n, const std::string &postfix) {
  if(postfix == "GB") {
    return n * static_cast<double>(1 << 30);
  } else if(postfix == "MB") {
    return n * static_cast<double>(1 << 20);
  } else if(postfix == "KB") {
    return n * static_cast<double>(1 << 10);
  } else {
    return n;
  }
}

void addHardwareLine(std::map<uint64_t, HardwareBucket> &buckets,
                     HardwareLine &&line) {
  // auto bucket = line.time -= line.time % 1000;
  auto bucket = line.time;
  buckets[bucket].latencies.push_front(std::move(line));
}

void produceCSVLines(std::ofstream &ofl,
                     const std::map<uint64_t, HardwareBucket> &buckets) {

  ofl << "date,time,1min,5min,free_mem_gb,used_mem_gb,percentage_used"
      << std::endl;
  for(const auto &t : buckets) {
    auto begin = t.second.latencies.begin();
    auto end = t.second.latencies.end();
    double cpu1 = std::accumulate(
      begin, end, double(0),
      [](const double &acc, const auto &l) { return acc + l.cpu1min; });
    double cpu5 = std::accumulate(
      begin, end, double(0),
      [](const double &acc, const auto &l) { return acc + l.cpu5min; });

    double freeMem = std::accumulate(
      begin, end, double(0),
      [](const double &acc, const auto &l) { return acc + l.freeMem; });

    double size = (double)t.second.latencies.size();
    cpu1 /= size;
    cpu5 /= size;
    freeMem /= size;

    double totalMem = t.second.latencies.front().totalMem;
    double usedMem = totalMem - freeMem;
    double usedMemPercentage = (usedMem / totalMem) * 100.0;

    static const double kOneGB = static_cast<double>(1 << 30);
    totalMem /= kOneGB;
    usedMem /= kOneGB;
    freeMem /= kOneGB;

    ofl << concord::excelTime(t.first) << "," << std::fixed
        << std::setprecision(4) << cpu1 << "," << cpu5 << "," << freeMem << ","
        << usedMem << "," << usedMemPercentage << std::endl;
  }
}


int main(int argc, char *argv[]) {
  google::ParseCommandLineFlags(&argc, &argv, true);
  google::InitGoogleLogging(argv[0]);
  google::InstallFailureSignalHandler();

  CHECK(!FLAGS_hardware_file.empty()) << "File cannot be empty";
  if(FLAGS_output_csv.empty()) {
    FLAGS_output_csv = FLAGS_hardware_file + "_out.csv";
  }


  static RE2 kHardwareRegex(
    "\\[(\\d{4})-(\\d{2})-(\\d{2})\\s(\\d{2}):(\\d{2}):(\\d{2})\\.\\d+\\].*"
    "cpu\\s1min:\\s(\\d+.\\d+),\\scpu\\s5min:\\s(\\d+\\.\\d+).*total\\smem:\\s("
    "\\d+.\\d+)\\s(\\w{2}),.*free\\smem:\\s(\\d+.\\d+)\\s(\\w{2}).*");

  LOG(INFO) << "Using regex" << kHardwareRegex.pattern();

  std::map<uint64_t, HardwareBucket> buckets{};

  std::ifstream ifl(FLAGS_hardware_file);
  CHECK(ifl) << "Couldn't open input file";
  std::ofstream ofl(FLAGS_output_csv);
  CHECK(ofl) << "Couldn't open output file";
  std::string line = "";

  while(std::getline(ifl, line)) {
    // clang-format off
    // [2016-04-06 21:19:22.255] [hardware_usage_monitor] [info] cpu 1min: 0.21, cpu 5min: 0.17, cpu 15min: 0.10, total mem: 29.453 GB, free mem: 23.505 GB, shared mem: 0.001 KB, buffer mem: 71.824 MB, total swap: 30.000 GB, free swap: 30.000 GB, # procs: 293
    // clang-format on
    struct HardwareLine l {};
    std::string totalMemPostfix, freeMemPostfix;
    std::tm time{};
    if(RE2::FullMatch(line, kHardwareRegex, &time.tm_year, &time.tm_mon,
                      &time.tm_mday, &time.tm_hour, &time.tm_min, &time.tm_sec,
                      &l.cpu1min, &l.cpu5min, &l.totalMem, &totalMemPostfix,
                      &l.freeMem, &freeMemPostfix)) {
      time.tm_year -= 1900;
      time.tm_mon -= 1;
      time.tm_hour -= 1;
      time_t t = std::mktime(&time);
      l.time = t * 1000;
      l.totalMem = asBytes(l.totalMem, totalMemPostfix);
      l.freeMem = asBytes(l.freeMem, freeMemPostfix);

      LOG_EVERY_N(INFO, 100) << "Line: " << line;
      LOG_EVERY_N(INFO, 100)
        << "ParsedLine: cpu1min: " << l.cpu1min << ", cpu5min: " << l.cpu5min
        << ", totalMem: " << l.totalMem << ", freeMem: " << l.freeMem;

      addHardwareLine(buckets, std::move(l));
    }
  }
  produceCSVLines(ofl, buckets);


  return 0;
}
