#include <memory>
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <fstream>
#include <folly/String.h>
#include <ios>
#include <folly/String.h>

DEFINE_string(latency_files, "", "coma delimited list of files");
DEFINE_string(hardware_files, "", "coma delimited list of files");
DEFINE_string(throughput_files, "", "coma delimited list of files");
DEFINE_string(output_csv, "aggregates.csv", "csv output file");


class FileIterator {
  public:
  virtual ~FileIterator() {}
  FileIterator(std::string filename) : filename_(filename) {
    ifl_.open(filename_, std::ios::in);
    if(ifl_.fail()) {
      ifl_.clear();
      LOG(FATAL) << "File " << filename_ << ", failed to open";
    }
  }
  virtual const std::string &fileName() const { return filename_; }
  virtual const std::string &basename() {
    if(basename_.empty()) {
      std::vector<std::string> parts;
      folly::split("/", filename_, parts);
      auto it =
        std::find_if(parts.begin(), parts.end(), [](const std::string s) {
          if(s.empty()) {
            return false;
          }
          int point = static_cast<int>(s[0]);
          // asci table map 48 - 57 -> '0' - '9'
          return point >= 48 && point <= 57;
        });

      while(it != parts.end()) {
        basename_ += *it;
        ++it;
      }
      if(basename_.empty()) {
        basename_ = filename_;
      } else {
        basename_ += "_";
      }
      auto &f = basename_;
      for(auto j = 0u; j < f.size(); ++j) {
        if(f[j] == ':' || f[j] == '/' || f[j] == '.' || f[j] == '-') {
          f[j] = '_';
        }
      }
      while(!f.empty() && f[0] == '_') {
        f = f.substr(1);
      }
    }
    return basename_;
  }
  virtual const std::string &delimiter() const = 0;
  virtual const std::string header() = 0;
  virtual uint32_t lines() { return lines_; }
  virtual std::string nextLine() {
    if(!ifl_.eof()) {
      std::string ret = ""; // empty csv
      std::getline(ifl_, ret);
      if(!ret.empty()) {
        ++lines_;
        return ret;
      }
    }
    return delimiter();
  }

  protected:
  uint32_t lines_{0};
  std::string filename_;
  std::string basename_;
  std::ifstream ifl_;
};


class LatencyFileIterator : public FileIterator {
  public:
  LatencyFileIterator(std::string filename) : FileIterator(filename) {}
  virtual const std::string &delimiter() const override {
    static const std::string kDelimiter = ",,,";
    return kDelimiter;
  }
  virtual const std::string header() override {
    auto f = this->basename();
    return f + "_date," + f + "_time," + f + "_p50," + f + "_p999";
  }
};

class HardwareFileIterator : public FileIterator {
  public:
  HardwareFileIterator(std::string filename) : FileIterator(filename) {}
  virtual const std::string &delimiter() const override {
    static const std::string kDelimiter = ",,,,,,";
    return kDelimiter;
  }
  virtual const std::string header() override {
    auto f = basename();
    return f + "_date," + f + "_time," + f + "_1min," + f + "_5min," + f
           + "_free_mem_gb," + f + "_used_mem_gb," + f + "_percentage_used";
  }
};

class ThroughputFileIterator : public FileIterator {
  public:
  ThroughputFileIterator(std::string filename) : FileIterator(filename) {}
  virtual const std::string &delimiter() const override {
    static const std::string kDelimiter = ",,";
    return kDelimiter;
  }
  virtual const std::string header() override {
    auto f = basename();
    return f + "_date," + f + "_time," + f + "_qps";
  }
};


std::vector<std::string> split_files(const std::string &line) {
  std::vector<std::string> out{};
  folly::split(",", line, out, true);
  return out;
}

int main(int argc, char *argv[]) {
  google::ParseCommandLineFlags(&argc, &argv, true);
  google::InitGoogleLogging(argv[0]);
  google::InstallFailureSignalHandler();
  LOG_IF(WARNING, FLAGS_latency_files.empty()) << "No latency files to process";
  LOG_IF(WARNING, FLAGS_hardware_files.empty()) << "cannot be empty";
  LOG_IF(WARNING, FLAGS_throughput_files.empty()) << "cannot be empty";

  std::vector<std::string> latencyFiles = split_files(FLAGS_latency_files);
  std::vector<std::string> hardwareFiles = split_files(FLAGS_hardware_files);
  std::vector<std::string> throughputFiles =
    split_files(FLAGS_throughput_files);

  std::vector<std::unique_ptr<FileIterator>> itrs{};

  for(auto &f : latencyFiles) {
    auto it = std::make_unique<LatencyFileIterator>(f);
    LOG(INFO) << "Adding latency file: " << it->basename();
    itrs.push_back(std::move(it));
  }
  for(auto &f : hardwareFiles) {
    auto it = std::make_unique<HardwareFileIterator>(f);
    LOG(INFO) << "Adding hardware file: " << it->basename();
    itrs.push_back(std::move(it));
  }
  for(auto &f : throughputFiles) {
    auto it = std::make_unique<ThroughputFileIterator>(f);
    LOG(INFO) << "Adding throughput file: " << it->basename();
    itrs.push_back(std::move(it));
  }

  LOG(INFO) << "Processing " << itrs.size() << " files";

  std::ofstream ofl(FLAGS_output_csv);
  if(ofl.fail()) {
    ofl.clear();
    LOG(FATAL) << "could not open " << FLAGS_output_csv;
  }
  for(auto i = 0u; i < itrs.size(); ++i) {
    auto &it = itrs[i];
    if(i != 0) {
      ofl << ",";
    }
    ofl << it->header();
  }
  // add new line after header
  ofl << std::endl;
  // skip the header
  for(const auto &i : itrs) {
    i->nextLine();
  }

  while(true) {
    int64_t lines = itrs.size() - 1;
    for(auto i = 0u; i < itrs.size(); ++i) {
      auto &it = itrs[i];
      auto tmp = std::move(it->nextLine());
      if(i != 0) {
        ofl << ",";
      }
      if(tmp == it->delimiter()) {
        lines -= 1;
      }
      ofl << tmp;
    }
    if(lines <= 0) {
      LOG(INFO) << "Finished processing";
      break;
    }
    ofl << std::endl;
  }

  return 0;
}
