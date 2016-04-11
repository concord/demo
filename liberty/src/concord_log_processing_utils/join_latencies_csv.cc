#include <memory>
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <fstream>
#include <folly/String.h>
#include <ios>

DEFINE_string(latency_files, "", "coma delimited list of files");
DEFINE_string(hardware_files, "", "coma delimited list of files");
DEFINE_string(output_csv, "latency_aggregate.csv", "csv output file");


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
  const std::string &fileName() const { return filename_; }
  virtual const std::string &delimiter() const = 0;
  virtual const std::string header() const = 0;
  std::string nextLine() {
    ++lines_;
    std::string ret = ""; // empty csv
    if(!ifl_.eof()) {
      std::getline(ifl_, ret);
    }
    if(ret.empty()) {
      return delimiter();
    }
    return ret;
  }

  protected:
  uint32_t lines_{0};
  std::string filename_;
  std::ifstream ifl_;
};


class LatencyFileIterator : public FileIterator {
  public:
  LatencyFileIterator(std::string filename) : FileIterator(filename) {}
  virtual const std::string &delimiter() const override {
    static const std::string kDelimiter = ",,,";
    return kDelimiter;
  }
  virtual const std::string header() const override {
    auto f = filename_;
    if(f.size() > 25) {
      f = f.substr(f.size() - 25, 21);
    }
    for(auto j = 0u; j < f.size(); ++j) {
      if(f[j] == ':' || f[j] == '/' || f[j] == '.' || f[j] == '-') {
        f[j] = '_';
      }
    }
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
  virtual const std::string header() const override {
    auto f = filename_;
    if(f.size() > 25) {
      f = f.substr(f.size() - 25, 21);
    }
    for(auto j = 0u; j < f.size(); ++j) {
      if(f[j] == ':' || f[j] == '/' || f[j] == '.' || f[j] == '-') {
        f[j] = '_';
      }
    }
    return f + "_date," + f + "_time," + f + "_1min," + f + "_5min," + f
           + "_free_mem_gb," + f + "_used_mem_gb," + f + "_percentage_used";
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
  CHECK(!FLAGS_latency_files.empty() || !FLAGS_hardware_files.empty())
    << "cannot be empty";

  std::vector<std::string> latencyFileNames = split_files(FLAGS_latency_files);
  std::vector<std::string> hardwareFileNames =
    split_files(FLAGS_hardware_files);

  std::vector<std::unique_ptr<FileIterator>> itrs{};

  for(auto &f : latencyFileNames) {
    auto it = std::make_unique<LatencyFileIterator>(f);
    itrs.push_back(std::move(it));
  }
  for(auto &f : hardwareFileNames) {
    auto it = std::make_unique<HardwareFileIterator>(f);
    itrs.push_back(std::move(it));
  }
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
    LOG(INFO) << it->fileName() << " -> " << it->header();
    ofl << it->header();
  }
  // add new line after header
  ofl << std::endl;
  // skip the header
  for(const auto &i : itrs) {
    i->nextLine();
  }

  auto lines = itrs.size();
  while(true) {
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
      break;
    }
    ofl << std::endl;
  }

  return 0;
}
