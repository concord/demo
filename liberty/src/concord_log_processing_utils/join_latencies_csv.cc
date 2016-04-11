#include <memory>
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <fstream>
#include <folly/String.h>
#include <ios>

DEFINE_string(latency_files, "", "coma delimited list of files");
DEFINE_string(output_csv, "latency_aggregate.csv", "csv output file");


class LatencyFileIterator {
  public:
  LatencyFileIterator(std::string filename) : filename_(filename) {
    ifl_.open(filename_, std::ios::in);
    if(ifl_.fail()) {
      ifl_.clear();
      LOG(FATAL) << "File " << filename_ << ", failed to open";
    }
  }
  std::string nextLine() {
    static const std::string kDelimiter = ",,,";
    ++lines_;
    std::string ret = ""; // empty csv
    if(!ifl_.eof()) {
      std::getline(ifl_, ret);
    } else {
      LOG(INFO) << "Got eof for file: " << filename_
                << " produced lines: " << lines_;
    }
    if(ret.empty()) {
      return kDelimiter;
    }
    return ret;
  }

  private:
  uint32_t lines_{0};
  std::string filename_;
  std::ifstream ifl_;
};


int main(int argc, char *argv[]) {
  google::ParseCommandLineFlags(&argc, &argv, true);
  google::InitGoogleLogging(argv[0]);
  google::InstallFailureSignalHandler();
  CHECK(!FLAGS_latency_files.empty()) << "cannot be empty";
  std::vector<std::string> files{};
  folly::split(",", FLAGS_latency_files, files);
  std::vector<LatencyFileIterator> itrs{};
  for(auto &f : files) {
    if(!f.empty()) {
      itrs.emplace_back(f);
    }
  }
  std::ofstream ofl(FLAGS_output_csv);
  if(ofl.fail()) {
    ofl.clear();
    LOG(FATAL) << "could not open " << FLAGS_output_csv;
  }
  for(auto i = 0u; i < files.size(); ++i) {
    auto f = files[i];
    if(f.size() > 25) {
      f = f.substr(std::min(f.size(), f.size() - 25), 21);
    }
    // replace / and : and other chars
    for(auto j = 0u; j < f.size(); ++j) {
      if(f[j] == ':' || f[j] == '/' || f[j] == '.' || f[j] == '-') {
        f[j] = '_';
      }
    }
    if(i != 0) {
      ofl << ",";
    }
    ofl << f + "_date," << f + "_time," << f + "_p50," << f + "_p999";
  }
  // add new line after header
  ofl << std::endl;
  // skip the header
  for(auto &i : itrs) {
    i.nextLine();
  }

  auto lines = itrs.size();
  while(true) {
    for(auto i = 0u; i < itrs.size(); ++i) {
      auto tmp = std::move(itrs[i].nextLine());
      if(i != 0) {
        ofl << ",";
      }
      if(tmp == ",,,") {
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
