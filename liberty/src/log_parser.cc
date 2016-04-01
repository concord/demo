#include <ctime>
#include <string>
#include <re2/re2.h>
namespace concord {
/// This class expects a specfic type to string to parse.. eg:
/// - 1102911148 2004.12.12 ladmin2 Dec 12 20:12:28 src@ladmin2 ....... : LTS :
class LogParser {
  public:
  LogParser(const std::string &log) {
    // CHECK(!pattern_.ok()) << "Compile failed"
  }

  bool parseSuccess() const { return success_; }

  private:
  void parseData(const std::string &log) {
    if(!log.empty()) {
      int year, month, day;
      std::string monthStr;
      struct tm *tmi = (struct tm*)malloc(sizeof(struct tm));
      if(RE2::FullMatch(log, pattern_, &tag_, &year, &month, &day, &user_,
                        &monthStr, &tmi->tm_mday, &tmi->tm_hour, &tmi->tm_min,
                        &tmi->tm_sec, &email_, &msg_)) {
        tmi->tm_year = year - 1900; // Years since 1900
        date_ = std::mktime(tmi);
        success_ = true;
      }
      free(tmi);
    }
  }

  bool success_{false};
  RE2 pattern_{"-\\s(\\d+)\\s(\\d+)\\.(\\d+)\\.(\\d+)\\s(\\w+)\\s(\\w+)\\s(\\d+"
               ")\\s(\\d+):(\\d+):(\\d+)\\s(\\S+)(.*)"};
  uint64_t tag_;
  time_t date_;
  std::string user_;
  std::string email_;
  std::string msg_;
};
}
