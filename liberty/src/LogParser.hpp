#include <ctime>
#include <string>
#include <memory>
#include <re2/re2.h>
namespace concord {
/// This class parses a specific log file line. It will attempt to pattern
/// match and extract specific fields based on the regex 'pattern_'.
/// No excpetions will be thrown on failure, in order to check if parsing
/// succeeded call the 'parseSuccess()' method.
class LogParser {
  public:
  explicit LogParser(const std::string &log) { parseData(log); }

  bool parseSuccess() const { return success_; }

  uint64_t getTag() const { return tag_; }
  time_t getDate() const { return date_; }
  std::string getUser() const { return user_; }
  std::string getEmail() const { return email_; }
  std::string getLog() const { return msg_; }

  private:
  void parseData(const std::string &log) {
    if(!log.empty() && !success_) {
      // Regex used for parsing log line
      static RE2 regex(
        "-\\s(\\d+)\\s(\\d+)\\.(\\d+)\\.(\\d+)\\s(\\w+)\\s(\\w+)\\s(\\d+"
        ")\\s(\\d+):(\\d+):(\\d+)\\s(\\S+)(.*)");

      // Memory used for when fields are extracted into data
      int year, month, day;
      std::string monthStr;
      auto tmi = std::make_unique<struct tm>();

      // Call to RE2::FullMatch uses 'regex' to pull fields from 'log' and
      // insert values into the provided fields
      if(RE2::FullMatch(log, regex, &tag_, &year, &month, &day, &user_,
                        &monthStr, &tmi->tm_mday, &tmi->tm_hour, &tmi->tm_min,
                        &tmi->tm_sec, &email_, &msg_)) {
	// Convert date field to time_t
        tmi->tm_year = year - 1900; // Years since 1900
        date_ = std::mktime(tmi.get());

	// Mark parse as successful
        success_ = true;
      }
    }
  }

  private:
  bool success_{false};
  uint64_t tag_;
  time_t date_;
  std::string user_;
  std::string email_;
  std::string msg_;
};
}
