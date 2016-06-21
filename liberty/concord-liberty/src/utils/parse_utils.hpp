#pragma once
#include <re2/re2.h>
#include <city.h>
#include <concord/time_utils.hpp>

namespace concord {
using LogTuple = std::tuple<int64_t, std::string>;

LogTuple buildKeyAndValue(const std::string &log) {
  // Current timestamp in ISO-8601 format
  // Message timestamp converted to ISO-8601 format
  // username associated with the event (if any, e.g. src@ladmin)
  // node name (e.g. src@ladmin)
  // The line’s log payload
  static const RE2 valueRegex(
    "-\\s(\\d+)(?:\\s\\S+){5}\\s(\\w+)(.)(\\w+)\\s(.*)$");
  int key, timestamp; // value is in seconds
  char nodeChar;
  std::string username, nodename, msg, value;
  if(RE2::FullMatch(log, valueRegex, &timestamp, &username, &nodeChar,
                    &nodename, &msg)) {
    key = static_cast<int64_t>(CityHash64(log.data(), log.size()) + timestamp);
    value = bolt::timeInMillisAsIso8601(bolt::timeNowMilli()) + ":"
            + std::to_string(timestamp * 1000) + ":" + username + nodeChar
            + nodename + ":" + msg;
  }
  return LogTuple(key, value);
}
}
