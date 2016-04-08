#pragma once
#include <chrono>
#include <vector>
#include <ctime>

namespace concord {
// Extracted out of gmock
// Converts the given epoch time in milliseconds to a date string in the ISO
// 8601 format, without the timezone information.
std::string timeInMillisAsIso8601(uint64_t ms) {
  // Using non-reentrant version as localtime_r is not portable.
  time_t seconds = static_cast<time_t>(ms / 1000);
#ifdef _MSC_VER
#pragma warning(push)           // Saves the current warning state.
#pragma warning(disable : 4996) // Temporarily disables warning 4996
  // (function or variable may be unsafe).
  const struct tm *const time_struct = localtime(&seconds); // NOLINT
#pragma warning(pop) // Restores the warning state again.
#else
  const struct tm *const time_struct = localtime(&seconds); // NOLINT
#endif

  if(time_struct == NULL) {
    return ""; // Invalid ms value
  }
  // YYYY-MM-DDThh:mm:ss
  return std::to_string(time_struct->tm_year + 1900) + "-"
         + std::to_string(time_struct->tm_mon + 1) + "-"
         + std::to_string(time_struct->tm_mday) + "T"
         + std::to_string(time_struct->tm_hour) + ":"
         + std::to_string(time_struct->tm_min) + ":"
         + std::to_string(time_struct->tm_sec);
}
}
