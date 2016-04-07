#pragma once
#include <chrono>
#include <functional>
#include <concord/Computation.hpp> // for bolt::Metadata
namespace concord {
template <class SubType, class ReducerType> class WindowOptions {
  public:
  using ReducerFn =
    std::function<void(ReducerType &, const bolt::FrameworkRecord *)>;
  using ResultFn = std::function<void(const uint64_t, const ReducerType &)>;
  using SerializerFn = std::function<std::string(const ReducerType &)>;

  // The following static_casts are fine since SubType template is always
  // intended to be a decendent class.
  SubType &setReducerFunction(ReducerFn reducer) {
    reducerFn = reducer;
    return static_cast<SubType &>(*this);
  }

  SubType &setDownstreamSerializer(SerializerFn serializer) {
    serializerFn = serializer;
    return static_cast<SubType &>(*this);
  }

  SubType &setWindowerResultFunction(ResultFn result) {
    resultFn = result;
    return static_cast<SubType &>(*this);
  }

  SubType &setComputationMetadata(const bolt::Metadata &md) {
    metadata = md;
    return static_cast<SubType &>(*this);
  }
  bolt::Metadata metadata;
  ReducerFn reducerFn;
  SerializerFn serializerFn;
  ResultFn resultFn;
};

// The only two differences between TimeWindowOptions and CountWindowOptions
// are that they interpret their window lengths and intervals in different
// ways
template <class ReducerType>
class TimeWindowOptions
  : public WindowOptions<TimeWindowOptions<ReducerType>, ReducerType> {
  public:
  template <class Rep, class Period>
  TimeWindowOptions &
  setWindowLength(const std::chrono::duration<Rep, Period> &length) {
    windowLength =
      std::chrono::duration_cast<std::chrono::milliseconds>(length);
    return *this;
  }

  template <class Rep, class Period>
  TimeWindowOptions &
  setSlideInterval(const std::chrono::duration<Rep, Period> &interval) {
    slideInterval =
      std::chrono::duration_cast<std::chrono::milliseconds>(interval);
    return *this;
  }
  std::chrono::milliseconds windowLength;
  std::chrono::milliseconds slideInterval;
};

template <class ReducerType>
class CountWindowOptions
  : public WindowOptions<CountWindowOptions<ReducerType>, ReducerType> {
  public:
  CountWindowOptions &setWindowLength(const uint64_t &length) {
    windowLength = length;
    return *this;
  }

  CountWindowOptions &setSlideInterval(const uint64_t &interval) {
    slideInterval = interval;
    return *this;
  }
  uint64_t windowLength;
  uint64_t slideInterval;
};
}
