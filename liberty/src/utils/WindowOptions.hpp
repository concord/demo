#pragma once
#include <chrono>
#include <functional>
#include <concord/Computation.hpp> // for bolt::Metadata
namespace concord {
template <class ReducerType> struct WindowOptions {
  public:
  using ReducerFn =
    std::function<ReducerType(ReducerType &, const bolt::FrameworkRecord *)>;
  using ResultFn = std::function<void(const uint64_t, const ReducerType &)>;
  using SerializerFn = std::function<std::string(const ReducerType &)>;

  template <class Rep, class Period>
  WindowOptions &
  setWindowLength(const std::chrono::duration<Rep, Period> &length) {
    windowLength =
      std::chrono::duration_cast<std::chrono::milliseconds>(length);
    return *this;
  }

  template <class Rep, class Period>
  WindowOptions &
  setSlideInterval(const std::chrono::duration<Rep, Period> &interval) {
    slideInterval =
      std::chrono::duration_cast<std::chrono::milliseconds>(interval);
    return *this;
  }

  WindowOptions &setReducerFunction(ReducerFn reducer) {
    reducerFn = reducer;
    return *this;
  }

  WindowOptions &setDownstreamSerializer(SerializerFn serializer) {
    serializerFn = serializer;
    return *this;
  }

  WindowOptions &setWindowerResultFunction(ResultFn result) {
    resultFn = result;
    return *this;
  }

  WindowOptions &setComputationMetadata(const bolt::Metadata &md) {
    metadata = md;
    return *this;
  }

  std::chrono::milliseconds windowLength;
  std::chrono::milliseconds slideInterval;
  bolt::Metadata metadata;
  ReducerFn reducerFn;
  SerializerFn serializerFn;
  ResultFn resultFn;
};
}
