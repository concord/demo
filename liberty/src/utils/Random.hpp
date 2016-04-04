#pragma once
#include <random>

namespace concord {
class Random {
  public:
  Random() {
    std::random_device rd;
    rand_.seed(rd());
  }

  uint64_t nextRand() { return dist_(rand_); }

  int64_t nextRandI64() {
    uint64_t rand = nextRand();
    return reinterpret_cast<int64_t &>(rand);
  }

  private:
  // mersenne twister
  std::mt19937 rand_;
  // by default range [0, MAX]
  std::uniform_int_distribution<uint64_t> dist_;
};
}
