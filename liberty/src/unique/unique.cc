#include <memory>
#include <unordered_map>
#include <algorithm>
#include <sstream>
#include <bloom.h>

#include <concord/glog_init.hpp>
#include <concord/Computation.hpp>
#include <concord/time_utils.hpp>

std::ostream &operator<<(std::ostream &o, const struct bloom &bloom) {
  o << "Bloom filter: { ->entries = " << bloom.entries << ","
    << " ->error = " << bloom.error << ", ->bits = " << bloom.bits
    << ", ->bits per elem = " << bloom.bpe << ", ->bytes = " << bloom.bytes
    << ", ->buckets = " << bloom.buckets
    << ", ->bucket_bytes = " << bloom.bucket_bytes
    << ", ->bucket_bytes_exponent = " << bloom.bucket_bytes_exponent
    << " ->bucket_bits_fast_mod_operand = "
    << bloom.bucket_bits_fast_mod_operand
    << ", ->hash functions = " << bloom.hashes << "}";
  return o;
}


class Unique final : public bolt::Computation {
  public:
  using CtxPtr = bolt::Computation::CtxPtr;
  // 10 Million unique entries
  Unique() { bloom_init(&bloom_, 10'000'000llu, 0.01); }
  virtual void init(CtxPtr ctx) override {
    ctx->setTimer("print_loop", bolt::timeNowMilli());
    LOG(INFO) << "Initializing unique with bloom_: " << bloom_;
  }

  virtual void destroy() override {
    LOG(INFO) << "Destructing unique with bloom_: " << bloom_;
    bloom_free(&bloom_);
  }

  virtual void processRecord(CtxPtr ctx, bolt::FrameworkRecord &&r) override {
    ++recordCount_;
    if(bloom_check(&bloom_, (void *)r.value.c_str(), r.value.length()) == 0) {
      ++uniqueRecords_;
      bloom_add(&bloom_, (void *)r.value.c_str(), r.value.length());
    }
  }

  virtual void
  processTimer(CtxPtr ctx, const std::string &key, int64_t time) override {
    ctx->setTimer(key, bolt::timeNowMilli() + 10000);
    LOG(INFO) << "unique records: " << uniqueRecords_
              << ", total records: " << recordCount_ << ", bloom_: " << bloom_;
  }

  virtual bolt::Metadata metadata() override {
    bolt::Metadata m;
    m.name = "unique";
    m.istreams.insert({"words", bolt::Grouping::GROUP_BY});
    return m;
  }


  private:
  struct bloom bloom_;
  uint64_t recordCount_{0};
  uint64_t uniqueRecords_{0};
};

int main(int argc, char *argv[]) {
  bolt::logging::glog_init(argv[0]);
  bolt::client::serveComputation(std::make_shared<Unique>(), argc, argv);
  return 0;
}
