#include <iostream>
#include <fstream>
#include <memory>
#include <string>
#include <vector>
#include <unistd.h>
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <city.h>
#include <librdkafka/rdkafkacpp.h>
#include <thread>

static uint64_t count = 0;
static uint64_t bytesSent = 0;

DEFINE_string(file_name, "", "Name of file to produce");
DEFINE_string(topic_name, "", "Name of topic to produce onto");
DEFINE_string(broker_addr, "", "Address of kafka broker");
DEFINE_int64(partitions, 144, "Number of broker partitions");
DEFINE_int32(limit_gb, 1, "Estimated amount of data to send");

void produce_line(std::string &line,
                  RdKafka::Producer *producer,
                  RdKafka::Topic *topic) {
  std::string key = line.size() < 24 ? line : line.substr(0, 24);
  uint64_t partition = CityHash64(key.data(), key.size())
                       % static_cast<uint64_t>(FLAGS_partitions);
  // 1. topic
  // 2. partition
  // 3. flags
  // 4. payload
  // 5. payload len
  // 6. std::string key
  // 7. msg_opaque?
  RdKafka::ErrorCode resp;
  while(
    (resp = producer->produce(topic, partition, RdKafka::Producer::RK_MSG_COPY,
                              const_cast<char *>(line.c_str()), line.size(),
                              &key, NULL)) && resp != RdKafka::ERR_NO_ERROR) {
    LOG(ERROR) << "Issue when producing: " << RdKafka::err2str(resp)
               << " .. attempting again";
  }

  bytesSent += line.size();
  if(++count % 1000000 == 0) {
    producer->poll(5);
    LOG(INFO) << "Total lines sent: " << count;
  }
}

int main(int argc, char **argv) {
  google::SetUsageMessage("KafkaFill\n"
                          "Usage:\n"
                          "\t./kafka_fill\t--file_name fn \t--topic_name tn "
                          "\t--broker_addr localhost:9092 \t --limit_gb 1 \\\n"
                          "\n");
  google::ParseCommandLineFlags(&argc, &argv, true);

  std::string err;
  auto *conf = RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);
  auto *tconf = RdKafka::Conf::create(RdKafka::Conf::CONF_TOPIC);
  auto status = conf->set("metadata.broker.list", FLAGS_broker_addr, err);
  if(status != RdKafka::Conf::CONF_OK) {
    throw std::runtime_error(err);
  }
  // Default value: 100000
  status = conf->set("queue.buffering.max.messages", "10000000", err);
  if(status != RdKafka::Conf::CONF_OK) {
    throw std::runtime_error(err);
  }

  auto *producer = RdKafka::Producer::create(conf, err);
  if(!producer) {
    throw std::runtime_error(err);
  }

  auto *topic = RdKafka::Topic::create(producer, FLAGS_topic_name, tconf, err);
  if(!topic) {
    throw std::runtime_error(err);
  }

  std::ifstream infile(FLAGS_file_name);
  std::string line;
  const uint64_t bytesLimit = FLAGS_limit_gb * 1000000000;
  while(std::getline(infile, line) && bytesSent < bytesLimit) {
    produce_line(line, producer, topic);
  }

  int outq;
  while((outq = producer->outq_len()) > 0) {
    LOG(INFO) << "Waiting to drain queue: " << outq;
    producer->poll(5000);
  }

  delete topic;
  delete producer;

  return 0;
}
