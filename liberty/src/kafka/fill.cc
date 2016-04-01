#include <iostream>
#include <fstream>
#include <memory>
#include <string>
#include <vector>
#include <unistd.h>
#include <gflags/gflags.h>
#include <city.h>
#include <librdkafka/rdkafkacpp.h>

static uint64_t count = 0;

DEFINE_string(file_name, "", "Name of file to produce");
DEFINE_string(topic_name, "", "Name of topic to produce onto");
DEFINE_string(broker_addr, "", "Address of kafka broker");

void produce_line(std::string &line,
                  RdKafka::Producer *producer,
                  RdKafka::Topic *topic) {
  std::string key = line.size() < 24 ? line : line.substr(0, 24);
  uint64_t partition = CityHash64(line.data(), line.size());
  // 1. topic
  // 2. partition
  // 3. flags
  // 4. payload
  // 5. payload len
  // 6. std::string key
  // 7. msg_opaque?
  producer->produce(topic, partition, RdKafka::Producer::RK_MSG_COPY,
                    const_cast<char *>(line.c_str()), line.size(), &key, NULL);

  if(++count % 1000000 == 0) {
    std::cout << "Total lines sent: " << count << std::endl;
  }
}

int main(int argc, char **argv) {
  google::SetUsageMessage("KafkaFill\n"
                          "Usage:\n"
                          "\t./kafka_fill\t--file_name fn \t--topic_name tn "
                          "\t--broker_addr 0.0.0.0:9092 \\\n"
                          "\n");
  google::ParseCommandLineFlags(&argc, &argv, true);

  std::string err;
  auto *conf = RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);
  auto *tconf = RdKafka::Conf::create(RdKafka::Conf::CONF_TOPIC);
  auto status = conf->set("metadata.broker.list", FLAGS_broker_addr, err);
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
  while(std::getline(infile, line)) {
    produce_line(line, producer, topic);
  }

  int outq;
  while((outq = producer->outq_len()) > 0) {
    std::cout << "Waiting to drain queue: " << outq << std::endl;
    producer->poll(5000);
  }

  delete topic;
  delete producer;

  return 0;
}
