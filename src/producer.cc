#include <string>
#include <iostream>
#include <cassert>

#include <src/kafka.hh>

int main (int argc, char **argv) {
    KafkaConf kafkaConf;
    kafkaConf.SetConf("bootstrap.servers", "localhost:9092");

    if (Kafka::RunProducer("kafka-cpp-producer", 10, kafkaConf) == -1) {
        return 1;
    }
    std::clog << "producer work ended" << std::endl;

    return 0;
}

//int main (int argc, char **argv) {
//    using namespace RdKafka;
//
//    Conf* conf = Conf::create(Conf::ConfType::CONF_TOPIC);
//    std::string error_str;
//    std::string type = "bootstrap.servers\0";
//    if (conf->set(type, "localhost:9092", error_str) != 0) {
//        std::clog << "ERROR: " << error_str << std::endl;
//    }
//
//    Handle* handle = nullptr;
//
//    //Topic* topic = Topic::create(handle, "rdkafka_test", conf, error_str);
//
//    std::clog << "producer work ended" << std::endl;
//
//    return 0;
//}

