#include <stdio.h>
#include <signal.h>

#include <string>
#include <iostream>
#include <cassert>

#include <librdkafka/rdkafka.h>

//class KafkaConf;


/**
 * @brief Delivery report callback, triggered by from poll() or flush()
 *        once for each produce():ed message to propagate its final delivery status.
 *
 *        A non-zero \c rkmessage->err indicates delivery failed permanently.
 */
static void dr_cb (rd_kafka_t *rk,
                   const rd_kafka_message_t *rkmessage, void *opaque);

class KafkaConf {
public:
    KafkaConf() { conf = rd_kafka_conf_new(); }
    ~KafkaConf() { }
    rd_kafka_conf_t* Get() { return conf; }
    
    void SetConf(const char* name, const char* value) {
        if (rd_kafka_conf_set(conf, name, value, errstr, sizeof(errstr)) 
            != RD_KAFKA_CONF_OK) {
            std::clog << errstr << std::endl;
        }
    }
    
    void SetConf(std::string& name, std::string& value) {
        SetConf(name.data(), value.data());
    }


private:
    rd_kafka_conf_t* conf;
    char errstr[512];
};


class Kafka {
public:
    using string = std::string;

    static int CreateTopic(rd_kafka_t* rk, const char* topic, int num_partitions);
    static int RunProducer(const char* topic, int msgcnt, KafkaConf& conf);
    static int RunConsumer(const char* topic, KafkaConf& conf);
};
