#include <src/kafka.hh>

void dr_cb (rd_kafka_t *rk, const rd_kafka_message_t *rkmessage, void *opaque) {
    using namespace std;
    int *delivery_counterp = (int *)rkmessage->_private;

    if (rkmessage->err) {
        clog << "Failed, message: " << endl << 
            "len: " << (int)rkmessage->len << endl << 
            "err: " <<rd_kafka_err2str(rkmessage->err) << endl;
    } else {
        clog << "=================================================" << endl;
        clog << "Delivered: " << endl <<
            "  topic: " << rd_kafka_topic_name(rkmessage->rkt) << endl <<
            "  partition: " << (int)rkmessage->partition << endl <<
            "  message: " <<(const char *)rkmessage->payload << endl;
            (*delivery_counterp)++;
    }
}


int Kafka::RunProducer(const char* topic, int msgcnt, KafkaConf& conf) {
    std::clog << "start creating producer" << std::endl;
    rd_kafka_t *rk = nullptr;
    char errstr[512];
    int i;
    int delivery_counter = 0;

    rd_kafka_conf_set_dr_msg_cb(conf.Get(), dr_cb);
    rk = rd_kafka_new(RD_KAFKA_PRODUCER, conf.Get(), errstr, sizeof(errstr));
    
    if (!rk) {
        std::clog << "Failed to create producer: " << errstr << std::endl;
        rd_kafka_conf_destroy(conf.Get());
        return -1;
    }

    if (CreateTopic(rk, topic, 1) == -1) {
        rd_kafka_destroy(rk);
        return -1;
    }

    int run = 1;
    /* Produce messages */
    for (i = 0 ; run && i < msgcnt ; i++) {
        string user = "kapi";
        string data = "some data";
        rd_kafka_resp_err_t err;

        /* Asynchronous produce */
        err = rd_kafka_producev(
            rk,
            RD_KAFKA_V_TOPIC(topic),
            RD_KAFKA_V_KEY(user.data(), user.size()),
            RD_KAFKA_V_VALUE(&data[0], data.size()),
            /* producev() will make a copy of the message
             * value (the key is always copied), so we
             * can reuse the same json buffer on the
             * next iteration. */
            RD_KAFKA_V_MSGFLAGS(RD_KAFKA_MSG_F_COPY),
            RD_KAFKA_V_OPAQUE(&delivery_counter),
            RD_KAFKA_V_END);
        if (err) {
            std::clog << "Produce failed: " << rd_kafka_err2str(err) << std::endl;
            break;
        }

        /* Poll for delivery report callbacks to know the final
         * delivery status of previously produced messages. */
        rd_kafka_poll(rk, 0);
    }

    if (run) {
            /* Wait for outstanding messages to be delivered,
             * unless user is terminating the application. */
        std::clog << "waiting for " << msgcnt - delivery_counter << " messages" <<
            std::endl;
            rd_kafka_flush(rk, 15*1000);
    }

    /* Destroy the producer instance. */
    rd_kafka_destroy(rk);

    fprintf(stderr, "%d/%d messages delivered\n", delivery_counter, msgcnt);

    return 0;
    
}


int Kafka::CreateTopic(rd_kafka_t* rk, const char* topic, int num_partitions) {
    rd_kafka_NewTopic_t *newt;
    char errstr[256];
    rd_kafka_queue_t *queue;
    rd_kafka_event_t *rkev;
    const rd_kafka_CreateTopics_result_t *res;
    const rd_kafka_topic_result_t **restopics;
    const int replication_factor_or_use_default = -1;
    size_t restopic_cnt;
    int ret = 0;

    std::clog << "Creating topic: " << topic << std::endl;

    newt = rd_kafka_NewTopic_new(
        topic, num_partitions, 
        replication_factor_or_use_default,
        errstr, sizeof(errstr)
    );
    
    if (!newt) {
        std::clog << "Failed to create NewTopic object: " << errstr << std::endl;
        return -1;
    }

    /* Use a temporary queue for the asynchronous Admin result */
    queue = rd_kafka_queue_new(rk);

    /* Asynchronously create topic, result will be available on \c queue */
    rd_kafka_CreateTopics(rk, &newt, 1, NULL, queue);

    rd_kafka_NewTopic_destroy(newt);

    /* Wait for result event */
    rkev = rd_kafka_queue_poll(queue, 15*1000);
    if (!rkev) {
            /* There will eventually be a result, after operation
             * and request timeouts, but in this example we'll only
             * wait 15s to avoid stalling too long when cluster
             * is not available. */
        std::clog << "No create topics result in 15s" << std::endl;
        return -1;
    }

    if (rd_kafka_event_error(rkev)) {
        std::clog << "Failed to create topic: " << 
            rd_kafka_event_error_string(rkev);
        rd_kafka_event_destroy(rkev);
        return -1;
    }

    /* Extract the result type from the event. */
    res = rd_kafka_event_CreateTopics_result(rkev);
    assert(res); /* Since we're using a dedicated queue we know this is
                  * a CreateTopics result type. */

    /* Extract the per-topic results from the result type. */
    restopics = rd_kafka_CreateTopics_result_topics(res, &restopic_cnt);
    assert(restopics && restopic_cnt == 1);

    if (rd_kafka_topic_result_error(restopics[0]) ==
        RD_KAFKA_RESP_ERR_TOPIC_ALREADY_EXISTS) 
    {
        std::clog << rd_kafka_topic_result_name(restopics[0]) <<
            " already exist" << std::endl;
    } else if (rd_kafka_topic_result_error(restopics[0])) {
        std::clog << rd_kafka_topic_result_name(restopics[0]) <<
            " failed to create : " <<
            rd_kafka_topic_result_error_string(restopics[0]) <<
            std::endl;
        ret = -1;
    } else {
        std::clog << rd_kafka_topic_result_name(restopics[0]) <<
            " created" << std::endl;
    }

    rd_kafka_event_destroy(rkev);

    return ret;
}



//struct user {
//        char *name;
//        int sum;
//};

/* Only track the first 4 users seen, for keeping the example simple. */
//#define TRACK_USER_CNT 4
//static struct user users[TRACK_USER_CNT];

//static struct user *find_user (const char *name, size_t namelen) {
//        int i;
//
//        for (i = 0 ; i < TRACK_USER_CNT ; i++) {
//                if (!users[i].name) {
//                        /* Free slot, populate */
//                        users[i].name = strndup(name, namelen);
//                        users[i].sum = 0;
//                        return &users[i];
//                } else if (!strncmp(users[i].name, name, namelen))
//                        return &users[i];
//        }
//
//        return NULL; /* No free slots */
//}


/**
 * @brief Handle a JSON-formatted message and update our counter state
 *        for the user specified in the message key.
 *
 * @returns 0 on success or -1 on parse error (non-fatal).
 */
static int handle_message (rd_kafka_message_t *rkm) {
        //json_value *obj;
        //static char errstr[json_error_max];
        //json_settings settings = { .max_memory = 100000 };
        //const char *expected_user = "alice";
        //struct user *user;
        int i;
    using namespace std;

    clog << (const char *)rkm->payload << endl;
    return 0;

//        if (!rkm->key)
//                return 0;
//
//        if (!(user = find_user(rkm->key, rkm->key_len)))
//                return 0;
//
//        if (rkm->key_len != strlen(expected_user) ||
//            strncmp(rkm->key, expected_user, rkm->key_len))
//                return 0;
//
//        /* Value: expected a json object: { "count": 3 } */
//        obj = json_parse_ex(&settings, rkm->payload, rkm->len, errstr);
//        if (!obj) {
//                fprintf(stderr, "Failed to parse JSON: %s\n", errstr);
//                return -1;
//        }
//
//        if (obj->type != json_object) {
//                fprintf(stderr, "Expected JSON object\n");
//                json_value_free(obj);
//                return -1;
//        }
//
//        for (i = 0 ; i < obj->u.object.length ; i++) {
//                const json_object_entry *v = &obj->u.object.values[i];
//
//                if (strcmp(v->name, "count") ||
//                    v->value->type != json_integer)
//                        continue;
//
//                user->sum += v->value->u.integer;
//                printf("User %s sum %d\n", user->name, user->sum);
//                break;
//        }
//
//        json_value_free(obj);
//
//        return 0;
}


int Kafka::RunConsumer(const char* topic, KafkaConf& conf) {
    rd_kafka_t *rk;
    char errstr[512];
    rd_kafka_resp_err_t err;
    rd_kafka_topic_partition_list_t *topics;
    int i;

    using namespace std;
    rk = rd_kafka_new(RD_KAFKA_CONSUMER, conf.Get(), errstr, sizeof(errstr));
    if (!rk) {
        clog << "Failed to create consumer: " << errstr << endl;
        rd_kafka_conf_destroy(conf.Get());
        return -1;
    }
        
    rd_kafka_poll_set_consumer(rk);
        
    topics = rd_kafka_topic_partition_list_new(1);
    rd_kafka_topic_partition_list_add(topics, topic, RD_KAFKA_PARTITION_UA);
    //conf.SetConf()
    
    fprintf(stderr,
        "Subscribed to %s, waiting for assignment and messages...\n"
        "Press Ctrl-C to exit.\n", topic);
    err = rd_kafka_subscribe(rk, topics);
    rd_kafka_topic_partition_list_destroy(topics);

    if (err) {
        fprintf(stderr, "Subscribe(%s) failed: %s\n",
            topic, rd_kafka_err2str(err));
        rd_kafka_destroy(rk);
        return -1;
    }

    int run = 1;
        /* Consume messages */
    while (run) {
        rd_kafka_message_t *rkm;

        /* Poll for a single message or an error event.
         * Use a finite timeout so that Ctrl-C (run==0) is honoured. */
        rkm = rd_kafka_consumer_poll(rk, 1000);
        if (!rkm)
            continue;

        if (rkm->err) {
            /* Consumer error: typically just informational. */
            fprintf(stderr, "Consumer error: %s\n",
                        rd_kafka_message_errstr(rkm));
        } else {
            /* Proper message */
            fprintf(stderr,
                "Received message on %s [%d] "
                "at offset %"PRId64": %.*s\n",
                rd_kafka_topic_name(rkm->rkt),
                (int)rkm->partition, rkm->offset,
                (int)rkm->len, (const char *)rkm->payload);
            handle_message(rkm);
        }

        rd_kafka_message_destroy(rkm);
    }

    /* Close the consumer to have it gracefully leave the consumer group
     * and commit final offsets. */
    rd_kafka_consumer_close(rk);

    /* Destroy the consumer instance. */
    rd_kafka_destroy(rk);

    //for (i = 0 ; i < TRACK_USER_CNT ; i++)
    //        if (users[i].name)
    //                free(users[i].name);

    return 0;
}
