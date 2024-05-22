/*
* m_kafkamod -- Kafka chat-logging module for InspIRCd
*/

#ifndef KAFKAMOD_H
#define KAFKAMOD_H

#include <chrono>
#include <cstring>
#include <errno.h>
#include <iostream>
#include <queue>
#include <sstream>
#include <string>
#include <thread>
#include <vector>
#include <unistd.h>

#include <boost/algorithm/string.hpp>
#include <json/json.h>
#include <librdkafka/rdkafkacpp.h>
#include <zookeeper/zookeeper.h>

#include "inspircd.h"
#include "threadengine.h"
#include "m_zookeepermod.h"
#include "m_statsdmod.h"

using namespace std;
using namespace chrono;
using namespace RdKafka;
#define BACKOFF_SIZE 5

/* $CompileFlags: -L../../lib -I../../include/ -std=c++0x */
/* $LinkerFlags: -L../../lib -lz -lrdkafka++ -lpthread -lsnappy -lz -ljson -lzookeeper_mt -llog4cxx */

/*
This module listens on all channels, and forwards any privmsg to that channel
to the Kafka topic "chat".

This is the message we log to kafka when a user posts in a channel.
This structure is encoded to JSON before being sent.
(note the nested JSON is treated as plain text, and is thus double-encoded.)
{
"type":"cm",
"m":"<json text message from App iPhone client>",
"c":"#pr-5100a885e21e5cdf651072abd9c3c94d",
"ts":1390450179,
"from":"uf5b48401c91d49c6927dafe8c1f17551",
"recip":["udd0f83eb7ed511e38cfc14109fd4dd1d", "ue01a22b87ed511e392b514109fd4dd1d", "u36b463e372e041a998cef5c58946161b"]
}
type cm means "channel message"

*/

// Misc -----------------------------------------------------------------------
class KafkaThread: public QueuedThread{
    protected:
        unsigned int backoff[BACKOFF_SIZE];
        unsigned int backoffReset;  // After this much time, backoff resets.
        unsigned int currentBackoff;
        unsigned long long lastReconnect;
        string topicStr;
        Topic* topic;
        Producer* client;
        string zookeeper_kafka_hosts_node;
        bool isConnected;         // True if viable connection to Cassandra.
        bool Publish(const string& message);

    public:
        std::queue<string> incomingQueue;  // Inspircd enqueues to here.
        std::queue<string> readyQueue;  // Kafkamod snarfs to here and publishes from here.
        unsigned long long lastPost;  // Timestamp of last post.
        bool confReloadRequested;
        int currentRehash;    // Id to catch and ignore old async results.
        string hosts;
        bool confLoaded;          // True if conf loaded since init or rehash.
        KafkaThread() : backoff({1000, 2000, 3000, 5000, 10000}),
                        backoffReset(60000),
                        currentBackoff(0),
                        lastReconnect(0),
                        confReloadRequested(false),
                        currentRehash(0),
                        confLoaded(false),
                        isConnected(false),
                        client(NULL){};
        virtual void Run();
        void Enqueue(const string& encodedMessage);
};

// InspIRCd Module ------------------------------------------------------------

class KafkaMod : public Module{
  public:
    KafkaThread *kafkaThread;
    void init();
	virtual ~KafkaMod(){
	}

	virtual Version GetVersion();

	ModResult OnUserPreMessage(User* user,
	                           void* dest,
	                           int target_type,
	                           std::string &text,
	                           char status,
	                           CUList &exempt_list);

	void OnRehash(User* user);
};

#endif