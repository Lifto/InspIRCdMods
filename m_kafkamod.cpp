/*
* m_kafkamod -- Kafka chat-logging module for InspIRCd
*/

#include "inspircd.h"
#include "threadengine.h"
#include "m_zookeepermod.h"
#include "m_kafkamod.h"

/* $CompileFlags: -L../../lib -I../../include/ -std=c++0x */
/* $LinkerFlags: -L../../lib -L/usr/local/lib -lrdkafka++ -lpthread -lsnappy -lz -ljson -lzookeeper_mt -llog4cxx -lstatsd */
/* -L/usr/local/lib is needed on OS X Yosemite to find -lsnappy. */

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

// log - convenient synchronized logging function.
Mutex* loggingMutex = new Mutex();
void log(string s){
    loggingMutex->Lock();
    ServerInstance->Logs->Log("m_kafkamod", DEFAULT, s);
    loggingMutex->Unlock();
}

unsigned long long milliseconds_since_epoch(){
    return (((unsigned long long)ServerInstance->Time()) * 1000.0) + (((unsigned long long)ServerInstance->Time_ns()) / 1000000.0);
}

string ullToStr(unsigned long long i){
    string asStr;
    asStr = static_cast<ostringstream*>( &(ostringstream() << i) )->str();
    return asStr;
}

// StatsD commands
StatsDMod* statsDMod;
StatsDMod* getStatsDMod(){
    if(NULL==statsDMod){
        statsDMod = (StatsDMod*)ServerInstance->Modules->Find("m_statsdmod.so");
    }
    return statsDMod;
}

void statsDMessageCount(){
    StatsDMod* statsdmod = getStatsDMod();
    if(NULL!=statsdmod){
        statsdmod->count("kafkaPublish", 1, 1.0);
    }
}

void statsDErrorCount(){
    StatsDMod* statsdmod = getStatsDMod();
    if(NULL!=statsdmod){
        statsdmod->count("kafkaError", 1, 1.0);
    }
}

class KafkaZKCallback {
  protected:
    int rehashId;
  public:
    KafkaZKCallback(int rehashId) : rehashId(rehashId){}
    void process(int rc, const char* value, int value_len);
    // This is the function the zk lib calls, using this obj as 'data'
    static void callback(int rc, const char *value, int value_len,
                         const struct Stat *stat, const void *data){
        KafkaZKCallback* obj = (KafkaZKCallback*)data;
        obj->process(rc, value, value_len);
        delete obj;
    }
};

class KafkaZKWatch {
  protected:
    int rehashId;
  public:
    KafkaZKWatch(int rehashId) : rehashId(rehashId){}
    void process(int event, int state, const char* path);
    // This is the function the zk lib calls, using this obj as 'watcherCtx'
    static void watch(zhandle_t *zh, int type,
                      int state, const char *path, void *watcherCtx){
        KafkaZKWatch* obj = (KafkaZKWatch*)watcherCtx;
        obj->process(type, state, path);
        delete obj;
    }
};

// InspIRCd Module ------------------------------------------------------------

void KafkaMod::init(){
    kafkaThread = new KafkaThread();
    ServerInstance->Threads->Start(kafkaThread);
    log("kafkamod: threads started");
    Implementation eventlist[] = { I_OnUserPreMessage, I_OnRehash };
    ServerInstance->Modules->Attach(eventlist,
                                    this,
                                    sizeof(eventlist)/sizeof(Implementation));
    log("kafkamod: events attached");
}

Version KafkaMod::GetVersion(){
    return Version("App KafkaMod");
}

ModResult KafkaMod::OnUserPreMessage(User* user,
                                     void* dest,
                                     int target_type,
                                     std::string &text,
                                     char status,
                                     CUList &exempt_list){
    if (target_type == TYPE_CHANNEL && status == 0){
        // Determine if this is already timestampmod encoded.
        bool isTimestamp = false;
        Json::Value root;
        Json::Reader reader;
        bool parsingSuccessful = reader.parse(text, root);
        if(parsingSuccessful){
            std::string content = root.get("ts", "NONE" ).asString();
            if(content!="NONE"){
                // This is already timestamped, and came via spanningtree
                // so ignore it.
                return MOD_RES_PASSTHRU;
            }
        }

        Channel* c = (Channel*)dest;
        unsigned long long timestamp = milliseconds_since_epoch();
        Json::Value kafkaMessage;
        kafkaMessage["type"] = "cm";
        kafkaMessage["m"] = text;
        kafkaMessage["ts"] = timestamp;
        kafkaMessage["c"] = c->name;
        kafkaMessage["from"] = user->nick.substr(1, 32);
        UserMembCIter iter;
        for (iter=c->userlist.begin(); iter!=c->userlist.end(); ++iter){
            kafkaMessage["recip"].append(iter->first->nick.substr(1, 32));
        }
        Json::FastWriter kafkaWriter;
        string encodedKafkaMessage = kafkaWriter.write(kafkaMessage);
        kafkaThread->Enqueue(encodedKafkaMessage);
    }
    return MOD_RES_PASSTHRU;
}

void KafkaMod::OnRehash(User* user){
    log("kafkamod: OnRehash");
    kafkaThread->LockQueue();
    kafkaThread->confReloadRequested = true;
    kafkaThread->UnlockQueueWakeup();
}

// KafkaThread ----------------------------------------------------------------

// KafkaThread -- Threaded service for sending messages to Kafka.
//     This object manages a KafkaConnection.  KafkaThread handles the
//     threaded control of the connection.  It publishes enqueued messages,
//     and it has the KafkaConnection reconnect when the connection goes down
//     or there is a signal to change the urls or topic of the connection.

void KafkaThread::Run(){
    LockQueue();
    confReloadRequested = true;
    while(!GetExitFlag()){
        // To keep from spinning the loop, test all of the conditions here:
        bool willDoSomething =
            confReloadRequested ||
            (!isConnected && confLoaded) ||
            !incomingQueue.empty() ||
            (isConnected && !readyQueue.empty());
        if(!willDoSomething){
            WaitForQueue();
        }
        unsigned long long startTime = milliseconds_since_epoch();

        bool checkConfReloadRequested = confReloadRequested;
        UnlockQueue();

        if(checkConfReloadRequested){
            log("kafkamod: configuring");
            confReloadRequested = false;
            confLoaded = false;  // True if conf loaded since init or rehash.
            isConnected = false; // True if viable connection to Cassandra.
            if(NULL!=client){
                log("kafkamod: existing client was not NULL, deleting");
                delete client;
                client = NULL;
            }
            log("kafkamod: getting conf from server config file");
            ConfigTag* conf = ServerInstance->Config->ConfValue("kafka");
            zookeeper_kafka_hosts_node = conf->getString("zookeeper_kafka_hosts_node");
            log("kafkamod: zookeeper_kafka_hosts_node " + zookeeper_kafka_hosts_node);
            hosts = conf->getString("hosts");
            log("kafkamod: (conf) hosts " + hosts);
            topicStr = conf->getString("topic", "chat");
            log("kafkamod: topic " + topicStr);
            bool useZk = zookeeper_kafka_hosts_node.length()!=0;
            ZooKeeperMod* zkmod;
            if(useZk){
                log("kafkamod: zookeeper_kafka_hosts_node not empty, checking zookeeper for hosts");
                zkmod = (ZooKeeperMod*)ServerInstance->Modules->Find("m_zookeepermod.so");
                // You crash if you try to use zkmod before it's connected.
                for(int attempt=0; attempt<6 && !confReloadRequested; attempt++){
                    if(zkmod->connected)
                        break;
                    log("kafkamod: waiting for zookeepermod to connect");
                    unsigned long long sleepResult;
                    sleepResult = sleep(5);
                    if(sleepResult!=0){
                        log("kafkamod: zookeeper connect sleep fail " + ullToStr(sleepResult));
                    }
                }
                // This attempt was interrupted by a new rehash, so start over.
                LockQueue();
                bool checkConfReloadRequested = confReloadRequested;
                UnlockQueue();
                if(checkConfReloadRequested){
                    log("kafkamod: re-trying conf, reload requested again");
                    LockQueue();
                    continue;
                }
                if(!zkmod->connected){
                    log("kafkamod: zookeeper couldn't connect, using conf file instead");
                    useZk = false;
                }else
                    log("kafkamod: zookeeper connection found");
            }
            confLoaded = !useZk;
            if(useZk){
                // Note: There could be a race here if last rehash is pending
                // and then we request the data again.  The old data could
                // arrive and be interpreted as a response to this request.
                // Submit our request to get called back with host names.
                // For that reason we use and check the rehashId.
                currentRehash++;
                log("kafkamod: requesting hosts from ZooKeeper @ " + zookeeper_kafka_hosts_node);
                zkmod->mutex.Lock();
                zoo_awget(zkmod->client,
                    zookeeper_kafka_hosts_node.c_str(),
                    KafkaZKWatch::watch, new KafkaZKWatch(currentRehash),
                    KafkaZKCallback::callback, new KafkaZKCallback(currentRehash));
                zkmod->mutex.Unlock();
            }else{
                log("kafkamod: conf loaded");
            }
        }

        LockQueue();
        bool checkConfLoaded = confLoaded;
        UnlockQueue();
        if(!isConnected && checkConfLoaded){
            log("kafkamod: preparing to connect");
            // Has it been long enough since our last reconnect attempt?
            unsigned long long now = milliseconds_since_epoch();
            unsigned long diff = now - lastReconnect;
            if(diff>backoffReset){
                currentBackoff = 0;
            }
            unsigned int thisBackoff = backoff[currentBackoff];
            if(currentBackoff != (BACKOFF_SIZE-1)){
                currentBackoff++;
            }
            if (diff < thisBackoff){
                unsigned long long sleepResult;
                sleepResult = sleep((thisBackoff - diff) / 1000);
                if(sleepResult!=0){
                    log("kafkamod: connect sleep fail " + ullToStr(sleepResult));
                }
            }
            lastReconnect = milliseconds_since_epoch();
            vector<string> host_and_ports;
            boost::split(host_and_ports,
                         hosts,
                         boost::is_any_of(","));

            // Choose a host at random.
            int h_a_p_index = rand() % host_and_ports.size();
            string host_and_port = host_and_ports[h_a_p_index];
            log("kafkamod: connecting to " + host_and_port);
            //----
            // I'm worried that if there is an async kafka operation pending
            // with the old topic and we delete it on rehash that we could
            // crash inspird so we are going to leak topics.  They are not
            // expected to change so this ought to be OK. (another option
            // would be to hack rdkafka to use an autopointer, or to poll
            // old messages and delete topic after they are done.)
            // if(topic!=NULL){
            //     delete topic;
            // }
            ///----fromexample
            RdKafka::Conf *rdConf = RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);
            RdKafka::Conf *tConf = RdKafka::Conf::create(RdKafka::Conf::CONF_TOPIC);
            std::string errstr;

            bool rdConfError = false;
            if(rdConf->set("metadata.broker.list", host_and_port, errstr)!=RdKafka::Conf::CONF_OK){
                log("kafkamod: rdConf error setting brokers");
                rdConfError = true;
            }
            if(rdConfError){
                log("kafkamod: rdConf error, retry in 2 seconds");
                unsigned long long sleepResult;
                sleepResult = sleep(2);
                if(sleepResult!=0){
                    log("kafkamod: rdConf sleep fail " + ullToStr(sleepResult));
                }
                LockQueue();
                continue;
            }
//            //Enable compression:  none|gzip|snappy\
//            if (conf->set("compression.codec", optarg, errstr) !=
//                  RdKafka::Conf::CONF_OK) {
//                std::cerr << errstr << std::endl;
//                exit(1);
//            }

//            //Enable statistics
//            if (conf->set("statistics.interval.ms", optarg, errstr) !=
//                RdKafka::Conf::CONF_OK) {
//                std::cerr << errstr << std::endl;
//                exit(1);
//            }

//            // -- from the example, looks to be conf file read related --
//            //Try "topic." prefixed properties on topic
//            // * conf first, and then fall through to global if
//            // * it didnt match a topic configuration property.
//            RdKafka::Conf::ConfResult res;
//            if (!strncmp(name, "topic.", strlen("topic.")))
//                  res = tconf->set(name+strlen("topic."), val, errstr);
//                else
//              res = conf->set(name, val, errstr);
//
//            if (res != RdKafka::Conf::CONF_OK) {
//                  std::cerr << errstr << std::endl;
//              exit(1);
//            }

            // Create producer using accumulated global configuration.
            client = RdKafka::Producer::create(rdConf, errstr);
            if(!client){
                log("kafkamod: Failed to create producer, retry in 10 seconds");
                log(errstr);
                unsigned long long sleepResult;
                sleepResult = sleep(10);
                if(sleepResult!=0){
                    log("kafkamod: create producer sleep fail " + ullToStr(sleepResult));
                }
                LockQueue();
                continue;
            }else{
                log("kafkamod: Created producer " + client->name());
            }

            // Create topic handle.
            topic = RdKafka::Topic::create(client, topicStr, tConf, errstr);
            if(!topic){
                log("kafkamod: Failed to create topic, retry in 10 seconds");
                log(errstr);
                log("kafkamod: deleting client");
                delete client;
                client = NULL;
                unsigned long long sleepResult;
                sleepResult = sleep(10);
                if(sleepResult!=0){
                    log("kafkamod: create topic sleep fail " + ullToStr(sleepResult));
                }
                LockQueue();
                continue;
            }else{
                log("kafkamod: Created topic object");
            }

            if(client && topic){
                log("kafkamod: Kafka Connected");
                isConnected = true;
            }
        }

        if(isConnected && !readyQueue.empty()){
            if(Publish(readyQueue.front())){
                readyQueue.pop();
            }else{
                log("kafkamod: Lost Kafka connection, reconnecting.");
                delete client;
                client = NULL;
                isConnected = false;
            }
        }

        LockQueue();
        int snarfCount = 0;
        while(!incomingQueue.empty() && snarfCount < 10){
            readyQueue.push(incomingQueue.front());
            incomingQueue.pop();
            snarfCount++;
        }
        unsigned long long took = milliseconds_since_epoch() - startTime;
        log("kafkamod: looptime " + ullToStr(took));
        if (took > 1000){
            log("kafkamod: warning loop took over one second");
        }

    }
}

// Enqueue -- Enqueue a new message to be sent to Kafka on its thread.
void KafkaThread::Enqueue(const string& encodedMessage){
    LockQueue();
    incomingQueue.push(encodedMessage);
    UnlockQueueWakeup();
}

bool KafkaThread::Publish(const string& message){
    //const char *message_string = "This is a test message.";
    unsigned long long startTime = milliseconds_since_epoch();
    log("kafkamod: preparing " + message);
    //--from the examlpe
    bool hadError = false;
    RdKafka::ErrorCode resp;
    try{
        resp = client->produce(topic, RdKafka::Topic::PARTITION_UA,
                               RdKafka::Producer::MSG_COPY /* Copy payload */,
                               const_cast<char *>(message.c_str()),
                               message.size(),
                               NULL, NULL);
    }catch(...){
        hadError = true;
    }
    bool retval = true;
    if(hadError){
        log("kafka: an exception raised while sending the produce request");
        retval = false;
    }else if(resp!=RdKafka::ERR_NO_ERROR){
        log("kafka: an error ocurred while sending the produce request, error was: " + RdKafka::err2str(resp));
        retval = false;
    }

    lastPost = milliseconds_since_epoch();
    if(retval){
        log("kafkamod: sent " + message);
        statsDMessageCount();
    }else{
        log("kafkamod: error sending " + message);
        statsDErrorCount();
    }
    unsigned long long took = milliseconds_since_epoch() - startTime;
    log("kafkamod: publishtime " + ullToStr(took));
    if (took > 1000){
        log("kafkamod: warning publish took over one second");
    }
    return retval;
}

// -- ZooKeeper ---------------------------------------------------------------
void KafkaZKCallback::process(int rc, const char* value, int value_len){
    log("kafkamod: zookeeper callback");
    KafkaMod* kmod = (KafkaMod*)ServerInstance->Modules->Find("m_kafkamod.so");
    kmod->kafkaThread->LockQueue();
    if(ZCONNECTIONLOSS==rc){
        log("kafkamod: Lost connection to ZooKeeper");
        kmod->kafkaThread->confReloadRequested = true;
    }else{
        if(kmod->kafkaThread->currentRehash==rehashId){
            log("kafkamod: zookeeper get called back cassandra hosts");
            if(value_len==0){
                log("kafkamod: zookeeper data empty, using conf");
            }else{
                kmod->kafkaThread->hosts.assign(value, value_len);
                log("kafkamod: using zk host '" + kmod->kafkaThread->hosts + "'");
            }
            kmod->kafkaThread->confLoaded = true;
        }else{
            log("kafkamod: zookeeper call back is old, ignoring");
        }

    }
    kmod->kafkaThread->UnlockQueueWakeup();
    log("kafkamod: zookeeper callback done");
}

void KafkaZKWatch::process(int type, int state, const char* path){
    log("kafkamod: zookeeper watch");
    if(ZOO_CONNECTED_STATE==state){
        log("kafkamod: hosts watch triggered, re-processing conf");
        KafkaMod* kmod = (KafkaMod*)ServerInstance->Modules->Find("m_kafkamod.so");
        kmod->kafkaThread->LockQueue();
        kmod->kafkaThread->confReloadRequested = true;
        kmod->kafkaThread->UnlockQueueWakeup();
    }
    log("kafkamod: zookeeper watch done");
}
MODULE_INIT(KafkaMod)

