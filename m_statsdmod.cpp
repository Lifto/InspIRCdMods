/*
* m_statsdmod -- StatsD module for InspIRCd
*/

#include "inspircd.h"
#include "m_zookeepermod.h"
#include "m_statsdmod.h"

/* $CompileFlags: -L../../lib -I../../include/ -std=c++0x */
/* $LinkerFlags: -L../../lib  -lzookeeper_mt -lstatsd */

// log - convenient synchronized logging function.
Mutex* loggingMutex = new Mutex();
void log(string s){
    loggingMutex->Lock();
    ServerInstance->Logs->Log("m_statsdmod", DEFAULT, s);
    loggingMutex->Unlock();
}

class StatsDZKCallback {
  protected:
    int rehashId;
  public:
    StatsDZKCallback(int rehashId) : rehashId(rehashId){}
    void process(int rc, const char* value, int value_len);
    // This is the function the zk lib calls, using this obj as 'data'
    static void callback(int rc, const char *value, int value_len,
                           const struct Stat *stat, const void *data){
        StatsDZKCallback* obj = (StatsDZKCallback*)data;
        obj->process(rc, value, value_len);
        delete obj;
    }
};

class StatsDZKWatch {
  protected:
    int rehashId;
  public:
    StatsDZKWatch(int rehashId) : rehashId(rehashId){}
    void process(int event, int state, const char* path);
    // This is the function the zk lib calls, using this obj as 'watcherCtx'
    static void watch(zhandle_t *zh, int type,
                       int state, const char *path, void *watcherCtx){
        StatsDZKWatch* obj = (StatsDZKWatch*)watcherCtx;
        obj->process(type, state, path);
        delete obj;
    }

};

// InspIRCd Module ------------------------------------------------------------

void StatsDMod::init(){
    link = NULL;
    confReloadRequested = true;
    currentRehash = 0;
    OnBackgroundTimer(0);
    Implementation eventlist[] = { I_OnBackgroundTimer, I_OnRehash };
    ServerInstance->Modules->Attach(eventlist,
                                    this,
                                    sizeof(eventlist)/sizeof(Implementation));
    log("statsdmod: events attached");
}

Version StatsDMod::GetVersion(){
    return Version("App StatsDMod");
}

void StatsDMod::OnBackgroundTimer(time_t curtime){
    // See if a zookeeper event requires us to reconnect.
    confMutex.Lock();
    bool doConfReload = confReloadRequested;
    confReloadRequested = false;
    bool doCheckZookeeper = checkZookeeper;
    checkZookeeper = false;
    bool doReconnect = reconnectRequested;
    reconnectRequested = false;
    confMutex.Unlock();

    if(doConfReload){
        doCheckZookeeper = false;
        doReconnect = false;
        // Read config info from inspircd.conf
        ConfigTag* conf = ServerInstance->Config->ConfValue("statsd");
        name = conf->getString("name");
        zookeeperNode = conf->getString(
            "zookeeper_statsd_host_node",
            "/statsd");
        log("statsdmod: zookeeperNode " + zookeeperNode);
        host = conf->getString("host", "127.0.0.1");
        log("statsdmod: host " + host);
        port = conf->getInt("port", 8125);
        log("statsdmod: port " + static_cast<ostringstream*>( &(ostringstream() << port) )->str());
        if(zookeeperNode.size()!=0){
            doCheckZookeeper = true;
            checkZookeeperCount = 0;
        }else{
            doReconnect = true;
        }
    }

    // We periodically check in case zookeepermod was not ready on first check.
    if(doCheckZookeeper){
        doReconnect = false;
        currentRehash++;
        log("statsdmod: requesting hosts from ZooKeeper");
        ZooKeeperMod* zkmod;
        zkmod = (ZooKeeperMod*)ServerInstance->Modules->Find("m_zookeepermod.so");
        // You crash if you try to use zkmod before it's connected.
        if(zkmod->connected){
            zkmod->mutex.Lock();
            zoo_awget(zkmod->client,
                      zookeeperNode.c_str(),
                      StatsDZKWatch::watch, new StatsDZKWatch(currentRehash),
                      StatsDZKCallback::callback, new StatsDZKCallback(currentRehash));
            zkmod->mutex.Unlock();
        }else{
            if(checkZookeeperCount > 10){
                doReconnect = true;
                log("statsdmod: could not connect to zookeeper, using conf.");
            }else{
                checkZookeeperCount += 1;
                confMutex.Lock();
                checkZookeeper = true;
                confMutex.Unlock();
            }
        }
    }

    if(doReconnect){
        if(NULL!=link){
            log("statsdmod: finalizing old link");
            statsd_finalize(link);
        }
        log("statsdmod: linking to " + host + " port " + static_cast<ostringstream*>( &(ostringstream() << port) )->str());
        link = statsd_init(host.c_str(), port);
    }

    // Keep a guage of user stats (nowhere better to put it.)
    if(NULL!=link){
        statsDMutex.Lock();
//        statsd_gauge(link, (name + "_users").c_str(), ServerInstance->Users->LocalUserCount());
        statsDMutex.Unlock();
    }
}//

void StatsDMod::OnRehash(User* user){
    log("statsdmod: OnRehash");
    confMutex.Lock();
    confReloadRequested = true;
    confMutex.Unlock();
    OnBackgroundTimer(0);
}


// -- ZooKeeper ---------------------------------------------------------------
void StatsDZKCallback::process(int rc, const char* value, int value_len){
    log("statsdmod: zookeeper callback");
    StatsDMod* smod = (StatsDMod*)ServerInstance->Modules->Find("m_statsdmod.so");
    if(ZCONNECTIONLOSS==rc){
        log("statsdmod: Lost connection to ZooKeeper");
        smod->confMutex.Lock();
        smod->confReloadRequested = true;
        smod->confMutex.Unlock();
    }else{
        if(smod->currentRehash==rehashId){
            log("statsdmod: zookeeper get called back");
            if(value_len==0){
                log("statsdmod: zookeeper data empty, using conf");
            }else{
                string data;
                data.assign(value, value_len);
                log("statsdmod: zk found " + data);
                // Split the host:port into separate strings.
                vector<string> split_host_and_port;
                boost::split(split_host_and_port, data, boost::is_any_of(":"));
                smod->host = split_host_and_port[0];;
                smod->port = atoi(split_host_and_port[1].c_str());
                log("statsdmod: got host and port from zookeeper " + data);
            }
            smod->confMutex.Lock();
            smod->reconnectRequested = true;
            smod->confMutex.Unlock();
        }else{
            log("statsdmod: zookeeper call back is old, ignoring");
        }
    }
    log("statsdmod: zookeeper callback done");
}

void StatsDZKWatch::process(int event, int state, const char* path){
    log("statsdmod: zookeeper watch");
    if(ZOO_CONNECTED_STATE==state){
        log("statsdmod: host watch triggered, re-processing conf");
        StatsDMod* smod = (StatsDMod*)ServerInstance->Modules->Find("m_statsdmod.so");
        smod->confMutex.Lock();
        smod->confReloadRequested = true;
        smod->confMutex.Unlock();
    }
    log("statsdmod: zookeeper watch done");
}
MODULE_INIT(StatsDMod)

