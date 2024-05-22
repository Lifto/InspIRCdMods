/*
* m_statusmod -- publish status to zookeeper module
*/

#include <chrono>
#include <cstring>
#include <errno.h>
#include <iostream>
#include <queue>
#include <sstream>
#include <string>
#include <thread>
#include <time.h>
#include <vector>
#include <unistd.h>

#include <boost/algorithm/string.hpp>
#include <zookeeper/zookeeper.h>

#include "inspircd.h"
#include "threadengine.h"
#include "m_zookeepermod.h"
#include "m_statsdmod.h"

using namespace std;
using namespace chrono;

/* $CompileFlags: -L../../lib -I../../include/ -std=c++0x */
/* $LinkerFlags: -L../../lib -lzookeeper_mt -llog4cxx */

// Misc -----------------------------------------------------------------------
// log - convenient synchronized logging function.
Mutex* loggingMutex = new Mutex();
void log(string s){
    loggingMutex->Lock();
    ServerInstance->Logs->Log("m_statusmod", DEFAULT, s);
    loggingMutex->Unlock();
}

string timestamp(){
    // seconds since epoch
    string asStr;
    asStr = static_cast<ostringstream*>( &(ostringstream() << ServerInstance->Time()) )->str();
    return asStr;
}

string ullToStr(unsigned long long i){
    string asStr;
    asStr = static_cast<ostringstream*>( &(ostringstream() << i) )->str();
    return asStr;
}

string zkReturnCodeStr(int i){
    switch(i){
        case ZOK:
            return "Everything is OK";
        case ZRUNTIMEINCONSISTENCY:
            return "A runtime inconsistency was found";
        case ZDATAINCONSISTENCY:
            return "A data inconsistency was found";
        case ZCONNECTIONLOSS:
            return "Connection to the server has been lost";
        case ZMARSHALLINGERROR:
            return "Error while marshalling or unmarshalling data";
        case ZUNIMPLEMENTED:
            return "Operation is unimplemented";
        case ZOPERATIONTIMEOUT:
            return "Operation timeout";
        case ZBADARGUMENTS:
            return "Invalid arguments";
        case ZINVALIDSTATE:
            return "Invliad zhandle state";
        case ZNONODE:
            return "Node does not exist";
        case ZNOAUTH:
            return "Not authenticated";
        case ZBADVERSION:
            return "Version conflict";
        case ZNOCHILDRENFOREPHEMERALS:
            return "Ephemeral nodes may not have children";
        case ZNODEEXISTS:
            return "The node already exists";
        case ZNOTEMPTY:
            return "The node has children";
        case ZSESSIONEXPIRED:
            return "The session has been expired by the server";
        case ZINVALIDCALLBACK:
            return "Invalid callback specified";
        case ZINVALIDACL:
            return "Invalid ACL specified";
        case ZAUTHFAILED:
            return "Client authentication failed";
        case ZCLOSING:
            return "ZooKeeper is closing";
        case ZNOTHING:
            return "(not error) no server responses to process";
        case ZSESSIONMOVED:
            return "session moved to another server, so operation is ignored";
        default:
            ;
    }

    if(i<ZAPIERROR){
        return "Unknown API error";
    }

    if(i<ZSYSTEMERROR){
        return "Unknown System error";
    }
}

string timeToStr(timespec t){
    return ullToStr(t.tv_sec) + " (nanos) " + ullToStr(t.tv_nsec);
}

string userCount(){
    unsigned int uc = ServerInstance->Users->LocalUserCount();
    string asStr;
    asStr = static_cast<ostringstream*>( &(ostringstream() << uc) )->str();
    return asStr;
}

class StatusThread: public QueuedThread{
  public:
    ACL_vector acl[1]; // ZooKeeper permissions
// we are using the fixed ZOO_OPEN_ACL_UNSAFE
    ZooKeeperMod* zkmod;
    string internalIp;
    string zookeeperNode;
    string dc;
    string rack;
    string sslCertName;
    string name;
    unsigned int populationLimit;
    int confirmPeriodSeconds;
    bool shuttingDown;
    bool updateRequested;
    // Create confirmation bits
    bool topNodeCreated;
    bool zookeeperNodeCreated;
    bool zookeeperNameCreated;
    bool timestampCreated;
    bool populationCreated;
    bool connectionsAllowedCreated;
    bool dcCreated;
    bool internalIpCreated;
    bool rackCreated;
    bool sslCertNameCreated;

    StatusThread(ZooKeeperMod* zk) :
            zkmod(zk),
            shuttingDown(false),
            topNodeCreated(false),
            zookeeperNodeCreated(false),
            zookeeperNameCreated(false),
            timestampCreated(false),
            populationCreated(false),
            connectionsAllowedCreated(false),
            internalIpCreated(false),
            dcCreated(false),
            rackCreated(false),
            sslCertNameCreated(false){
        // we will used the static ZOO_OPEN_ACL_UNSAFE
        acl[0] = ZOO_OPEN_ACL_UNSAFE;
        //data::ACL perm;
        //perm.getid().getscheme() = "world";
        //perm.getid().getid() = "anyone";
        //perm.setperms(Permission::All);
        //acl.push_back(perm);
    };
    void Run();
    void WaitForZKConnected();
    void CreateNodes();
    void DoHackUpdate();
    void MakeOps(zoo_op_t* ops);
    void UpdateNodes(zoo_op_t* ops);
    void Update(string node, string data);
    void Create(string node, string data, bool &flag);
};

class TimerThread: public QueuedThread{
  public:
    StatusThread* statusThread;
    int period_seconds;
    TimerThread(StatusThread* st) : statusThread(st){}
    void Run();
};

class DeadmanThread: public QueuedThread{
  public:
    ZooKeeperMod* zkmod;
    StatusThread* statusThread;
    string zookeeperNode;
    int timeoutSeconds;
    int periodSeconds;

    DeadmanThread(StatusThread* st,
                  ZooKeeperMod* zk) : statusThread(st), zkmod(zk){}
    void Run();
    void WaitForZKConnected();
    void HandleDeadServers(const string &node);
};

class StatusMod : public Module{
  public:
    StatusThread* statusThread;
    TimerThread* timerThread;
    DeadmanThread* deadmanThread;
    void init(){
        ZooKeeperMod* zkmod =
            (ZooKeeperMod*)ServerInstance->Modules->Find("m_zookeepermod.so");
        statusThread = new StatusThread(zkmod);
        timerThread = new TimerThread(statusThread);
        deadmanThread = new DeadmanThread(statusThread, zkmod);
        OnRehash(NULL);
        ServerInstance->Threads->Start(statusThread);
        ServerInstance->Threads->Start(timerThread);
        ServerInstance->Threads->Start(deadmanThread);
        log("statusmod: threads started");
        Implementation eventlist[] = { I_OnRehash, I_OnUnloadModule };
        ServerInstance->Modules->Attach(eventlist,
                                        this,
                                        sizeof(eventlist)/sizeof(Implementation));
    }

	virtual ~StatusMod(){
	}

	virtual Version GetVersion(){
		return Version("App StatusMod");
	}

	void OnRehash(User* user){
        ConfigTag* conf = ServerInstance->Config->ConfValue("status");
        timerThread->LockQueue();
        timerThread->period_seconds =
            conf->getInt("update_period_seconds", 10);
        timerThread->UnlockQueue();
        log("statusmod: update_period_seconds " + ullToStr(conf->getInt("update_period_seconds", 300)));

        deadmanThread->LockQueue();
        deadmanThread->zookeeperNode = conf->getString("zookeeper_server_status_node");
        deadmanThread->timeoutSeconds =
            conf->getInt("deadman_timeout_seconds");
        deadmanThread->periodSeconds =
            conf->getInt("deadman_period_seconds");
        deadmanThread->UnlockQueue();
        log("statusmod: zookeeper_server_status_node " + conf->getString("zookeeper_server_status_node"));
        log("statusmod: deadman_timeout_seconds " + conf->getString("deadman_timeout_seconds"));
        log("statusmod: deadman_period_seconds " + conf->getString("deadman_period_seconds"));

        statusThread->LockQueue();
        statusThread->zookeeperNode = conf->getString("zookeeper_server_status_node");
        statusThread->dc = conf->getString("dc");
        statusThread->rack = conf->getString("rack");
        statusThread->sslCertName = conf->getString("ssl_cert_name");
        statusThread->name = conf->getString("name");
        statusThread->internalIp = conf->getString("internal_ip");
        statusThread->populationLimit = conf->getInt("population_limit");
        statusThread->confirmPeriodSeconds =
            conf->getInt("update_confirm_period_seconds", 30);
        statusThread->updateRequested = true;
        statusThread->UnlockQueueWakeup();
        log("statusmod: zookeeper_server_status_node " + conf->getString("zookeeper_server_status_node"));
        log("statusmod: dc " + conf->getString("dc"));
        log("statusmod: rack " + conf->getString("rack"));
        log("statusmod: ssl_cert_name " + conf->getString("ssl_cert_name"));
        log("statusmod: name " + conf->getString("name"));
        log("statusmod: internalIp " + conf->getString("internal_ip"));
        log("statusmod: population_limit " + conf->getString("population_limit"));
        log("statusmod: update_confirm_period_seconds " + ullToStr(conf->getInt("update_confirm_period_seconds", 30)));
    }

    void OnUnloadModule(Module* mod){
        // This is based on the assumption that this gets called before
        // zookeepermod goes away. The assumption is that these are loaded
        // and unloaded in the order they appear in conf.
        statusThread->LockQueue();
        statusThread->shuttingDown = true;
        statusThread->updateRequested = true;
        statusThread->UnlockQueueWakeup();
        // The intention here is to ensure the status loop runs once before
        // this function exits.
        statusThread->LockQueue();
        statusThread->UnlockQueue();
    }

};

class StatusCreateCB{
  public:
    string node;
    string data;
    bool &flag;
    StatusThread* statusThread;
    StatusCreateCB(StatusThread* st, string node, string data, bool &f) : statusThread(st), node(node), data(data), flag(f){}
    void process(int rc){
        if(ZOK!=rc){
            log("statusmod: StatusCreate '" + node + "' '" + data + "' callback got non OK status '" + zkReturnCodeStr(rc) + "'");
        }
        statusThread->LockQueue();
        flag = true;
        statusThread->UnlockQueueWakeup();
    }
    // This is the function the zk lib calls, using this obj as 'data'
    static void callback(int rc, const char *value, const void *data){
        StatusCreateCB* obj = (StatusCreateCB*)data;
        obj->process(rc);
        delete obj;
    }
};

class StatusUpdateMCB{
  public:
    zoo_op_result_t results[6];
    StatusUpdateMCB(){}
    static void callback(int rc, const void *data){
        StatusUpdateMCB* obj = (StatusUpdateMCB*)data;
        obj->process(rc, data);
        delete obj;
    }

    virtual void process(int rc, const void *data){
        if(ZOK!=rc){
            log("statusmod: update zookeeper had error '" + zkReturnCodeStr(rc) + "'");
        }else{
            bool gotError = false;
            for(int i=0;i<6;i++){
                if(results[i].err!=ZOK){
                    log("statusmod: update zookeeper had inner error '" + zkReturnCodeStr(results[i].err) + "'");
                    gotError = true;
                }
            }
            if(gotError){
                log("statusmod: update zookeeper had inner error(s)");
            }else{
                log("statusmod: zookeeper updated");
            }
        }
    }
};

class StatusThreadConfirmCallback {
  public:
    StatusThread* statusThread;
    string serverName;

    StatusThreadConfirmCallback(StatusThread* dt, string sn) :
        statusThread(dt),
        serverName(sn){}

    static void callback(int rc,
                         const struct String_vector *strings,
                         const void *data){
        StatusThreadConfirmCallback* obj = (StatusThreadConfirmCallback*)data;
        obj->process(rc, strings);
        delete obj;
    }

    void process(int rc, const struct String_vector *strings){
        if(ZOK!=rc){
            log("statusmod: confirm getchildren callback " + serverName + " got non OK status '" + zkReturnCodeStr(rc) + "'");
            statusThread->CreateNodes();
        }else{
            log("statusmod: inspircd status present in zookeeper confirmed");
        }
    }
};

void StatusThread::Run(){
    LockQueue();
    WaitForZKConnected();
    CreateNodes();
    updateRequested = true;
    unsigned long long lastConfirm = ServerInstance->Time();
    while(!GetExitFlag()){
        if(!updateRequested)
            WaitForQueue();
        if(updateRequested){
            updateRequested = false;
            //Begin hack
            UnlockQueue();
            DoHackUpdate();
            LockQueue();
            //End hack
            /* hack replaces this. put back when zk lib is fixed.
            zoo_op_t ops[6];
            MakeOps(ops);
            UnlockQueue();
            UpdateNodes(ops);
            LockQueue();
            */
            unsigned long long currentTime = ServerInstance->Time();
            if((currentTime > lastConfirm) && (currentTime-lastConfirm) > confirmPeriodSeconds){
                log("statusmod: confirming inspircd status present in zookeeper");
                zkmod->mutex.Lock();
                zoo_awget_children(zkmod->client,
                                   (zookeeperNode + "/" + name).c_str(),
                                   NULL,
                                   NULL,
                                   StatusThreadConfirmCallback::callback,
                                   new StatusThreadConfirmCallback(this, name));
                zkmod->mutex.Unlock();
                lastConfirm = currentTime;
            }
        }
    }
}

void StatusThread::WaitForZKConnected(){
    while(!zkmod->connected){
        log("statusmod: updater waiting for zookeepermod to connect");
        UnlockQueue();
        unsigned long long sleepResult;
        sleepResult = sleep(5);
        if(sleepResult!=0){
            log("statusmod: updater wait zookeepermod sleep fail " + ullToStr(sleepResult));
        }
        LockQueue();
    }
}

void StatusThread::CreateNodes(){
    // Note: I tried doing this with a multi, but if any of the higher level
    // nodes exist the subsequent calls all fail.
    zkmod->mutex.Lock();
    zoo_acreate(zkmod->client,
                "", "", 0,
                acl, 0,
                StatusCreateCB::callback,
                new StatusCreateCB(this, "", "", topNodeCreated));
    zkmod->mutex.Unlock();
    while(!topNodeCreated)
        WaitForQueue();
    zkmod->mutex.Lock();
    zoo_acreate(zkmod->client,
                zookeeperNode.c_str(), "", 0,
                acl, 0,
                StatusCreateCB::callback,
                new StatusCreateCB(this, zookeeperNode, "", zookeeperNodeCreated));
    zkmod->mutex.Unlock();
    while(!zookeeperNodeCreated)
        WaitForQueue();
    zkmod->mutex.Lock();
    zoo_acreate(zkmod->client,
                (zookeeperNode + '/' + name).c_str(), "", 0,
                acl, 0,
                StatusCreateCB::callback,
                new StatusCreateCB(this, zookeeperNode + '/' + name, "", zookeeperNameCreated));
    zkmod->mutex.Unlock();
    while(!zookeeperNameCreated)
        WaitForQueue();
    Create("timestamp", timestamp(), timestampCreated);
    Create("internal_ip", internalIp, internalIpCreated);
    Create("dc", dc, dcCreated);
    Create("rack", rack, rackCreated);
    Create("ssl_cert_name", sslCertName, sslCertNameCreated);
    Create("population", userCount(), populationCreated);
    Create("connections_allowed", "0", connectionsAllowedCreated);
    while(!(timestampCreated &&
            populationCreated &&
            connectionsAllowedCreated &&
            dcCreated &&
            internalIpCreated &&
            rackCreated &&
            sslCertNameCreated)){
        WaitForQueue();
    }
}

void hack_update_callback(int, const Stat*, const void*){
}
void StatusThread::DoHackUpdate(){
//    ops.push_back(new Op::SetData(zookeeperNode + "/" + name + "/" + "timestamp", timestamp(), -1));
    if(!zkmod->connected){
        return;
    }
    zkmod->mutex.Lock();
    string arg = timestamp();
    log("statusmod do hack update");
    zoo_aset(zkmod->client,
             (zookeeperNode + "/" + name + "/timestamp").c_str(),
             arg.c_str(),
             arg.size(),
             -1,
             hack_update_callback,
             NULL);
    zoo_aset(zkmod->client,
             (zookeeperNode + "/" + name + "/dc").c_str(),
             dc.c_str(),
             dc.size(),
             -1,
             hack_update_callback,
             NULL);
    zoo_aset(zkmod->client,
             (zookeeperNode + "/" + name + "/rack").c_str(),
             rack.c_str(),
             rack.size(),
             -1,
             hack_update_callback,
             NULL);
    zoo_aset(zkmod->client,
             (zookeeperNode + "/" + name + "/ssl_cert_name").c_str(),
             sslCertName.c_str(),
             sslCertName.size(),
             -1,
             hack_update_callback,
             NULL);
    arg = userCount();
    zoo_aset(zkmod->client,
             (zookeeperNode + "/" + name + "/population").c_str(),
             arg.c_str(),
             arg.size(),
             -1,
             hack_update_callback,
             NULL);
    if(ServerInstance->Users->LocalUserCount() <= populationLimit && !shuttingDown){
        arg = "1";
    }else{
        arg = "0";
    }
    zoo_aset(zkmod->client,
             (zookeeperNode + "/" + name + "/connections_allowed").c_str(),
             arg.c_str(),
             arg.size(),
             -1,
             hack_update_callback,
             NULL);
    zkmod->mutex.Unlock();
    log("statusmod do hack update done");
}

void StatusThread::MakeOps(zoo_op_t* ops){
//    ops.push_back(new Op::SetData(zookeeperNode + "/" + name + "/" + "timestamp", timestamp(), -1));
    string arg = timestamp();
    zoo_set_op_init(&ops[0],
                    (zookeeperNode + "/" + name + "/timestamp").c_str(),
                    arg.c_str(),
                    arg.size(),
                    -1, NULL);
//    ops.push_back(new Op::SetData(zookeeperNode + "/" + name + "/" + "dc", dc, -1));
    zoo_set_op_init(&ops[1],
                    (zookeeperNode + "/" + name + "/dc").c_str(),
                    dc.c_str(),
                    dc.size(),
                    -1, NULL);
//    ops.push_back(new Op::SetData(zookeeperNode + "/" + name + "/" + "rack", rack, -1));
    zoo_set_op_init(&ops[2],
                    (zookeeperNode + "/" + name + "/rack").c_str(),
                    rack.c_str(),
                    rack.size(),
                    -1, NULL);

//    ops.push_back(new Op::SetData(zookeeperNode + "/" + name + "/" + "ssl_cert_name", sslCertName, -1));
    zoo_set_op_init(&ops[3],
                    (zookeeperNode + "/" + name + "/ssl_cert_name").c_str(),
                    sslCertName.c_str(),
                    sslCertName.size(),
                    -1, NULL);

//    ops.push_back(new Op::SetData(zookeeperNode + "/" + name + "/" + "population", userCount(), -1));
    arg = userCount();
    zoo_set_op_init(&ops[4],
                    (zookeeperNode + "/" + name + "/population").c_str(),
                    arg.c_str(),
                    arg.size(),
                    -1, NULL);

//        ops.push_back(new Op::SetData(zookeeperNode + "/" + name + "/" + "connections_allowed", "1", -1));
    if(ServerInstance->Users->LocalUserCount() <= populationLimit && !shuttingDown){
        arg = "1";
    }else{
        arg = "0";
    }
    zoo_set_op_init(&ops[5],
                    (zookeeperNode + "/" + name + "/connections_allowed").c_str(),
                    arg.c_str(),
                    arg.size(),
                    -1, NULL);
    log("statusmod: update op args complete");
    //return ops;
}

void StatusThread::UpdateNodes(zoo_op_t* ops){
// Multiops are a crasher in the current zk lib so we do these separately.
    log("UpdateNodes disabled, DON'T CALL THIS UNTIL ZK LIB IS FIXED");
    // call zk async update with local vars.
    if(!zkmod->connected){
        return;
    }
    log("statusmod: updating zookeeper");
    //zkmod->client->multi(ops, boost::shared_ptr<MultiCallback>(new StatusUpdateMCB()));
    StatusUpdateMCB* cb = new StatusUpdateMCB();
    zkmod->mutex.Lock();
    zoo_amulti(zkmod->client, 6, ops,
        cb->results,
        StatusUpdateMCB::callback,
        cb);
    zkmod->mutex.Unlock();
}

void StatusThread::Create(string node, string data, bool &flag){
    zkmod->mutex.Lock();
    zoo_acreate(zkmod->client,
                (zookeeperNode + '/' + name + '/' + node).c_str(),
                data.c_str(),
                data.size(),
                acl,
                0,
                StatusCreateCB::callback,
                new StatusCreateCB(this, node, data, flag));
    zkmod->mutex.Unlock();
}

void TimerThread::Run(){
    struct timespec sleepTime, sleepRemaining;
    sleepTime.tv_sec = 0;
    sleepTime.tv_nsec = 0;
    sleepRemaining.tv_sec = 0;
    sleepRemaining.tv_nsec = 0;
    while(!GetExitFlag()){
        LockQueue();
        sleepTime.tv_sec = period_seconds;
        sleepTime.tv_nsec = 0;
        UnlockQueue();

        if(0!=sleepRemaining.tv_sec || 0!=sleepRemaining.tv_nsec){
            sleepTime.tv_sec = sleepRemaining.tv_sec;
            sleepTime.tv_nsec = sleepRemaining.tv_nsec;
            sleepRemaining.tv_sec = 0;
            sleepRemaining.tv_nsec = 0;
        }
        if(nanosleep(&sleepTime, &sleepRemaining)<0){
          log("statusmod: (TimerThread::Run) nanosleep had error, sleepTime " + timeToStr(sleepTime) + " sleepRemaining " + timeToStr(sleepRemaining));
        }else{
            statusThread->LockQueue();
            statusThread->updateRequested = true;
            statusThread->UnlockQueueWakeup();
        }
    }
}

class DeadmanMultiCallback{
  public:
    zoo_op_t* ops;
    zoo_op_result_t* results;
    DeadmanMultiCallback(int size){
        ops = new zoo_op_t[size];
        results = new zoo_op_result_t[size];
    }

    virtual ~DeadmanMultiCallback(){
        delete ops;
        delete results;
    }

    static void callback(int rc, const void *data){
    DeadmanMultiCallback* obj = (DeadmanMultiCallback*)data;
        obj->process(rc);
        delete obj;
    }

    void process(int rc){
        log("DeadmanMultiCallback");
        if(ZOK!=rc){
            log("statusmod: deadmancallback, zookeeper had error '" + zkReturnCodeStr(rc) + "'");
        }
    //        for(boost::ptr_vector<OpResult>::const_iterator it = results.begin();
    //            it!=results.end();
    //            ++it){
    //            log(zkReturnCodeStr(it->getReturnCode()));
    //        }
    }
};

class DeadmanGetServerChildrenCallback {
  public:
    DeadmanThread* deadmanThread;
    string serverName;

    DeadmanGetServerChildrenCallback(DeadmanThread* dt, string sn) :
        deadmanThread(dt),
        serverName(sn){}

    static void callback(int rc,
                         const struct String_vector *strings,
                         const void *data){
        DeadmanGetServerChildrenCallback* obj = (DeadmanGetServerChildrenCallback*)data;
        obj->process(rc, strings);
        delete obj;
    }
    void process(int rc,
                 const struct String_vector *strings){
        if(ZOK==rc){
            log("statusmod: deadman getserverchildren callback " + serverName);
            string node = deadmanThread->zookeeperNode;
            // HACK take this out when zk lib is fixed.
            deadmanThread->zkmod->mutex.Lock();
            for(int i=0;i<strings->count;i++){
                string nodeName = (strings->data[i]);
                log(node + "/" + serverName + "/" + nodeName);
                zoo_delete(deadmanThread->zkmod->client,
                           (node + "/" + serverName + "/" + nodeName).c_str(),
                           -1);
            }
            zoo_delete(deadmanThread->zkmod->client,
                       (node + "/" + serverName).c_str(),
                       -1);
            deadmanThread->zkmod->mutex.Unlock();
            // END HACK

// ZKUP when the zklib gets fixed put this back.
//            DeadmanMultiCallback* cb = new DeadmanMultiCallback(strings->count+1);
//            for(int i=0;i<strings->count;i++){
//                string nodeName = (strings->data[i]);
//                log(node + "/" + serverName + "/" + nodeName);
//                zoo_delete_op_init(&(cb->ops)[i], (node + "/" + serverName + "/" + nodeName).c_str(), -1);
//            }
//            zoo_delete_op_init(&(cb->ops)[strings->count], (node + "/" + serverName).c_str(), -1);
//            deadmanThread->zkmod->mutex.Lock();
//            log("yyyyy");
//            /* zkup put this back
//            zoo_amulti(deadmanThread->zkmod->client, strings->count+1, cb->ops,
//                cb->results,
//                DeadmanMultiCallback::callback,
//                cb);
//                */
//            log("zzzzz");
//            deadmanThread->zkmod->mutex.Unlock();
//            log("z1z1z1");
        }
    }
};

class DeadmanGetTimestampCallback{
  public:
    DeadmanThread* deadmanThread;
    string serverName;
    string path;

    DeadmanGetTimestampCallback(DeadmanThread* dt, string sn, string path) :
        deadmanThread(dt),
        serverName(sn),
        path(path){}

    static void callback(int rc, const char *value, int value_len,
                         const struct Stat *stat, const void *data){
        DeadmanGetTimestampCallback* obj = (DeadmanGetTimestampCallback*)data;
        obj->process(rc, value, value_len);
        delete obj;
    }

    virtual void process(int rc, const char *value, int value_len){
        if(ZOK==rc){
            log("statusmod: deadman gettimestamp callback " + serverName);
            string data;
            data.assign(value, value_len);
            log(data);
            int serverTime = atoi(data.c_str());
            unsigned long long current = ServerInstance->Time();
            int timeout;
            deadmanThread->LockQueue();
            timeout = deadmanThread->timeoutSeconds;
            deadmanThread->UnlockQueue();
            if( (current > serverTime) && ((current - serverTime) > timeout) ){
                vector<string> split_path;
                boost::split(split_path, path, boost::is_any_of("/"));
                string node = split_path[1];
                string name = split_path[2];
                log("statusmod: old inspircd detected, removing from zookeeper /" + node + "/" + name + " serverTime " + data.c_str() + " current " + ullToStr(current) + " diff " + ullToStr(current - serverTime));
                deadmanThread->zkmod->mutex.Lock();
                zoo_aget_children(deadmanThread->zkmod->client,
                                  ("/" + node + "/" + name).c_str(), 0,
                                  DeadmanGetServerChildrenCallback::callback,
                                  new DeadmanGetServerChildrenCallback(deadmanThread, name));
                deadmanThread->zkmod->mutex.Unlock();
            }
        }
    }
};

class DeadmanGetServersCallback {
  public:
    DeadmanThread* deadmanThread;
    DeadmanGetServersCallback(DeadmanThread* dt) : deadmanThread(dt){}
    static void callback(int rc,
                         const struct String_vector *strings,
                         const void *data){
        DeadmanGetServersCallback* obj = (DeadmanGetServersCallback*)data;
        obj->process(rc, strings);
        delete obj;
    }
    void process(int rc,
                 const struct String_vector *strings){
        if(ZOK==rc){
            log("statusmod: deadman getservers callback");
            for(int i=0;i<strings->count;i++){
                deadmanThread->LockQueue();
                string node = deadmanThread->zookeeperNode;
                deadmanThread->UnlockQueue();
                string serverName(strings->data[i]);
                deadmanThread->zkmod->mutex.Lock();
                string path = node + "/" + serverName + "/timestamp";
                zoo_awget(deadmanThread->zkmod->client,
                          (path).c_str(),
                          NULL, NULL,
                          DeadmanGetTimestampCallback::callback,
                          new DeadmanGetTimestampCallback(deadmanThread, serverName, path));
                deadmanThread->zkmod->mutex.Unlock();
            }
        }
    }
};

void DeadmanThread::Run(){
    log("statusmod: starting deadman thread");
    int period;
    LockQueue();
    period = periodSeconds;
    UnlockQueue();
    unsigned long long sleepResult;
    sleepResult = sleep(rand() % period);
    if(sleepResult!=0){
        log("statusmod: deadman start sleep fail " + ullToStr(sleepResult));
    }
    WaitForZKConnected();
    log("statusmod: deadmanthread ready to talk to zookeeper");
    unsigned int sleep_remaining = 0;
    while(!GetExitFlag()){
        LockQueue();
        string node = zookeeperNode;
        period = periodSeconds;
        UnlockQueue();
        if(0==sleep_remaining){
            HandleDeadServers(node);
            sleep_remaining = sleep(period);
        }else{
            log("statusmod: deadman sleep fail " + ullToStr(sleep_remaining));
            sleep_remaining = sleep(sleep_remaining);
        }
    }
    log("statusmod: deadmanthread exiting");
}

void DeadmanThread::WaitForZKConnected(){
    while(!zkmod->connected){
        log("statusmod: deadman waiting for zookeepermod to connect");
        UnlockQueue();
        unsigned long long sleepResult;
        sleepResult = sleep(5);
        if(sleepResult!=0){
            log("statusmod: deadman wait zookeepermod sleep fail " + ullToStr(sleepResult));
        }
        LockQueue();
    }
}

void DeadmanThread::HandleDeadServers(const string &node){
    log("statusmod: HandleDeadServers " + node);
    zkmod->mutex.Lock();
    zoo_aget_children(zkmod->client,
                      node.c_str(), 0,
                      DeadmanGetServersCallback::callback,
                      new DeadmanGetServersCallback(this));
    zkmod->mutex.Unlock();
}

MODULE_INIT(StatusMod)

