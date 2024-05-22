/*
* m_zookeepermod -- ZooKeeper settings module for InspIRCd
*/

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
#include <zookeeper/zookeeper.h>

#include "inspircd.h"
#include "threadengine.h"
#include "m_zookeepermod.h"

using namespace std;
using namespace chrono;

/* $CompileFlags: -L../../lib -I../../include/ -std=c++0x */
/* $LinkerFlags: -L../../lib -lzookeeper_mt -llog4cxx -lgtest */

/*
    * Get settings from ZooKeeper.
    * Listen for changes to settings.

See:
	http://zookeeper.apache.org/doc/r3.2.2/zookeeperProgrammers.html#ch_zkSessions

Endpoing setting should be something like:
	zookeeper_endpoint="127.0.0.1:3000,127.0.0.1:3001,127.0.0.1:3002/app/a”

notes:
I.e. one setting, NOT three separate settings. That is, you shouldn’t be internally managing
the chroot; the connection you use should be giving the chroot path.

We're going to have a separate mod_zookeeper which will handle ZK. Then
mod_zookeeper, mod_cassandra_auth, etc will get data from mod_zookeeper.  (That
sort of separation is standard practice in Inspricd-modules)

Note that modzookeeper et al will keep their conf settings.  If they are present,
and zookeeper can not be contacted, then those conf settings get used.

thoughts:
This module is going to get config data for other modules and it is going to
watch for changes to those modules.  How do we know what we are watching and
how do we know which modules want to be updated with this information?

An idea:
In the config we list the module names, and the (endpoint relative) zdir.
we maintain the data from each zdir location (getting, setting watches).

what is the zookeeper module providing?
* It establishes connection to zookeeper.
* It threads the zookeeper interaction.
* It handles re-subbing watches and any server-changeover stuff.
    You subscribe to a zdir, and you get every update that happens on it sent
     to your callback.  (If we want one-time watches, we'll add it.)
     I think it is only non-blocking at the moment.  You subscribe, and
     you wait 'til your callback gets called.  Your callback is only ever called
     from one thread.

From the zookeeper docs - not sure which the c++ binding uses, but this is
about the c-lib.  as we are using event-driven inspircd
"The single-threaded library allows ZooKeeper to be used in event driven
applications by exposing the event loop used in the multi-threaded library."

*/

// log - convenient synchronized logging function.
Mutex* loggingMutex = new Mutex();
void log(string s){
    loggingMutex->Lock();
    ServerInstance->Logs->Log("m_zookeepermod", DEFAULT, s);
    loggingMutex->Unlock();
}

ZooKeeperMod* getMod(){
    return (ZooKeeperMod*)ServerInstance->Modules->Find("m_zookeepermod.so");
}

void initWatch(zhandle_t *zh, int type, int state, const char *path,
               void *watcherCtx){
    log("zookeepermod:  initWatch");
    ZooKeeperMod* mod = getMod();
    if(ZOO_EXPIRED_SESSION_STATE==state || ZOO_AUTH_FAILED_STATE==state){
        log("zookeepermod: Session not connected, re-initing");
        mod->Disconnect();
        mod->Connect();
    }else if(mod->firstConnect){
        if(ZOO_CONNECTING_STATE==state){
            log("zookeepermod: connecting");
            mod->connected = false;
        }else if(ZOO_ASSOCIATING_STATE==state){
            log("zookeepermod: associating");
            mod->connected = false;
        }else if(ZOO_CONNECTED_STATE==state){
            log("zookeepermod: initWatch: connection established");
            mod->firstConnect = false;
            mod->connected = true;
        }
    }else{
        if(ZOO_CONNECTING_STATE==state){
            log("zookeepermod: connection lost, reconnecting");
            mod->connected = false;
        }else if(ZOO_ASSOCIATING_STATE==state){
            log("zookeepermod: reconnect associating");
            mod->connected = false;
        }else if(ZOO_CONNECTED_STATE==state){
            log("zookeepermod: initWatch: connection established");
            mod->connected = true;
        }
    }
}

// InspIRCd Module ------------------------------------------------------------

void ZooKeeperMod::Connect(){
    log("zookeepermod: attempting to connect to " + endpoints);
    // Note there was something about re-establishing a connection to
    // a previous client (see the '0' in the call here) but this "port" to
    // the new driver is adding no such functionality.
    client = zookeeper_init(endpoints.c_str(), initWatch,
                            30000, 0,
                            NULL, 0);
// Not sure where or when this comes in.
/*        std::vector<data::ACL> acl;
    data::ACL perm;
    data::Stat stat;
    perm.getid().getscheme() = "world";
    perm.getid().getid() = "anyone";
    perm.setperms(Permission::All);
    acl.push_back(perm);*/
}

void ZooKeeperMod::Disconnect(){
    connected = false;
    zookeeper_close(client);
    client = NULL;
}

void ZooKeeperMod::init(){
    LoadConf();
    Connect();
    Implementation eventlist[] = { I_OnRehash };
    ServerInstance->Modules->Attach(eventlist,
                                    this,
                                    sizeof(eventlist)/sizeof(Implementation));
    log("zookeepermod: started");
}

void ZooKeeperMod::OnRehash(User* user){
    LoadConf();
    Disconnect();
    Connect();
}

Version ZooKeeperMod::GetVersion(){
    return Version("App ZooKeeperMod");
}

void ZooKeeperMod::LoadConf(){
    // Read config info from inspircd.conf
    ConfigTag* conf = ServerInstance->Config->ConfValue("zookeeper");
    endpoints = conf->getString(
        "endpoints",
        "127.0.0.1:2181,127.0.0.2:2181/inspircd_module");
    log("zookeepermod: using endpoints " + endpoints);
}

MODULE_INIT(ZooKeeperMod)

