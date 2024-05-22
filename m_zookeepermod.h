/*
* m_zookeepermod -- ZooKeeper settings module for InspIRCd
*/
#ifndef ZOOKEEPERMOD_H
#define ZOOKEEPERMOD_H

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

//ZKUP
//class InitWatch : public Watch {
//  //protected:
//  public:
//    /**
//     * @param event Event type that caused this watch to trigger.
//     * @param state State of the zookeeper session.
//     * @param path Znode path where this watch was set to.
//     */
//    void process(WatchEvent::type event,
//                 SessionState::type state,
//                 const std::string& path);
//};
//END ZKUP
void initWatch(zhandle_t *zh, int type, int state, const char *path,
               void *watcherCtx);

// InspIRCd Module ------------------------------------------------------------
class ZooKeeperMod : public Module{
  protected:
    string endpoints;

  public:
    zhandle_t* client;
    Mutex mutex;
    bool firstConnect;
    bool connected;
    void Connect();
    void Disconnect();
    void init();
    ZooKeeperMod() : firstConnect(true), connected(false){}
	virtual ~ZooKeeperMod(){
	}
    void OnRehash(User* user);
	virtual Version GetVersion();
    void LoadConf();
};

#endif