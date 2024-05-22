/*
* m_statsdmod -- StatsD module for InspIRCd
*/

#ifndef STATSDMOD_H
#define STATSDMOD_H

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
extern "C"{
#include <statsd-client.h>
}
#include <zookeeper/zookeeper.h>

#include "inspircd.h"
#include "threadengine.h"
#include "m_zookeepermod.h"

using namespace std;

/* $CompileFlags: -L../../lib -I../../include/ -std=c++0x */
/* $LinkerFlags: -L../../lib -lzookeeper_mt -lstatsd */

// InspIRCd Module ------------------------------------------------------------

class StatsDMod : public Module{
  public:
    string name;
    statsd_link *link;
    Mutex statsDMutex;
    string zookeeperNode;
    string host;
    int port;
    int currentRehash;
    Mutex confMutex;
    bool confReloadRequested;
    bool checkZookeeper;
    int checkZookeeperCount;
    bool reconnectRequested;
    void init();
	virtual ~StatsDMod(){}
	virtual Version GetVersion();
    void OnBackgroundTimer(time_t curtime);
	void OnRehash(User* user);

    // We have an issue linking across modules, but if these are inlined
    // then the other modules can use them.
    int inc(string stat, float sample_rate){
        if(NULL!=link){
            statsDMutex.Lock();
            statsd_inc(link,
                       const_cast<char*>((name + stat).c_str()),
                       sample_rate);
            statsDMutex.Unlock();
        }
    }

    int dec(string stat, float sample_rate){
        if(NULL!=link){
            statsDMutex.Lock();
            statsd_dec(link,
                       const_cast<char*>((name + stat).c_str()),
                       sample_rate);
            statsDMutex.Unlock();
        }
    }

    int count(string stat, size_t count, float sample_rate){
        if(NULL!=link){
            statsDMutex.Lock();
            statsd_count(link,
                         const_cast<char*>((name + stat).c_str()),
                         count,
                         sample_rate);
            statsDMutex.Unlock();
        }
    }

    int gauge(string stat, size_t value){
        if(NULL!=link){
            statsDMutex.Lock();
            statsd_gauge(link,
                         const_cast<char*>((name + stat).c_str()),
                         value);
            statsDMutex.Unlock();
        }
    }

    int timing(string stat, size_t ms){
        if(NULL!=link){
            statsDMutex.Lock();
            statsd_timing(link,
                          const_cast<char*>((name + stat).c_str()),
                          ms);
            statsDMutex.Unlock();
        }
    }
};

#endif
