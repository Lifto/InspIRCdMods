/*
* InspIRCd Cassandra Auth Module
*/

#ifndef AUTHMOD_H
#define AUTHMOD_H

/* $CompileFlags: -L../../lib -I../../include/ -std=c++0x */
/* $LinkerFlags: -L../../lib -lzookeeper_mt -llog4cxx -lgtest -lcassandra -lstatsd */

#include "inspircd.h"
#include "socketengine.h"
#include "m_zookeepermod.h"
#include "m_statsdmod.h"

#include <queue>
#include <vector>

#include <boost/algorithm/string.hpp>
#include <boost/asio.hpp>
#include <boost/format.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_io.hpp> // Needed to convert UUID to string
#include <boost/uuid/uuid_generators.hpp>
#include <cassandra.h>
#include <zookeeper/zookeeper.h>

using namespace std;
using namespace boost::uuids;
using boost::shared_ptr;

class CQLQuery;
class AuthQuery;
class CQL;
class ModuleCQLAuth;
typedef boost::function<void()> event_callback_t;

#define EVENT_READ_BUFFER_SIZE 8192

class CQL: public QueuedThread{
    protected:
        ModuleCQLAuth* parent;
    public:
        std::queue<CQLQuery*> newFutureQueries;  // Queries pending send.
        std::queue<CQLQuery*> readyQueue;
        bool confReloadRequested;
        int currentRehash;    // Id to catch and ignore old async results.
        bool confLoaded;          // True if conf loaded since init or rehash.
        bool isConnected;         // True if viable connection to Cassandra.
        CQL(ModuleCQLAuth* parent) : parent(parent),
                                     confReloadRequested(false),
                                     currentRehash(0),
                                     confLoaded(false),
                                     isConnected(false){}
        virtual void Run();
        void Enqueue(CQLQuery* query);
};

enum AuthState {
	AUTH_STATE_NONE = 0,
	AUTH_STATE_BUSY = 1,
	AUTH_STATE_FAIL = 2
};

class CQLQuery : public classbase{
public:
    CQLQuery(const string& format) : format(format){}
    virtual ~CQLQuery(){}
    unsigned long long startTime;
    const string format;
    virtual void callback(CassFuture* result_future) = 0;
};

class AuthQuery : public CQLQuery{
public:
	ModuleCQLAuth* creator;
	const string uid;
	LocalIntExt& pendingExt;
	bool verbose;
	AuthQuery(ModuleCQLAuth* me, const string& u, LocalIntExt& e, bool v, const string& format)
		: creator(me), uid(u), pendingExt(e), verbose(v), CQLQuery(format){
	}
	virtual ~AuthQuery() {}

    virtual void callback(CassFuture* result_future);

    void doAuthSuccess(User* user);
    void doAuthFail(User* user);
};

class AppEventHandler : public EventHandler{
  public:
    int writeFd;  // write to this file descriptor to get socketengine to
                  // to call HandleEvent on the main thread.
    // Buffer for reading in from the pipe, cautiously overlarge.
    unsigned long long readBuffer[EVENT_READ_BUFFER_SIZE];
    Mutex eventMutex;  // Lock this before modifying eventQueue.
    std::queue<event_callback_t> eventQueue;
	void HandleEvent(EventType et, int errornum=0);
	void ProcessQueue();
};

class ChanAuthQuery : public CQLQuery{
public:
	ModuleCQLAuth* creator;
	const string uid;
	const string chanid;
	bool verbose;
	ChanAuthQuery(ModuleCQLAuth* me, const string& u, const string& c, bool v, const string& format)
		: creator(me), uid(u), chanid(c), verbose(v), CQLQuery(format){
	}
	virtual ~ChanAuthQuery() {}

    virtual void callback(CassFuture* result_future);

    void doAuthSuccess(User* user, const string& chanid);
    void doAuthFail();
};

class ChanAuthSuccess{
  public:
    const string uid;
	const string chanid;
    ChanAuthSuccess(const string& uid, const string& chanid) : uid(uid), chanid(chanid){}
    void doAuthSuccess();
};

class ModuleCQLAuth : public Module{
	LocalIntExt pendingExt;
public:
    int activeQueryCount;
    int pipefd[2];  // Pipe for attaching appEventHandler to socket engine.
    AppEventHandler appEventHandler;
    CQL* cqlThread;
    string zookeeper_dse_hosts_node;
    string hosts;
	string authquery;
	string chanauthquery;
	string killreason;
	string allowpattern;
	bool verbose;

	ModuleCQLAuth() : pendingExt("cqlauth", this){
	}

	void init();
	void OnRehash(User* user);
	ModResult OnUserRegister(LocalUser* user);
	ModResult OnCheckReady(LocalUser* user);
	ModResult OnUserPreJoin(User* user,
                            Channel* chan,
                            const char* cname,
                            string &privs,
                            const string &keygiven);
	Version GetVersion();
};

class CqlCallback{
  protected:
    int rehashId;
  public:
    CqlCallback(int rehashId) : rehashId(rehashId){}
    void process(int rc, const char* value, int value_len);
    // This is the function the zk lib calls, using this obj as 'data'
    static void callback(int rc, const char *value, int value_len,
                         const struct Stat *stat, const void *data){
        CqlCallback* obj = (CqlCallback*)data;
        obj->process(rc, value, value_len);
        delete obj;
    }
};

class CqlWatch{
  protected:
    int rehashId;
  public:
    CqlWatch(int rehashId) : rehashId(rehashId){}
    void process(int event, int state, const char* path);
    // This is the function the zk lib calls, using this obj as 'watcherCtx'
    static void watch(zhandle_t *zh, int type,
                      int state, const char *path, void *watcherCtx){
        CqlWatch* obj = (CqlWatch*)watcherCtx;
        obj->process(type, state, path);
        delete obj;
    }
};

#endif