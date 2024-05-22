/*
* InspIRCd Cassandra Auth Module
*/

/* $CompileFlags: -L../../lib -I../../include/ -std=c++0x */
/* $LinkerFlags: -L../../lib -lzookeeper_mt -llog4cxx -lgtest -lcassandra -lstatsd  -lboost_system -lboost_filesystem -lstdc++ */
/* $ModDesc: Allow/Deny connections and joins based on Cassandra db. */

#include "m_authmod.h"

// log - convenient synchronized logging function.
Mutex* loggingMutex = new Mutex();
void log(string s){
    loggingMutex->Lock();
    ServerInstance->Logs->Log("m_authmod", DEFAULT, s);
    loggingMutex->Unlock();
}

unsigned long long milliseconds_since_epoch(){
    return (((unsigned long long)ServerInstance->Time()) * 1000.0) + (((unsigned long long)ServerInstance->Time_ns()) / 1000000.0);
}

string string_time(){
    unsigned long long ts = milliseconds_since_epoch();
    string asStr;
    asStr = static_cast<ostringstream*>( &(ostringstream() << ts) )->str();
    return asStr;
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

void statsDAuthSuccessTimer(unsigned long long milliseconds){
    StatsDMod* statsdmod = getStatsDMod();
    if(NULL!=statsdmod){
        statsdmod->timing("authSuccess", milliseconds);
    }
}

void statsDAuthFailTimer(unsigned long long milliseconds){
    StatsDMod* statsdmod = getStatsDMod();
    if(NULL!=statsdmod){
        statsdmod->timing("authFail", milliseconds);
    }
}

void statsDChanAuthSuccessTimer(unsigned long long milliseconds){
    StatsDMod* statsdmod = getStatsDMod();
    if(NULL!=statsdmod){
        statsdmod->timing("chanAuthSuccess", milliseconds);
    }
}

void statsDChanAuthFailTimer(unsigned long long milliseconds){
    StatsDMod* statsdmod = getStatsDMod();
    if(NULL!=statsdmod){
        statsdmod->timing("chanAuthFail", milliseconds);
    }
}

void cql_callback(CassFuture* future, void* data){
    ((CQLQuery *)data)->callback(future);
}

void cql_log_callback(cass_uint64_t time,
                      CassLogLevel severity,
                      CassString message,
                      void* data){
    string string_message = string(message.data, message.length);
    log("cql: lib callback - " + string_message);
}

// Used to make a UUID from a string (in this case, the string is a user's ID)
string_generator uuidGen;

// replaceAll - Utility for escaping strings.
void replaceAll(string& str,
                const string& from,
                const string& to){
    if(from.empty())
        return;
    size_t start_pos = 0;
    while((start_pos = str.find(from, start_pos)) != string::npos){
        str.replace(start_pos, from.length(), to);
        start_pos += to.length(); // In case 'to' contains 'from', like replacing 'x' with 'yx'
    }
}

void AuthQuery::callback(CassFuture* result_future){
    creator->activeQueryCount -= 1;
    User* user = ServerInstance->FindNick(uid);
    if(!user){
        // Can this happen if they disconnect before auth completes?
        delete this;
        return;
    }

    CassError rc = cass_future_error_code(result_future);

    if(CASS_ERROR_LIB_NO_HOSTS_AVAILABLE == rc){
        log("authmod: cassandra is down, requeing query");
        creator->cqlThread->LockQueue();
        creator->cqlThread->isConnected = false;
        creator->cqlThread->Enqueue(this);
        creator->cqlThread->UnlockQueueWakeup();
        return;
    }else if(CASS_OK != rc){
        CassString cmessage = cass_future_error_message(result_future);
        string message = string(cmessage.data, cmessage.length);
        log("authmod: ERROR, NULL result indicates incorrect auth query " + message);
        doAuthFail(user);
        return;
    }else{
        const CassResult* result_set = cass_future_get_result(result_future);
        if (0==cass_result_row_count(result_set)){
            log("authmod: auth fail, token not found");
            doAuthFail(user);
            return;
        }

        const CassRow* row = cass_result_first_row(result_set);
        CassString uuid1;
        cass_value_get_string(cass_row_get_column(row, 0), &uuid1);

        cass_int64_t deleted = 0;
        cass_value_get_int64(cass_row_get_column(row, 1), &deleted);

        uuid query_user_uuid;
        uuid existing_user_uuid;
        try{
            existing_user_uuid = uuidGen(user->nick.substr(1, 32));
        }catch(...){
            log("authmod: auth fail, user " + user->nick + " (1, 32) is not a valid UUID");
            doAuthFail(user);
            return;
        }
        if(16==uuid1.length){
            memcpy(&query_user_uuid, uuid1.data, 16);
        }else{
            log("authmod: auth fail, user_id in DSE was not length 16");
            doAuthFail(user);
            return;
        }
        if(0!=deleted){
            log("authmod: auth fail, User was deleted");
            doAuthFail(user);
            return;
        }
        if(query_user_uuid != existing_user_uuid){
            log("authmod: auth fail, UUIDs do not match - " +
                boost::lexical_cast<string>(query_user_uuid) +
                " " +
                boost::lexical_cast<string>(existing_user_uuid));
            doAuthFail(user);
            return;
        }
        doAuthSuccess(user);
    }
}

void AuthQuery::doAuthSuccess(User* user){
    pendingExt.set(user, AUTH_STATE_NONE);
    statsDAuthSuccessTimer(milliseconds_since_epoch() - startTime);
    delete this;
}

void AuthQuery::doAuthFail(User* user){
    pendingExt.set(user, AUTH_STATE_FAIL);
    statsDAuthFailTimer(milliseconds_since_epoch() - startTime);
    delete this;
}

void AppEventHandler::HandleEvent(EventType et, int errornum){
    if(et==EVENT_READ){
        // Read out the entire buffer.
        ssize_t readCount;
        readCount = read(GetFd(),
                         &readBuffer,
                         sizeof(unsigned long long) * EVENT_READ_BUFFER_SIZE);
        if(readCount<0){
            log("authmod: app event handler error " + ullToStr(errornum));
        }else if(readCount>0){
            ProcessQueue();
        }
    }
}

void AppEventHandler::ProcessQueue(){
    bool done = false;
    while(!done){
        eventMutex.Lock();
        if(eventQueue.empty()){
            eventMutex.Unlock();
            done = true;
        }else{
            event_callback_t callback = eventQueue.front();
            eventQueue.pop();
            eventMutex.Unlock();
            callback();
        }
    }
}

void ChanAuthQuery::callback(CassFuture* result_future){
    creator->activeQueryCount -= 1;
    User* user = ServerInstance->FindNick(uid);
    if(!user){
        // Can this happen if they disconnect before chanauth completes?
        doAuthFail();
        return;
    }

    CassError rc = cass_future_error_code(result_future);

    if(CASS_ERROR_LIB_NO_HOSTS_AVAILABLE == rc){
        log("chanauthmod: cassandra is down, requeing query");
        creator->cqlThread->LockQueue();
        creator->cqlThread->isConnected = false;
        creator->cqlThread->Enqueue(this);
        creator->cqlThread->UnlockQueueWakeup();
        return;
    }else if(CASS_OK != rc){
        CassString cmessage = cass_future_error_message(result_future);
        string message = string(cmessage.data, cmessage.length);
        log("chanauthmod: ERROR, NULL result indicates incorrect chanauth query - " + message);
        doAuthFail();
        return;
    }else{
        const CassResult* result_set = cass_future_get_result(result_future);
        if (0==cass_result_row_count(result_set)){
            log("chanauthmod: auth fail, user_uuid, post_uuid match not found");
            doAuthFail();
            return;
        }

        const CassRow* row = cass_result_first_row(result_set);
        CassString uuid1;
        cass_value_get_string(cass_row_get_column(row, 0), &uuid1);

        uuid query_user_uuid, existing_user_uuid;
        try{
            existing_user_uuid = uuidGen(user->nick.substr(1, 32));
        }catch(...){
            log("chanauthmod: auth fail, user " + user->nick + " (1, 32) is not a valid UUID");
            doAuthFail();
            return;
        }

        if(16==uuid1.length){
            memcpy(&query_user_uuid, uuid1.data, 16);
        }else{
            log("chanauthmod: auth fail, user_id in DSE was not length 16");
            doAuthFail();
            return;
        }

        if(query_user_uuid != existing_user_uuid){
            log("chanauthmod: auth fail, UUIDs do not match - " +
                boost::lexical_cast<string>(query_user_uuid) +
                " " +
                boost::lexical_cast<string>(existing_user_uuid));
            doAuthFail();
            return;
        }

        ChanAuthSuccess* chanAuthSuccess = new ChanAuthSuccess(uid, chanid);
        auto callback = bind(&ChanAuthSuccess::doAuthSuccess, chanAuthSuccess);
        creator->appEventHandler.eventMutex.Lock();
        creator->appEventHandler.eventQueue.push(callback);
        creator->appEventHandler.eventMutex.Unlock();
        unsigned long long ull = 1;
        // Write the the 'writeFd' side of the pipe so that the socketengine
        // will kick the event handler and process the queue of join-actions
        // on the main thread.
        write(creator->appEventHandler.writeFd, &ull, sizeof(unsigned long long));
    }
}

void ChanAuthSuccess::doAuthSuccess(){
    User* user = ServerInstance->FindNick(uid);
    if(!user){
        // Can this happen if they disconnect before chanauth completes?
        log("chanauthmod: user not available, chanauth not joining");
        delete this;
        return;
    }
    Channel::JoinUser(user, chanid.c_str(), true, "", false);
    log("chanauthmod: chanauth success, user: " + user->nick + " chan: " + chanid.c_str() + " from " + user->host);
    delete this;
}

void ChanAuthQuery::doAuthFail(){
    // Note: We do nothing if no success, as the join is already denied.
    delete this;
}

void ModuleCQLAuth::init(){
    activeQueryCount = 0;
    int flags;
    if (pipe(pipefd) == -1) {
        log("authmod: Error - Could not create chanauth event pipe");
    }
    // Make read and write ends of pipe nonblocking
    flags = fcntl(pipefd[0], F_GETFL);
    if (flags == -1){
        log("authmod: Error - Could not get pipe read flags");
    }
    flags |= O_NONBLOCK; // Make read end nonblocking
    if (fcntl(pipefd[0], F_SETFL, flags) == -1){
        log("authmod: Error - Could not set pipe read flags");
    }

    flags = fcntl(pipefd[1], F_GETFL);
    if (flags == -1){
        log("authmod: Error - Could not get pipe write flags");
    }
    flags |= O_NONBLOCK; // Make write end nonblocking
    if (fcntl(pipefd[1], F_SETFL, flags) == -1){
        log("authmod: Error - Could not set pipe write flags");
    }
    appEventHandler.writeFd = pipefd[1];
    appEventHandler.SetFd(pipefd[0]);
    ServerInstance->SE->AddFd(&appEventHandler,
                              FD_WANT_FAST_READ | FD_WANT_NO_WRITE);
    cqlThread = new CQL(this);
    ServerInstance->Threads->Start(cqlThread);
    log("authmod: threads started");
    ServerInstance->Modules->AddService(pendingExt);
    // Subscribe to these events.
    Implementation eventlist[] = { I_OnCheckReady,
                                   I_OnRehash,
                                   I_OnUserRegister,
                                   I_OnUserPreJoin };
    ServerInstance->Modules->Attach(eventlist, this, sizeof(eventlist)/sizeof(Implementation));
}

void ModuleCQLAuth::OnRehash(User* user){
    log("authmod: OnRehash");
    cqlThread->LockQueue();
    cqlThread->confReloadRequested = true;
    cqlThread->UnlockQueueWakeup();
}

ModResult ModuleCQLAuth::OnUserRegister(LocalUser* user){
    ConfigTag* tag = user->MyClass->config;
    // If the conf has 'usecqlauth' false, don't check auth.
    if(!tag->getBool("usecqlauth", true))
        return MOD_RES_PASSTHRU;

    // If we've defined an 'allowpattern' and this username uses it,
    // then skip auth.  (We use this for local statuschecks.)
    // This will only work on localhost access.
    if(!allowpattern.empty() &&
       user->host == "127.0.0.1" &&
       InspIRCd::Match(user->nick, allowpattern))
        return MOD_RES_PASSTHRU;

    // If the user already has the authmod vars set on it, don't kick off
    // another auth query for them.
    if(pendingExt.get(user)){
        return MOD_RES_PASSTHRU;
    }

    // User name must be 33 or more characters, with a 'u' at the start.
    if(user->nick.size()<33){
        log("authmod: invalid nick '" + user->nick + "' must be at least 33 chars");
        pendingExt.set(user, AUTH_STATE_FAIL);
        return MOD_RES_PASSTHRU;
    }
    if(user->nick[0]!='u'){
        log("authmod: invalid nick '" + user->nick + "' must start with a 'u'");
        pendingExt.set(user, AUTH_STATE_FAIL);
        return MOD_RES_PASSTHRU;
    }

    // Keep track of this user in this module, and set their state to BUSY.
    pendingExt.set(user, AUTH_STATE_BUSY);

    // Escape the password (replace ' with '') so you can't do injection
    // attacks with your password.
    string escaped_pass = user->password;
    replaceAll(escaped_pass, "'", "''");

    // Insert the escaped password into the query defined in conf.
    // (replace :pass with escaped password.)
    string queryString = authquery;
    replaceAll(queryString, ":pass", escaped_pass);

    // Create the Query object to track the CQL request and kick it off.
    AuthQuery* query = new AuthQuery(this,
                                     user->uuid,
                                     pendingExt,
                                     verbose,
                                     queryString);
    query->startTime = milliseconds_since_epoch();

    cqlThread->LockQueue();
    cqlThread->Enqueue(query);
    cqlThread->UnlockQueueWakeup();
    return MOD_RES_PASSTHRU;
}

ModResult ModuleCQLAuth::OnCheckReady(LocalUser* user){
    switch(pendingExt.get(user)){
        // None - You've passed auth.
        case AUTH_STATE_NONE:
            //log("authmod: timetest OnCheckReady DONE - PASSED" + user->nick + " " + string_time());
            return MOD_RES_PASSTHRU;
        // Busy - We're waiting to get your query back from Cassandra.
        case AUTH_STATE_BUSY:
            return MOD_RES_DENY;
        // Fail - You are known to be unauthorized.
        case AUTH_STATE_FAIL:
            ServerInstance->Users->QuitUser(user, killreason);
            return MOD_RES_DENY;
    }
    return MOD_RES_PASSTHRU;
}

ModResult ModuleCQLAuth::OnUserPreJoin(User* user,
                                       Channel* chan,
                                       const char* cname,
                                       string &privs,
                                       const string &keygiven){
    // If we've defined an 'allowpattern' and this username uses it,
    // then skip auth.  (We use this for local statuschecks.)
    // This will only work on localhost access.
    if(!allowpattern.empty() && user->host == "127.0.0.1" &&
       InspIRCd::Match(user->nick, allowpattern))
        return MOD_RES_PASSTHRU;

    // Escape (replace ' with '') to protect against injection attacks.

    // We are expecting a query like this:
    // SELECT user_id FROM app.inbox where user_id=':userid' and post_id=':postid';
    string escaped_user_uuid = user->nick.substr(1, 32);
    replaceAll(escaped_user_uuid, "'", "''");
    // Convert the UUIDs to the dash-separated format like so:
    // '054e1d94-aed5-11e3-a682-c8e0eb16059b'
    boost::format user_uuid_fmter("%1%-%2%-%3%-%4%-%5%");
    user_uuid_fmter % escaped_user_uuid.substr(0, 8);
    user_uuid_fmter % escaped_user_uuid.substr(8, 4);
    user_uuid_fmter % escaped_user_uuid.substr(12, 4);
    user_uuid_fmter % escaped_user_uuid.substr(16, 4);
    user_uuid_fmter % escaped_user_uuid.substr(20, 12);
    escaped_user_uuid = user_uuid_fmter.str();
    // Expecting chan names like this:
    // #f6eb46757d4411e3ac7cc8e0eb16059b'
    string escaped_post_uuid = cname;
    escaped_post_uuid = escaped_post_uuid.substr(1, 32);
    replaceAll(escaped_post_uuid, "'", "''");
    boost::format post_uuid_fmter("%1%-%2%-%3%-%4%-%5%");
    post_uuid_fmter % escaped_post_uuid.substr(0, 8);
    post_uuid_fmter % escaped_post_uuid.substr(8, 4);
    post_uuid_fmter % escaped_post_uuid.substr(12, 4);
    post_uuid_fmter % escaped_post_uuid.substr(16, 4);
    post_uuid_fmter % escaped_post_uuid.substr(20, 12);
    escaped_post_uuid = post_uuid_fmter.str();

    string queryString = chanauthquery;
    replaceAll(queryString, ":postid", escaped_post_uuid);
    replaceAll(queryString, ":userid", escaped_user_uuid);

    // Create the Query object to track the CQL request and kick it off.
    ChanAuthQuery* query = new ChanAuthQuery(this,
                                             user->uuid,
                                             cname,
                                             verbose,
                                             queryString);
    query->startTime = milliseconds_since_epoch();

    cqlThread->LockQueue();
    cqlThread->Enqueue(query);
    cqlThread->UnlockQueueWakeup();
    return MOD_RES_DENY;
}

Version ModuleCQLAuth::GetVersion(){
    return Version("Allow/Deny connections and joins based on Cassandra db",
                    VF_VENDOR);
}

/*
Notes about the loop.
There are three inputs that kick the loop
* new query queued
* conf data changed
* async requested data from zookeeper comes back

The difficulty is that all three of these events could be pending when the
loop gets kicked.  (I think.)  Also, I need to lock the loop and check these
conditions because they could lock-and-wake the queue while it is unlocked and
unwaited on and then I could enter a wait not knowing an event is pending.

I need to be able to clearly detect the status of this issues.
* new query queued -- newFutureQueries.size()!=0
* conf data changed -- confLoaded == false
* async zk data comes back -- //!isConnected && zookeeperRequired && !waitingForZookeeper
    !isConnected && !confLoaded catches this condition

We need to lock, check any of those conditions, execute on them, and loop.
if we lock and none of those conditions are met, then and only then do we
wait.

*/
void CQL::Run(){
    log("cql: starting CQL thread");
    LockQueue();
    // These state flags are declared on the CQL object.
    confReloadRequested = true;
    CassCluster* cluster = NULL;
    CassSession* session;
    while(!GetExitFlag()){
        // To keep from spinning the loop, test all of the conditions here:
        bool willDoSomething =
            confReloadRequested ||
            (!isConnected && confLoaded) ||
            (isConnected && (!newFutureQueries.empty() || !readyQueue.empty()));
        if(!willDoSomething){
            WaitForQueue();
        }else{
        }
        if(confReloadRequested){
            log("cql: conf reload requested");
            confReloadRequested = false;
            confLoaded = false;          // True if conf loaded since init or rehash.
            isConnected = false;         // True if viable connection to Cassandra.
            log("cql: getting configuration");
            ConfigTag* conf = ServerInstance->Config->ConfValue("authmod");
            parent->zookeeper_dse_hosts_node = conf->getString("zookeeper_dse_hosts_node");
            log("cql: zookeeper_dse_hosts_node " + parent->zookeeper_dse_hosts_node);
            bool useZk = parent->zookeeper_dse_hosts_node.length()!=0;
            ZooKeeperMod* zkmod;
            if(useZk){
                log("cql: zookeeper_dse_hosts_node not empty, checking zookeeper for hosts");
                zkmod = (ZooKeeperMod*)ServerInstance->Modules->Find("m_zookeepermod.so");
                // TODO: How do we know if zkmod is initialized?
                // You crash if you try to use zkmod before it's connected.
                for(int attempt=0; attempt<6 && !confReloadRequested; attempt++){
                    if(zkmod->connected)
                        break;
                    log("cql: waiting for zookeepermod to connect");
                    UnlockQueue();
                    unsigned long long sleepResult;
                    sleepResult = sleep(5);
                    if(sleepResult!=0){
                        log("cql: zookeeper connect sleep fail " + ullToStr(sleepResult));
                    }
                    LockQueue();
                }
                // This attempt was interrupted by a new rehash, so start over.
                if(confReloadRequested)
                    continue;
                if(!zkmod->connected){
                    log("cql: zookeeper couldn't connect, using conf instead");
                    useZk = false;
                }else
                    log("cql: zookeeper connection found");
            }
            if(useZk){
                // Note: There could be a race here if last rehash is pending
                // and then we request the data again.  The old data could
                // arrive and be interpreted as a response to this request.
                // Submit our request to get called back with host names.
                // For that reason we use and check the rehashId.
                currentRehash++;
                log("cql: requesting hosts from ZooKeeper");
                zkmod->mutex.Lock();
                zoo_awget(zkmod->client,
                          parent->zookeeper_dse_hosts_node.c_str(),
                          CqlWatch::watch, new CqlWatch(currentRehash),
                          CqlCallback::callback, new CqlCallback(currentRehash));
                zkmod->mutex.Unlock();
            }else{
                log("cql conf loaded");
            }
            parent->hosts = conf->getString("hosts");
            log("cql: (conf) hosts " + parent->hosts);
            parent->authquery = conf->getString("authquery");
            log("cql: (conf) authquery " + parent->authquery);
            parent->chanauthquery = conf->getString("chanauthquery");
            log("cql: (conf) chanauthquery " + parent->chanauthquery);
            parent->killreason = conf->getString("killreason");
            log("cql: (conf) killreason " + parent->killreason);
            parent->allowpattern = conf->getString("allowpattern");
            log("cql: (conf) allowpattern " + parent->allowpattern);
            parent->verbose = conf->getBool("verbose");
            if(parent->verbose){
                log("cql: (conf) verbose true");
            }else{
                log("cql: (conf) verbose false");
            }
            confLoaded = !useZk;
        }

        if(!isConnected && confLoaded){
            if(session!=NULL){
                try{
                    CassFuture* close_future = NULL;
                    close_future = cass_session_close(session);
                    //cass_future_wait(close_future); // seems to not return?
                    cass_future_free(close_future);
                }catch(...){
                    log("cql: error closing session");
                }
                session=NULL;
                log("cql: session closed");
            }
            if(cluster!=NULL){
                try{
                    cass_cluster_free(cluster);
                }catch(...){
                    log("cql: error closing cluster");
                }
                cluster=NULL;
                log("cql: cluster freed");
            }
            log("cql: preparing to connect");
            bool error = false;
            string host;
            int port;
            try{
                // Choose a host at random.
                vector<string> host_and_ports;
                boost::split(host_and_ports,
                             parent->hosts,
                             boost::is_any_of(","));
                int h_a_p_index = rand() % host_and_ports.size();
                string host_and_port = host_and_ports[h_a_p_index];
                log("cql: will connect to " + host_and_port);
                // Split the host:port into separate strings.
                vector<string> split_host_and_port;
                boost::split(split_host_and_port, host_and_port, boost::is_any_of(":"));
                host = split_host_and_port[0];
                port = atoi(split_host_and_port[1].c_str());
            }catch(...){
                log("cql: error with host string, attempt conf reload - " + parent->hosts);
                confLoaded = false;
                error = true;
            }
            CassError rc = CASS_OK;
            if(!error){
                cluster = cass_cluster_new();
                CassFuture* session_future = NULL;

                cass_cluster_set_contact_points(cluster, host.c_str());
                cass_cluster_set_port(cluster, port);
                cass_cluster_set_num_threads_io(cluster, 8);
                cass_cluster_set_log_callback(cluster,
                                              cql_log_callback,
                                              NULL);
                session_future = cass_cluster_connect(cluster);
                cass_future_wait(session_future);
                rc = cass_future_error_code(session_future);
                if(CASS_OK == rc) {
                    session = cass_future_get_session(session_future);
                }else{
                    error = true;
                }
                cass_future_free(session_future);
            }
            if(!error){
                // The connection won't complain if DSE is down, so run a
                // test query.
                CassFuture* result_future = NULL;
                CassString query = cass_string_init("SELECT * FROM system.schema_keyspaces;");
                CassStatement* statement = cass_statement_new(query, 0);
                result_future = cass_session_execute(session, statement);
                UnlockQueue();
                cass_future_wait(result_future);
                LockQueue();
                rc = cass_future_error_code(result_future);
                if(CASS_OK != rc) {
                    log("cql: test query had error, no connect");
                    error = true;
                }
                cass_future_free(result_future);
            }
            if(error){
                log("cql: CQL did not connect, retry in 10 seconds");
                UnlockQueue();
                unsigned long long sleepResult;
                sleepResult = sleep(10);
                if(sleepResult!=0){
                    log("cql: reconnect sleep fail " + ullToStr(sleepResult));
                }
                LockQueue();
            }else{
                log("cql: CQL Connected");
                isConnected = true;
            }
            continue;
        }

        if(isConnected && !readyQueue.empty()){
            UnlockQueue();
            CQLQuery* next = readyQueue.front();
            parent->activeQueryCount += 1;
            CassStatement* statement = cass_statement_new(cass_string_init(next->format.c_str()), 0);
            CassFuture* result_future = NULL;
            result_future = cass_session_execute(session, statement);
            cass_statement_free(statement);
            CassError rc = cass_future_set_callback(result_future,
                                                    cql_callback,
                                                    next);
            // Based on the example you can free the future here, but it seems
            // weird to me. Is it not the same future in the callback?
            // the callback does not seem to need to free the future.
            cass_future_free(result_future);

            if (CASS_OK != rc){
                log("cql: ERROR could not set callback, assuming disconnected");
                LockQueue();
                isConnected = false;
                continue;
            }
            readyQueue.pop();
            LockQueue();
        }

        if(isConnected && !newFutureQueries.empty()){
            int snarfCount = 0;
            while(!newFutureQueries.empty() && snarfCount < 10){
                readyQueue.push(newFutureQueries.front());
                newFutureQueries.pop();
                snarfCount++;
            }
        }
    }
    log("cql: loop exit");
    CassFuture* close_future = NULL;
    close_future = cass_session_close(session);
    cass_future_wait(close_future);
    cass_future_free(close_future);
    cass_cluster_free(cluster);
}

void CQL::Enqueue(CQLQuery* query){
    newFutureQueries.push(query);
}

// -- ZooKeeper ---------------------------------------------------------------

void CqlCallback::process(int rc, const char* value, int value_len){
    log("cql: zookeeper callback");
    ModuleCQLAuth* cmod = (ModuleCQLAuth*)ServerInstance->Modules->Find("m_authmod.so");
    cmod->cqlThread->LockQueue();
    if(ZCONNECTIONLOSS==rc){
        log("cql: Lost connection to ZooKeeper");
        cmod->cqlThread->confReloadRequested = true;
    }else{
        if(cmod->cqlThread->currentRehash==rehashId){
            log("cql: zookeeper get called back cassandra hosts");
            if(value_len==0){
                log("cql: zookeeper data empty, using conf");
            }else{
                cmod->hosts.assign(value, value_len);
                log("cql: using hosts '" + cmod->hosts + "'");
            }
            cmod->cqlThread->confLoaded = true;
        }else{
            log("cql: zookeeper call back is old, ignoring");
        }

    }
    cmod->cqlThread->UnlockQueueWakeup();
    log("cql: zookeeper callback done");
}

void CqlWatch::process(int type, int state, const char* path){
    log("cql: zookeeper watch");
    if(ZOO_CONNECTED_STATE==state){
        log("cql: hosts watch triggered, re-processing conf");
        ModuleCQLAuth* cmod = (ModuleCQLAuth*)ServerInstance->Modules->Find("m_authmod.so");
        cmod->cqlThread->LockQueue();
        cmod->cqlThread->confReloadRequested = true;
        cmod->cqlThread->UnlockQueueWakeup();
    }
    log("cql: zookeeper watch done");
}

MODULE_INIT(ModuleCQLAuth)
