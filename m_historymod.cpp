
#include <json/json.h>

#include "inspircd.h"

/* $ModDesc: App filtered auto-enabled channel history */
/* $CompileFlags: -L../../lib -I../../include/ -std=c++0x */
/* $LinkerFlags: -L../../lib -ljson */

// log - convenient synchronized logging function.
Mutex* loggingMutex = new Mutex();
void log(std::string s){
    loggingMutex->Lock();
    ServerInstance->Logs->Log("m_historymod", DEFAULT, s);
    loggingMutex->Unlock();
}

struct HistoryItem{
	time_t ts;
	std::string line;
	HistoryItem(const std::string& Line) : ts(ServerInstance->Time()), line(Line) {}
};

struct HistoryList{
	std::deque<HistoryItem> lines;
	unsigned int maxlen, maxtime;
	HistoryList(unsigned int len, unsigned int time) : maxlen(len), maxtime(time) {}
};

// See m_chanhistory.cpp
// TODO: When do we clear history?
// I think we'd do this but I don't see history chan doing it?
// i think it assumes the mode gets unset before the channel dies?  or does
// ext notice when its map-key dies?  In any case, when do we call delete?
//			ext.unset(channel);

class HistoryMod : public Module{
	SimpleExtItem<HistoryList> ext;
	unsigned int maxlines;
	unsigned int maxtime;

  public:
    HistoryMod() : ext("historymod", this), maxlines(100) {}

    void init(){
	    maxtime = ServerInstance->Duration("86400");
	    // ext is like a map.  We map channel to historylist.
		ServerInstance->Modules->AddService(ext);

		Implementation eventlist[] = {
		    I_OnPostJoin,
		    I_OnUserMessage
		};
		ServerInstance->Modules->Attach(eventlist, this, sizeof(eventlist)/sizeof(Implementation));
	}

	virtual ~HistoryMod(){
	}

	void OnUserMessage(User* user,
	                   void* dest,
	                   int target_type,
	                   const std::string &text,
	                   char status,
	                   const CUList&){
		if (target_type==TYPE_CHANNEL && status==0){
            Json::Value root;
            Json::Reader reader;
            bool parsingSuccessful = reader.parse(text, root);
            if(!parsingSuccessful){
                log("historymod: Could not parse JSON from message, ignoring.");
                log(reader.getFormattedErrorMessages());
                return;
            }
            std::string content = root.get("m", "NONE" ).asString();
            if(content!="NONE"){
                Json::Value rootInner;
                Json::Reader readerInner;
                parsingSuccessful = readerInner.parse(content, rootInner);
                if(!parsingSuccessful){
                    log("historymod: Could not parse JSON from message, ignoring.");
                    log(readerInner.getFormattedErrorMessages());
                    return;
                }
                std::string messageType = rootInner.get("t", "NONE" ).asString();
                if(messageType=="m"){
                    Channel* c = (Channel*)dest;
                    HistoryList* list = ext.get(c);
                    if(list){
                        char buf[MAXBUF];
                        snprintf(buf, MAXBUF, ":%s PRIVMSG %s :%s",
                            user->GetFullHost().c_str(), c->name.c_str(), text.c_str());
                        list->lines.push_back(HistoryItem(buf));
                        if (list->lines.size() > list->maxlen){
                            list->lines.pop_front();
                        }
                    }else{
                        log("historymod: No history for channel");
                    }
                }
            }
		}
	}

	void OnPostJoin(Membership* memb){
		if(IS_REMOTE(memb->user))
			return;
        Channel* channel = memb->chan;
		HistoryList* list = ext.get(channel);
		if(!list){
		    list = new HistoryList(maxlines, maxtime);
            ext.set(channel, list);
        }
		time_t mintime = ServerInstance->Time() - list->maxtime;
		for(std::deque<HistoryItem>::iterator i = list->lines.begin();
		    i!=list->lines.end();
		    ++i)
		{
			if (i->ts >= mintime){
				memb->user->Write(i->line);
            }
		}
	}

	Version GetVersion(){
		return Version("App filtered channel history replayed on join");
	}
};

MODULE_INIT(HistoryMod)
