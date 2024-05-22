/*
* m_timestampmod -- Replace messages with timestamped JSON.
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
#include <kafka/ApiConstants.h>
#include <kafka/Client.h>
#include <kafka/Message.h>
#include <kafka/MessageSet.h>
#include <kafka/produce/ProduceMessageSet.h>
#include <kafka/produce/ProduceRequest.h>
#include <kafka/produce/ProduceResponse.h>
#include <kafka/produce/ProduceResponsePartition.h>
#include <kafka/TopicNameBlock.h>

#include "inspircd.h"
#include "threadengine.h"

using namespace std;
using namespace chrono;
using namespace LibKafka;

/* $CompileFlags: -L../../lib -I../../include/ -std=c++0x */

/* $LinkerFlags: -L../../lib -L/usr/local/lib -lkafka -lsnappy -lz -ljson */
/* Include /usr/local/lib for -lsnappy on OS X */

// Misc -----------------------------------------------------------------------
unsigned long long milliseconds_since_epoch(){
    return (((unsigned long long)ServerInstance->Time()) * 1000.0) + (((unsigned long long)ServerInstance->Time_ns()) / 1000000.0);
}

// InspIRCd Module ------------------------------------------------------------

class TimestampMod : public Module{
    public:
        void init(){
            Implementation eventlist[] = { I_OnUserPreMessage };
            ServerInstance->Modules->Attach(eventlist,
                                            this,
                                            sizeof(eventlist)/sizeof(Implementation));

            ServerInstance->Logs->Log("timestampmod", DEFAULT,
                                      "App timestampmod started");
        }

	virtual ~TimestampMod(){
	}

	virtual Version GetVersion(){
		return Version("App TimestampMod");
	}

    void Prioritize(){
        // Make sure we come after kafkamod because we modify the message.
        ServerInstance->Modules->SetPriority(
            this,
            I_OnUserPreMessage,
            PRIORITY_AFTER,
            ServerInstance->Modules->Find("m_kafkamod.so"));
	}

	/** Called whenever a user is about to PRIVMSG A user or a channel, before any processing is done.
	 * Returning any nonzero value from this function stops the process immediately, causing no
	 * output to be sent to the user by the core. If you do this you must produce your own numerics,
	 * notices etc. This is useful for modules which may want to filter or redirect messages.
	 * target_type can be one of TYPE_USER or TYPE_CHANNEL. If the target_type value is a user,
	 * you must cast dest to a User* otherwise you must cast it to a Channel*, this is the details
	 * of where the message is destined to be sent.
	 * @param user The user sending the message
	 * @param dest The target of the message (Channel* or User*)
	 * @param target_type The type of target (TYPE_USER or TYPE_CHANNEL)
	 * @param text Changeable text being sent by the user
	 * @param status The status being used, e.g. PRIVMSG @#chan has status== '@', 0 to send to everyone.
	 * @param exempt_list A list of users not to send to. For channel messages, this will usually contain just the sender.
	 * It will be ignored for private messages.
	 * @return 1 to deny the message, 0 to allow it
	 */
	ModResult OnUserPreMessage(User* user,
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
			// Edit the message to include the timestamp.
            Json::Value ircMessage;
            ircMessage["m"] = text;
            ircMessage["ts"] = timestamp;
            Json::FastWriter ircWriter;
            string encodedIrcMessage = ircWriter.write(ircMessage);
            text = encodedIrcMessage;
            // Edit the exempt_list to remove the sender.
            exempt_list.erase(user);
		}
        return MOD_RES_PASSTHRU;
	}
};

MODULE_INIT(TimestampMod)

