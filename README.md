# InspIRCdMods
A collection of event-driven modules for InspIRCd server

### Points of Interest

# Cassandra Auth in `m_authmod.cpp`

The sample app keeps its user's auth information in Cassandra, which is 
accessed via the CQL library. The interactions with Cassandra occur on their 
own thread. This thread's interactions with the main thread are synchronized
with InspIRCd's main event loop using a pipe file descriptor `pipefd`. Without 
`pipefd`, returning CQL queries could modify InspIRCd datastructures at any
point in their processing causing unstable behavior. This synchronization 
allows the slow process of accessing auth data from Cassandra to occur on its
own thread and not interrupting the fast operation of the otherwise 
single-threaded event-driven InspIRCd server.

