# InspIRCdMods
A collection of event-driven modules for InspIRCd server

# Points of Interest

## Cassandra Auth in `m_authmod.cpp`

The sample app keeps its users' auth information in Cassandra, which is 
accessed via the CQL library. The interactions with Cassandra occur on their 
own thread. This thread synchronizes its interactions
with InspIRCd's main event loop using a pipe file descriptor `pipefd`. Without 
`pipefd`, returning CQL queries could modify InspIRCd datastructures at any
point in their processing causing unstable behavior. This synchronization 
allows the slow process of accessing auth data from Cassandra to occur on its
own thread and not interrupting the fast operation of the otherwise 
single-threaded event-driven InspIRCd server.

## Zookeeper in `m_zookeepermod.cpp`

The modules watch zookeeper mod for asynchronous updates to their settings

## Metitulous stats in `m_statsdmod.cpp`

Fast lightweight stats are reported to statsd

## All messages go to Kafka using `m_kafkamod.cpp`

Kafka configuration can be changed using zookeeper. All messages sent through
InspIRCd are logged to the Kafka message queue. 
