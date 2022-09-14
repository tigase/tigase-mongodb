.. java:import:: com.mongodb BasicDBObject

.. java:import:: com.mongodb MongoClient

.. java:import:: com.mongodb MongoClientURI

.. java:import:: com.mongodb.client MongoCollection

.. java:import:: com.mongodb.client MongoDatabase

.. java:import:: org.bson Document

.. java:import:: tigase.component.exceptions RepositoryException

.. java:import:: tigase.db DBInitException

.. java:import:: tigase.db DataSource

.. java:import:: tigase.db Repository

.. java:import:: tigase.mongodb MongoDataSource

.. java:import:: tigase.stats CounterDataLogger

.. java:import:: tigase.stats.db CounterDataLoggerRepositoryIfc

.. java:import:: java.util Date

.. java:import:: java.util.logging Level

.. java:import:: java.util.logging Logger

CounterDataLoggerMongo
======================

.. java:package:: tigase.mongodb.stats
   :noindex:

.. java:type:: @Repository.Meta public class CounterDataLoggerMongo implements CounterDataLoggerRepositoryIfc<MongoDataSource>

   :author: Wojciech Kapcia

Methods
-------
addStatsLogEntry
^^^^^^^^^^^^^^^^

.. java:method:: @Override public void addStatsLogEntry(String hostname, float cpu_usage, float mem_usage, long uptime, int vhosts, long sm_packets, long muc_packets, long pubsub_packets, long c2s_packets, long ws2s_packets, long s2s_packets, long ext_packets, long presences, long messages, long iqs, long registered, int c2s_conns, int ws2s_conns, int bosh_conns, int s2s_conns, int sm_sessions, int sm_connections)
   :outertype: CounterDataLoggerMongo

setDataSource
^^^^^^^^^^^^^

.. java:method:: @Override public void setDataSource(MongoDataSource dataSource) throws RepositoryException
   :outertype: CounterDataLoggerMongo

