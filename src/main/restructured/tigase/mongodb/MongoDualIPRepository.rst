.. java:import:: com.mongodb BasicDBObject

.. java:import:: com.mongodb.client MongoCollection

.. java:import:: com.mongodb.client MongoDatabase

.. java:import:: org.bson Document

.. java:import:: tigase.db Repository

.. java:import:: tigase.server.xmppclient SeeOtherHostDualIP.DualIPRepository

.. java:import:: tigase.util.stringprep TigaseStringprepException

.. java:import:: tigase.xmpp.jid BareJID

.. java:import:: java.sql SQLException

.. java:import:: java.util Map

.. java:import:: java.util.concurrent ConcurrentSkipListMap

.. java:import:: java.util.logging Level

.. java:import:: java.util.logging Logger

MongoDualIPRepository
=====================

.. java:package:: tigase.mongodb
   :noindex:

.. java:type:: @Repository.Meta public class MongoDualIPRepository implements DualIPRepository<MongoDataSource>

   :author: Wojtek

Methods
-------
queryAllDB
^^^^^^^^^^

.. java:method:: @Override public Map<BareJID, BareJID> queryAllDB() throws SQLException
   :outertype: MongoDualIPRepository

setDataSource
^^^^^^^^^^^^^

.. java:method:: @Override public void setDataSource(MongoDataSource dataSource)
   :outertype: MongoDualIPRepository

