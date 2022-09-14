.. java:import:: com.mongodb MongoException

.. java:import:: com.mongodb MongoNamespace

.. java:import:: com.mongodb.client FindIterable

.. java:import:: com.mongodb.client MongoCollection

.. java:import:: com.mongodb.client MongoDatabase

.. java:import:: com.mongodb.client.model IndexOptions

.. java:import:: com.mongodb.client.model UpdateOptions

.. java:import:: com.mongodb.client.model Updates

.. java:import:: org.bson Document

.. java:import:: org.bson.types ObjectId

.. java:import:: tigase.db Repository

.. java:import:: tigase.db Schema

.. java:import:: tigase.db TigaseDBException

.. java:import:: tigase.kernel.beans.config ConfigField

.. java:import:: tigase.server.amp.db MsgBroadcastRepository

.. java:import:: tigase.xml DomBuilderHandler

.. java:import:: tigase.xml Element

.. java:import:: tigase.xmpp.jid BareJID

.. java:import:: tigase.xmpp.jid JID

.. java:import:: java.nio.charset Charset

.. java:import:: java.security MessageDigest

.. java:import:: java.security NoSuchAlgorithmException

.. java:import:: java.util Date

.. java:import:: java.util HashSet

.. java:import:: java.util Queue

.. java:import:: java.util Set

.. java:import:: java.util.function Consumer

.. java:import:: java.util.logging Level

.. java:import:: java.util.logging Logger

MongoMsgBroadcastRepository
===========================

.. java:package:: tigase.mongodb
   :noindex:

.. java:type:: @Repository.Meta @Repository.SchemaId public class MongoMsgBroadcastRepository extends MsgBroadcastRepository<ObjectId, MongoDataSource>

   Created by andrzej on 04.10.2016.

Methods
-------
ensureBroadcastMessageRecipient
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. java:method:: @Override protected void ensureBroadcastMessageRecipient(String id, BareJID recipient)
   :outertype: MongoMsgBroadcastRepository

insertBroadcastMessage
^^^^^^^^^^^^^^^^^^^^^^

.. java:method:: @Override protected void insertBroadcastMessage(String id, Element msg, Date expire, BareJID recipient)
   :outertype: MongoMsgBroadcastRepository

loadMessagesToBroadcast
^^^^^^^^^^^^^^^^^^^^^^^

.. java:method:: @Override public void loadMessagesToBroadcast()
   :outertype: MongoMsgBroadcastRepository

setDataSource
^^^^^^^^^^^^^

.. java:method:: @Override public void setDataSource(MongoDataSource dataSource)
   :outertype: MongoMsgBroadcastRepository

