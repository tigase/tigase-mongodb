.. java:import:: com.mongodb MongoNamespace

.. java:import:: com.mongodb.client FindIterable

.. java:import:: com.mongodb.client MongoCollection

.. java:import:: com.mongodb.client MongoDatabase

.. java:import:: com.mongodb.client.model Filters

.. java:import:: org.bson Document

.. java:import:: org.bson.conversions Bson

.. java:import:: org.bson.types Binary

.. java:import:: tigase.component PacketWriter

.. java:import:: tigase.component.exceptions ComponentException

.. java:import:: tigase.db Repository

.. java:import:: tigase.db TigaseDBException

.. java:import:: tigase.db.util RepositoryVersionAware

.. java:import:: tigase.db.util SchemaLoader

.. java:import:: tigase.kernel.beans.config ConfigField

.. java:import:: tigase.mongodb MongoDataSource

.. java:import:: tigase.mongodb MongoRepositoryVersionAware

.. java:import:: tigase.muc Affiliation

.. java:import:: tigase.muc Room

.. java:import:: tigase.muc RoomConfig

.. java:import:: tigase.muc.history AbstractHistoryProvider

.. java:import:: tigase.muc.repository Schema

.. java:import:: tigase.server Packet

.. java:import:: tigase.util Version

.. java:import:: tigase.util.stringprep TigaseStringprepException

.. java:import:: tigase.xml Element

.. java:import:: tigase.xmpp Authorization

.. java:import:: tigase.xmpp.jid BareJID

.. java:import:: tigase.xmpp.jid JID

.. java:import:: tigase.xmpp.mam MAMRepository

.. java:import:: tigase.xmpp.mam Query

.. java:import:: tigase.xmpp.mam QueryImpl

.. java:import:: java.nio.charset Charset

.. java:import:: java.security MessageDigest

.. java:import:: java.security NoSuchAlgorithmException

.. java:import:: java.util.logging Level

MongoHistoryProvider
====================

.. java:package:: tigase.mongodb.muc
   :noindex:

.. java:type:: @Repository.Meta @Repository.SchemaId @RepositoryVersionAware.SchemaVersion public class MongoHistoryProvider extends AbstractHistoryProvider<MongoDataSource> implements MongoRepositoryVersionAware, MAMRepository

   :author: andrzej

Fields
------
historyCollection
^^^^^^^^^^^^^^^^^

.. java:field:: protected MongoCollection<Document> historyCollection
   :outertype: MongoHistoryProvider

Methods
-------
addJoinEvent
^^^^^^^^^^^^

.. java:method:: @Override public void addJoinEvent(Room room, Date date, JID senderJID, String nickName)
   :outertype: MongoHistoryProvider

addLeaveEvent
^^^^^^^^^^^^^

.. java:method:: @Override public void addLeaveEvent(Room room, Date date, JID senderJID, String nickName)
   :outertype: MongoHistoryProvider

addMessage
^^^^^^^^^^

.. java:method:: @Override public void addMessage(Room room, Element message, String body, JID senderJid, String senderNickname, Date time)
   :outertype: MongoHistoryProvider

addSubjectChange
^^^^^^^^^^^^^^^^

.. java:method:: @Override public void addSubjectChange(Room room, Element message, String subject, JID senderJid, String senderNickname, Date time)
   :outertype: MongoHistoryProvider

calculateHash
^^^^^^^^^^^^^

.. java:method:: protected byte[] calculateHash(String user) throws TigaseDBException
   :outertype: MongoHistoryProvider

destroy
^^^^^^^

.. java:method:: @Override public void destroy()
   :outertype: MongoHistoryProvider

generateId
^^^^^^^^^^

.. java:method:: protected byte[] generateId(BareJID user) throws TigaseDBException
   :outertype: MongoHistoryProvider

getHistoryMessages
^^^^^^^^^^^^^^^^^^

.. java:method:: @Override public void getHistoryMessages(Room room, JID senderJID, Integer maxchars, Integer maxstanzas, Integer seconds, Date since, PacketWriter writer)
   :outertype: MongoHistoryProvider

isPersistent
^^^^^^^^^^^^

.. java:method:: @Override public boolean isPersistent(Room room)
   :outertype: MongoHistoryProvider

newQuery
^^^^^^^^

.. java:method:: @Override public Query newQuery()
   :outertype: MongoHistoryProvider

queryItems
^^^^^^^^^^

.. java:method:: @Override public void queryItems(Query query, ItemHandler itemHandler) throws TigaseDBException, ComponentException
   :outertype: MongoHistoryProvider

removeHistory
^^^^^^^^^^^^^

.. java:method:: @Override public void removeHistory(Room room)
   :outertype: MongoHistoryProvider

setDataSource
^^^^^^^^^^^^^

.. java:method:: @Override public void setDataSource(MongoDataSource dataSource)
   :outertype: MongoHistoryProvider

updateSchema
^^^^^^^^^^^^

.. java:method:: @Override public SchemaLoader.Result updateSchema(Optional<Version> oldVersion, Version newVersion) throws TigaseDBException
   :outertype: MongoHistoryProvider

