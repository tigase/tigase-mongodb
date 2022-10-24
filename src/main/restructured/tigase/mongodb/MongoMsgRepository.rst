.. java:import:: com.mongodb MongoException

.. java:import:: com.mongodb MongoNamespace

.. java:import:: com.mongodb.client FindIterable

.. java:import:: com.mongodb.client MongoCollection

.. java:import:: com.mongodb.client MongoDatabase

.. java:import:: com.mongodb.client.model Filters

.. java:import:: com.mongodb.client.model Projections

.. java:import:: com.mongodb.client.model Sorts

.. java:import:: com.mongodb.client.result DeleteResult

.. java:import:: org.bson Document

.. java:import:: org.bson.conversions Bson

.. java:import:: org.bson.types Binary

.. java:import:: org.bson.types ObjectId

.. java:import:: tigase.db.util RepositoryVersionAware

.. java:import:: tigase.db.util SchemaLoader

.. java:import:: tigase.kernel.beans.config ConfigField

.. java:import:: tigase.server Packet

.. java:import:: tigase.server.amp.db MsgRepository

.. java:import:: tigase.util Version

.. java:import:: tigase.util.datetime TimestampHelper

.. java:import:: tigase.xml DomBuilderHandler

.. java:import:: tigase.xml Element

.. java:import:: tigase.xmpp XMPPResourceConnection

.. java:import:: tigase.xmpp.jid BareJID

.. java:import:: tigase.xmpp.jid JID

.. java:import:: java.nio.charset Charset

.. java:import:: java.security MessageDigest

.. java:import:: java.security NoSuchAlgorithmException

.. java:import:: java.util.logging Level

.. java:import:: java.util.logging Logger

.. java:import:: java.util.stream Collectors

MongoMsgRepository
==================

.. java:package:: tigase.mongodb
   :noindex:

.. java:type:: @Repository.Meta @Repository.SchemaId @RepositoryVersionAware.SchemaVersion public class MongoMsgRepository extends MsgRepository<ObjectId, MongoDataSource> implements MongoRepositoryVersionAware

   :author: andrzej

Methods
-------
deleteMessage
^^^^^^^^^^^^^

.. java:method:: @Override protected void deleteMessage(ObjectId dbId)
   :outertype: MongoMsgRepository

deleteMessagesToJID
^^^^^^^^^^^^^^^^^^^

.. java:method:: @Override public int deleteMessagesToJID(List<String> db_ids, XMPPResourceConnection session) throws UserNotFoundException
   :outertype: MongoMsgRepository

getMessageExpired
^^^^^^^^^^^^^^^^^

.. java:method:: @Override public Element getMessageExpired(long time, boolean delete)
   :outertype: MongoMsgRepository

getMessagesCount
^^^^^^^^^^^^^^^^

.. java:method:: @Override public Map<Enum, Long> getMessagesCount(JID to) throws UserNotFoundException
   :outertype: MongoMsgRepository

getMessagesList
^^^^^^^^^^^^^^^

.. java:method:: @Override public List<Element> getMessagesList(JID to) throws UserNotFoundException
   :outertype: MongoMsgRepository

initRepository
^^^^^^^^^^^^^^

.. java:method:: @Override @Deprecated public void initRepository(String resource_uri, Map<String, String> params) throws DBInitException
   :outertype: MongoMsgRepository

loadExpiredQueue
^^^^^^^^^^^^^^^^

.. java:method:: @Override protected void loadExpiredQueue(int max)
   :outertype: MongoMsgRepository

loadExpiredQueue
^^^^^^^^^^^^^^^^

.. java:method:: @Override protected void loadExpiredQueue(Date expired)
   :outertype: MongoMsgRepository

loadMessagesToJID
^^^^^^^^^^^^^^^^^

.. java:method:: @Override public Queue<Element> loadMessagesToJID(XMPPResourceConnection session, boolean delete) throws UserNotFoundException
   :outertype: MongoMsgRepository

loadMessagesToJID
^^^^^^^^^^^^^^^^^

.. java:method:: public Queue<Element> loadMessagesToJID(XMPPResourceConnection session, boolean delete, OfflineMessagesProcessor proc) throws UserNotFoundException
   :outertype: MongoMsgRepository

loadMessagesToJID
^^^^^^^^^^^^^^^^^

.. java:method:: @Override public Queue<Element> loadMessagesToJID(List<String> db_ids, XMPPResourceConnection session, boolean delete, OfflineMessagesProcessor proc) throws UserNotFoundException
   :outertype: MongoMsgRepository

setDataSource
^^^^^^^^^^^^^

.. java:method:: @Override public void setDataSource(MongoDataSource dataSource)
   :outertype: MongoMsgRepository

storeMessage
^^^^^^^^^^^^

.. java:method:: @Override public boolean storeMessage(JID from, JID to, Date expired, Element msg, NonAuthUserRepository userRepo) throws UserNotFoundException
   :outertype: MongoMsgRepository

updateSchema
^^^^^^^^^^^^

.. java:method:: @Override public SchemaLoader.Result updateSchema(Optional<Version> oldVersion, Version newVersion) throws TigaseDBException
   :outertype: MongoMsgRepository

