.. java:import:: com.mongodb.client AggregateIterable

.. java:import:: com.mongodb.client FindIterable

.. java:import:: com.mongodb.client MongoCollection

.. java:import:: com.mongodb.client MongoDatabase

.. java:import:: com.mongodb.client.model Filters

.. java:import:: com.mongodb.client.model Projections

.. java:import:: com.mongodb.client.model UpdateOptions

.. java:import:: org.bson Document

.. java:import:: org.bson.conversions Bson

.. java:import:: org.bson.types Binary

.. java:import:: tigase.archive QueryCriteria

.. java:import:: tigase.archive.db AbstractMessageArchiveRepository

.. java:import:: tigase.archive.db MessageArchiveRepository

.. java:import:: tigase.archive.db Schema

.. java:import:: tigase.component.exceptions ComponentException

.. java:import:: tigase.db Repository

.. java:import:: tigase.db TigaseDBException

.. java:import:: tigase.db.util RepositoryVersionAware

.. java:import:: tigase.db.util SchemaLoader

.. java:import:: tigase.kernel.beans.config ConfigField

.. java:import:: tigase.mongodb MongoDataSource

.. java:import:: tigase.mongodb MongoRepositoryVersionAware

.. java:import:: tigase.util Version

.. java:import:: tigase.xml DomBuilderHandler

.. java:import:: tigase.xml Element

.. java:import:: tigase.xml SimpleParser

.. java:import:: tigase.xml SingletonFactory

.. java:import:: tigase.xmpp Authorization

.. java:import:: tigase.xmpp.jid BareJID

.. java:import:: tigase.xmpp.jid JID

.. java:import:: tigase.xmpp.mam MAMRepository

.. java:import:: tigase.xmpp.mam.util MAMUtil

.. java:import:: tigase.xmpp.mam.util Range

.. java:import:: tigase.xmpp.rsm RSM

.. java:import:: java.nio.charset Charset

.. java:import:: java.security MessageDigest

.. java:import:: java.security NoSuchAlgorithmException

.. java:import:: java.time LocalDateTime

.. java:import:: java.time ZoneOffset

.. java:import:: java.util.logging Level

.. java:import:: java.util.logging Logger

.. java:import:: java.util.regex Pattern

.. java:import:: java.util.stream Collectors

MongoMessageArchiveRepository
=============================

.. java:package:: tigase.mongodb.archive
   :noindex:

.. java:type:: @Repository.Meta @Repository.SchemaId @RepositoryVersionAware.SchemaVersion public class MongoMessageArchiveRepository extends AbstractMessageArchiveRepository<QueryCriteria, MongoDataSource, MongoMessageArchiveRepository.MongoDBAddMessageAdditionalDataProvider> implements MongoRepositoryVersionAware

   :author: andrzej

Methods
-------
archiveMessage
^^^^^^^^^^^^^^

.. java:method:: @Override protected void archiveMessage(BareJID ownerJid, BareJID buddyJid, Date timestamp, Element msg, String stableIdStr, String stanzaId, String refStableId, Set<String> tags, MongoDBAddMessageAdditionalDataProvider additionParametersProvider)
   :outertype: MongoMessageArchiveRepository

archiveMessage
^^^^^^^^^^^^^^

.. java:method:: @Override public void archiveMessage(BareJID owner, JID buddy, Date timestamp, Element msg, String stableId, Set tags)
   :outertype: MongoMessageArchiveRepository

createCriteriaDocument
^^^^^^^^^^^^^^^^^^^^^^

.. java:method:: public Document createCriteriaDocument(QueryCriteria query) throws TigaseDBException
   :outertype: MongoMessageArchiveRepository

deleteExpiredMessages
^^^^^^^^^^^^^^^^^^^^^

.. java:method:: @Override public void deleteExpiredMessages(BareJID owner, LocalDateTime before) throws TigaseDBException
   :outertype: MongoMessageArchiveRepository

getStableId
^^^^^^^^^^^

.. java:method:: @Override public String getStableId(BareJID owner, BareJID buddy, String stanzaId) throws TigaseDBException
   :outertype: MongoMessageArchiveRepository

getTags
^^^^^^^

.. java:method:: @Override public List<String> getTags(BareJID owner, String startsWith, QueryCriteria criteria) throws TigaseDBException
   :outertype: MongoMessageArchiveRepository

newQuery
^^^^^^^^

.. java:method:: @Override public QueryCriteria newQuery()
   :outertype: MongoMessageArchiveRepository

queryCollections
^^^^^^^^^^^^^^^^

.. java:method:: @Override public void queryCollections(QueryCriteria query, CollectionHandler<QueryCriteria, Collection> collectionHandler) throws TigaseDBException
   :outertype: MongoMessageArchiveRepository

queryItems
^^^^^^^^^^

.. java:method:: @Override public void queryItems(QueryCriteria query, ItemHandler<QueryCriteria, MAMRepository.Item> itemHandler) throws TigaseDBException
   :outertype: MongoMessageArchiveRepository

removeItems
^^^^^^^^^^^

.. java:method:: @Override public void removeItems(BareJID owner, String with, Date start, Date end) throws TigaseDBException
   :outertype: MongoMessageArchiveRepository

setDataSource
^^^^^^^^^^^^^

.. java:method:: public void setDataSource(MongoDataSource dataSource)
   :outertype: MongoMessageArchiveRepository

updateSchema
^^^^^^^^^^^^

.. java:method:: @Override public SchemaLoader.Result updateSchema(Optional<Version> oldVersion, Version newVersion) throws TigaseDBException
   :outertype: MongoMessageArchiveRepository

