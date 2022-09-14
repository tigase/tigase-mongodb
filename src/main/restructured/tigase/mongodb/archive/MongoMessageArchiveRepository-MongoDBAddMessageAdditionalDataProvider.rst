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

MongoMessageArchiveRepository.MongoDBAddMessageAdditionalDataProvider
=====================================================================

.. java:package:: tigase.mongodb.archive
   :noindex:

.. java:type:: public static class MongoDBAddMessageAdditionalDataProvider implements AbstractMessageArchiveRepository.AddMessageAdditionalDataProvider
   :outertype: MongoMessageArchiveRepository

