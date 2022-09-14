.. java:import:: com.mongodb MongoException

.. java:import:: com.mongodb.client FindIterable

.. java:import:: com.mongodb.client MongoCollection

.. java:import:: com.mongodb.client MongoDatabase

.. java:import:: org.bson Document

.. java:import:: org.bson.conversions Bson

.. java:import:: org.bson.types Binary

.. java:import:: org.bson.types ObjectId

.. java:import:: tigase.component.exceptions ComponentException

.. java:import:: tigase.component.exceptions RepositoryException

.. java:import:: tigase.db Repository

.. java:import:: tigase.db.util RepositoryVersionAware

.. java:import:: tigase.db.util SchemaLoader

.. java:import:: tigase.kernel.beans.config ConfigField

.. java:import:: tigase.mongodb MongoDataSource

.. java:import:: tigase.mongodb MongoRepositoryVersionAware

.. java:import:: tigase.pubsub.modules.mam ExtendedQueryImpl

.. java:import:: tigase.pubsub.repository.stateless NodeMeta

.. java:import:: tigase.pubsub.repository.stateless UsersAffiliation

.. java:import:: tigase.pubsub.repository.stateless UsersSubscription

.. java:import:: tigase.util Version

.. java:import:: tigase.util.stringprep TigaseStringprepException

.. java:import:: tigase.xml Element

.. java:import:: tigase.xmpp Authorization

.. java:import:: tigase.xmpp.jid BareJID

.. java:import:: tigase.xmpp.mam MAMRepository

.. java:import:: tigase.xmpp.mam.util MAMUtil

.. java:import:: tigase.xmpp.mam.util Range

.. java:import:: tigase.xmpp.rsm RSM

.. java:import:: java.nio.charset Charset

.. java:import:: java.security MessageDigest

.. java:import:: java.security NoSuchAlgorithmException

.. java:import:: java.util.stream Collectors

PubSubDAOMongo
==============

.. java:package:: tigase.mongodb.pubsub
   :noindex:

.. java:type:: @Repository.Meta @Repository.SchemaId @RepositoryVersionAware.SchemaVersion public class PubSubDAOMongo extends PubSubDAO<ObjectId, MongoDataSource, tigase.pubsub.modules.mam.ExtendedQueryImpl> implements MongoRepositoryVersionAware

   :author: andrzej

Fields
------
PUBSUB_AFFILIATIONS
^^^^^^^^^^^^^^^^^^^

.. java:field:: public static final String PUBSUB_AFFILIATIONS
   :outertype: PubSubDAOMongo

PUBSUB_ITEMS
^^^^^^^^^^^^

.. java:field:: public static final String PUBSUB_ITEMS
   :outertype: PubSubDAOMongo

PUBSUB_MAM
^^^^^^^^^^

.. java:field:: public static final String PUBSUB_MAM
   :outertype: PubSubDAOMongo

PUBSUB_NODES
^^^^^^^^^^^^

.. java:field:: public static final String PUBSUB_NODES
   :outertype: PubSubDAOMongo

PUBSUB_SERVICE_JIDS
^^^^^^^^^^^^^^^^^^^

.. java:field:: public static final String PUBSUB_SERVICE_JIDS
   :outertype: PubSubDAOMongo

PUBSUB_SUBSCRIPTIONS
^^^^^^^^^^^^^^^^^^^^

.. java:field:: public static final String PUBSUB_SUBSCRIPTIONS
   :outertype: PubSubDAOMongo

Constructors
------------
PubSubDAOMongo
^^^^^^^^^^^^^^

.. java:constructor:: public PubSubDAOMongo()
   :outertype: PubSubDAOMongo

Methods
-------
addMAMItem
^^^^^^^^^^

.. java:method:: @Override public void addMAMItem(BareJID serviceJid, ObjectId nodeId, String uuid, Element message, String itemId) throws RepositoryException
   :outertype: PubSubDAOMongo

createNode
^^^^^^^^^^

.. java:method:: @Override public ObjectId createNode(BareJID serviceJid, String nodeName, BareJID ownerJid, AbstractNodeConfig nodeConfig, NodeType nodeType, ObjectId collectionId, boolean autocreate) throws RepositoryException
   :outertype: PubSubDAOMongo

createService
^^^^^^^^^^^^^

.. java:method:: @Override public void createService(BareJID serviceJid, boolean isPublic) throws RepositoryException
   :outertype: PubSubDAOMongo

deleteItem
^^^^^^^^^^

.. java:method:: @Override public void deleteItem(BareJID serviceJid, ObjectId nodeId, String id) throws RepositoryException
   :outertype: PubSubDAOMongo

deleteNode
^^^^^^^^^^

.. java:method:: @Override public void deleteNode(BareJID serviceJid, ObjectId nodeId) throws RepositoryException
   :outertype: PubSubDAOMongo

deleteService
^^^^^^^^^^^^^

.. java:method:: @Override public void deleteService(BareJID serviceJid) throws RepositoryException
   :outertype: PubSubDAOMongo

getAllNodesList
^^^^^^^^^^^^^^^

.. java:method:: @Override public String[] getAllNodesList(BareJID serviceJid) throws RepositoryException
   :outertype: PubSubDAOMongo

getChildNodes
^^^^^^^^^^^^^

.. java:method:: @Override public String[] getChildNodes(BareJID serviceJid, String nodeName) throws RepositoryException
   :outertype: PubSubDAOMongo

getItem
^^^^^^^

.. java:method:: @Override public IItems.IItem getItem(BareJID serviceJid, ObjectId nodeId, String id) throws RepositoryException
   :outertype: PubSubDAOMongo

getItems
^^^^^^^^

.. java:method:: @Override public List<IItems.IItem> getItems(BareJID serviceJid, List<ObjectId> nodeIds, Date afterDate, Date beforeDate, RSM rsm, CollectionItemsOrdering collectionItemsOrdering) throws RepositoryException
   :outertype: PubSubDAOMongo

getItemsIds
^^^^^^^^^^^

.. java:method:: @Override public String[] getItemsIds(BareJID serviceJid, ObjectId nodeId, CollectionItemsOrdering order) throws RepositoryException
   :outertype: PubSubDAOMongo

getItemsIdsSince
^^^^^^^^^^^^^^^^

.. java:method:: @Override public String[] getItemsIdsSince(BareJID serviceJid, ObjectId nodeId, CollectionItemsOrdering order, Date since) throws RepositoryException
   :outertype: PubSubDAOMongo

getItemsMeta
^^^^^^^^^^^^

.. java:method:: @Override public List<IItems.ItemMeta> getItemsMeta(BareJID serviceJid, ObjectId nodeId, String nodeName) throws RepositoryException
   :outertype: PubSubDAOMongo

getNodeAffiliations
^^^^^^^^^^^^^^^^^^^

.. java:method:: @Override public Map<BareJID, UsersAffiliation> getNodeAffiliations(BareJID serviceJid, ObjectId nodeId) throws RepositoryException
   :outertype: PubSubDAOMongo

getNodeMeta
^^^^^^^^^^^

.. java:method:: @Override public INodeMeta<ObjectId> getNodeMeta(BareJID serviceJid, String nodeName) throws RepositoryException
   :outertype: PubSubDAOMongo

getNodeSubscriptions
^^^^^^^^^^^^^^^^^^^^

.. java:method:: @Override public Map<BareJID, UsersSubscription> getNodeSubscriptions(BareJID serviceJid, ObjectId nodeId) throws RepositoryException
   :outertype: PubSubDAOMongo

getNodesCount
^^^^^^^^^^^^^

.. java:method:: @Override public long getNodesCount(BareJID serviceJid) throws RepositoryException
   :outertype: PubSubDAOMongo

getNodesList
^^^^^^^^^^^^

.. java:method:: @Override public String[] getNodesList(BareJID serviceJid, String nodeName) throws RepositoryException
   :outertype: PubSubDAOMongo

getServices
^^^^^^^^^^^

.. java:method:: @Override public List<BareJID> getServices(BareJID bareJID, Boolean isPublic) throws RepositoryException
   :outertype: PubSubDAOMongo

getUserAffiliations
^^^^^^^^^^^^^^^^^^^

.. java:method:: @Override public Map<String, UsersAffiliation> getUserAffiliations(BareJID serviceJid, BareJID jid) throws RepositoryException
   :outertype: PubSubDAOMongo

getUserSubscriptions
^^^^^^^^^^^^^^^^^^^^

.. java:method:: @Override public Map<String, UsersSubscription> getUserSubscriptions(BareJID serviceJid, BareJID jid) throws RepositoryException
   :outertype: PubSubDAOMongo

newQuery
^^^^^^^^

.. java:method:: @Override public ExtendedQueryImpl newQuery(BareJID serviceJid)
   :outertype: PubSubDAOMongo

queryItems
^^^^^^^^^^

.. java:method:: @Override public void queryItems(ExtendedQueryImpl query, ObjectId nodeId, MAMRepository.ItemHandler<ExtendedQueryImpl, IPubSubRepository.Item> itemHandler) throws RepositoryException
   :outertype: PubSubDAOMongo

readAllValuesForField
^^^^^^^^^^^^^^^^^^^^^

.. java:method:: protected <T> List<T> readAllValuesForField(MongoCollection<Document> collection, String field, Bson filter) throws MongoException
   :outertype: PubSubDAOMongo

readAllValuesForField
^^^^^^^^^^^^^^^^^^^^^

.. java:method:: protected <T> List<T> readAllValuesForField(MongoCollection<Document> collection, String field, Bson filter, Bson sort) throws MongoException
   :outertype: PubSubDAOMongo

removeAllFromRootCollection
^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. java:method:: public void removeAllFromRootCollection(BareJID serviceJid) throws RepositoryException
   :outertype: PubSubDAOMongo

removeNodeSubscription
^^^^^^^^^^^^^^^^^^^^^^

.. java:method:: @Override public void removeNodeSubscription(BareJID serviceJid, ObjectId nodeId, BareJID jid) throws RepositoryException
   :outertype: PubSubDAOMongo

setDataSource
^^^^^^^^^^^^^

.. java:method:: public void setDataSource(MongoDataSource dataSource)
   :outertype: PubSubDAOMongo

updateNodeAffiliation
^^^^^^^^^^^^^^^^^^^^^

.. java:method:: @Override public void updateNodeAffiliation(BareJID serviceJid, ObjectId nodeId, String nodeName, UsersAffiliation userAffiliation) throws RepositoryException
   :outertype: PubSubDAOMongo

updateNodeConfig
^^^^^^^^^^^^^^^^

.. java:method:: @Override public void updateNodeConfig(BareJID serviceJid, ObjectId nodeId, String serializedNodeConfig, ObjectId collectionId) throws RepositoryException
   :outertype: PubSubDAOMongo

updateNodeSubscription
^^^^^^^^^^^^^^^^^^^^^^

.. java:method:: @Override public void updateNodeSubscription(BareJID serviceJid, ObjectId nodeId, String nodeName, UsersSubscription userSubscription) throws RepositoryException
   :outertype: PubSubDAOMongo

updateSchema
^^^^^^^^^^^^

.. java:method:: @Override public SchemaLoader.Result updateSchema(Optional<Version> oldVersion, Version newVersion) throws RepositoryException
   :outertype: PubSubDAOMongo

writeItem
^^^^^^^^^

.. java:method:: @Override public void writeItem(BareJID serviceJid, ObjectId nodeId, long timeInMilis, String id, String publisher, Element item, String uuid) throws RepositoryException
   :outertype: PubSubDAOMongo

