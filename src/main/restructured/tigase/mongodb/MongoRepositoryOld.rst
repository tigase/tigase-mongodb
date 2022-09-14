.. java:import:: com.mongodb BasicDBObject

.. java:import:: com.mongodb ErrorCategory

.. java:import:: com.mongodb MongoException

.. java:import:: com.mongodb MongoWriteException

.. java:import:: com.mongodb.client FindIterable

.. java:import:: com.mongodb.client MongoCollection

.. java:import:: com.mongodb.client MongoDatabase

.. java:import:: com.mongodb.client.model DeleteManyModel

.. java:import:: com.mongodb.client.model InsertOneModel

.. java:import:: com.mongodb.client.model UpdateOptions

.. java:import:: com.mongodb.client.model WriteModel

.. java:import:: com.mongodb.client.result UpdateResult

.. java:import:: org.bson Document

.. java:import:: org.bson.conversions Bson

.. java:import:: org.bson.types Binary

.. java:import:: tigase.annotations TigaseDeprecated

.. java:import:: tigase.auth.credentials Credentials

.. java:import:: tigase.db.util RepositoryVersionAware

.. java:import:: tigase.db.util SchemaLoader

.. java:import:: tigase.kernel.beans.config ConfigField

.. java:import:: tigase.util StringUtilities

.. java:import:: tigase.util Version

.. java:import:: tigase.xmpp.jid BareJID

.. java:import:: java.nio.charset Charset

.. java:import:: java.security MessageDigest

.. java:import:: java.security NoSuchAlgorithmException

.. java:import:: java.time Duration

.. java:import:: java.util.logging Level

.. java:import:: java.util.logging Logger

.. java:import:: java.util.regex Pattern

MongoRepositoryOld
==================

.. java:package:: tigase.mongodb
   :noindex:

.. java:type:: @Repository.Meta @Repository.SchemaId @RepositoryVersionAware.SchemaVersion @Deprecated @TigaseDeprecated public class MongoRepositoryOld implements AuthRepository, UserRepository, DataSourceAware<MongoDataSource>, MongoRepositoryVersionAware

   MongoRepository is implementation of UserRepository and AuthRepository which supports MongoDB data store.

   :author: andrzej

Fields
------
DOMAIN_KEY
^^^^^^^^^^

.. java:field:: protected static final String DOMAIN_KEY
   :outertype: MongoRepositoryOld

ID_KEY
^^^^^^

.. java:field:: protected static final String ID_KEY
   :outertype: MongoRepositoryOld

NODES_COLLECTION
^^^^^^^^^^^^^^^^

.. java:field:: protected static final String NODES_COLLECTION
   :outertype: MongoRepositoryOld

USERS_COLLECTION
^^^^^^^^^^^^^^^^

.. java:field:: protected static final String USERS_COLLECTION
   :outertype: MongoRepositoryOld

autoCreateUser
^^^^^^^^^^^^^^

.. java:field:: @ConfigField protected boolean autoCreateUser
   :outertype: MongoRepositoryOld

Methods
-------
addDataList
^^^^^^^^^^^

.. java:method:: @Override public void addDataList(BareJID user, String subnode, String key, String[] list) throws UserNotFoundException, TigaseDBException
   :outertype: MongoRepositoryOld

addUser
^^^^^^^

.. java:method:: @Override public void addUser(BareJID user) throws UserExistsException, TigaseDBException
   :outertype: MongoRepositoryOld

addUser
^^^^^^^

.. java:method:: @Override public void addUser(BareJID user, String password) throws UserExistsException, TigaseDBException
   :outertype: MongoRepositoryOld

calculateHash
^^^^^^^^^^^^^

.. java:method:: protected byte[] calculateHash(String user) throws TigaseDBException
   :outertype: MongoRepositoryOld

generateId
^^^^^^^^^^

.. java:method:: protected byte[] generateId(BareJID user) throws TigaseDBException
   :outertype: MongoRepositoryOld

getAccountStatus
^^^^^^^^^^^^^^^^

.. java:method:: @Override public AccountStatus getAccountStatus(BareJID user) throws TigaseDBException
   :outertype: MongoRepositoryOld

getActiveUsersCountIn
^^^^^^^^^^^^^^^^^^^^^

.. java:method:: @Override public long getActiveUsersCountIn(Duration duration)
   :outertype: MongoRepositoryOld

getCredentials
^^^^^^^^^^^^^^

.. java:method:: @Override public Credentials getCredentials(BareJID user, String credentialId) throws TigaseDBException
   :outertype: MongoRepositoryOld

getData
^^^^^^^

.. java:method:: @Override public String getData(BareJID user, String subnode, String key, String def) throws UserNotFoundException, TigaseDBException
   :outertype: MongoRepositoryOld

getData
^^^^^^^

.. java:method:: @Override public String getData(BareJID user, String subnode, String key) throws UserNotFoundException, TigaseDBException
   :outertype: MongoRepositoryOld

getData
^^^^^^^

.. java:method:: @Override public String getData(BareJID user, String key) throws UserNotFoundException, TigaseDBException
   :outertype: MongoRepositoryOld

getDataList
^^^^^^^^^^^

.. java:method:: @Override public String[] getDataList(BareJID user, String subnode, String key) throws UserNotFoundException, TigaseDBException
   :outertype: MongoRepositoryOld

getKeys
^^^^^^^

.. java:method:: @Override public String[] getKeys(BareJID user, String subnode) throws UserNotFoundException, TigaseDBException
   :outertype: MongoRepositoryOld

getKeys
^^^^^^^

.. java:method:: @Override public String[] getKeys(BareJID user) throws UserNotFoundException, TigaseDBException
   :outertype: MongoRepositoryOld

getPassword
^^^^^^^^^^^

.. java:method:: @Override public String getPassword(BareJID user) throws UserNotFoundException, TigaseDBException
   :outertype: MongoRepositoryOld

getResourceUri
^^^^^^^^^^^^^^

.. java:method:: @Override public String getResourceUri()
   :outertype: MongoRepositoryOld

getSubnodes
^^^^^^^^^^^

.. java:method:: @Override public String[] getSubnodes(BareJID user) throws UserNotFoundException, TigaseDBException
   :outertype: MongoRepositoryOld

getSubnodes
^^^^^^^^^^^

.. java:method:: @Override public String[] getSubnodes(BareJID user, String subnode) throws UserNotFoundException, TigaseDBException
   :outertype: MongoRepositoryOld

getUserUID
^^^^^^^^^^

.. java:method:: @Override @Deprecated public long getUserUID(BareJID user) throws TigaseDBException
   :outertype: MongoRepositoryOld

   Should be removed an only relational DB are using this and it is not required by any other code {@inheritDoc}

getUsers
^^^^^^^^

.. java:method:: @Override public List<BareJID> getUsers() throws TigaseDBException
   :outertype: MongoRepositoryOld

getUsersCount
^^^^^^^^^^^^^

.. java:method:: @Override public long getUsersCount()
   :outertype: MongoRepositoryOld

getUsersCount
^^^^^^^^^^^^^

.. java:method:: @Override public long getUsersCount(String domain)
   :outertype: MongoRepositoryOld

initRepository
^^^^^^^^^^^^^^

.. java:method:: @Override @Deprecated public void initRepository(String resource_uri, Map<String, String> params) throws DBInitException
   :outertype: MongoRepositoryOld

isUserDisabled
^^^^^^^^^^^^^^

.. java:method:: @Override public boolean isUserDisabled(BareJID user) throws UserNotFoundException, TigaseDBException
   :outertype: MongoRepositoryOld

loggedIn
^^^^^^^^

.. java:method:: @Override public void loggedIn(BareJID jid) throws TigaseDBException
   :outertype: MongoRepositoryOld

logout
^^^^^^

.. java:method:: @Override public void logout(BareJID user) throws UserNotFoundException, TigaseDBException
   :outertype: MongoRepositoryOld

otherAuth
^^^^^^^^^

.. java:method:: @Override public boolean otherAuth(Map<String, Object> authProps) throws UserNotFoundException, TigaseDBException, AuthorizationException
   :outertype: MongoRepositoryOld

queryAuth
^^^^^^^^^

.. java:method:: @Override public void queryAuth(Map<String, Object> authProps)
   :outertype: MongoRepositoryOld

readAllDistinctValuesForField
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. java:method:: protected <T> List<T> readAllDistinctValuesForField(MongoCollection<Document> collection, String field, Document crit) throws MongoException
   :outertype: MongoRepositoryOld

removeCredential
^^^^^^^^^^^^^^^^

.. java:method:: @Override public void removeCredential(BareJID user, String credentialId) throws TigaseDBException
   :outertype: MongoRepositoryOld

removeData
^^^^^^^^^^

.. java:method:: @Override public void removeData(BareJID user, String key) throws UserNotFoundException, TigaseDBException
   :outertype: MongoRepositoryOld

removeData
^^^^^^^^^^

.. java:method:: @Override public void removeData(BareJID user, String subnode, String key) throws UserNotFoundException, TigaseDBException
   :outertype: MongoRepositoryOld

removeSubnode
^^^^^^^^^^^^^

.. java:method:: @Override public void removeSubnode(BareJID user, String subnode) throws UserNotFoundException, TigaseDBException
   :outertype: MongoRepositoryOld

removeUser
^^^^^^^^^^

.. java:method:: @Override public void removeUser(BareJID user) throws UserNotFoundException, TigaseDBException
   :outertype: MongoRepositoryOld

setAccountStatus
^^^^^^^^^^^^^^^^

.. java:method:: @Override public void setAccountStatus(BareJID user, AccountStatus status) throws TigaseDBException
   :outertype: MongoRepositoryOld

setData
^^^^^^^

.. java:method:: @Override public void setData(BareJID user, String key, String value) throws UserNotFoundException, TigaseDBException
   :outertype: MongoRepositoryOld

setData
^^^^^^^

.. java:method:: @Override public void setData(BareJID user, String subnode, String key, String value) throws UserNotFoundException, TigaseDBException
   :outertype: MongoRepositoryOld

setDataList
^^^^^^^^^^^

.. java:method:: @Override public void setDataList(BareJID user, String subnode, String key, String[] list) throws UserNotFoundException, TigaseDBException
   :outertype: MongoRepositoryOld

setDataSource
^^^^^^^^^^^^^

.. java:method:: @Override public void setDataSource(MongoDataSource dataSource)
   :outertype: MongoRepositoryOld

setUserDisabled
^^^^^^^^^^^^^^^

.. java:method:: @Override public void setUserDisabled(BareJID user, Boolean value) throws UserNotFoundException, TigaseDBException
   :outertype: MongoRepositoryOld

updateCredential
^^^^^^^^^^^^^^^^

.. java:method:: @Override public void updateCredential(BareJID user, String credentialId, String password) throws TigaseDBException
   :outertype: MongoRepositoryOld

updatePassword
^^^^^^^^^^^^^^

.. java:method:: @Override public void updatePassword(BareJID user, String password) throws UserNotFoundException, TigaseDBException
   :outertype: MongoRepositoryOld

updateSchema
^^^^^^^^^^^^

.. java:method:: @Override public SchemaLoader.Result updateSchema(Optional<Version> oldVersion, Version newVersion) throws TigaseDBException
   :outertype: MongoRepositoryOld

userExists
^^^^^^^^^^

.. java:method:: @Override public boolean userExists(BareJID user)
   :outertype: MongoRepositoryOld

