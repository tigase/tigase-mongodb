.. java:import:: com.mongodb BasicDBObject

.. java:import:: com.mongodb ErrorCategory

.. java:import:: com.mongodb MongoException

.. java:import:: com.mongodb MongoWriteException

.. java:import:: com.mongodb.client FindIterable

.. java:import:: com.mongodb.client MongoCollection

.. java:import:: com.mongodb.client MongoDatabase

.. java:import:: org.bson Document

.. java:import:: org.bson.conversions Bson

.. java:import:: org.bson.types Binary

.. java:import:: tigase.auth.credentials Credentials

.. java:import:: tigase.auth.credentials.entries PlainCredentialsEntry

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

.. java:import:: java.util.function Consumer

.. java:import:: java.util.function Function

.. java:import:: java.util.logging Level

.. java:import:: java.util.logging Logger

.. java:import:: java.util.regex Pattern

MongoRepository
===============

.. java:package:: tigase.mongodb
   :noindex:

.. java:type:: @Repository.Meta @Repository.SchemaId @RepositoryVersionAware.SchemaVersion public class MongoRepository extends AbstractAuthRepositoryWithCredentials implements UserRepository, DataSourceAware<MongoDataSource>, MongoRepositoryVersionAware

   MongoRepository is implementation of UserRepository and AuthRepository which supports MongoDB data store.

   :author: andrzej

Fields
------
DOMAIN_KEY
^^^^^^^^^^

.. java:field:: protected static final String DOMAIN_KEY
   :outertype: MongoRepository

ID_KEY
^^^^^^

.. java:field:: protected static final String ID_KEY
   :outertype: MongoRepository

NODES_COLLECTION
^^^^^^^^^^^^^^^^

.. java:field:: protected static final String NODES_COLLECTION
   :outertype: MongoRepository

USERS_COLLECTION
^^^^^^^^^^^^^^^^

.. java:field:: protected static final String USERS_COLLECTION
   :outertype: MongoRepository

USER_CREDENTIALS_COLLECTION
^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. java:field:: protected static final String USER_CREDENTIALS_COLLECTION
   :outertype: MongoRepository

autoCreateUser
^^^^^^^^^^^^^^

.. java:field:: @ConfigField protected boolean autoCreateUser
   :outertype: MongoRepository

Methods
-------
addDataList
^^^^^^^^^^^

.. java:method:: @Override public void addDataList(BareJID user, String subnode, String key, String[] list) throws UserNotFoundException, TigaseDBException
   :outertype: MongoRepository

addUser
^^^^^^^

.. java:method:: @Override public void addUser(BareJID user) throws UserExistsException, TigaseDBException
   :outertype: MongoRepository

addUser
^^^^^^^

.. java:method:: @Override public void addUser(BareJID user, String password) throws UserExistsException, TigaseDBException
   :outertype: MongoRepository

calculateHash
^^^^^^^^^^^^^

.. java:method:: protected byte[] calculateHash(String user) throws TigaseDBException
   :outertype: MongoRepository

generateId
^^^^^^^^^^

.. java:method:: protected byte[] generateId(BareJID user) throws TigaseDBException
   :outertype: MongoRepository

getAccountStatus
^^^^^^^^^^^^^^^^

.. java:method:: @Override public AccountStatus getAccountStatus(BareJID user) throws TigaseDBException
   :outertype: MongoRepository

getActiveUsersCountIn
^^^^^^^^^^^^^^^^^^^^^

.. java:method:: @Override public long getActiveUsersCountIn(Duration duration)
   :outertype: MongoRepository

getCredentialIds
^^^^^^^^^^^^^^^^

.. java:method:: @Override public Collection<String> getCredentialIds(BareJID user) throws TigaseDBException
   :outertype: MongoRepository

getCredentials
^^^^^^^^^^^^^^

.. java:method:: @Override public Credentials getCredentials(BareJID user, String credentialId) throws TigaseDBException
   :outertype: MongoRepository

getData
^^^^^^^

.. java:method:: @Override public String getData(BareJID user, String subnode, String key, String def) throws UserNotFoundException, TigaseDBException
   :outertype: MongoRepository

getData
^^^^^^^

.. java:method:: @Override public String getData(BareJID user, String subnode, String key) throws UserNotFoundException, TigaseDBException
   :outertype: MongoRepository

getData
^^^^^^^

.. java:method:: @Override public String getData(BareJID user, String key) throws UserNotFoundException, TigaseDBException
   :outertype: MongoRepository

getDataList
^^^^^^^^^^^

.. java:method:: @Override public String[] getDataList(BareJID user, String subnode, String key) throws UserNotFoundException, TigaseDBException
   :outertype: MongoRepository

getDataMap
^^^^^^^^^^

.. java:method:: @Override public Map<String, String> getDataMap(BareJID user, String subnode) throws TigaseDBException
   :outertype: MongoRepository

getDataMap
^^^^^^^^^^

.. java:method:: @Override public <T> Map<String, T> getDataMap(BareJID user, String subnode, Function<String, T> converter) throws TigaseDBException
   :outertype: MongoRepository

getKeys
^^^^^^^

.. java:method:: @Override public String[] getKeys(BareJID user, String subnode) throws UserNotFoundException, TigaseDBException
   :outertype: MongoRepository

getKeys
^^^^^^^

.. java:method:: @Override public String[] getKeys(BareJID user) throws UserNotFoundException, TigaseDBException
   :outertype: MongoRepository

getResourceUri
^^^^^^^^^^^^^^

.. java:method:: @Override public String getResourceUri()
   :outertype: MongoRepository

getSubnodes
^^^^^^^^^^^

.. java:method:: @Override public String[] getSubnodes(BareJID user) throws UserNotFoundException, TigaseDBException
   :outertype: MongoRepository

getSubnodes
^^^^^^^^^^^

.. java:method:: @Override public String[] getSubnodes(BareJID user, String subnode) throws UserNotFoundException, TigaseDBException
   :outertype: MongoRepository

getUserUID
^^^^^^^^^^

.. java:method:: @Override @Deprecated public long getUserUID(BareJID user) throws TigaseDBException
   :outertype: MongoRepository

   Should be removed as only relational DB are using this and it is not required by any other code {@inheritDoc}

getUsers
^^^^^^^^

.. java:method:: @Override public List<BareJID> getUsers() throws TigaseDBException
   :outertype: MongoRepository

getUsersCount
^^^^^^^^^^^^^

.. java:method:: @Override public long getUsersCount()
   :outertype: MongoRepository

getUsersCount
^^^^^^^^^^^^^

.. java:method:: @Override public long getUsersCount(String domain)
   :outertype: MongoRepository

initRepository
^^^^^^^^^^^^^^

.. java:method:: @Override @Deprecated public void initRepository(String resource_uri, Map<String, String> params) throws DBInitException
   :outertype: MongoRepository

loggedIn
^^^^^^^^

.. java:method:: @Override public void loggedIn(BareJID jid) throws TigaseDBException
   :outertype: MongoRepository

logout
^^^^^^

.. java:method:: @Override public void logout(BareJID user) throws UserNotFoundException, TigaseDBException
   :outertype: MongoRepository

otherAuth
^^^^^^^^^

.. java:method:: @Override public boolean otherAuth(Map<String, Object> authProps) throws UserNotFoundException, TigaseDBException, AuthorizationException
   :outertype: MongoRepository

queryAuth
^^^^^^^^^

.. java:method:: @Override public void queryAuth(Map<String, Object> authProps)
   :outertype: MongoRepository

readAllDistinctValuesForField
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. java:method:: protected <T> List<T> readAllDistinctValuesForField(MongoCollection<Document> collection, String field, Document crit) throws MongoException
   :outertype: MongoRepository

removeCredential
^^^^^^^^^^^^^^^^

.. java:method:: @Override public void removeCredential(BareJID user, String credentialId) throws TigaseDBException
   :outertype: MongoRepository

removeData
^^^^^^^^^^

.. java:method:: @Override public void removeData(BareJID user, String key) throws UserNotFoundException, TigaseDBException
   :outertype: MongoRepository

removeData
^^^^^^^^^^

.. java:method:: @Override public void removeData(BareJID user, String subnode, String key) throws UserNotFoundException, TigaseDBException
   :outertype: MongoRepository

removeSubnode
^^^^^^^^^^^^^

.. java:method:: @Override public void removeSubnode(BareJID user, String subnode) throws UserNotFoundException, TigaseDBException
   :outertype: MongoRepository

removeUser
^^^^^^^^^^

.. java:method:: @Override public void removeUser(BareJID user) throws UserNotFoundException, TigaseDBException
   :outertype: MongoRepository

setAccountStatus
^^^^^^^^^^^^^^^^

.. java:method:: @Override public void setAccountStatus(BareJID user, AccountStatus status) throws TigaseDBException
   :outertype: MongoRepository

setData
^^^^^^^

.. java:method:: @Override public void setData(BareJID user, String key, String value) throws UserNotFoundException, TigaseDBException
   :outertype: MongoRepository

setData
^^^^^^^

.. java:method:: @Override public void setData(BareJID user, String subnode, String key, String value) throws UserNotFoundException, TigaseDBException
   :outertype: MongoRepository

setDataList
^^^^^^^^^^^

.. java:method:: @Override public void setDataList(BareJID user, String subnode, String key, String[] list) throws UserNotFoundException, TigaseDBException
   :outertype: MongoRepository

setDataSource
^^^^^^^^^^^^^

.. java:method:: @Override public void setDataSource(MongoDataSource dataSource)
   :outertype: MongoRepository

updateCredential
^^^^^^^^^^^^^^^^

.. java:method:: @Override public void updateCredential(BareJID user, String credentialId, String password) throws TigaseDBException
   :outertype: MongoRepository

updatePassword
^^^^^^^^^^^^^^

.. java:method:: @Override public void updatePassword(BareJID user, String password) throws UserNotFoundException, TigaseDBException
   :outertype: MongoRepository

updateSchema
^^^^^^^^^^^^

.. java:method:: @Override public SchemaLoader.Result updateSchema(Optional<Version> oldVersion, Version newVersion) throws TigaseDBException
   :outertype: MongoRepository

userExists
^^^^^^^^^^

.. java:method:: @Override public boolean userExists(BareJID user)
   :outertype: MongoRepository

