.. java:import:: com.mongodb MongoClient

.. java:import:: com.mongodb MongoClientURI

.. java:import:: com.mongodb MongoException

.. java:import:: com.mongodb.client MongoDatabase

.. java:import:: com.mongodb.client.model Filters

.. java:import:: com.mongodb.client.model UpdateOptions

.. java:import:: org.bson Document

.. java:import:: tigase.db DBInitException

.. java:import:: tigase.db.util DBSchemaLoader

.. java:import:: tigase.db.util RepositoryVersionAware

.. java:import:: tigase.db.util SchemaLoader

.. java:import:: tigase.db.util SchemaManager

.. java:import:: tigase.util Version

.. java:import:: tigase.util.log LogFormatter

.. java:import:: tigase.util.ui.console CommandlineParameter

.. java:import:: tigase.xmpp.jid BareJID

.. java:import:: java.util.function Function

.. java:import:: java.util.logging ConsoleHandler

.. java:import:: java.util.logging Handler

.. java:import:: java.util.logging Level

.. java:import:: java.util.logging Logger

.. java:import:: java.util.stream Collectors

.. java:import:: java.util.stream Stream

MongoSchemaLoader.Parameters
============================

.. java:package:: tigase.mongodb
   :noindex:

.. java:type:: public static class Parameters implements SchemaLoader.Parameters
   :outertype: MongoSchemaLoader

Methods
-------
getAdminPassword
^^^^^^^^^^^^^^^^

.. java:method:: @Override public String getAdminPassword()
   :outertype: MongoSchemaLoader.Parameters

getAdmins
^^^^^^^^^

.. java:method:: @Override public List<BareJID> getAdmins()
   :outertype: MongoSchemaLoader.Parameters

getDbHostname
^^^^^^^^^^^^^

.. java:method:: public String getDbHostname()
   :outertype: MongoSchemaLoader.Parameters

getDbName
^^^^^^^^^

.. java:method:: public String getDbName()
   :outertype: MongoSchemaLoader.Parameters

getDbOptions
^^^^^^^^^^^^

.. java:method:: public String getDbOptions()
   :outertype: MongoSchemaLoader.Parameters

getDbPass
^^^^^^^^^

.. java:method:: public String getDbPass()
   :outertype: MongoSchemaLoader.Parameters

getDbRootPass
^^^^^^^^^^^^^

.. java:method:: public String getDbRootPass()
   :outertype: MongoSchemaLoader.Parameters

getDbRootUser
^^^^^^^^^^^^^

.. java:method:: public String getDbRootUser()
   :outertype: MongoSchemaLoader.Parameters

getDbUser
^^^^^^^^^

.. java:method:: public String getDbUser()
   :outertype: MongoSchemaLoader.Parameters

getLogLevel
^^^^^^^^^^^

.. java:method:: @Override public Level getLogLevel()
   :outertype: MongoSchemaLoader.Parameters

init
^^^^

.. java:method:: protected void init(Optional<SchemaManager.RootCredentialsCache> rootCredentialsCache)
   :outertype: MongoSchemaLoader.Parameters

isForceReloadSchema
^^^^^^^^^^^^^^^^^^^

.. java:method:: @Override public boolean isForceReloadSchema()
   :outertype: MongoSchemaLoader.Parameters

isUseSSL
^^^^^^^^

.. java:method:: public boolean isUseSSL()
   :outertype: MongoSchemaLoader.Parameters

parseUri
^^^^^^^^

.. java:method:: @Override public void parseUri(String uri)
   :outertype: MongoSchemaLoader.Parameters

setAdmins
^^^^^^^^^

.. java:method:: @Override public void setAdmins(List<BareJID> admins, String password)
   :outertype: MongoSchemaLoader.Parameters

setDbRootCredentials
^^^^^^^^^^^^^^^^^^^^

.. java:method:: @Override public void setDbRootCredentials(String username, String password)
   :outertype: MongoSchemaLoader.Parameters

setForceReloadSchema
^^^^^^^^^^^^^^^^^^^^

.. java:method:: @Override public void setForceReloadSchema(boolean forceReloadSchema)
   :outertype: MongoSchemaLoader.Parameters

setLogLevel
^^^^^^^^^^^

.. java:method:: @Override public void setLogLevel(Level level)
   :outertype: MongoSchemaLoader.Parameters

setProperties
^^^^^^^^^^^^^

.. java:method:: @Override public void setProperties(Properties props)
   :outertype: MongoSchemaLoader.Parameters

toString
^^^^^^^^

.. java:method:: @Override public String toString()
   :outertype: MongoSchemaLoader.Parameters

