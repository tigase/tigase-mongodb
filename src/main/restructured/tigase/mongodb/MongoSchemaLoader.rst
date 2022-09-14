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

MongoSchemaLoader
=================

.. java:package:: tigase.mongodb
   :noindex:

.. java:type:: public class MongoSchemaLoader extends SchemaLoader<MongoSchemaLoader.Parameters>

   Created by andrzej on 05.05.2017.

Fields
------
SCHEMA_VERSION
^^^^^^^^^^^^^^

.. java:field:: protected static final String SCHEMA_VERSION
   :outertype: MongoSchemaLoader

Methods
-------
addXmppAdminAccount
^^^^^^^^^^^^^^^^^^^

.. java:method:: @Override public Result addXmppAdminAccount(SchemaManager.SchemaInfo schemaInfo)
   :outertype: MongoSchemaLoader

createParameters
^^^^^^^^^^^^^^^^

.. java:method:: @Override public Parameters createParameters()
   :outertype: MongoSchemaLoader

destroyDataSource
^^^^^^^^^^^^^^^^^

.. java:method:: public Result destroyDataSource()
   :outertype: MongoSchemaLoader

execute
^^^^^^^

.. java:method:: @Override public void execute(SchemaLoader.Parameters params)
   :outertype: MongoSchemaLoader

getCommandlineParameters
^^^^^^^^^^^^^^^^^^^^^^^^

.. java:method:: @Override public List<CommandlineParameter> getCommandlineParameters()
   :outertype: MongoSchemaLoader

getComponentVersionFromDb
^^^^^^^^^^^^^^^^^^^^^^^^^

.. java:method:: @Override public Optional<Version> getComponentVersionFromDb(String component)
   :outertype: MongoSchemaLoader

getDBUri
^^^^^^^^

.. java:method:: @Override public String getDBUri()
   :outertype: MongoSchemaLoader

getMinimalRequiredComponentVersionForUpgrade
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. java:method:: @Override public Optional<Version> getMinimalRequiredComponentVersionForUpgrade(SchemaManager.SchemaInfo schemaInfo)
   :outertype: MongoSchemaLoader

getSetupOptions
^^^^^^^^^^^^^^^

.. java:method:: public List<CommandlineParameter> getSetupOptions()
   :outertype: MongoSchemaLoader

getSupportedTypes
^^^^^^^^^^^^^^^^^

.. java:method:: @Override public List<TypeInfo> getSupportedTypes()
   :outertype: MongoSchemaLoader

init
^^^^

.. java:method:: @Override public void init(Parameters params, Optional<SchemaManager.RootCredentialsCache> rootCredentialsCache)
   :outertype: MongoSchemaLoader

loadSchema
^^^^^^^^^^

.. java:method:: @Override public Result loadSchema(SchemaManager.SchemaInfo schema, String version)
   :outertype: MongoSchemaLoader

loadSchemaFile
^^^^^^^^^^^^^^

.. java:method:: @Override public Result loadSchemaFile(String fileName)
   :outertype: MongoSchemaLoader

postInstallation
^^^^^^^^^^^^^^^^

.. java:method:: @Override public Result postInstallation()
   :outertype: MongoSchemaLoader

printInfo
^^^^^^^^^

.. java:method:: @Override public Result printInfo()
   :outertype: MongoSchemaLoader

setComponentVersion
^^^^^^^^^^^^^^^^^^^

.. java:method:: @Override public Result setComponentVersion(String component, String version)
   :outertype: MongoSchemaLoader

shutdown
^^^^^^^^

.. java:method:: @Override public Result shutdown()
   :outertype: MongoSchemaLoader

validateDBConnection
^^^^^^^^^^^^^^^^^^^^

.. java:method:: @Override public Result validateDBConnection()
   :outertype: MongoSchemaLoader

validateDBExists
^^^^^^^^^^^^^^^^

.. java:method:: @Override public Result validateDBExists()
   :outertype: MongoSchemaLoader

