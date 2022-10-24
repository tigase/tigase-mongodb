.. java:import:: com.mongodb MongoClient

.. java:import:: com.mongodb MongoClientURI

.. java:import:: com.mongodb MongoException

.. java:import:: com.mongodb.client MongoDatabase

.. java:import:: org.bson Document

.. java:import:: tigase.db DBInitException

.. java:import:: tigase.db DataSource

.. java:import:: tigase.db Repository

.. java:import:: tigase.kernel.beans UnregisterAware

.. java:import:: tigase.kernel.beans.config ConfigField

.. java:import:: tigase.util Version

.. java:import:: java.util Map

.. java:import:: java.util Optional

.. java:import:: java.util.logging Level

.. java:import:: java.util.logging Logger

MongoDataSource
===============

.. java:package:: tigase.mongodb
   :noindex:

.. java:type:: @Repository.Meta public class MongoDataSource implements DataSource, UnregisterAware

   Created by andrzej on 04.10.2016.

Methods
-------
automaticSchemaManagement
^^^^^^^^^^^^^^^^^^^^^^^^^

.. java:method:: @Override public boolean automaticSchemaManagement()
   :outertype: MongoDataSource

beforeUnregister
^^^^^^^^^^^^^^^^

.. java:method:: @Override public void beforeUnregister()
   :outertype: MongoDataSource

getDatabase
^^^^^^^^^^^

.. java:method:: public MongoDatabase getDatabase()
   :outertype: MongoDataSource

getResourceUri
^^^^^^^^^^^^^^

.. java:method:: @Override public String getResourceUri()
   :outertype: MongoDataSource

getSchemaVersion
^^^^^^^^^^^^^^^^

.. java:method:: @Override public Optional<Version> getSchemaVersion(String component)
   :outertype: MongoDataSource

initRepository
^^^^^^^^^^^^^^

.. java:method:: @Override @Deprecated public void initRepository(String resource_uri, Map<String, String> params) throws DBInitException
   :outertype: MongoDataSource

initialize
^^^^^^^^^^

.. java:method:: @Override public void initialize(String resource_uri) throws DBInitException
   :outertype: MongoDataSource

