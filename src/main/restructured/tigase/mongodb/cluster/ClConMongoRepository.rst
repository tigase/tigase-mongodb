.. java:import:: com.mongodb.client FindIterable

.. java:import:: com.mongodb.client MongoCollection

.. java:import:: com.mongodb.client MongoDatabase

.. java:import:: com.mongodb.client.model UpdateOptions

.. java:import:: org.bson Document

.. java:import:: tigase.cluster.repo ClConConfigRepository

.. java:import:: tigase.cluster.repo ClusterRepoConstants

.. java:import:: tigase.cluster.repo ClusterRepoItem

.. java:import:: tigase.db DBInitException

.. java:import:: tigase.db Repository

.. java:import:: tigase.db Schema

.. java:import:: tigase.db.comp ComponentRepositoryDataSourceAware

.. java:import:: tigase.kernel.beans.config ConfigField

.. java:import:: tigase.mongodb MongoDataSource

.. java:import:: java.util Date

.. java:import:: java.util Map

.. java:import:: java.util.logging Level

.. java:import:: java.util.logging Logger

ClConMongoRepository
====================

.. java:package:: tigase.mongodb.cluster
   :noindex:

.. java:type:: @Repository.Meta @Repository.SchemaId public class ClConMongoRepository extends ClConConfigRepository implements ClusterRepoConstants, ComponentRepositoryDataSourceAware<ClusterRepoItem, MongoDataSource>

Methods
-------
destroy
^^^^^^^

.. java:method:: @Override public void destroy()
   :outertype: ClConMongoRepository

getItemInstance
^^^^^^^^^^^^^^^

.. java:method:: @Override public ClusterRepoItem getItemInstance()
   :outertype: ClConMongoRepository

initRepository
^^^^^^^^^^^^^^

.. java:method:: @Override @Deprecated public void initRepository(String resource_uri, Map<String, String> params) throws DBInitException
   :outertype: ClConMongoRepository

reload
^^^^^^

.. java:method:: @Override public void reload()
   :outertype: ClConMongoRepository

removeItem
^^^^^^^^^^

.. java:method:: @Override public void removeItem(String key)
   :outertype: ClConMongoRepository

setDataSource
^^^^^^^^^^^^^

.. java:method:: @Override public void setDataSource(MongoDataSource dataSource)
   :outertype: ClConMongoRepository

store
^^^^^

.. java:method:: @Override public void store()
   :outertype: ClConMongoRepository

storeItem
^^^^^^^^^

.. java:method:: @Override public void storeItem(tigase.cluster.repo.ClusterRepoItem item)
   :outertype: ClConMongoRepository

