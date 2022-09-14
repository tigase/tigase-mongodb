.. java:import:: com.mongodb.client MongoCollection

.. java:import:: com.mongodb.client MongoDatabase

.. java:import:: com.mongodb.client.model IndexOptions

.. java:import:: org.bson Document

.. java:import:: java.util Map

Helper
======

.. java:package:: tigase.mongodb
   :noindex:

.. java:type:: public class Helper

   Created by andrzej on 23.03.2016.

Methods
-------
collectionExists
^^^^^^^^^^^^^^^^

.. java:method:: public static boolean collectionExists(MongoDatabase db, String collection)
   :outertype: Helper

createIndexName
^^^^^^^^^^^^^^^

.. java:method:: public static String createIndexName(Document index)
   :outertype: Helper

indexCreateOrReplace
^^^^^^^^^^^^^^^^^^^^

.. java:method:: public static void indexCreateOrReplace(MongoCollection<Document> collection, Document index, IndexOptions options)
   :outertype: Helper

