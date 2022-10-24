.. java:import:: com.mongodb MongoWriteException

.. java:import:: com.mongodb.client MongoCollection

.. java:import:: com.mongodb.client MongoDatabase

.. java:import:: com.mongodb.client.model Filters

.. java:import:: com.mongodb.client.model Projections

.. java:import:: com.mongodb.client.model Sorts

.. java:import:: com.mongodb.client.model Updates

.. java:import:: org.bson Document

.. java:import:: tigase.db Repository

.. java:import:: tigase.db TigaseDBException

.. java:import:: tigase.http.db Schema

.. java:import:: tigase.http.upload.db FileUploadRepository

.. java:import:: tigase.mongodb MongoDataSource

.. java:import:: tigase.xmpp.jid BareJID

.. java:import:: tigase.xmpp.jid JID

.. java:import:: java.time LocalDateTime

.. java:import:: java.time ZoneOffset

.. java:import:: java.util ArrayList

.. java:import:: java.util Date

.. java:import:: java.util List

MongoFileUploadRepository
=========================

.. java:package:: tigase.mongodb.http
   :noindex:

.. java:type:: @Repository.Meta @Repository.SchemaId public class MongoFileUploadRepository implements FileUploadRepository<MongoDataSource>

   Created by andrzej on 14.03.2017.

Methods
-------
allocateSlot
^^^^^^^^^^^^

.. java:method:: @Override public Slot allocateSlot(JID sender, String slotId, String filename, long filesize, String contentType) throws TigaseDBException
   :outertype: MongoFileUploadRepository

getSlot
^^^^^^^

.. java:method:: @Override public Slot getSlot(BareJID sender, String slotId) throws TigaseDBException
   :outertype: MongoFileUploadRepository

listExpiredSlots
^^^^^^^^^^^^^^^^

.. java:method:: @Override public List<Slot> listExpiredSlots(BareJID domain, LocalDateTime before, int limit) throws TigaseDBException
   :outertype: MongoFileUploadRepository

removeExpiredSlots
^^^^^^^^^^^^^^^^^^

.. java:method:: @Override public void removeExpiredSlots(BareJID domain, LocalDateTime before, int limit) throws TigaseDBException
   :outertype: MongoFileUploadRepository

setDataSource
^^^^^^^^^^^^^

.. java:method:: @Override public void setDataSource(MongoDataSource dataSource)
   :outertype: MongoFileUploadRepository

updateSlot
^^^^^^^^^^

.. java:method:: @Override public void updateSlot(BareJID userJid, String slotId) throws TigaseDBException
   :outertype: MongoFileUploadRepository

