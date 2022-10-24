.. java:import:: com.mongodb Block

.. java:import:: com.mongodb.client MongoCollection

.. java:import:: com.mongodb.client MongoDatabase

.. java:import:: com.mongodb.client.model IndexOptions

.. java:import:: com.mongodb.client.model Projections

.. java:import:: com.mongodb.client.model UpdateOptions

.. java:import:: com.mongodb.client.model Updates

.. java:import:: org.bson Document

.. java:import:: org.bson.conversions Bson

.. java:import:: tigase.component.exceptions RepositoryException

.. java:import:: tigase.db Repository

.. java:import:: tigase.db TigaseDBException

.. java:import:: tigase.kernel.beans Inject

.. java:import:: tigase.mongodb MongoDataSource

.. java:import:: tigase.muc.repository AbstractMucDAO

.. java:import:: tigase.muc.repository Schema

.. java:import:: tigase.util.stringprep TigaseStringprepException

.. java:import:: tigase.xmpp.jid BareJID

.. java:import:: java.nio.charset Charset

.. java:import:: java.security MessageDigest

.. java:import:: java.security NoSuchAlgorithmException

.. java:import:: java.util ArrayList

.. java:import:: java.util Date

.. java:import:: java.util HashMap

.. java:import:: java.util Map

.. java:import:: java.util.function Consumer

.. java:import:: java.util.logging Logger

MongoMucDAO
===========

.. java:package:: tigase.mongodb.muc
   :noindex:

.. java:type:: @Repository.Meta @Repository.SchemaId public class MongoMucDAO extends AbstractMucDAO<MongoDataSource, byte[]>

   Created by andrzej on 20.10.2016.

Fields
------
roomAffilaitionsCollection
^^^^^^^^^^^^^^^^^^^^^^^^^^

.. java:field:: protected MongoCollection<Document> roomAffilaitionsCollection
   :outertype: MongoMucDAO

roomsCollection
^^^^^^^^^^^^^^^

.. java:field:: protected MongoCollection<Document> roomsCollection
   :outertype: MongoMucDAO

Methods
-------
calculateHash
^^^^^^^^^^^^^

.. java:method:: protected byte[] calculateHash(String user) throws TigaseDBException
   :outertype: MongoMucDAO

createRoom
^^^^^^^^^^

.. java:method:: @Override public byte[] createRoom(RoomWithId<byte[]> room) throws RepositoryException
   :outertype: MongoMucDAO

destroyRoom
^^^^^^^^^^^

.. java:method:: @Override public void destroyRoom(BareJID roomJID) throws RepositoryException
   :outertype: MongoMucDAO

generateId
^^^^^^^^^^

.. java:method:: protected byte[] generateId(BareJID user) throws TigaseDBException
   :outertype: MongoMucDAO

getAffiliations
^^^^^^^^^^^^^^^

.. java:method:: @Override public Map<BareJID, RoomAffiliation> getAffiliations(RoomWithId<byte[]> room) throws RepositoryException
   :outertype: MongoMucDAO

getRoom
^^^^^^^

.. java:method:: @Override public RoomWithId<byte[]> getRoom(BareJID roomJID) throws RepositoryException
   :outertype: MongoMucDAO

getRoomAvatar
^^^^^^^^^^^^^

.. java:method:: @Override public String getRoomAvatar(RoomWithId<byte[]> room) throws RepositoryException
   :outertype: MongoMucDAO

getRoomsJIDList
^^^^^^^^^^^^^^^

.. java:method:: @Override public ArrayList<BareJID> getRoomsJIDList() throws RepositoryException
   :outertype: MongoMucDAO

setAffiliation
^^^^^^^^^^^^^^

.. java:method:: @Override public void setAffiliation(RoomWithId<byte[]> room, BareJID jid, RoomAffiliation affiliation) throws RepositoryException
   :outertype: MongoMucDAO

setDataSource
^^^^^^^^^^^^^

.. java:method:: @Override public void setDataSource(MongoDataSource dataSource)
   :outertype: MongoMucDAO

setSubject
^^^^^^^^^^

.. java:method:: @Override public void setSubject(RoomWithId<byte[]> room, String subject, String creatorNickname, Date changeDate) throws RepositoryException
   :outertype: MongoMucDAO

updateRoomAvatar
^^^^^^^^^^^^^^^^

.. java:method:: @Override public void updateRoomAvatar(RoomWithId<byte[]> room, String encodedAvatar, String hash) throws RepositoryException
   :outertype: MongoMucDAO

updateRoomConfig
^^^^^^^^^^^^^^^^

.. java:method:: @Override public void updateRoomConfig(RoomConfig roomConfig) throws RepositoryException
   :outertype: MongoMucDAO

