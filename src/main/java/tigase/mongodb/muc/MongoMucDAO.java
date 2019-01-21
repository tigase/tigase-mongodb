/**
 * Tigase MongoDB - Tigase MongoDB support library
 * Copyright (C) 2014 Tigase, Inc. (office@tigase.com)
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, version 3 of the License.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. Look for COPYING file in the top folder.
 * If not, see http://www.gnu.org/licenses/.
 */
package tigase.mongodb.muc;

import com.mongodb.Block;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.Updates;
import org.bson.Document;
import org.bson.conversions.Bson;
import tigase.component.exceptions.RepositoryException;
import tigase.db.Repository;
import tigase.db.TigaseDBException;
import tigase.kernel.beans.Inject;
import tigase.mongodb.MongoDataSource;
import tigase.muc.Affiliation;
import tigase.muc.Room;
import tigase.muc.RoomConfig;
import tigase.muc.RoomWithId;
import tigase.muc.repository.AbstractMucDAO;
import tigase.muc.repository.Schema;
import tigase.util.stringprep.TigaseStringprepException;
import tigase.xmpp.jid.BareJID;

import java.nio.charset.Charset;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;
import java.util.logging.Logger;

import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Projections.include;
import static tigase.mongodb.Helper.collectionExists;

/**
 * Created by andrzej on 20.10.2016.
 */
@Repository.Meta(supportedUris = {"mongodb:.*"})
@Repository.SchemaId(id = Schema.MUC_SCHEMA_ID, name = Schema.MUC_SCHEMA_NAME, external = false)
public class MongoMucDAO
		extends AbstractMucDAO<MongoDataSource, byte[]> {

	private static final Logger log = Logger.getLogger(MongoMucDAO.class.getCanonicalName());

	private static final int DEF_BATCH_SIZE = 100;
	private static final String HASH_ALG = "SHA-256";
	private static final String ROOMS_COLLECTION = "tig_muc_rooms";
	private static final String ROOM_AFFILIATIONS_COLLECTION = "tig_muc_room_affiliations";
	private static final Charset UTF8 = Charset.forName("UTF-8");
	protected MongoCollection<Document> roomAffilaitionsCollection;
	protected MongoCollection<Document> roomsCollection;
	private MongoDatabase db;
	@Inject
	private Room.RoomFactory roomFactory;

	protected byte[] calculateHash(String user) throws TigaseDBException {
		try {
			MessageDigest md = MessageDigest.getInstance(HASH_ALG);
			return md.digest(user.getBytes(UTF8));
		} catch (NoSuchAlgorithmException ex) {
			throw new TigaseDBException("Should not happen!!", ex);
		}
	}

	@Override
	public byte[] createRoom(RoomWithId<byte[]> room) throws RepositoryException {
		try {
			byte[] roomId = generateId(room.getRoomJID());
			String roomName = room.getConfig().getRoomName();

			Document roomDoc = new Document("_id", roomId).append("jid", room.getRoomJID().toString())
					.append("name", (roomName != null && !roomName.isEmpty()) ? roomName : null)
					.append("config", room.getConfig().getAsElement().toString())
					.append("creator", room.getCreatorJid().toString())
					.append("creation_date", room.getCreationDate());

			roomsCollection.insertOne(roomDoc);
			room.setId(roomId);

			for (BareJID affJid : room.getAffiliations()) {
				final Affiliation a = room.getAffiliation(affJid);
				setAffiliation(room, affJid, a);
			}
			return roomId;
		} catch (Exception ex) {
			throw new RepositoryException("Error while saving room " + room.getRoomJID() + " to database", ex);
		}
	}

	@Override
	public void destroyRoom(BareJID roomJID) throws RepositoryException {
		try {
			byte[] roomId = generateId(roomJID);
			roomAffilaitionsCollection.deleteMany(new Document("room_id", roomId));
			roomsCollection.deleteOne(new Document("_id", roomId));
		} catch (Exception ex) {
			throw new RepositoryException("Error while removing room " + roomJID + " from database", ex);
		}

	}

	protected byte[] generateId(BareJID user) throws TigaseDBException {
		return calculateHash(user.toString().toLowerCase());
	}

	@Override
	public Map<BareJID, Affiliation> getAffiliations(RoomWithId<byte[]> room) throws RepositoryException {
		Map<BareJID, Affiliation> affiliations = new HashMap<>();

		try {
			byte[] roomId = room.getId();
			roomAffilaitionsCollection.find(eq("room_id", roomId))
					.projection(include("jid", "affiliation"))
					.forEach((Block<? super Document>) (Document doc) -> {
						try {
							BareJID jid = BareJID.bareJIDInstance(doc.getString("jid"));
							Affiliation affiliation = Affiliation.valueOf(doc.getString("affiliation"));
							affiliations.put(jid, affiliation);
						} catch (TigaseStringprepException ex) {
							throw new RuntimeException(ex);
						}
					});
		} catch (Exception ex) {
			throw new RepositoryException(
					"Error while reading room " + room.getRoomJID() + " affiliations from database", ex);
		}

		return affiliations;
	}

	@Override
	public RoomWithId<byte[]> getRoom(BareJID roomJID) throws RepositoryException {
		try {
			byte[] roomId = generateId(roomJID);

			Document roomDoc = roomsCollection.find(eq("_id", roomId)).first();

			if (roomDoc == null) {
				return null;
			}

			Date date = roomDoc.getDate("creation_date");
			BareJID creator = BareJID.bareJIDInstance(roomDoc.getString("creator"));
			RoomConfig roomConfig = new RoomConfig(roomJID);
			roomConfig.readFromElement(parseConfigElement(roomDoc.getString("config")));

			RoomWithId room = roomFactory.newInstance(roomId, roomConfig, date, creator);

			String subject = roomDoc.getString("subject");
			String subjectCreator = roomDoc.getString("subject_creator_nick");

			room.setNewSubject(subject, subjectCreator);
			Date subjectDate = roomDoc.getDate("subject_date");
			room.setSubjectChangeDate(subjectDate);

			return room;
		} catch (Exception ex) {
			throw new RepositoryException("Error while reading room " + roomJID + " from database", ex);
		}
	}

	@Override
	public ArrayList<BareJID> getRoomsJIDList() throws RepositoryException {
		ArrayList<BareJID> jids = new ArrayList<>();
		try {
			roomsCollection.find().projection(include("jid")).map(doc -> {
				try {
					return BareJID.bareJIDInstance(doc.getString("jid"));
				} catch (TigaseStringprepException ex) {
					throw new RuntimeException(ex);
				}
			}).forEach((Consumer<? super BareJID>) jids::add);
		} catch (Exception ex) {
			throw new RepositoryException("Error while reading list of rooms jids from database", ex);
		}
		return jids;
	}

	@Override
	public void setAffiliation(RoomWithId<byte[]> room, BareJID jid, Affiliation affiliation)
			throws RepositoryException {
		try {
			byte[] jidId = generateId(jid);
			Bson crit = and(eq("room_id", room.getId()), eq("jid_id", jidId));
			if (affiliation == Affiliation.none) {
				roomAffilaitionsCollection.deleteOne(crit);
			} else {
				Bson update = Updates.combine(Updates.setOnInsert("jid", jid.toString()),
				                              Updates.set("affiliation", affiliation.name()));
				roomAffilaitionsCollection.updateOne(crit, update, new UpdateOptions().upsert(true));
			}
		} catch (Exception ex) {
			throw new RepositoryException(
					"Error while setting affiliation for room " + room.getRoomJID() + " for jid " + jid + " to " +
							affiliation.name(), ex);
		}
	}

	@Override
	public void setDataSource(MongoDataSource dataSource) {
		db = dataSource.getDatabase();

		if (!collectionExists(db, ROOMS_COLLECTION)) {
			db.createCollection(ROOMS_COLLECTION);
		}
		roomsCollection = db.getCollection(ROOMS_COLLECTION);
		roomsCollection.createIndex(new Document("jid", 1), new IndexOptions().unique(true));

		if (!collectionExists(db, ROOM_AFFILIATIONS_COLLECTION)) {
			db.createCollection(ROOM_AFFILIATIONS_COLLECTION);
		}
		roomAffilaitionsCollection = db.getCollection(ROOM_AFFILIATIONS_COLLECTION);
		roomAffilaitionsCollection.createIndex(new Document("room_id", 1));
		roomAffilaitionsCollection.createIndex(new Document("room_id", 1).append("jid_id", 1),
		                                       new IndexOptions().unique(true));
	}

	@Override
	public void setSubject(RoomWithId<byte[]> room, String subject, String creatorNickname, Date changeDate)
			throws RepositoryException {
		try {
			Bson crit = eq("_id", room.getId());
			Bson update = new Document("subject", subject).append("subject_creator_nick", creatorNickname)
					.append("subject_date", changeDate);
			roomsCollection.updateOne(crit, new Document("$set", update));
		} catch (Exception ex) {
			throw new RepositoryException(
					"Error while setting subject for room " + room.getRoomJID() + " to " + subject + " by " +
							creatorNickname, ex);
		}
	}

	@Override
	public void updateRoomConfig(RoomConfig roomConfig) throws RepositoryException {
		try {
			String roomName = roomConfig.getRoomName();
			Bson crit = eq("_id", generateId(roomConfig.getRoomJID()));
			Bson update = Updates.combine(Updates.set("config", roomConfig.getAsElement().toString()),
			                              Updates.set("name",
			                                          (roomName != null && !roomName.isEmpty()) ? roomName : null));

			roomsCollection.updateOne(crit, update);
		} catch (Exception ex) {
			throw new RepositoryException("Error updating configuration of room " + roomConfig.getRoomJID(), ex);
		}
	}
}
