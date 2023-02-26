/*
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

import com.mongodb.MongoNamespace;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Projections;
import com.mongodb.client.model.Updates;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.Binary;
import tigase.annotations.TigaseDeprecated;
import tigase.component.PacketWriter;
import tigase.component.exceptions.ComponentException;
import tigase.component.exceptions.RepositoryException;
import tigase.db.Repository;
import tigase.db.TigaseDBException;
import tigase.db.util.RepositoryVersionAware;
import tigase.db.util.SchemaLoader;
import tigase.kernel.beans.config.ConfigField;
import tigase.mongodb.MongoDataSource;
import tigase.mongodb.MongoRepositoryVersionAware;
import tigase.muc.Affiliation;
import tigase.muc.Room;
import tigase.muc.RoomConfig;
import tigase.muc.history.AbstractHistoryProvider;
import tigase.muc.history.ExtendedMAMRepository;
import tigase.muc.repository.Schema;
import tigase.server.Packet;
import tigase.util.Version;
import tigase.util.stringprep.TigaseStringprepException;
import tigase.xml.Element;
import tigase.xmpp.Authorization;
import tigase.xmpp.jid.BareJID;
import tigase.xmpp.jid.JID;
import tigase.xmpp.mam.Query;
import tigase.xmpp.mam.QueryImpl;

import java.nio.charset.Charset;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.logging.Level;

import static com.mongodb.client.model.Accumulators.first;
import static com.mongodb.client.model.Aggregates.group;
import static tigase.mongodb.Helper.collectionExists;

/**
 * @author andrzej
 */
@Repository.Meta(supportedUris = {"mongodb:.*"})
@Repository.SchemaId(id = Schema.MUC_SCHEMA_ID + "-history", name = "Tigase MUC Component (History)", external = false)
@RepositoryVersionAware.SchemaVersion
public class MongoHistoryProvider
		extends AbstractHistoryProvider<MongoDataSource>
		implements MongoRepositoryVersionAware, ExtendedMAMRepository {

	private static final int DEF_BATCH_SIZE = 100;
	private static final String HASH_ALG = "SHA-256";
	private static final String HISTORY_COLLECTION = "tig_muc_room_history";
	private static final String HISTORY_COLLECTION_OLD = "muc_history";
	private static final Charset UTF8 = Charset.forName("UTF-8");
	protected MongoCollection<Document> historyCollection;
	@ConfigField(desc = "Batch size", alias = "batch-size")
	private int batchSize = DEF_BATCH_SIZE;
	private MongoDatabase db;

	@Override
	public void addJoinEvent(Room room, Date date, JID senderJID, String nickName) {
	}

	@Override
	public void addLeaveEvent(Room room, Date date, JID senderJID, String nickName) {
	}

	@TigaseDeprecated(removeIn = "3.0.0", note = "Use method with `stableId`", since = "2.4.0")
	@Deprecated
	@Override
	public void addMessage(Room room, Element message, String body, JID senderJid, String senderNickname, Date time) {
		addMessage(room, message, body, senderJid, senderNickname, time, UUID.randomUUID().toString());
	}
	
	@Override
	public void addMessage(Room room, Element message, String body, JID senderJid, String senderNickname, Date time, String stableId) {
		try {
			byte[] rid = generateId(room.getRoomJID());
			Document dto = new Document("room_jid_id", rid).append("room_jid", room.getRoomJID().toString())
					.append("event_type", 1)
					.append("sender_jid", senderJid.toString())
					.append("sender_nickname", senderNickname)
					.append("stable_id", UUID.fromString(stableId))
					.append("body", body)
					.append("public_event", room.getConfig().isLoggingEnabled());
			if (time != null) {
				dto.append("timestamp", time);
			}
			if (message != null) {
				dto.append("msg", message.toString());
			}
			historyCollection.insertOne(dto);
		} catch (Exception ex) {
			log.log(Level.WARNING, "Can't add MUC message to database", ex);
			throw new RuntimeException(ex);
		}
	}

	@Override
	public void addSubjectChange(Room room, Element message, String subject, JID senderJid, String senderNickname,
	                             Date time) {
	}

	@Override
	public Item getItem(BareJID owner, String stableId) throws RepositoryException {
		try {
			byte[] rid = generateId(owner);
			Document result = historyCollection.find(
							Filters.and(Filters.eq("room_jid_id", rid), Filters.eq("stable_id", UUID.fromString(stableId))))
					.projection(Projections.include("msg", "timestamp"))
					.first();
			if (result == null) {
				return null;
			}

			String msgStr = result.getString("msg");
			Element msg = parseMessage(msgStr);
			Date timestamp = result.getDate("timestamp");

			return new Item() {
				@Override
				public String getId() {
					return stableId;
				}

				@Override
				public Element getMessage() {
					return msg;
				}

				@Override
				public Date getTimestamp() {
					return timestamp;
				}
			};
		} catch (Exception ex) {
			log.log(Level.WARNING, "Can't retrieve MUC message to database", ex);
			throw new RuntimeException(ex);
		}
	}

	@Override
	public void updateMessage(BareJID owner, String stableId, Element msg, String body) throws RepositoryException {
		try {
			byte[] rid = generateId(owner);
			historyCollection.updateOne(
							Filters.and(Filters.eq("room_jid_id", rid), Filters.eq("stable_id", UUID.fromString(stableId))),
							Updates.combine(Updates.set("body", body), Updates.set("msg", msg.toString())));
		} catch (Exception ex) {
			log.log(Level.WARNING, "Can't update MUC message in database", ex);
			throw new RuntimeException(ex);
		}
	}

	protected byte[] calculateHash(String user) throws TigaseDBException {
		try {
			MessageDigest md = MessageDigest.getInstance(HASH_ALG);
			return md.digest(user.getBytes(UTF8));
		} catch (NoSuchAlgorithmException ex) {
			throw new TigaseDBException("Should not happen!!", ex);
		}
	}

	private Packet createMessage(BareJID roomJid, JID senderJID, Document dto, boolean addRealJids)
			throws TigaseStringprepException {
		String sender_nickname = (String) dto.get("sender_nickname");
		String msg = (String) dto.get("msg");
		String body = (String) dto.get("body");
		String sender_jid = (String) dto.get("sender_jid");
		Date timestamp = (Date) dto.get("timestamp");
		UUID stableId = (UUID) dto.get("stable_id");

		return createMessage(roomJid, senderJID, sender_nickname, msg, body, sender_jid, addRealJids, timestamp, stableId.toString());
	}

	@Override
	public void destroy() {
	}

	protected byte[] generateId(BareJID user) throws TigaseDBException {
		return calculateHash(user.toString().toLowerCase());
	}

	@Override
	public void getHistoryMessages(Room room, JID senderJID, Integer maxchars, Integer maxstanzas, Integer seconds,
	                               Date since, PacketWriter writer) {
		Affiliation recipientAffiliation = room.getAffiliation(senderJID.getBareJID()).getAffiliation();
		boolean addRealJids = room.getConfig().getRoomAnonymity() == RoomConfig.Anonymity.nonanonymous ||
				room.getConfig().getRoomAnonymity() == RoomConfig.Anonymity.semianonymous &&
						(recipientAffiliation == Affiliation.owner || recipientAffiliation == Affiliation.admin);

		try {
			if (maxchars != null && maxchars == 0) {
				return;
			}
			byte[] rid = generateId(room.getRoomJID());
			int maxMessages = room.getConfig().getMaxHistory();
			int limit = maxstanzas != null ? Math.min(maxMessages, maxstanzas) : maxMessages;
			if (since == null && seconds != null && maxstanzas == null) {
				since = new Date(new Date().getTime() - seconds * 1000);
			}

			Document crit = new Document("room_jid_id", rid);
			if (since != null) {
				crit.append("timestamp", new Document("$gte", since));
				Document order = new Document("timestamp", 1);
				FindIterable<Document> cursor = historyCollection.find(crit)
						.batchSize(batchSize)
						.limit(limit)
						.sort(order);
				for (Document dto : cursor) {
					Packet packet = createMessage(room.getRoomJID(), senderJID, dto, addRealJids);
					writer.write(packet);
				}
			} else {
				Document order = new Document("timestamp", -1);
				FindIterable<Document> cursor = historyCollection.find(crit)
						.batchSize(batchSize)
						.limit(limit)
						.sort(order);
				List<Packet> results = new ArrayList<Packet>();
				for (Document dto : cursor) {
					Packet packet = createMessage(room.getRoomJID(), senderJID, dto, addRealJids);
					results.add(packet);
				}
				Collections.reverse(results);
				writer.write(results);
			}
		} catch (Exception ex) {
			if (log.isLoggable(Level.SEVERE)) {
				log.log(Level.SEVERE, "Can't get history", ex);
			}
			throw new RuntimeException(ex);
		}
	}

	private Long getItemPosition(String msgId, Bson filter) throws ComponentException {
		if (msgId == null) {
			return null;
		}
		try {
			Document dto = historyCollection.find(Filters.and(filter, Filters.eq("stable_id", UUID.fromString(msgId))))
					.projection(Projections.include("timestamp"))
					.first();
			if (dto == null) {
				return null;
			}

			return historyCollection.countDocuments(Filters.and(filter, Filters.lt("timestamp", dto.getDate("timestamp"))));
		} catch (NumberFormatException ex) {
			throw new ComponentException(Authorization.ITEM_NOT_FOUND, "Not found message with id = " + msgId);
		}
	}

	@Override
	public boolean isPersistent(Room room) {
		return true;
	}

	@Override
	public Query newQuery() {
		return new QueryImpl();
	}

	@Override
	public void queryItems(Query query, ItemHandler itemHandler) throws TigaseDBException, ComponentException {
		try {
			byte[] rid = generateId(query.getComponentJID().getBareJID());

			List<Bson> filters = new ArrayList<>();
			filters.add(Filters.eq("room_jid_id", rid));

			if (query.getStart() != null) {
				filters.add(Filters.gte("timestamp", query.getStart()));
			}
			if (query.getEnd() != null) {
				filters.add(Filters.lte("timestamp", query.getEnd()));
			}
			if (query.getWith() != null) {
				filters.add(Filters.eq("sender_nickname", query.getWith().toString()));
			}

			Bson filter = Filters.and(filters);
			long count = historyCollection.countDocuments(filter);

			Long after = getItemPosition(query.getRsm().getAfter(), filter);
			Long before = getItemPosition(query.getRsm().getBefore(), filter);

			AbstractHistoryProvider.calculateOffsetAndPosition(query, (int) count,
			                                                   before == null ? null : before.intValue(),
			                                                   after == null ? null : after.intValue());

			Document order = new Document("timestamp", 1);
			FindIterable<Document> cursor = historyCollection.find(filter)
					.sort(order)
					.skip(query.getRsm().getIndex())
					.limit(query.getRsm().getMax());
			for (Document dto : cursor) {
				String sender_nickname = (String) dto.get("sender_nickname");
				String msg = (String) dto.get("msg");
				String body = (String) dto.get("body");
				Date timestamp = (Date) dto.get("timestamp");
				UUID stableId = (UUID) dto.get("stable_id");

				Element msgEl = createMessageElement(query.getComponentJID().getBareJID(), query.getQuestionerJID(),
				                                     sender_nickname, msg, body, stableId.toString());
				Item item = new Item() {
					@Override
					public String getId() {
						return stableId.toString();
					}

					@Override
					public Element getMessage() {
						return msgEl;
					}

					@Override
					public Date getTimestamp() {
						return timestamp;
					}
				};
				itemHandler.itemFound(query, item);
			}
		} catch (Exception ex) {
			if (log.isLoggable(Level.SEVERE)) {
				log.log(Level.SEVERE, "Can't get history", ex);
			}
			throw new RuntimeException(ex);
		}

	}

	@Override
	public void removeHistory(Room room) {
		try {
			byte[] rid = generateId(room.getRoomJID());
			Document crit = new Document("room_jid_id", rid);
			db.getCollection(HISTORY_COLLECTION).deleteMany(crit);
		} catch (Exception ex) {
			if (log.isLoggable(Level.SEVERE)) {
				log.log(Level.SEVERE, "Can't remove history", ex);
			}
			throw new RuntimeException(ex);
		}
	}

	@Override
	public void setDataSource(MongoDataSource dataSource) {
		db = dataSource.getDatabase();

		if (!collectionExists(db, HISTORY_COLLECTION)) {
			if (collectionExists(db, HISTORY_COLLECTION_OLD)) {
				db.getCollection(HISTORY_COLLECTION_OLD)
						.renameCollection(new MongoNamespace(db.getName(), HISTORY_COLLECTION));
			} else {
				db.createCollection(HISTORY_COLLECTION);
			}
		}
		historyCollection = db.getCollection(HISTORY_COLLECTION);

		historyCollection.createIndex(new Document("room_jid_id", 1));
		historyCollection.createIndex(new Document("room_jid_id", 1).append("timestamp", 1));
		historyCollection.createIndex(new Document("room_jid_id", 1).append("stable_id", 1));
	}

	@Override
	public SchemaLoader.Result updateSchema(Optional<Version> oldVersion, Version newVersion) throws TigaseDBException {
		List<Bson> aggregationQuery = Arrays.asList(group("$room_jid_id", first("room_jid", "$room_jid")));
		for (Document doc : historyCollection.aggregate(aggregationQuery).batchSize(100)) {
			String roomJid = (String) doc.get("room_jid");

			byte[] oldRoomJidId = ((Binary) doc.get("_id")).getData();
			byte[] newRoomJidId = calculateHash(roomJid.toString().toLowerCase());

			if (Arrays.equals(oldRoomJidId, newRoomJidId)) {
				continue;
			}

			historyCollection.updateMany(new Document("room_jid_id", oldRoomJidId),
			                             new Document("$set", new Document("room_jid_id", newRoomJidId)));
		}
		for (Document doc : historyCollection.find(Filters.exists("stable_id", false))
				.projection(Projections.include("_id"))
				.batchSize(100)) {
			historyCollection.updateOne(Filters.eq("_id", doc.getObjectId("_id")), Updates.set("stable_id", UUID.randomUUID()));
		}
		return SchemaLoader.Result.ok;
	}
}
