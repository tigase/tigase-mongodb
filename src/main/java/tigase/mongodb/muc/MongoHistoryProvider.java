/*
 * MongoHistoryProvider.java
 *
 * Tigase Jabber/XMPP Server - MongoDB support
 * Copyright (C) 2004-2016 "Tigase, Inc." <office@tigase.com>
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License,
 * or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. Look for COPYING file in the top folder.
 * If not, see http://www.gnu.org/licenses/.
 *
 */
package tigase.mongodb.muc;

import com.mongodb.*;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import tigase.component.PacketWriter;
import tigase.db.DBInitException;
import tigase.db.Repository;
import tigase.db.TigaseDBException;
import tigase.muc.Affiliation;
import tigase.muc.Room;
import tigase.muc.RoomConfig;
import tigase.muc.history.AbstractHistoryProvider;
import tigase.server.Packet;
import tigase.util.TigaseStringprepException;
import tigase.xml.Element;
import tigase.xmpp.BareJID;
import tigase.xmpp.JID;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.logging.Level;

import static tigase.mongodb.Helper.collectionExists;


/**
 *
 * @author andrzej
 */
@Repository.Meta( supportedUris = { "mongodb:.*" } )
public class MongoHistoryProvider extends AbstractHistoryProvider {

	private static final int DEF_BATCH_SIZE = 100;
	private static final String HASH_ALG = "SHA-256";
	private static final String HISTORY_COLLECTION = "muc_history";
	
	private String resourceUri;
	private MongoClient mongo;
	private MongoDatabase db;
	private MongoCollection<Document> historyCollection;

	private int batchSize = DEF_BATCH_SIZE;
	
	private byte[] generateId(BareJID user) throws TigaseDBException {
		try {
			MessageDigest md = MessageDigest.getInstance(HASH_ALG);
			return md.digest(user.toString().getBytes());
		} catch (NoSuchAlgorithmException ex) {
			throw new TigaseDBException("Should not happen!!", ex);
		}
	}	
	
	@Override
	public void addJoinEvent(Room room, Date date, JID senderJID, String nickName) {
	}

	@Override
	public void addLeaveEvent(Room room, Date date, JID senderJID, String nickName) {
	}

	@Override
	public void addMessage(Room room, Element message, String body, JID senderJid, String senderNickname, Date time) {
		try {
			byte[] rid = generateId(room.getRoomJID());
			Document dto = new Document("room_jid_id", rid).append("room_jid", room.getRoomJID().toString())
					.append("event_type", 1)
					.append("sender_jid", senderJid.toString()).append("sender_nickname", senderNickname)
					.append("body", body).append("public_event", room.getConfig().isLoggingEnabled());
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
	public void addSubjectChange(Room room, Element message, String subject, JID senderJid, String senderNickname, Date time) {
	}
	
	@Override
	public void destroy() {
		if (mongo != null) {
			// if we have instance of MongoClient then close it and release resources
			mongo.close();
		}
	}

	@Override
	public void getHistoryMessages(Room room, JID senderJID, Integer maxchars, Integer maxstanzas, Integer seconds, Date since, PacketWriter writer) {
		Affiliation recipientAffiliation = room.getAffiliation(senderJID.getBareJID());
		boolean addRealJids = room.getConfig().getRoomAnonymity() == RoomConfig.Anonymity.nonanonymous
				|| room.getConfig().getRoomAnonymity() == RoomConfig.Anonymity.semianonymous
				&& (recipientAffiliation == Affiliation.owner || recipientAffiliation == Affiliation.admin);

		try {
			byte[] rid = generateId(room.getRoomJID());
			int maxMessages = room.getConfig().getMaxHistory();
			int limit = maxstanzas != null ? Math.min(maxMessages, maxstanzas) : maxMessages;
			if (since == null && seconds != null && maxstanzas == null) {
				since = new Date(new Date().getTime() - seconds * 1000);
			}
			
			Document crit = new Document("room_jid_id", rid).append("room_jid", room.getRoomJID().toString());
			if (since != null) {
				crit.append("timestamp", new Document("$gte", since));
				Document order = new Document("timestamp", 1);
				FindIterable<Document> cursor = historyCollection.find(crit).batchSize(batchSize).limit(limit).sort(order);
				for (Document dto : cursor) {
					Packet packet = createMessage(room.getRoomJID(), senderJID, dto, addRealJids);
					writer.write(packet);
				}
			} else {
				Document order = new Document("timestamp", -1);
				FindIterable<Document> cursor = historyCollection.find(crit).batchSize(batchSize).limit(limit).sort(order);
				List<Packet> results = new ArrayList<Packet>();
				for (Document dto : cursor) {
					Packet packet = createMessage(room.getRoomJID(), senderJID, dto, addRealJids);
					results.add(packet);
				}
				Collections.reverse(results);
				writer.write(results);
			}
		} catch (Exception ex) {
			if (log.isLoggable(Level.SEVERE))
				log.log(Level.SEVERE, "Can't get history", ex);
			throw new RuntimeException(ex);
		}
	}

	@Override
	public void init(Map<String, Object> props) {
		if (props != null) {
			if (props.containsKey("batch-size")) {
				batchSize = Integer.parseInt((String) props.get("batch-size"));
			} else {
				batchSize = DEF_BATCH_SIZE;
			}
		}

		if (!collectionExists(db, HISTORY_COLLECTION)) {
			db.createCollection(HISTORY_COLLECTION);
		}
		historyCollection = db.getCollection(HISTORY_COLLECTION);
		
		historyCollection.createIndex(new Document("room_jid_id", 1));
		historyCollection.createIndex(new Document("room_jid_id", 1).append("timestamp", 1));
	}

	@Override
	public boolean isPersistent() {
		return true;
	}

	@Override
	public void removeHistory(Room room) {
		try {
			byte[] rid = generateId(room.getRoomJID());
			Document crit = new Document("room_jid_id", rid).append("room_jid", room.getRoomJID().toString());
			db.getCollection(HISTORY_COLLECTION).deleteMany(crit);
		} catch (Exception ex) {
			if (log.isLoggable(Level.SEVERE))
				log.log(Level.SEVERE, "Can't remove history", ex);
			throw new RuntimeException(ex);
		}		
	}

	@Override
	public void initRepository(String resource_uri, Map<String, String> params) throws DBInitException {
		try {
			resourceUri = resource_uri;
			MongoClientURI uri = new MongoClientURI(resource_uri);
			mongo = new MongoClient(uri);
			db = mongo.getDatabase(uri.getDatabase());
			init((Map) params);
		} catch (MongoException ex) {
			throw new DBInitException("Could not connect to MongoDB server using URI = " + resource_uri, ex);
		}
	}
	
	private Packet createMessage(BareJID roomJid, JID senderJID, Document dto, boolean addRealJids) throws TigaseStringprepException {
		String sender_nickname = (String) dto.get("sender_nickname");
		String msg = (String) dto.get("msg");
		String body = (String) dto.get("body");
		String sender_jid = (String) dto.get("sender_jid");
		Date timestamp = (Date) dto.get("timestamp");
		
		return createMessage(roomJid, senderJID, sender_nickname, msg, body, sender_jid, addRealJids, timestamp);
	}
}
