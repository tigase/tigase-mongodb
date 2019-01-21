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
package tigase.mongodb;

import com.mongodb.MongoException;
import com.mongodb.MongoNamespace;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.Updates;
import org.bson.Document;
import org.bson.types.ObjectId;
import tigase.db.Repository;
import tigase.db.Schema;
import tigase.db.TigaseDBException;
import tigase.kernel.beans.config.ConfigField;
import tigase.server.amp.db.MsgBroadcastRepository;
import tigase.xml.DomBuilderHandler;
import tigase.xml.Element;
import tigase.xmpp.jid.BareJID;
import tigase.xmpp.jid.JID;

import java.nio.charset.Charset;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Date;
import java.util.HashSet;
import java.util.Queue;
import java.util.Set;
import java.util.function.Consumer;
import java.util.logging.Level;
import java.util.logging.Logger;

import static tigase.mongodb.Helper.collectionExists;

/**
 * Created by andrzej on 04.10.2016.
 */
@Repository.Meta(supportedUris = {"mongodb:.*"})
@Repository.SchemaId(id = Schema.SERVER_SCHEMA_ID +
		"-offline-message-broadcast", name = "Tigase XMPP Server (Offline Messages [broadcast])", external = false)
public class MongoMsgBroadcastRepository
		extends MsgBroadcastRepository<ObjectId, MongoDataSource> {

	private static final Logger log = Logger.getLogger(MongoMsgBroadcastRepository.class.getCanonicalName());

	private static final String JID_HASH_ALG = "SHA-256";

	private static final int DEF_BATCH_SIZE = 100;

	private static final String MSG_BROADCAST_COLLECTION = "tig_broadcast_messages";
	private static final String MSG_BROADCAST_RECP_COLLECTION = "tig_broadcast_recipients";

	private static final Charset UTF8 = Charset.forName("UTF-8");
	@ConfigField(desc = "Batch size", alias = "batch-size")
	private int batchSize = DEF_BATCH_SIZE;
	private MongoCollection<Document> broadcastMsgCollection;
	private MongoCollection<Document> broadcastMsgRecpCollection;
	private MongoDatabase db;

	@Override
	protected void ensureBroadcastMessageRecipient(String id, BareJID recipient) {
		try {
			byte[] recipientId = generateId(recipient);
			Document crit = new Document("msg_id", id).append("recipient_id", recipientId)
					.append("recipient", recipient.toString());
			broadcastMsgRecpCollection.updateOne(crit, Updates.set("recipient_id", recipientId),
			                                     new UpdateOptions().upsert(true));
		} catch (Exception ex) {
			log.log(Level.WARNING, "Problem inserting messages recipients for broadcast to db: ", ex);
		}
	}

	private byte[] generateId(BareJID user) throws TigaseDBException {
		try {
			MessageDigest md = MessageDigest.getInstance(JID_HASH_ALG);
			return md.digest(user.toString().getBytes(UTF8));
		} catch (NoSuchAlgorithmException ex) {
			throw new TigaseDBException("Should not happen!!", ex);
		}
	}

	@Override
	protected void insertBroadcastMessage(String id, Element msg, Date expire, BareJID recipient) {
		try {
			broadcastMsgCollection.updateOne(new Document("_id", id), new Document("$setOnInsert",
			                                                                       new Document("expire",
			                                                                                    expire).append("msg",
			                                                                                                   msg.toString())),
			                                 new UpdateOptions().upsert(true));
		} catch (MongoException ex) {
			log.log(Level.WARNING, "Problem inserting messages for broadcast to db: ", ex);
		}
	}

	@Override
	public void loadMessagesToBroadcast() {
		try {
			Set<String> oldMessages = new HashSet<String>(broadcastMessages.keySet());
			FindIterable<Document> cursor = broadcastMsgCollection.find(
					new Document("expire", new Document("$gt", new Date()))).batchSize(batchSize);
			DomBuilderHandler domHandler = new DomBuilderHandler();
			for (Document dto : cursor) {
				String id = dto.getString("_id");
				oldMessages.remove(id);
				if (broadcastMessages.containsKey(id)) {
					continue;
				}

				Date expire = (Date) dto.get("expire");
				char[] msgChars = ((String) dto.get("msg")).toCharArray();

				parser.parse(domHandler, msgChars, 0, msgChars.length);

				Queue<Element> elems = domHandler.getParsedElements();
				Element msg = elems.poll();
				if (msg == null) {
					continue;
				}

				broadcastMessages.put(id, new BroadcastMsg(null, msg, expire));
			}
			for (String key : oldMessages) {
				broadcastMessages.remove(key);
			}
			broadcastMessages.entrySet().forEach(e -> {
				broadcastMsgRecpCollection.find(new Document("msg_id", e.getKey()))
						.forEach((Consumer<? super Document>) dto -> {
							JID recipient = JID.jidInstanceNS(dto.getString("recipient"));
							e.getValue().markAsSent(recipient);
						});
			});
		} catch (MongoException ex) {
			log.log(Level.WARNING, "Problem loading messages for broadcast from db: ", ex);
		}
	}

	@Override
	public void setDataSource(MongoDataSource dataSource) {
		db = dataSource.getDatabase();

		if (!collectionExists(db, MSG_BROADCAST_COLLECTION)) {
			if (collectionExists(db, "msg_broadcast")) {
				db.getCollection("msg_broadcast")
						.renameCollection(new MongoNamespace(db.getName(), MSG_BROADCAST_COLLECTION));
			} else {
				db.createCollection(MSG_BROADCAST_COLLECTION);
			}
		}
		broadcastMsgCollection = db.getCollection(MSG_BROADCAST_COLLECTION);

		broadcastMsgCollection.createIndex(new Document("_id", 1).append("expire", 1));

		if (!collectionExists(db, MSG_BROADCAST_RECP_COLLECTION)) {
			if (collectionExists(db, "msg_broadcast_recp")) {
				db.getCollection("msg_broadcast_recp")
						.renameCollection(new MongoNamespace(db.getName(), MSG_BROADCAST_RECP_COLLECTION));
			} else {
				db.createCollection(MSG_BROADCAST_RECP_COLLECTION);
			}
		}
		broadcastMsgRecpCollection = db.getCollection(MSG_BROADCAST_RECP_COLLECTION);

		broadcastMsgRecpCollection.createIndex(new Document("msg_id", 1));
		broadcastMsgRecpCollection.createIndex(new Document("msg_id", 1).append("recipient_id", 1),
		                                       new IndexOptions().unique(true));
	}
}
