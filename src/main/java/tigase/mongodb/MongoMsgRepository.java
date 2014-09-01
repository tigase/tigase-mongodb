/*
 * MongoMsgRepository.java
 *
 * Tigase Jabber/XMPP Server - MongoDB support
 * Copyright (C) 2004-2014 "Tigase, Inc." <office@tigase.com>
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
package tigase.mongodb;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.MongoException;
import com.mongodb.ReadPreference;
import com.mongodb.WriteConcern;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.bson.types.ObjectId;
import tigase.db.DBInitException;
import tigase.db.TigaseDBException;
import tigase.db.UserNotFoundException;
import tigase.server.Packet;
import tigase.server.amp.JDBCMsgRepository;
import tigase.server.amp.MsgRepository;
import tigase.xml.DomBuilderHandler;
import tigase.xml.Element;
import tigase.xml.SimpleParser;
import tigase.xml.SingletonFactory;
import tigase.xmpp.BareJID;
import tigase.xmpp.JID;

/**
 *
 * @author andrzej
 */
public class MongoMsgRepository extends MsgRepository<ObjectId> {

	private static final Logger log = Logger.getLogger(MongoMsgRepository.class.getCanonicalName());
	private static final String JID_HASH_ALG = "SHA-256";
	private static final String MSG_HISTORY_COLLECTION = "msg_history";

	private static final Comparator<DBObject> MSG_COMPARATOR = new Comparator<DBObject>() {
		@Override
		public int compare(DBObject o1, DBObject o2) {
			return ((Date) o1.get("ts")).compareTo((Date) o2.get("ts"));
		}		
	};
	
	private long msgs_store_limit = MSGS_STORE_LIMIT_VAL;
	
	private String resourceUri;
	private MongoClient mongo;
	private DB db;

	@Override
	public Element getMessageExpired(long time, boolean delete) {
		if (expiredQueue.size() == 0) {

			// If the queue is empty load it with some elements
			loadExpiredQueue(MAX_QUEUE_SIZE);
		} else {

			// If the queue is not empty, check whether recently saved off-line
			// message
			// is due to expire sooner then the head of the queue.
			MsgDBItem item = expiredQueue.peek();

			if ((item != null) && (earliestOffline < item.expired.getTime())) {

				// There is in fact off-line message due to expire sooner then the head
				// of the
				// queue. Load all off-line message due to expire sooner then the first
				// element
				// in the queue.
				loadExpiredQueue(item.expired);
			}
		}

		MsgDBItem item = null;

		while (item == null) {
			try {
				item = expiredQueue.take();
			} catch (InterruptedException ex) {
			}
		}

		if (delete) {
			deleteMessage(item.db_id);
		}

		return item.msg;		
	}

	@Override
	public void initRepository(String resource_uri, Map<String, String> map) throws DBInitException {
		try {
			resourceUri = resource_uri;
			MongoClientURI uri = new MongoClientURI(resource_uri);
			mongo = new MongoClient(uri);
			db = mongo.getDB(uri.getDatabase());
			
			DBCollection msgHistoryCollection = null;
			if (!db.collectionExists(MSG_HISTORY_COLLECTION)) {
				msgHistoryCollection = db.createCollection(MSG_HISTORY_COLLECTION, new BasicDBObject());
			} else {
				msgHistoryCollection = db.getCollection(MSG_HISTORY_COLLECTION);
			}

			msgHistoryCollection.createIndex(new BasicDBObject("ts", 1));
			msgHistoryCollection.createIndex(new BasicDBObject("to_hash", 1));
			
			if (map != null) {
				String msgs_store_limit_str = map.get(MSGS_STORE_LIMIT_KEY);

				if (msgs_store_limit_str != null) {
					msgs_store_limit = Long.parseLong(msgs_store_limit_str);
				}
			}
		} catch (UnknownHostException ex) {
			throw new DBInitException("Could not connect to MongoDB server using URI = " + resource_uri, ex);
		}
	}

	@Override
	public Queue<Element> loadMessagesToJID(JID to, boolean delete) throws UserNotFoundException {
		DBCursor cursor = null;
		Queue<Element> result = null;
		try {
			byte[] toHash = generateId(to.getBareJID());

			BasicDBObject crit = new BasicDBObject("to_hash", toHash).append("to", to.getBareJID().toString());
			
			cursor = db.getCollection(MSG_HISTORY_COLLECTION).find(crit);
			
			List<DBObject> list = new ArrayList<DBObject>();
			while (cursor.hasNext()) {
				DBObject it = cursor.next();
				if (it.containsField("expired-at") && ((Date)it.get("expired-at")).getTime() < System.currentTimeMillis()) {
					continue;
				}
				list.add(it);
			}
			
			Collections.sort(list, MSG_COMPARATOR);
			StringBuilder sb = new StringBuilder(1000);
			for (DBObject it : list) {
				sb.append(it.get("message"));
			}
			
			if (sb.length() > 0) {
				DomBuilderHandler domHandler = new DomBuilderHandler();
				parser.parse(domHandler, sb.toString().toCharArray(), 0, sb.length());
				result = domHandler.getParsedElements();
			}
			
			if (delete) {
				db.getCollection(MSG_HISTORY_COLLECTION).remove(crit, WriteConcern.UNACKNOWLEDGED);
			}
 		} catch (Exception ex) {
			log.log(Level.WARNING, "Problem adding new entry to DB: ", ex);
		} finally {
			if (cursor != null) {
				cursor.close();
			}
		}
		return result;
	}

	@Override
	public void storeMessage(JID from, JID to, Date expired, Element msg) throws UserNotFoundException {
		try {
			byte[] fromHash = generateId(from.getBareJID());
			byte[] toHash = generateId(to.getBareJID());
			
			BasicDBObject crit = new BasicDBObject("from_hash", fromHash).append("to_hash", toHash)
					.append("from", from.getBareJID().toString()).append("to", to.getBareJID().toString());
			
			long count = db.getCollection(MSG_HISTORY_COLLECTION).count(crit);
			
			if (msgs_store_limit <= count) {
				if (log.isLoggable(Level.FINEST)) {
					log.log(Level.FINEST, "Message store limit ({0}) exceeded for message: {1}",
							new Object[] { msgs_store_limit, Packet.elemToString(msg) });
				}
				return;
			}
			
			BasicDBObject dto = crit;
			if (expired != null) {
				dto = new BasicDBObject(crit);
				dto.append("expire-at", expired);
				crit.append("expired-at", new BasicDBObject("$lt", new Date()));
			}
			dto.append("ts", new Date());
			dto.append("message", msg.toString());
			db.getCollection(MSG_HISTORY_COLLECTION).insert(dto, WriteConcern.UNACKNOWLEDGED);
			
			if (expired != null) {
				if (expired.getTime() < earliestOffline) {
					earliestOffline = expired.getTime();
				}

				if (expiredQueue.size() == 0) {
					loadExpiredQueue(1);
				}
			}
		} catch (Exception ex) {
			log.log(Level.WARNING, "Problem adding new entry to DB: ", ex);
		}
	}

	@Override
	protected void deleteMessage(ObjectId dbId) {
		try {
			db.getCollection(MSG_HISTORY_COLLECTION).remove(new BasicDBObject("_id", dbId));
		} catch (MongoException ex) {
			
		}
	}
	
	@Override
	protected void loadExpiredQueue(int max) {
		DBCursor cursor = null;
		try {
			cursor = db.getCollection(MSGS_STORE_LIMIT_KEY).find(new BasicDBObject("ts",
					new BasicDBObject("$lt", new Date()))).sort(new BasicDBObject("ts", 1)).limit(max);

			DomBuilderHandler domHandler = new DomBuilderHandler();
			int counter = 0;

			while (cursor.hasNext() && counter < max) {
				DBObject it = cursor.next();
				String msg_str = (String) it.get("message");

				parser.parse(domHandler, msg_str.toCharArray(), 0, msg_str.length());

				Queue<Element> elems = domHandler.getParsedElements();
				Element msg = elems.poll();

				if (msg == null) {
					log.log(Level.INFO,
							"Something wrong, loaded offline message from DB but parsed no "
							+ "XML elements: {0}", msg_str);
				} else {
					Date ts = (Date) it.get("ts");
					MsgDBItem item = new MsgDBItem((ObjectId) it.get("_id"), msg, ts);

					expiredQueue.offer(item);
				}
				counter++;
			}
		} catch (MongoException ex) {
			if (cursor != null) {
				cursor.close();
			}
			log.log(Level.WARNING, "Problem getting offline messages from db: ", ex);
		}
		
		earliestOffline = Long.MAX_VALUE;
	}
	
	@Override
	protected void loadExpiredQueue(Date expired) {
		DBCursor cursor = null;
		try {
			if (expiredQueue.size() > 100 * MAX_QUEUE_SIZE) {
				expiredQueue.clear();
			}
			
			cursor = db.getCollection(MSGS_STORE_LIMIT_KEY).find(new BasicDBObject("ts",
					new BasicDBObject("$lt", expired))).sort(new BasicDBObject("ts", 1));

			DomBuilderHandler domHandler = new DomBuilderHandler();
			int counter = 0;

			while (cursor.hasNext() && counter++ < MAX_QUEUE_SIZE) {
				DBObject it = cursor.next();
				String msg_str = (String) it.get("message");

				parser.parse(domHandler, msg_str.toCharArray(), 0, msg_str.length());

				Queue<Element> elems = domHandler.getParsedElements();
				Element msg = elems.poll();

				if (msg == null) {
					log.log(Level.INFO,
							"Something wrong, loaded offline message from DB but parsed no "
							+ "XML elements: {0}", msg_str);
				} else {
					Date ts = (Date) it.get("ts");
					MsgDBItem item = new MsgDBItem((ObjectId) it.get("_id"), msg, ts);

					expiredQueue.offer(item);
				}
			}
		} catch (MongoException ex) {
			if (cursor != null) {
				cursor.close();
			}
			log.log(Level.WARNING, "Problem getting offline messages from db: ", ex);
		}
		
		earliestOffline = Long.MAX_VALUE;		
	}
		
	private byte[] generateId(BareJID user) throws TigaseDBException {
		try {
			MessageDigest md = MessageDigest.getInstance(JID_HASH_ALG);
			return md.digest(user.toString().getBytes());
		} catch (NoSuchAlgorithmException ex) {
			throw new TigaseDBException("Should not happen!!", ex);
		}
	}	
	
}
