/*
 * MongoMsgRepository.java
 *
 * Tigase Jabber/XMPP Server - MongoDB support
 * Copyright (C) 2004-2015 "Tigase, Inc." <office@tigase.com>
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

import tigase.db.DBInitException;
import tigase.db.Repository;
import tigase.db.TigaseDBException;
import tigase.db.UserNotFoundException;

import tigase.server.Packet;
import tigase.server.amp.MsgRepository;

import tigase.xmpp.BareJID;
import tigase.xmpp.JID;

import tigase.util.DateTimeFormatter;
import tigase.xml.DomBuilderHandler;
import tigase.xml.Element;

import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.MongoException;
import com.mongodb.WriteConcern;
import org.bson.types.ObjectId;

/**
 *
 * @author andrzej
 */
@Repository.Meta( supportedUris = { "mongodb:.*" } )
public class MongoMsgRepository extends MsgRepository<ObjectId> {

	private static final Logger log = Logger.getLogger(MongoMsgRepository.class.getCanonicalName());
	private static final String JID_HASH_ALG = "SHA-256";
	private static final String MSG_HISTORY_COLLECTION = "msg_history";
	private static final String MSG_BROADCAST_COLLECTION = "msg_broadcast";
	private static final String MSG_BROADCAST_RECP_COLLECTION = "msg_broadcast_recp";

	private static final DateTimeFormatter dt = new DateTimeFormatter();

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
	public int deleteMessagesToJID( List<String> db_ids, JID to ) throws UserNotFoundException {

		int count = 0;
		DBCursor cursor = null;
		try {
			byte[] toHash = generateId(to.getBareJID());

			BasicDBObject crit = new BasicDBObject("to_hash", toHash).append("to", to.getBareJID().toString());


			if ( db_ids == null || db_ids.size() == 0 ){
				db.getCollection(MSG_HISTORY_COLLECTION).remove(crit, WriteConcern.UNACKNOWLEDGED);
				
			} else {

				cursor = db.getCollection( MSG_HISTORY_COLLECTION ).find( crit );

				while ( cursor.hasNext() ) {
					DBObject it = cursor.next();

					if ( it.containsField( "ts" ) ){
						String msgId = dt.formatDateTime( (Date) it.get( "ts" ) );
						if ( db_ids.contains( msgId ) ){
							db.getCollection( MSG_HISTORY_COLLECTION ).remove( it );
							count++;
						}
					}
				}
			}

 		} catch (Exception ex) {
			log.log(Level.WARNING, "Problem adding new entry to DB: ", ex);
		} finally {
			if (cursor != null) {
				cursor.close();
			}
		}

		return count;
	}

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
	public Map<MSG_TYPES, Long> getMessagesCount( JID to ) throws UserNotFoundException {

		Map<MSG_TYPES, Long> result = new HashMap<>( MSG_TYPES.values().length );

		DBCursor cursor = null;
		try {
			byte[] toHash = generateId( to.getBareJID() );

			BasicDBObject crit = new BasicDBObject( "to_hash", toHash ).append( "to", to.getBareJID().toString() );

			for ( MSG_TYPES type : MSG_TYPES.values() ) {
				long count = db.getCollection( MSG_HISTORY_COLLECTION ).find( crit.append( "msg_type", type.toString() ) ).count();
				if ( count > 0 ){
					result.put( type, count );
				}
			}

		} catch ( Exception ex ) {
			log.log( Level.WARNING, "Problem adding new entry to DB: ", ex );
		} finally {
			if ( cursor != null ){
				cursor.close();
			}
		}
		return result;
	}

	@Override
	public List<Element> getMessagesList( JID to ) throws UserNotFoundException {
		// TODO: temporary

		DBCursor cursor = null;
		List<Element> result = new LinkedList<Element>();

		try {
			byte[] toHash = generateId( to.getBareJID() );

			BasicDBObject crit = new BasicDBObject( "to_hash", toHash ).append( "to", to.getBareJID().toString() );

			cursor = db.getCollection( MSG_HISTORY_COLLECTION ).find( crit );

			while ( cursor.hasNext() ) {
				DBObject it = cursor.next();

				String msgId = null;
				if ( it.containsField( "ts" ) ){
					msgId = dt.formatDateTime( (Date) it.get( "ts" ) );
				}
				String sender = null;
				if ( it.containsField( "to" ) ){
					sender =  (String) it.get( "to" ) ;
				}
				MSG_TYPES messageType = MSG_TYPES.none;
				if ( it.containsField( "msg_type" ) ){
					messageType = MSG_TYPES.valueOf( (String) it.get( "msg_type" ) );
				}

				if ( msgId != null && messageType != null && messageType != MSG_TYPES.none && sender != null ){
					Element item = new Element( "item",
																			new String[] { "jid", "node", "type", "name" },
																			new String[] { to.getBareJID().toString(), msgId,
																										 messageType.name(), sender } );
					result.add( item );
				}

			}

		} catch ( Exception ex ) {
			log.log( Level.WARNING, "Problem retrieving itmes from DB: ", ex );
		} finally {
			if ( cursor != null ){
				cursor.close();
			}
		}
		return result;

	}

	@Override
	public void initRepository(String resource_uri, Map<String, String> map) throws DBInitException {
		try {
			resourceUri = resource_uri;
			MongoClientURI uri = new MongoClientURI(resource_uri);

			// as instances of this MsgRepositoryIfc implemetations are cached
			// this instance may be reinitialized but then there is no point 
			// in recreation MongoClient instance
			if (mongo == null) {
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
				
				DBCollection  broadcastMsgCollection = !db.collectionExists(MSG_BROADCAST_COLLECTION)
						? db.createCollection(MSG_BROADCAST_COLLECTION, new BasicDBObject())
						: db.getCollection(MSG_BROADCAST_COLLECTION);
				
				broadcastMsgCollection.createIndex(new BasicDBObject("id", 1).append("expire", 1));
				
				DBCollection  broadcastMsgRecpCollection = !db.collectionExists(MSG_BROADCAST_RECP_COLLECTION)
						? db.createCollection(MSG_BROADCAST_RECP_COLLECTION, new BasicDBObject())
						: db.getCollection(MSG_BROADCAST_RECP_COLLECTION);
				
				broadcastMsgRecpCollection.createIndex(new BasicDBObject("msg_id", 1));
				broadcastMsgRecpCollection.createIndex(new BasicDBObject("msg_id", 1).append("recipient_id", 1), new BasicDBObject("unique", true));
			}
			
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
	public Queue<Element> loadMessagesToJID( JID to, boolean delete )
			throws UserNotFoundException {
		return loadMessagesToJID( to, delete, null );
	}

	public Queue<Element> loadMessagesToJID(JID to, boolean delete, OfflineMessagesProcessor proc)
			throws UserNotFoundException {
		return loadMessagesToJID( null, to, delete, proc );

	}

	@Override
	public Queue<Element> loadMessagesToJID( List<String> db_ids, JID to, boolean delete, OfflineMessagesProcessor proc ) throws UserNotFoundException {
		// TODO: temporary

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

				if ( db_ids != null && db_ids.size() >= 0 ){
					if ( it.containsField( "ts" ) ){
						String msgId = dt.formatDateTime( (Date) it.get( "ts" ) );
						if ( db_ids.contains( msgId ) ){
							list.add( it );
						}
					}
				} else {
					list.add( it );
				}
			}

			Collections.sort(list, MSG_COMPARATOR);

			result = parseLoadedMessages( proc, list);

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

			MSG_TYPES valueOf;
			try {
				final String name = msg.getName();
				valueOf = MSG_TYPES.valueOf( name );
			} catch ( IllegalArgumentException e ) {
				valueOf = MSG_TYPES.none;
			}

			dto.append( "msg_type", valueOf.toString());
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

	@Override
	public void loadMessagesToBroadcast() {
		DBCursor cursor = null;
		try {
			Set<String> oldMessages = new HashSet<String>(broadcastMessages.keySet());
			cursor = db.getCollection(MSG_BROADCAST_COLLECTION).find(new BasicDBObject("expire", new BasicDBObject("$gt", new Date())));
			DomBuilderHandler domHandler = new DomBuilderHandler();
			while (cursor.hasNext()) {
				DBObject dto = cursor.next();
				String id = (String) dto.get("_id");
				oldMessages.remove(id);
				if (broadcastMessages.containsKey(id))
					continue;
				
				Date expire = (Date) dto.get("expire");
				char[] msgChars = ((String) dto.get("msg")).toCharArray();
					
				parser.parse(domHandler, msgChars, 0, msgChars.length);
					
				Queue<Element> elems = domHandler.getParsedElements();
				Element msg = elems.poll();
				if (msg == null)
					continue;
					
				broadcastMessages.put(id, new BroadcastMsg(null, msg, expire));
			}
			for (String key : oldMessages) {
				broadcastMessages.remove(key);
			}
		} catch (MongoException ex) {
			if (cursor != null) {
				cursor.close();
			}
			log.log(Level.WARNING, "Problem loading messages for broadcast from db: ", ex);
		}	
	}
	

	@Override
	protected void ensureBroadcastMessageRecipient(String id, BareJID recipient) {
		try {
			byte[] recipientId = generateId(recipient);
			BasicDBObject crit = new BasicDBObject("msg_id", id).append("recipient_id", recipientId).append("recipient", recipient.toString());
			db.getCollection(MSG_BROADCAST_RECP_COLLECTION).update(crit, crit, true, false);
		} catch (Exception ex) {
			log.log(Level.WARNING, "Problem inserting messages recipients for broadcast to db: ", ex);
		}		
	}

	@Override
	protected void insertBroadcastMessage(String id, Element msg, Date expire, BareJID recipient) {
		try {
			db.getCollection(MSG_BROADCAST_COLLECTION).update(new BasicDBObject("id", id), 
					new BasicDBObject("$setOnInsert", new BasicDBObject("expire", expire).append("msg", msg.toString())), true, false);
		} catch (MongoException ex) {
			log.log(Level.WARNING, "Problem inserting messages for broadcast to db: ", ex);
		}
	}

	private Queue<Element> parseLoadedMessages( OfflineMessagesProcessor proc, List<DBObject> list ) {
		StringBuilder sb = new StringBuilder( 1000 );
		Queue<Element> result = new LinkedList<Element>();
		if ( proc != null ){

			for ( DBObject it : list ) {

				final String msg = (String) it.get( "message" );

				String msgId = null;
				if ( it.containsField( "ts" ) ){
					msgId = dt.formatDateTime( (Date) it.get( "ts" ) );
				}

				if ( msg != null ){
					DomBuilderHandler domHandler = new DomBuilderHandler();

					parser.parse( domHandler, msg.toCharArray(), 0, msg.length() );
					final Queue<Element> parsedElements = domHandler.getParsedElements();
					Element msgEl = parsedElements.poll();
					if ( msgEl != null && msgId != null ){

						proc.stamp( msgEl, msgId );

						result.add( msgEl );
					}
				}

			}

		} else {
			result = new LinkedList<Element>();

			for ( DBObject it : list ) {
				sb.append( it.get( "message" ) );
			}

			if ( sb.length() > 0 ){
				DomBuilderHandler domHandler = new DomBuilderHandler();
				parser.parse( domHandler, sb.toString().toCharArray(), 0, sb.length() );
				result = domHandler.getParsedElements();
			}
		}
		return result;
	}

}
