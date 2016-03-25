/*
 * MongoMsgRepository.java
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
package tigase.mongodb;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.MongoException;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.UpdateOptions;
import org.bson.Document;
import org.bson.types.ObjectId;
import tigase.db.*;
import tigase.server.Packet;
import tigase.server.amp.MsgRepository;
import tigase.util.DateTimeFormatter;
import tigase.xml.DomBuilderHandler;
import tigase.xml.Element;
import tigase.xmpp.BareJID;
import tigase.xmpp.JID;
import tigase.xmpp.XMPPResourceConnection;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

import static tigase.mongodb.Helper.collectionExists;

/**
 *
 * @author andrzej
 */
@Repository.Meta( supportedUris = { "mongodb:.*" } )
public class MongoMsgRepository extends MsgRepository<ObjectId> {

	private static final Logger log = Logger.getLogger(MongoMsgRepository.class.getCanonicalName());

	private static final String JID_HASH_ALG = "SHA-256";

	private static final int DEF_BATCH_SIZE = 100;

	private static final String MSG_HISTORY_COLLECTION = "msg_history";
	private static final String MSG_BROADCAST_COLLECTION = "msg_broadcast";
	private static final String MSG_BROADCAST_RECP_COLLECTION = "msg_broadcast_recp";

	private static final DateTimeFormatter dt = new DateTimeFormatter();

	private static final Comparator<Document> MSG_COMPARATOR = (o1, o2) -> ((Date) o1.get("ts")).compareTo((Date) o2.get("ts"));
	
	private String resourceUri;
	private MongoClient mongo;
	private MongoCollection<Document> msgHistoryCollection;
	private MongoDatabase db;
	private MongoCollection<Document> broadcastMsgCollection;
	private MongoCollection<Document> broadcastMsgRecpCollection;

	private int batchSize = DEF_BATCH_SIZE;

	@Override
	public int deleteMessagesToJID( List<String> db_ids, XMPPResourceConnection session ) throws UserNotFoundException {

		int count = 0;
		BareJID to = null;
		try {
			to = session.getBareJID();
			byte[] toHash = generateId(to);

			Document crit = new Document("to_hash", toHash).append("to", to.toString());


			if ( db_ids == null || db_ids.size() == 0 ){
				msgHistoryCollection.deleteMany(crit);
				
			} else {

				FindIterable<Document> cursor = msgHistoryCollection.find( crit );

				for ( Document it : cursor ) {

					if ( it.containsKey( "ts" ) ){
						String msgId = dt.formatDateTime( (Date) it.get( "ts" ) );
						if ( db_ids.contains( msgId ) ){
							msgHistoryCollection.deleteOne( it );
							count++;
						}
					}
				}
			}

 		} catch (Exception ex) {
			log.log(Level.WARNING, "Problem adding new entry to DB: ", ex);
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
	public Map<Enum, Long> getMessagesCount( JID to ) throws UserNotFoundException {

		Map<Enum, Long> result = new HashMap<>( MSG_TYPES.values().length );

		try {
			byte[] toHash = generateId( to.getBareJID() );

			Document crit = new Document( "to_hash", toHash ).append( "to", to.getBareJID().toString() );

			for ( MSG_TYPES type : MSG_TYPES.values() ) {
				long count = msgHistoryCollection.count( crit.append( "msg_type", type.toString() ) );
				if ( count > 0 ){
					result.put( type, count );
				}
			}

		} catch ( Exception ex ) {
			log.log( Level.WARNING, "Problem adding new entry to DB: ", ex );
		}
		return result;
	}

	@Override
	public List<Element> getMessagesList( JID to ) throws UserNotFoundException {
		// TODO: temporary

		List<Element> result = new LinkedList<Element>();

		try {
			byte[] toHash = generateId( to.getBareJID() );

			Document crit = new Document( "to_hash", toHash ).append( "to", to.getBareJID().toString() );

			FindIterable<Document> cursor = msgHistoryCollection.find( crit ).batchSize(batchSize);

			for ( Document it : cursor ) {
				String msgId = null;
				if ( it.containsKey( "ts" ) ){
					msgId = dt.formatDateTime( (Date) it.get( "ts" ) );
				}
				String sender = null;
				if ( it.containsKey( "to" ) ){
					sender =  (String) it.get( "to" ) ;
				}
				MSG_TYPES messageType = MSG_TYPES.none;
				if ( it.containsKey( "msg_type" ) ){
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
		}
		return result;

	}

	@Override
	public void initRepository(String resource_uri, Map<String, String> params) throws DBInitException {
		try {
			if (params != null) {
				if (params.containsKey("batch-size")) {
					batchSize = Integer.parseInt(params.get("batch-size"));
				} else {
					batchSize = DEF_BATCH_SIZE;
				}
			}

			resourceUri = resource_uri;
			MongoClientURI uri = new MongoClientURI(resource_uri);

			// as instances of this MsgRepositoryIfc implemetations are cached
			// this instance may be reinitialized but then there is no point 
			// in recreation MongoClient instance
			if (mongo == null) {
				mongo = new MongoClient(uri);
				db = mongo.getDatabase(uri.getDatabase());

				if (!collectionExists(db, MSG_HISTORY_COLLECTION)) {
					 db.createCollection(MSG_HISTORY_COLLECTION);
				}
				msgHistoryCollection = db.getCollection(MSG_HISTORY_COLLECTION);

				msgHistoryCollection.createIndex(new Document("ts", 1));
				msgHistoryCollection.createIndex(new Document("to_hash", 1));

				if (!collectionExists(db, MSG_BROADCAST_COLLECTION)) {
					db.createCollection(MSG_BROADCAST_COLLECTION);
				}
				broadcastMsgCollection = db.getCollection(MSG_BROADCAST_COLLECTION);
				
				broadcastMsgCollection.createIndex(new Document("id", 1).append("expire", 1));

				if (!collectionExists(db, MSG_BROADCAST_RECP_COLLECTION)) {
					db.createCollection(MSG_BROADCAST_RECP_COLLECTION);
				}
				broadcastMsgRecpCollection = db.getCollection(MSG_BROADCAST_RECP_COLLECTION);
				
				broadcastMsgRecpCollection.createIndex(new Document("msg_id", 1));
				broadcastMsgRecpCollection.createIndex(new Document("msg_id", 1).append("recipient_id", 1), new IndexOptions().unique(true));
			}
			
			super.initRepository(resourceUri, params);
		} catch (MongoException ex) {
			throw new DBInitException("Could not connect to MongoDB server using URI = " + resource_uri, ex);
		}
	}

	@Override
	public Queue<Element> loadMessagesToJID( XMPPResourceConnection session, boolean delete )
			throws UserNotFoundException {
		return loadMessagesToJID( session, delete, null );
	}

	public Queue<Element> loadMessagesToJID(XMPPResourceConnection session, boolean delete, OfflineMessagesProcessor proc)
			throws UserNotFoundException {
		return loadMessagesToJID( null, session, delete, proc );

	}

	@Override
	public Queue<Element> loadMessagesToJID( List<String> db_ids, XMPPResourceConnection session, boolean delete, OfflineMessagesProcessor proc ) throws UserNotFoundException {
		// TODO: temporary

		Queue<Element> result = null;
		BareJID to = null;
		
		try {
			to = session.getBareJID();
			byte[] toHash = generateId(to);

			Document crit = new Document("to_hash", toHash).append("to", to.toString());

			FindIterable<Document> cursor = msgHistoryCollection.find(crit).batchSize(batchSize);

			List<Document> list = new ArrayList<Document>();
			for ( Document it : cursor ) {
				if (it.containsKey("expired-at") && ((Date)it.get("expired-at")).getTime() < System.currentTimeMillis()) {
					continue;
				}

				if ( db_ids != null && db_ids.size() >= 0 ){
					if ( it.containsKey( "ts" ) ){
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
				msgHistoryCollection.deleteMany(crit);
			}
 		} catch (Exception ex) {
			log.log(Level.WARNING, "Problem adding new entry to DB: ", ex);
		}
		return result;

	}

	@Override
	public boolean storeMessage(JID from, JID to, Date expired, Element msg, NonAuthUserRepository userRepo) throws UserNotFoundException {
		try {
			byte[] fromHash = generateId(from.getBareJID());
			byte[] toHash = generateId(to.getBareJID());
			
			Document crit = new Document("from_hash", fromHash).append("to_hash", toHash)
					.append("from", from.getBareJID().toString()).append("to", to.getBareJID().toString());
			
			long count = msgHistoryCollection.count(crit);
			long msgs_store_limit = getMsgsStoreLimit(to.getBareJID(), userRepo);
			if (msgs_store_limit <= count) {
				if (log.isLoggable(Level.FINEST)) {
					log.log(Level.FINEST, "Message store limit ({0}) exceeded for message: {1}",
							new Object[] { msgs_store_limit, Packet.elemToString(msg) });
				}
				return false;
			}
			
			Document dto = crit;
			if (expired != null) {
				dto = new Document(crit);
				dto.append("expire-at", expired);
				crit.append("expired-at", new Document("$lt", new Date()));
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
			msgHistoryCollection.insertOne(dto);
		
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
		return true;
	}

	@Override
	protected void deleteMessage(ObjectId dbId) {
		try {
			msgHistoryCollection.deleteOne(new Document("_id", dbId));
		} catch (MongoException ex) {
			
		}
	}
	
	@Override
	protected void loadExpiredQueue(int max) {
		try {
			FindIterable<Document> cursor = msgHistoryCollection.find(new Document("ts",
					new Document("$lt", new Date()))).sort(new Document("ts", 1)).batchSize(batchSize).limit(max);

			DomBuilderHandler domHandler = new DomBuilderHandler();
			int counter = 0;

			for ( Document it : cursor ) {
				if (counter >= max)
					break;

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
			log.log(Level.WARNING, "Problem getting offline messages from db: ", ex);
		}
		
		earliestOffline = Long.MAX_VALUE;
	}
	
	@Override
	protected void loadExpiredQueue(Date expired) {
		try {
			if (expiredQueue.size() > 100 * MAX_QUEUE_SIZE) {
				expiredQueue.clear();
			}
			
			FindIterable<Document> cursor = msgHistoryCollection.find(new Document("ts",
					new Document("$lt", expired))).sort(new Document("ts", 1)).batchSize(batchSize);

			DomBuilderHandler domHandler = new DomBuilderHandler();
			int counter = 0;

			for (Document it : cursor) {
				if (counter++ >= MAX_QUEUE_SIZE)
					break;

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
		try {
			Set<String> oldMessages = new HashSet<String>(broadcastMessages.keySet());
			FindIterable<Document >cursor = broadcastMsgCollection.find(new Document("expire", new Document("$gt", new Date()))).batchSize(batchSize);
			DomBuilderHandler domHandler = new DomBuilderHandler();
			for ( Document dto : cursor ) {
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
			log.log(Level.WARNING, "Problem loading messages for broadcast from db: ", ex);
		}	
	}
	

	@Override
	protected void ensureBroadcastMessageRecipient(String id, BareJID recipient) {
		try {
			byte[] recipientId = generateId(recipient);
			Document crit = new Document("msg_id", id).append("recipient_id", recipientId).append("recipient", recipient.toString());
			broadcastMsgCollection.updateOne(crit, crit, new UpdateOptions().upsert(true));
		} catch (Exception ex) {
			log.log(Level.WARNING, "Problem inserting messages recipients for broadcast to db: ", ex);
		}		
	}

	@Override
	protected void insertBroadcastMessage(String id, Element msg, Date expire, BareJID recipient) {
		try {
			broadcastMsgCollection.updateOne(new Document("id", id),
					new Document("$setOnInsert", new Document("expire", expire).append("msg", msg.toString())), new UpdateOptions().upsert(true));
		} catch (MongoException ex) {
			log.log(Level.WARNING, "Problem inserting messages for broadcast to db: ", ex);
		}
	}

	private Queue<Element> parseLoadedMessages( OfflineMessagesProcessor proc, List<Document> list ) {
		StringBuilder sb = new StringBuilder( 1000 );
		Queue<Element> result = new LinkedList<Element>();
		if ( proc != null ){

			for ( Document it : list ) {

				final String msg = (String) it.get( "message" );

				String msgId = null;
				if ( it.containsKey( "ts" ) ){
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

			for ( Document it : list ) {
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
