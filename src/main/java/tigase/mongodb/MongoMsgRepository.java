/*
 * MongoMsgRepository.java
 *
 * Tigase Jabber/XMPP Server - MongoDB support
 * Copyright (C) 2004-2017 "Tigase, Inc." <office@tigase.com>
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
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Projections;
import com.mongodb.client.model.Sorts;
import com.mongodb.client.result.DeleteResult;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.Binary;
import org.bson.types.ObjectId;
import tigase.db.*;
import tigase.db.util.RepositoryVersionAware;
import tigase.db.util.SchemaLoader;
import tigase.kernel.beans.config.ConfigField;
import tigase.server.Packet;
import tigase.server.amp.db.MsgRepository;
import tigase.util.Version;
import tigase.util.datetime.DateTimeFormatter;
import tigase.xml.DomBuilderHandler;
import tigase.xml.Element;
import tigase.xmpp.XMPPResourceConnection;
import tigase.xmpp.jid.BareJID;
import tigase.xmpp.jid.JID;

import java.nio.charset.Charset;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import static com.mongodb.client.model.Projections.fields;
import static com.mongodb.client.model.Projections.include;
import static tigase.mongodb.Helper.collectionExists;

/**
 *
 * @author andrzej
 */
@Repository.Meta( supportedUris = { "mongodb:.*" } )
@Repository.SchemaId(id = Schema.SERVER_SCHEMA_ID+"-offline-message", name = "Tigase XMPP Server (Offline Messages)")
@RepositoryVersionAware.SchemaVersion
public class MongoMsgRepository extends MsgRepository<ObjectId,MongoDataSource> implements MongoRepositoryVersionAware {

	private static final Logger log = Logger.getLogger(MongoMsgRepository.class.getCanonicalName());

	private static final String JID_HASH_ALG = "SHA-256";

	private static final int DEF_BATCH_SIZE = 100;

	private static final String MSG_HISTORY_COLLECTION = "tig_offline_messages";

	private static final DateTimeFormatter dt = new DateTimeFormatter();

	//private static final Comparator<Document> MSG_COMPARATOR = (o1, o2) -> ((Date) o1.get("ts")).compareTo((Date) o2.get("ts"));

	private static final Charset UTF8 = Charset.forName("UTF-8");

	private MongoCollection<Document> msgHistoryCollection;
	private MongoDatabase db;

	@ConfigField(desc = "Batch size", alias = "batch-size")
	private int batchSize = DEF_BATCH_SIZE;

	@Override
	public int deleteMessagesToJID( List<String> db_ids, XMPPResourceConnection session ) throws UserNotFoundException {

		int count = 0;
		BareJID to = null;
		try {
			to = session.getBareJID();
			byte[] toHash = generateId(to);

			Bson crit = Filters.eq("to_hash", toHash);


			if ( db_ids == null || db_ids.size() == 0 ){
				msgHistoryCollection.deleteMany(crit);
				
			} else {
				crit = Filters.and(crit, Filters.in("_id", db_ids.stream()
						.map(id -> new ObjectId(id))
						.collect(Collectors.toList())));

				DeleteResult result = msgHistoryCollection.deleteMany(crit );
				count = (int) result.getDeletedCount();
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

		MsgDBItem<ObjectId> item = null;

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

			Document crit = new Document( "to_hash", toHash );

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

			Document crit = new Document( "to_hash", toHash );

			FindIterable<Document> cursor = msgHistoryCollection.find(crit)
					.projection(Projections.include("_id", "from", "msg_type"))
					.sort(Sorts.ascending("ts"))
					.batchSize(batchSize);

			for ( Document it : cursor ) {
				String msgId = it.getObjectId("_id").toHexString();
				String sender = null;
				if ( it.containsKey( "from" ) ){
					sender =  (String) it.get( "from" ) ;
				}
				MSG_TYPES messageType = MSG_TYPES.none;
				if ( it.containsKey( "msg_type" ) ){
					messageType = MSG_TYPES.valueOf( (String) it.get( "msg_type" ) );
				}

				if (msgId != null && messageType != null && messageType != MSG_TYPES.none && sender != null) {
					Element item = new Element("item", new String[]{"jid", "node", "type", "name"},
											   new String[]{to.getBareJID().toString(), msgId, messageType.name(), sender});
					result.add(item);
				}

			}

		} catch ( Exception ex ) {
			log.log( Level.WARNING, "Problem retrieving itmes from DB: ", ex );
		}
		return result;

	}

	@Override
	public void setDataSource(MongoDataSource dataSource) {
		db = dataSource.getDatabase();

		if (!collectionExists(db, MSG_HISTORY_COLLECTION)) {
			if (collectionExists(db, "msg_history")) {
				db.getCollection("msg_history").renameCollection(new MongoNamespace(db.getName(), MSG_HISTORY_COLLECTION));
			} else {
				db.createCollection(MSG_HISTORY_COLLECTION);
			}
		}
		msgHistoryCollection = db.getCollection(MSG_HISTORY_COLLECTION);

		msgHistoryCollection.createIndex(new Document("ts", 1));
		msgHistoryCollection.createIndex(new Document("to_hash", 1));
	}

	@Override
	@Deprecated
	public void initRepository(String resource_uri, Map<String, String> params) throws DBInitException {
		try {
			if (params != null) {
				if (params.containsKey("batch-size")) {
					batchSize = Integer.parseInt(params.get("batch-size"));
				} else {
					batchSize = DEF_BATCH_SIZE;
				}
			}

			// as instances of this MsgRepositoryIfc implemetations are cached
			// this instance may be reinitialized but then there is no point 
			// in recreation MongoClient instance
			if (db == null) {
				MongoDataSource ds = new MongoDataSource();
				ds.initRepository(resource_uri, params);
				setDataSource(ds);
			}
			
			super.initRepository(resource_uri, params);
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
		Queue<Element> result = null;
		BareJID to = null;
		
		try {
			to = session.getBareJID();
			byte[] toHash = generateId(to);

			Bson crit = Filters.eq("to_hash", toHash);
			if (db_ids != null && !db_ids.isEmpty()) {
				crit = Filters.and(crit, Filters.in("_id", db_ids.stream()
						.map(id -> new ObjectId(id))
						.collect(Collectors.toList())));
			}

			FindIterable<Document> cursor = msgHistoryCollection.find(crit).sort(Sorts.ascending("ts")).batchSize(batchSize);

			List<Document> list = new ArrayList<Document>();
			for ( Document it : cursor ) {
				if (it.containsKey("expire-at") && ((Date)it.get("expire-at")).getTime() < System.currentTimeMillis()) {
					continue;
				}

				list.add(it);
			}

			//Collections.sort(list, MSG_COMPARATOR);
			result = parseLoadedMessages( proc, list);
			result.stream().map(it -> it.getAttributeStaticStr("id")).forEach(it -> System.out.println(it));

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
				crit.append("expire-at", new Document("$lt", new Date()));
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
	public SchemaLoader.Result updateSchema(Optional<Version> oldVersion, Version newVersion) throws TigaseDBException {
		for (Document doc : msgHistoryCollection.find().batchSize(1000).projection(fields(include("_id", "from", "to")))) {
			String from = (String) doc.get("from");
			String to = (String) doc.get("to");

			byte[] oldToHash = ((Binary) doc.get("to_hash")).getData();
			byte[] oldFromHash = ((Binary) doc.get("from_hash")).getData();

			byte[] newToHash = calculateHash(to.toLowerCase());
			byte[] newFromHash = calculateHash(from.toLowerCase());

			if (Arrays.equals(oldFromHash, newFromHash) && Arrays.equals(oldToHash, newToHash)) {
				continue;
			}

			Document update = new Document("from_hash", newFromHash)
					.append("to_hash", newToHash);

			msgHistoryCollection.updateOne(new Document("_id", doc.get("_id")), new Document("$set", update));
		}
		return SchemaLoader.Result.ok;
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
			FindIterable<Document> cursor = msgHistoryCollection.find(new Document("expire-at",
					new Document("$lt", new Date()))).sort(new Document("expire-at", 1)).batchSize(batchSize).limit(max);

			DomBuilderHandler domHandler = new DomBuilderHandler();

			for ( Document it : cursor ) {
				if (expiredQueue.size() < MAX_QUEUE_SIZE)
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
	
	@Override
	protected void loadExpiredQueue(Date expired) {
		try {
			if (expiredQueue.size() > 100 * MAX_QUEUE_SIZE) {
				expiredQueue.clear();
			}
			
			FindIterable<Document> cursor = msgHistoryCollection.find(new Document("expire-at",
					new Document("$lt", expired))).sort(new Document("expire-at", 1)).batchSize(batchSize);

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
		return  calculateHash(user.toString().toLowerCase());
	}

	private byte[] calculateHash(String user) throws TigaseDBException {
		try {
			MessageDigest md = MessageDigest.getInstance(JID_HASH_ALG);
			return md.digest(user.getBytes(UTF8));
		} catch (NoSuchAlgorithmException ex) {
			throw new TigaseDBException("Should not happen!!", ex);
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
