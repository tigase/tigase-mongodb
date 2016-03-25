/*
 * MongoMessageArchiveRepository.java
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
package tigase.mongodb.archive;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.MongoException;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.UpdateOptions;
import org.bson.Document;
import tigase.archive.AbstractCriteria;
import tigase.archive.db.AbstractMessageArchiveRepository;
import tigase.db.DBInitException;
import tigase.db.Repository;
import tigase.db.TigaseDBException;
import tigase.mongodb.archive.MongoMessageArchiveRepository.Criteria;
import tigase.xml.DomBuilderHandler;
import tigase.xml.Element;
import tigase.xml.SimpleParser;
import tigase.xml.SingletonFactory;
import tigase.xmpp.BareJID;
import tigase.xmpp.JID;
import tigase.xmpp.RSM;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Pattern;

import static tigase.mongodb.Helper.collectionExists;

/**
 *
 * @author andrzej
 */
@Repository.Meta( supportedUris = { "mongodb:.*" } )
public class MongoMessageArchiveRepository extends AbstractMessageArchiveRepository<Criteria> {

	private static final Logger log = Logger.getLogger(MongoMessageArchiveRepository.class.getCanonicalName());

	private static final int DEF_BATCH_SIZE = 100;

	private static final String HASH_ALG = "SHA-256";
	private static final String[] MSG_BODY_PATH = { "message", "body" };	
	private static final String MSGS_COLLECTION = "tig_ma_msgs";
	private static final String STORE_PLAINTEXT_BODY_KEY = "store-plaintext-body";
	
	private static final SimpleParser parser      = SingletonFactory.getParserInstance();
	
	private String resourceUri;
	private MongoClient mongo;
	private MongoDatabase db;
	private MongoCollection<Document> msgsCollection;

	private int batchSize = DEF_BATCH_SIZE;
	
	private boolean storePlaintextBody = true;
	
	private static byte[] generateId(BareJID user) throws TigaseDBException {
		try {
			MessageDigest md = MessageDigest.getInstance(HASH_ALG);
			return md.digest(user.toString().getBytes());
		} catch (NoSuchAlgorithmException ex) {
			throw new TigaseDBException("Should not happen!!", ex);
		}
	}		

	private static byte[] generateId(String user) throws TigaseDBException {
		try {
			MessageDigest md = MessageDigest.getInstance(HASH_ALG);
			return md.digest(user.getBytes());
		} catch (NoSuchAlgorithmException ex) {
			throw new TigaseDBException("Should not happen!!", ex);
		}
	}	
	
	@Override
	public void archiveMessage(BareJID owner, JID buddy, Direction direction, Date timestamp, Element msg, Set<String> tags) {
		try {
			byte[] oid = generateId(owner);
			byte[] bid = generateId(buddy.getBareJID());
			byte[] odid = generateId(owner.getDomain());
			
			String type = msg.getAttributeStaticStr("type");
			Date date = new Date(timestamp.getTime() - (timestamp.getTime() % (24*60*60*1000)));
			byte[] hash = generateHashOfMessage(direction, msg, null);
			
			Document crit = new Document("owner_id", oid).append("buddy_id", bid)
					.append("ts", timestamp).append("hash", hash);
			
			Document dto = new Document("owner", owner.toString()).append("owner_id", oid)
					.append("owner_domain_id", odid)
					.append("buddy", buddy.getBareJID().toString()).append("buddy_id", bid)
					.append("buddy_res", buddy.getResource())
					// adding date for aggregation
					.append("date", date)
					.append("direction", direction.name()).append("ts", timestamp)
					.append("type", type).append("msg", msg.toString())
					.append("hash", hash);
			
			if (storePlaintextBody) {
				String body = msg.getChildCData(MSG_BODY_PATH);
				if (body != null) {
					dto.append("body", body);
				}
			}
			
			if (tags != null && !tags.isEmpty()) {
				dto.append("tags", new ArrayList<String>(tags));
			}
			
			msgsCollection.updateOne(crit, new Document("$set", dto), new UpdateOptions().upsert(true));
		} catch (Exception ex) {
			log.log(Level.WARNING, "Problem adding new entry to DB: " + msg, ex);
		}
	}

	@Override
	public void deleteExpiredMessages(BareJID owner, LocalDateTime before) throws TigaseDBException {
		try {
			byte[] odid = generateId(owner.getDomain());
			long timestamp_long = before.toEpochSecond(ZoneOffset.UTC) * 1000;
			Document crit = new Document("owner_domain_id", odid)
					.append("ts", new Document("$lt", new Date(timestamp_long)));
		
			msgsCollection.deleteMany(crit);
		} catch (Exception ex) {
			throw new TigaseDBException("Cound not remove expired messages", ex);
		}
	}	
	
	@Override
	public List<Element> getCollections(BareJID owner, Criteria criteria) throws TigaseDBException {
		try {
			criteria.setOwner(owner);
			
			Document crit = criteria.getCriteriaDocument();
			List<Element> results = new ArrayList<Element>();

			List<Document> pipeline = new ArrayList<Document>();
			Document matchCrit = new Document("$match", crit);
			pipeline.add(matchCrit);
			Document groupCrit = new Document("$group", 
					new Document("_id", 
						new Document("ts", "$date").append("buddy", "$buddy"))
					.append("ts", new Document("$min", "$ts"))
					.append("buddy", new Document("$min", "$buddy"))
			);
			pipeline.add(groupCrit);
			Document countCrit = new Document("$group", new Document("_id", 1).append("count", new Document("$sum", 1)));
			pipeline.add(countCrit);

			AggregateIterable<Document> cursor = msgsCollection.aggregate(pipeline).allowDiskUse(true).useCursor(true);
			Document countDoc = cursor.first();
			int count = (countDoc != null) ? countDoc.getInteger("count") : 0;

			criteria.setSize(count);
			
			if (count > 0) {
				pipeline.clear();

				pipeline.add(matchCrit);
				pipeline.add(groupCrit);
				Document sort = new Document("$sort", new Document("ts", 1).append("buddy", 1));
				pipeline.add(sort);

				if (criteria.getOffset() > 0) {
					Document skipCrit = new Document("$skip", criteria.getOffset());
					pipeline.add(skipCrit);
				}
				
				Document limitCrit = new Document("$limit", criteria.getLimit());
				pipeline.add(limitCrit);

				cursor = msgsCollection.aggregate(pipeline).allowDiskUse(true).useCursor(true).batchSize(batchSize);
				
				for ( Document dto : cursor) {
					String buddy = (String) dto.get("buddy");
					Date ts = (Date) dto.get("ts");
					addCollectionToResults(results, criteria, buddy, ts, null);
				}
			}
			
			RSM rsm = criteria.getRSM();
			rsm.setResults(count, criteria.getOffset());
			if (!results.isEmpty()) {
				rsm.setFirst(String.valueOf(criteria.getOffset()));
				rsm.setLast(String.valueOf(criteria.getOffset() + (results.size() - 1)));
			}			
			
			return results;
		} catch (Exception ex) {
			throw new TigaseDBException("Cound not retrieve collections", ex);
		}
	}

	@Override
	public List<Element> getItems(BareJID owner, Criteria criteria) throws TigaseDBException {
		try {
			criteria.setOwner(owner);
			
			Document crit = criteria.getCriteriaDocument();
			List<Element> results = new ArrayList<Element>();

			long count = msgsCollection.count(crit);
			criteria.setSize((int) count);

			FindIterable<Document> cursor = msgsCollection.find(crit);
			if (criteria.getOffset() > 0) {
				cursor = cursor.skip(criteria.getOffset());
			}
			cursor = cursor.batchSize(batchSize).limit(criteria.getLimit()).sort(new Document("ts", 1));

			Iterator<Document> iter = cursor.iterator();
			if (iter.hasNext()) {
				Date startTimestamp = criteria.getStart();
				DomBuilderHandler domHandler = new DomBuilderHandler();
				while (iter.hasNext()) {
					Document dto = iter.next();

					String msgStr = (String) dto.get("msg");
					Date ts = (Date) dto.get("ts");
					Direction direction = Direction.valueOf((String) dto.get("direction"));

					if (startTimestamp == null)
						startTimestamp = ts;
					String with = (crit.containsKey("buddy")) ? null : (String) dto.get("buddy");
					
					parser.parse(domHandler, msgStr.toCharArray(), 0, msgStr.length());

					Queue<Element> queue = domHandler.getParsedElements();
					Element msg = null;
					while ((msg = queue.poll()) != null) {
						addMessageToResults(results, criteria, startTimestamp, msg, ts, direction, with);
					}
				}
			}
			
			RSM rsm = criteria.getRSM();
			rsm.setResults((int) count, criteria.getOffset());
			if (!results.isEmpty()) {
				rsm.setFirst(String.valueOf(criteria.getOffset()));
				rsm.setLast(String.valueOf(criteria.getOffset() + (results.size() - 1)));
			}			
			
			return results;
		} catch (Exception ex) {
			throw new TigaseDBException("Cound not retrieve collections", ex);
		}
	}

	@Override
	public void removeItems(BareJID owner, String withJid, Date start, Date end) throws TigaseDBException {
		try {
			byte[] oid = generateId(owner);
			byte[] wid = generateId(withJid);
			
			if (start == null) {
				start = new Date(0);
			}
			if (end == null) {
				end = new Date(0);
			}
			
			Document dateCrit = new Document("$gte", start).append("$lte", end);
			Document crit = new Document("owner_id", oid).append("owner", owner.toString())
					.append("buddy_id", wid).append("buddy", withJid).append("ts", dateCrit);
			
			msgsCollection.deleteMany(crit);
		} catch (Exception ex) {
			throw new TigaseDBException("Cound not remove items", ex);
		}
	}
	
	@Override
	public List<String> getTags(BareJID owner, String startsWith, Criteria criteria) throws TigaseDBException {
		List<String> results = new ArrayList<String>();
		try {
			byte[] oid = generateId(owner);
			Pattern tagPattern = Pattern.compile(startsWith + ".*");
			List<Document> pipeline = new ArrayList<Document>();
			Document crit = new Document("owner_id", oid).append("owner", owner.toString());
			Document matchCrit = new Document("$match", crit);
			pipeline.add(matchCrit);
			pipeline.add(new Document("$unwind", "$tags"));
			pipeline.add(new Document("$match", new Document("tags", tagPattern)));
			pipeline.add(new Document("$group", new Document("_id", "$tags")));
			pipeline.add(new Document("$group", new Document("_id", 1).append("count", new Document("$sum", 1))));
		
			AggregateIterable<Document> cursor = msgsCollection.aggregate(pipeline).allowDiskUse(true).useCursor(true);
			Document countDoc = cursor.first();
			int count = countDoc != null ? countDoc.getInteger("count") : null;

			criteria.setSize(count);
			if (count > 0) {
				pipeline.remove(pipeline.size() - 1);
				pipeline.add(new Document("$sort", new Document("_id", 1)));
				if (criteria.getOffset() > 0) {
					pipeline.add(new Document("$skip", criteria.getOffset()));
				}
				pipeline.add(new Document("$limit", criteria.getLimit()));
				cursor = msgsCollection.aggregate(pipeline).allowDiskUse(true).useCursor(true).batchSize(batchSize);
				for (Document dto : cursor) {
					results.add((String) dto.get("_id"));
				}

				RSM rsm = criteria.getRSM();
				rsm.setResults(count, criteria.getOffset());
				if (!results.isEmpty()) {
					rsm.setFirst(String.valueOf(criteria.getOffset()));
					rsm.setLast(String.valueOf(criteria.getOffset() + (results.size() - 1)));
				}
			}
		} catch (Exception ex) {
			throw new TigaseDBException("Could not retrieve list of used tags", ex);
		}
		return results;
	}

	@Override
	public void initRepository(String resource_uri, Map<String, String> params) throws DBInitException {
		try {
			if (params.containsKey(STORE_PLAINTEXT_BODY_KEY)) {
				storePlaintextBody = Boolean.parseBoolean(params.get(STORE_PLAINTEXT_BODY_KEY));
			} else {
				storePlaintextBody = true;
			}
			if (params.containsKey("batch-size")) {
				batchSize = Integer.parseInt(params.get("batch-size"));
			} else {
				batchSize = DEF_BATCH_SIZE;
			}

			resourceUri = resource_uri;
			MongoClientURI uri = new MongoClientURI(resource_uri);
			mongo = new MongoClient(uri);
			db = mongo.getDatabase(uri.getDatabase());

			if (!collectionExists(db, MSGS_COLLECTION)) {
				db.createCollection(MSGS_COLLECTION);
			}
			msgsCollection = db.getCollection(MSGS_COLLECTION);

			msgsCollection.createIndex(new Document("owner_id", 1).append("date", 1));
			msgsCollection.createIndex(new Document("owner_id", 1).append("buddy_id", 1).append("ts", 1));
			msgsCollection.createIndex(new Document("body", "text"));
			msgsCollection.createIndex(new Document("owner_id", 1).append("tags", 1));
			msgsCollection.createIndex(new Document("owner_id", 1).append("buddy_id", 1).append("ts", 1).append("hash", 1));
			msgsCollection.createIndex(new Document("owner_domain_id", 1).append("ts", 1));
		} catch (MongoException ex) {
			throw new DBInitException("Could not connect to MongoDB server using URI = " + resource_uri, ex);
		}
	}
	
	@Override
	public void destroy() {
		if (mongo != null) {
			// if there is instance of MongoClient then we should close it on destroy
			// to release resources
			mongo.close();
		}
	}
	
	@Override
	public AbstractCriteria newCriteriaInstance() {
		return new Criteria();
	}
	
	public static class Criteria extends AbstractCriteria<Date> {

		private BareJID owner;
		
		protected void setOwner(BareJID owner) {
			this.owner = owner;
		}
		
		@Override
		protected Date convertTimestamp(Date date) {
			return date;
		}
		
		protected Document getCriteriaDocument() throws TigaseDBException {
			byte[] oid = generateId(owner);
			Document crit = new Document("owner_id", oid).append("owner", owner.toString());
			
			if (getWith() != null) {
				String withJid = getWith();
				byte[] wid = generateId(withJid);
				crit.append("buddy_id", wid).append("buddy", withJid);
			}
			
			Document dateCrit = null;
			if (getStart() != null) {
				if (dateCrit == null) dateCrit = new Document();
				dateCrit.append("$gte", getStart());
			}
			if (getEnd() != null) {
				if (dateCrit == null) dateCrit = new Document();
				dateCrit.append("$lte", getEnd());
			}
			if (dateCrit != null) {
				crit.append("ts", dateCrit);
			}
			if (!getTags().isEmpty()) {
				crit.append("tags", new Document("$all", new ArrayList<String>(getTags())));
			}
			
			if (!getContains().isEmpty()) {
				StringBuilder containsSb = new StringBuilder();
				for (String contains : getContains()) {
					if (containsSb.length() > 0)
						containsSb.append(" ");
					containsSb.append(contains);
				}
				crit.append("$text", new Document("$search", containsSb.toString()));
			}
			
			return crit;
		}
		
	}
	
}
