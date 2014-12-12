/*
 * MongoMessageArchiveRepository.java
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
package tigase.mongodb.archive;

import com.mongodb.AggregationOptions;
import com.mongodb.AggregationOptions.OutputMode;
import com.mongodb.BasicDBObject;
import com.mongodb.Cursor;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import tigase.archive.AbstractCriteria;
import tigase.archive.RSM;
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

/**
 *
 * @author andrzej
 */
@Repository.Meta( supportedUris = { "mongodb:.*" } )
public class MongoMessageArchiveRepository extends AbstractMessageArchiveRepository<Criteria> {

	private static final Logger log = Logger.getLogger(MongoMessageArchiveRepository.class.getCanonicalName());
		
	private static final String HASH_ALG = "SHA-256";
	private static final String[] MSG_BODY_PATH = { "message", "body" };	
	private static final String MSGS_COLLECTION = "tig_ma_msgs";
	private static final String STORE_PLAINTEXT_BODY_KEY = "store-plaintext-body";
	
	private static final SimpleParser parser      = SingletonFactory.getParserInstance();
	
	private String resourceUri;
	private MongoClient mongo;
	private DB db;	
	
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
	public void archiveMessage(BareJID owner, BareJID buddy, Direction direction, Date timestamp, Element msg, Set<String> tags) {
		try {
			byte[] oid = generateId(owner);
			byte[] bid = generateId(buddy);
			
			String type = msg.getAttributeStaticStr("type");
			Date date = new Date(timestamp.getTime() - (timestamp.getTime() % (24*60*60*1000)));
			
			BasicDBObject dto = new BasicDBObject("owner", owner.toString()).append("owner_id", oid)
					.append("buddy", buddy.toString()).append("buddy_id", bid)
					// adding date for aggregation
					.append("date", date)
					.append("direction", direction.name()).append("ts", timestamp)
					.append("type", type).append("msg", msg.toString());
			
			if (storePlaintextBody) {
				String body = msg.getChildCData(MSG_BODY_PATH);
				if (body != null) {
					dto.append("body", body);
				}
			}
			
			if (tags != null && !tags.isEmpty()) {
				dto.append("tags", new ArrayList<String>(tags));
			}
			
			db.getCollection(MSGS_COLLECTION).insert(dto);
		} catch (Exception ex) {
			log.log(Level.WARNING, "Problem adding new entry to DB: " + msg, ex);
		}
	}

	@Override
	public List<Element> getCollections(BareJID owner, Criteria criteria) throws TigaseDBException {
		Cursor cursor = null;
		try {
			criteria.setOwner(owner);
			
			BasicDBObject crit = criteria.getCriteriaDBObject();
			List<Element> results = new ArrayList<Element>();
//			byte[] oid = generateId(owner);
//			BasicDBObject crit = new BasicDBObject("owner_id", oid).append("owner", owner.toString());
//			
//			if (criteria.getWith() != null) {
//				String withJid = criteria.getWith();
//				byte[] wid = generateId(withJid);
//				crit.append("buddy_id", wid).append("buddy", withJid);
//			}
//			
//			BasicDBObject dateCrit = null;
//			if (criteria.getStart() != null) {
//				if (dateCrit == null) dateCrit = new BasicDBObject();
//				dateCrit.append("$gte", criteria.getStart());
//			}
//			if (criteria.getEnd() != null) {
//				if (dateCrit == null) dateCrit = new BasicDBObject();
//				dateCrit.append("$lte", criteria.getEnd());
//			}
//			if (dateCrit != null) {
//				crit.append("ts", dateCrit);
//			}
			
			List<DBObject> pipeline = new ArrayList<DBObject>();
			DBObject matchCrit = new BasicDBObject("$match", crit);
			pipeline.add(matchCrit);
			DBObject groupCrit = new BasicDBObject("$group", 
					new BasicDBObject("_id", 
						new BasicDBObject("ts", "$date").append("buddy", "$buddy"))
					.append("ts", new BasicDBObject("$min", "$ts"))
					.append("buddy", new BasicDBObject("$min", "$buddy"))
			);
			pipeline.add(groupCrit);
			DBObject countCrit = new BasicDBObject("$group", new BasicDBObject("_id", 1).append("count", new BasicDBObject("$sum", 1)));
			pipeline.add(countCrit);
			
			cursor = db.getCollection(MSGS_COLLECTION).aggregate(pipeline, AggregationOptions.builder().allowDiskUse(true).outputMode(OutputMode.CURSOR).build());
			int count = 0;
			if (cursor.hasNext()) {
				count = (Integer) cursor.next().get("count");
			}
			cursor.close();
			cursor = null;
			criteria.setSize(count);
			
			if (count > 0) {
				pipeline.clear();

				pipeline.add(matchCrit);
				pipeline.add(groupCrit);
				DBObject sort = new BasicDBObject("$sort", new BasicDBObject("ts", 1).append("buddy", 1));
				pipeline.add(sort);

				if (criteria.getOffset() > 0) {
					DBObject skipCrit = new BasicDBObject("$skip", criteria.getOffset());
					pipeline.add(skipCrit);
				}
				
				DBObject limitCrit = new BasicDBObject("$limit", criteria.getLimit());
				pipeline.add(limitCrit);

				cursor = db.getCollection(MSGS_COLLECTION).aggregate(pipeline, 
						AggregationOptions.builder().allowDiskUse(true).outputMode(OutputMode.CURSOR).build());
				
				while (cursor.hasNext()) {
					DBObject dto = cursor.next();
					String buddy = (String) dto.get("buddy");
					Date ts = (Date) dto.get("ts");
					addCollectionToResults(results, buddy, ts);
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
		} finally {
			if (cursor != null) {
				cursor.close();
			}
		}
	}

	@Override
	public List<Element> getItems(BareJID owner, Criteria criteria) throws TigaseDBException {
		DBCursor cursor = null;
		try {
			criteria.setOwner(owner);
			
			BasicDBObject crit = criteria.getCriteriaDBObject();
			List<Element> results = new ArrayList<Element>();
//			byte[] oid = generateId(owner);
//			byte[] wid = generateId(withJid);
//			BasicDBObject crit = new BasicDBObject("owner_id", oid).append("owner", owner.toString())
//					.append("buddy_id", wid).append("buddy", withJid);
//			
//			BasicDBObject dateCrit = new BasicDBObject("$gte", start);
//			if (end != null) {
//				dateCrit.append("$lte", end);
//			}
//			crit.append("ts", dateCrit);
//			
			cursor = db.getCollection(MSGS_COLLECTION).find(crit);
			int count = cursor.count();
			criteria.setSize(count);
//			
//			int index = rsm.getIndex() == null ? 0 : rsm.getIndex();
//			int limit = rsm.getMax();
//			if (rsm.getAfter() != null) {
//				int after = Integer.parseInt(rsm.getAfter());
//				// it is ok, if we go out of range we will return empty result
//				index = after + 1;
//			} else if (rsm.getBefore() != null) {
//				int before = Integer.parseInt(rsm.getBefore());
//				index = before - rsm.getMax();
//					// if we go out of range we need to set index to 0 and reduce limit
//				// to return proper results
//				if (index < 0) {
//					index = 0;
//					limit = before;
//				}
//			} else if (rsm.hasBefore()) {
//				index = count - rsm.getMax();
//				if (index < 0) {
//					index = 0;
//				}
//			}			
			
			if (criteria.getOffset() > 0) {
				cursor.skip(criteria.getOffset());
			}
			cursor.limit(criteria.getLimit()).sort(new BasicDBObject("ts", 1));
			
			if (cursor.hasNext()) {
				Date startTimestamp = criteria.getStart();
				DomBuilderHandler domHandler = new DomBuilderHandler();
				while (cursor.hasNext()) {
					DBObject dto = cursor.next();

					String msgStr = (String) dto.get("msg");
					Date ts = (Date) dto.get("ts");
					Direction direction = Direction.valueOf((String) dto.get("direction"));

					if (startTimestamp == null)
						startTimestamp = ts;
					String with = (crit.containsField("buddy")) ? null : (String) dto.get("buddy");
					
					parser.parse(domHandler, msgStr.toCharArray(), 0, msgStr.length());

					Queue<Element> queue = domHandler.getParsedElements();
					Element msg = null;
					while ((msg = queue.poll()) != null) {
						addMessageToResults(results, startTimestamp, msg, ts, direction, with);
					}
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
		} finally {
			if (cursor != null) {
				cursor.close();
			}
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
			
			BasicDBObject dateCrit = new BasicDBObject("$gte", start).append("$lte", end);
			BasicDBObject crit = new BasicDBObject("owner_id", oid).append("owner", owner.toString())
					.append("buddy_id", wid).append("buddy", withJid).append("ts", dateCrit);
			
			db.getCollection(MSGS_COLLECTION).remove(crit);
		} catch (Exception ex) {
			throw new TigaseDBException("Cound not remove items", ex);
		}
	}

	@Override
	public void initRepository(String resource_uri, Map<String, String> params) throws DBInitException {
		try {
			if (params.containsKey(STORE_PLAINTEXT_BODY_KEY)) {
				storePlaintextBody = Boolean.parseBoolean(params.get(STORE_PLAINTEXT_BODY_KEY));
			} else {
				storePlaintextBody = true;
			}			
			
			resourceUri = resource_uri;
			MongoClientURI uri = new MongoClientURI(resource_uri);
			mongo = new MongoClient(uri);
			db = mongo.getDB(uri.getDatabase());
			
			DBCollection msgs = !db.collectionExists(MSGS_COLLECTION)
					? db.createCollection(MSGS_COLLECTION, new BasicDBObject())
					: db.getCollection(MSGS_COLLECTION);
			
			msgs.createIndex(new BasicDBObject("owner_id", 1).append("date", 1));
			msgs.createIndex(new BasicDBObject("owner_id", 1).append("buddy_id", 1).append("ts", 1));
			msgs.createIndex(new BasicDBObject("body", "text"));
		} catch (UnknownHostException ex) {
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
		
		protected BasicDBObject getCriteriaDBObject() throws TigaseDBException {
			byte[] oid = generateId(owner);
			BasicDBObject crit = new BasicDBObject("owner_id", oid).append("owner", owner.toString());
			
			if (getWith() != null) {
				String withJid = getWith();
				byte[] wid = generateId(withJid);
				crit.append("buddy_id", wid).append("buddy", withJid);
			}
			
			BasicDBObject dateCrit = null;
			if (getStart() != null) {
				if (dateCrit == null) dateCrit = new BasicDBObject();
				dateCrit.append("$gte", getStart());
			}
			if (getEnd() != null) {
				if (dateCrit == null) dateCrit = new BasicDBObject();
				dateCrit.append("$lte", getEnd());
			}
			if (dateCrit != null) {
				crit.append("ts", dateCrit);
			}
			
			if (!getContains().isEmpty()) {
				StringBuilder containsSb = new StringBuilder();
				for (String contains : getContains()) {
					if (containsSb.length() > 0)
						containsSb.append(" ");
					containsSb.append(contains);
				}
				crit.append("$text", new BasicDBObject("$search", containsSb.toString()));
			}
			
			return crit;
		}
		
	}
	
}
