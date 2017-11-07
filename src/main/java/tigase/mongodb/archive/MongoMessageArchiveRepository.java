/*
 * MongoMessageArchiveRepository.java
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
package tigase.mongodb.archive;

import com.mongodb.client.AggregateIterable;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.UpdateOptions;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.Binary;
import org.bson.types.ObjectId;
import tigase.archive.QueryCriteria;
import tigase.archive.db.AbstractMessageArchiveRepository;
import tigase.archive.db.MessageArchiveRepository;
import tigase.archive.db.Schema;
import tigase.component.exceptions.ComponentException;
import tigase.db.Repository;
import tigase.db.TigaseDBException;
import tigase.db.util.SchemaLoader;
import tigase.kernel.beans.config.ConfigField;
import tigase.mongodb.MongoDataSource;
import tigase.db.util.RepositoryVersionAware;
import tigase.mongodb.MongoRepositoryVersionAware;
import tigase.util.Version;
import tigase.xml.DomBuilderHandler;
import tigase.xml.Element;
import tigase.xml.SimpleParser;
import tigase.xml.SingletonFactory;
import tigase.xmpp.Authorization;
import tigase.xmpp.jid.BareJID;
import tigase.xmpp.jid.JID;
import tigase.xmpp.rsm.RSM;
import tigase.xmpp.mam.MAMRepository;

import java.nio.charset.Charset;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Pattern;

import static com.mongodb.client.model.Sorts.ascending;
import static com.mongodb.client.model.Sorts.orderBy;
import static tigase.mongodb.Helper.collectionExists;

import static  com.mongodb.client.model.Accumulators.*;
import static com.mongodb.client.model.Aggregates.*;

/**
 *
 * @author andrzej
 */
@Repository.Meta( supportedUris = { "mongodb:.*" } )
@Repository.SchemaId(id = Schema.MA_SCHEMA_ID, name = Schema.MA_SCHEMA_NAME)
@RepositoryVersionAware.SchemaVersion
public class MongoMessageArchiveRepository
		extends AbstractMessageArchiveRepository<QueryCriteria, MongoDataSource>
		implements MongoRepositoryVersionAware {

	private static final Logger log = Logger.getLogger(MongoMessageArchiveRepository.class.getCanonicalName());

	private static final int DEF_BATCH_SIZE = 100;

	private static final String HASH_ALG = "SHA-256";
	private static final String[] MSG_BODY_PATH = { "message", "body" };	
	private static final String MSGS_COLLECTION = "tig_ma_msgs";
	private static final String STORE_PLAINTEXT_BODY_KEY = "store-plaintext-body";
	private static final Charset UTF8 = Charset.forName("UTF-8");

	private static final SimpleParser parser      = SingletonFactory.getParserInstance();
	
	private MongoDatabase db;
	private MongoCollection<Document> msgsCollection;

	@ConfigField(desc = "Batch size", alias = "batch-size")
	private int batchSize = DEF_BATCH_SIZE;

	@ConfigField(desc = "Store plaintext body in database", alias = STORE_PLAINTEXT_BODY_KEY)
	private boolean storePlaintextBody = true;


	private static byte[] generateId(BareJID user) throws TigaseDBException {
		return calculateHash(user.toString().toLowerCase());
	}

	private static byte[] calculateHash(String user) throws TigaseDBException {
		try {
			MessageDigest md = MessageDigest.getInstance(HASH_ALG);
			return md.digest(user.getBytes(UTF8));
		} catch (NoSuchAlgorithmException ex) {
			throw new TigaseDBException("Should not happen!!", ex);
		}
	}

	public void setDataSource(MongoDataSource dataSource) {
		MongoDatabase db = dataSource.getDatabase();
		if (!collectionExists(db, MSGS_COLLECTION)) {
			db.createCollection(MSGS_COLLECTION);
		}

		msgsCollection = db.getCollection(MSGS_COLLECTION);

		msgsCollection.createIndex(new Document("owner_id", 1).append("date", 1));
		msgsCollection.createIndex(new Document("owner_id", 1).append("buddy_id", 1).append("ts", 1));
		msgsCollection.createIndex(new Document("body", "text"));
		msgsCollection.createIndex(new Document("owner_id", 1).append("tags", 1));
		msgsCollection.createIndex(new Document("owner_id", 1).append("buddy_id", 1).append("hash", 1).append("ts", 1));
		msgsCollection.createIndex(new Document("owner_domain_id", 1).append("ts", 1));

		this.db = db;
	}
	
	@Override
	public void archiveMessage(BareJID ownerJid, JID buddyJid, Direction direction, Date timestamp, Element msg, Set<String> tags) {
		try {
			byte[] oid = generateId(ownerJid);
			byte[] bid = generateId(buddyJid.getBareJID());
			byte[] odid = calculateHash(ownerJid.getDomain());
			
			String type = msg.getAttributeStaticStr("type");
			Date date = new Date(timestamp.getTime() - (timestamp.getTime() % (24*60*60*1000)));
			byte[] hash = generateHashOfMessage(direction, msg, timestamp, null);
			
			Document crit = new Document("owner_id", oid).append("buddy_id", bid)
					.append("hash", hash);

			if (type == null || !"groupchat".equals(type)) {
				crit.append("ts", timestamp);
			} else {
				crit.append("ts", new Document("$gte", new Date(timestamp.getTime() - (30 * 60 * 1000)))
						.append("$lte", new Date(timestamp.getTime() + (30 * 60 * 1000))));
			}
			
			Document dto = new Document("owner", ownerJid.toString()).append("owner_id", oid)
					.append("owner_domain_id", odid)
					.append("buddy", buddyJid.getBareJID().toString())
					.append("buddy_id", bid)
					.append("buddy_res", buddyJid.getResource())
					// adding date for aggregation
					.append("date", date)
					.append("direction", direction.name())
					.append("ts", timestamp)
					.append("type", type)
					.append("msg", msg.toString())
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
			byte[] odid = calculateHash(owner.getDomain());
			long timestamp_long = before.toEpochSecond(ZoneOffset.UTC) * 1000;
			Document crit = new Document("owner_domain_id", odid)
					.append("ts", new Document("$lt", new Date(timestamp_long)));
		
			msgsCollection.deleteMany(crit);
		} catch (Exception ex) {
			throw new TigaseDBException("Cound not remove expired messages", ex);
		}
	}

	private Integer getColletionPosition(String uid) {
		if (uid == null || uid.isEmpty())
			return null;

		return Integer.parseInt(uid);
	}

	@Override
	public void queryCollections(QueryCriteria query, CollectionHandler<QueryCriteria> collectionHandler) throws TigaseDBException {
		try {
			Bson crit = createCriteriaDocument(query);
			List<Element> results = new ArrayList<Element>();

			List<Bson> pipeline = new ArrayList<>();
			Bson matchCrit = match(crit);
			pipeline.add(matchCrit);
			Bson groupCrit = group(new Document("ts", "$date").append("buddy", "$buddy"), min("ts", "$ts"),
								   min("buddy", "$buddy"));
			pipeline.add(groupCrit);
			Bson countCrit = group(1, sum("count", 1));
			pipeline.add(countCrit);

			AggregateIterable<Document> cursor = msgsCollection.aggregate(pipeline).allowDiskUse(true).useCursor(true);
			Document countDoc = cursor.first();
			int count = (countDoc != null) ? countDoc.getInteger("count") : 0;

			Integer after = getColletionPosition(query.getRsm().getAfter());
			Integer before = getColletionPosition(query.getRsm().getBefore());

			calculateOffsetAndPosition(query, count, before, after);

			if (count > 0) {
				pipeline.clear();

				pipeline.add(matchCrit);
				pipeline.add(groupCrit);
				Bson sort = sort(orderBy(ascending("ts", "buddy")));
				pipeline.add(sort);

				if (query.getRsm().getIndex() > 0) {
					pipeline.add(skip(query.getRsm().getIndex()));
				}
				pipeline.add(limit(query.getRsm().getMax()));

				cursor = msgsCollection.aggregate(pipeline).allowDiskUse(true).useCursor(true).batchSize(batchSize);
				
				for ( Document dto : cursor) {
					String buddy = (String) dto.get("buddy");
					Date ts = (Date) dto.get("ts");
					collectionHandler.collectionFound(query, buddy, ts, null);
				}
			}

			List<Element> collections = query.getCollections();
			if (collections != null) {
				int first = query.getRsm().getIndex();
				query.getRsm().setFirst(String.valueOf(first));
				query.getRsm().setLast(String.valueOf(first + collections.size() - 1));
			}
		} catch (Exception ex) {
			throw new TigaseDBException("Cound not retrieve collections", ex);
		}
	}

	private Integer getItemPosition(String uid, QueryCriteria query, Document crit) throws SQLException, ComponentException {
		if (uid == null || uid.isEmpty())
			return null;

		System.out.println("getting position for " + uid);
		if (!query.getUseMessageIdInRsm())
			return Integer.parseInt(uid);

		Document idCrit = new Document(crit);
		idCrit.append("hash", tigase.util.Base64.decode(uid));

		FindIterable<Document> cursor = msgsCollection.find(idCrit);
		Document doc = cursor.first();
		if (doc == null) {
			System.out.println("item with " + uid + " not found");
			return null;
		}
		ObjectId id = cursor.first().getObjectId("_id");

		Document positionCrit = new Document(crit);
		positionCrit.append("_id", new Document("$lt", id));

		long position = msgsCollection.count(positionCrit);

		System.out.println("got position " + position + " for " + uid);

		if (position < 0)
			throw new ComponentException(Authorization.BAD_REQUEST, "Item with " + uid + " not found");

		return (int) (position);
	}


	@Override
	public void queryItems(QueryCriteria query, ItemHandler<QueryCriteria, MAMRepository.Item> itemHandler) throws TigaseDBException {
		try {
			Document crit = createCriteriaDocument(query);
			List<Element> results = new ArrayList<Element>();

			int count = (int) msgsCollection.count(crit);

			Integer after = getItemPosition(query.getRsm().getAfter(), query, crit);
			Integer before = getItemPosition(query.getRsm().getBefore(), query, crit);

			calculateOffsetAndPosition(query, count, before, after);


			FindIterable<Document> cursor = msgsCollection.find(crit);
			if (query.getRsm().getIndex() > 0) {
				cursor = cursor.skip(query.getRsm().getIndex());
			}
			cursor = cursor.batchSize(batchSize).limit(query.getRsm().getMax()).sort(new Document("ts", 1));

			Iterator<Document> iter = cursor.iterator();
			if (iter.hasNext()) {
				int idx = query.getRsm().getIndex();
				int i = 0;
				Date startTimestamp = query.getStart();
				DomBuilderHandler domHandler = new DomBuilderHandler();
				Item item = new Item();
				while (iter.hasNext()) {
					Document dto = iter.next();

					String msgStr = (String) dto.get("msg");
					item.timestamp = (Date) dto.get("ts");
					item.direction = Direction.valueOf((String) dto.get("direction"));

					item.with = (crit.containsKey("buddy")) ? null : (String) dto.get("buddy");
					if (query.getUseMessageIdInRsm()) {
						item.id = tigase.util.Base64.encode(((Binary) dto.get("hash")).getData());
					}

					parser.parse(domHandler, msgStr.toCharArray(), 0, msgStr.length());

					if (startTimestamp == null)
						startTimestamp = item.timestamp;

					Queue<Element> queue = domHandler.getParsedElements();
					Element msg = null;
					while ((msg = queue.poll()) != null) {
						if (!query.getUseMessageIdInRsm()) {
							item.id = String.valueOf(idx + i);
						}
						item.messageEl = msg;
						itemHandler.itemFound(query, item);
					}
					i++;
				}
				query.setStart(startTimestamp);
			}
		} catch (Exception ex) {
			throw new TigaseDBException("Cound not retrieve collections", ex);
		}
	}

	@Override
	public void removeItems(BareJID owner, String with, Date start, Date end) throws TigaseDBException {
		try {
			byte[] oid = generateId(owner);
			byte[] wid = calculateHash(with.toLowerCase());
			
			if (start == null) {
				start = new Date(0);
			}
			if (end == null) {
				end = new Date(0);
			}
			
			Document dateCrit = new Document("$gte", start).append("$lte", end);
			Document crit = new Document("owner_id", oid)
					.append("buddy_id", wid).append("ts", dateCrit);
			
			msgsCollection.deleteMany(crit);
		} catch (Exception ex) {
			throw new TigaseDBException("Cound not remove items", ex);
		}
	}
	
	@Override
	public List<String> getTags(BareJID owner, String startsWith, QueryCriteria criteria) throws TigaseDBException {
		List<String> results = new ArrayList<String>();
		try {
			byte[] oid = generateId(owner);
			Pattern tagPattern = Pattern.compile(startsWith + ".*");
			List<Document> pipeline = new ArrayList<Document>();
			Document crit = new Document("owner_id", oid);
			Document matchCrit = new Document("$match", crit);
			pipeline.add(matchCrit);
			pipeline.add(new Document("$unwind", "$tags"));
			pipeline.add(new Document("$match", new Document("tags", tagPattern)));
			pipeline.add(new Document("$group", new Document("_id", "$tags")));
			pipeline.add(new Document("$group", new Document("_id", 1).append("count", new Document("$sum", 1))));
		
			AggregateIterable<Document> cursor = msgsCollection.aggregate(pipeline).allowDiskUse(true).useCursor(true);
			Document countDoc = cursor.first();
			int count = countDoc != null ? countDoc.getInteger("count") : null;

			String beforeStr = criteria.getRsm().getBefore();
			String afterStr = criteria.getRsm().getAfter();
			calculateOffsetAndPosition(criteria, count, beforeStr == null ? null : Integer.parseInt(beforeStr), afterStr == null ? null : Integer.parseInt(afterStr));

			if (count > 0) {
				pipeline.remove(pipeline.size() - 1);
				pipeline.add(new Document("$sort", new Document("_id", 1)));
				if (criteria.getRsm().getIndex() > 0) {
					pipeline.add(new Document("$skip", criteria.getRsm().getIndex()));
				}
				pipeline.add(new Document("$limit", criteria.getRsm().getMax()));
				cursor = msgsCollection.aggregate(pipeline).allowDiskUse(true).useCursor(true).batchSize(batchSize);
				for (Document dto : cursor) {
					results.add((String) dto.get("_id"));
				}

				RSM rsm = criteria.getRsm();
				rsm.setResults(count, rsm.getIndex());
				if (results!= null && !results.isEmpty()) {
					rsm.setFirst(String.valueOf(rsm.getIndex()));
					rsm.setLast(String.valueOf(rsm.getIndex() + (results.size() - 1)));
				}
			}
		} catch (Exception ex) {
			throw new TigaseDBException("Could not retrieve list of used tags", ex);
		}
		return results;
	}

	@Override
	public QueryCriteria newQuery() {
		return new QueryCriteria();
	}

	public Document createCriteriaDocument(QueryCriteria query) throws TigaseDBException {
		BareJID owner = query.getQuestionerJID().getBareJID();
		byte[] oid = generateId(owner);
		Document crit = new Document("owner_id", oid);

		if (query.getWith() != null) {
			byte[] wid = generateId(query.getWith().getBareJID());
			crit.append("buddy_id", wid);
		}

		Document dateCrit = null;
		if (query.getStart() != null) {
			if (dateCrit == null) dateCrit = new Document();
			dateCrit.append("$gte", query.getStart());
		}
		if (query.getEnd() != null) {
			if (dateCrit == null) dateCrit = new Document();
			dateCrit.append("$lte", query.getEnd());
		}
		if (dateCrit != null) {
			crit.append("ts", dateCrit);
		}
		if (!query.getTags().isEmpty()) {
			crit.append("tags", new Document("$all", new ArrayList<>(query.getTags())));
		}

		if (!query.getContains().isEmpty()) {
			StringBuilder containsSb = new StringBuilder();
			for (String contains : query.getContains()) {
				if (containsSb.length() > 0)
					containsSb.append(" ");
				if (contains.contains(" ")) {
					containsSb.append("\"");
					containsSb.append(contains);
					containsSb.append("\"");
				} else {
					containsSb.append(contains);
				}
			}
			crit.append("$text", new Document("$search", containsSb.toString()));
		}

		return crit;
	}

	@Override
	public SchemaLoader.Result updateSchema(Optional<Version> oldVersion, Version newVersion) throws TigaseDBException {
		List<Bson> ownerAggregation = Arrays.asList(group("$owner_id", first("owner", "$owner")));
		for (Document doc : msgsCollection.aggregate(ownerAggregation).allowDiskUse(true).batchSize(1000)) {
			String owner = (String) doc.get("owner");
			byte[] oldOwnerId = ((Binary) doc.get("_id")).getData();
			byte[] newOwnerId = calculateHash(owner.toLowerCase());

			if (Arrays.equals(oldOwnerId, newOwnerId)) {
				continue;
			}

			Document update = new Document("owner_id", newOwnerId);
			msgsCollection.updateMany(new Document("owner_id", oldOwnerId), new Document("$set", update));
		}

		List<Bson> buddyAggregation = Arrays.asList(group("$buddy_id", first("buddy", "$buddy")));
		for (Document doc : msgsCollection.aggregate(buddyAggregation).allowDiskUse(true).batchSize(1000)) {
			String buddy = (String) doc.get("buddy");
			byte[] oldBuddyId = ((Binary) doc.get("_id")).getData();
			byte[] newBuddyId = calculateHash(buddy.toLowerCase());

			if (Arrays.equals(oldBuddyId, newBuddyId)) {
				continue;
			}

			Document update = new Document("buddy_id", newBuddyId);
			msgsCollection.updateMany(new Document("buddy_id", oldBuddyId), new Document("$set", update));
		}
		return SchemaLoader.Result.ok;
	}

	public static class Item<Q extends QueryCriteria> implements MessageArchiveRepository.Item {
		String id;
		Element messageEl;
		Date timestamp;
		Direction direction;
		String with;

		@Override
		public String getId() {
			return id;
		}

		@Override
		public Direction getDirection() {
			return direction;
		}

		@Override
		public Element getMessage() {
			return messageEl;
		}

		@Override
		public Date getTimestamp() {
			return timestamp;
		}

		@Override
		public String getWith() {
			return with;
		}
	}


}
