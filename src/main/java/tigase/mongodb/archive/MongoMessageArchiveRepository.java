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
package tigase.mongodb.archive;

import com.mongodb.client.AggregateIterable;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Projections;
import com.mongodb.client.model.UpdateOptions;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.Binary;
import tigase.archive.QueryCriteria;
import tigase.archive.db.AbstractMessageArchiveRepository;
import tigase.archive.db.MessageArchiveRepository;
import tigase.archive.db.Schema;
import tigase.component.exceptions.ComponentException;
import tigase.db.Repository;
import tigase.db.TigaseDBException;
import tigase.db.util.RepositoryVersionAware;
import tigase.db.util.SchemaLoader;
import tigase.kernel.beans.config.ConfigField;
import tigase.mongodb.MongoDataSource;
import tigase.mongodb.MongoRepositoryVersionAware;
import tigase.util.Version;
import tigase.xml.DomBuilderHandler;
import tigase.xml.Element;
import tigase.xml.SimpleParser;
import tigase.xml.SingletonFactory;
import tigase.xmpp.Authorization;
import tigase.xmpp.jid.BareJID;
import tigase.xmpp.jid.JID;
import tigase.xmpp.mam.MAMRepository;
import tigase.xmpp.mam.util.MAMUtil;
import tigase.xmpp.mam.util.Range;
import tigase.xmpp.rsm.RSM;

import java.nio.charset.Charset;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static com.mongodb.client.model.Accumulators.*;
import static com.mongodb.client.model.Aggregates.*;
import static com.mongodb.client.model.Sorts.ascending;
import static com.mongodb.client.model.Sorts.orderBy;
import static tigase.mongodb.Helper.collectionExists;

/**
 * @author andrzej
 */
@Repository.Meta(supportedUris = {"mongodb:.*"})
@Repository.SchemaId(id = Schema.MA_SCHEMA_ID, name = Schema.MA_SCHEMA_NAME, external = false)
@RepositoryVersionAware.SchemaVersion
public class MongoMessageArchiveRepository
		extends AbstractMessageArchiveRepository<QueryCriteria, MongoDataSource, MongoMessageArchiveRepository.MongoDBAddMessageAdditionalDataProvider>
		implements MongoRepositoryVersionAware {

	private static final Logger log = Logger.getLogger(MongoMessageArchiveRepository.class.getCanonicalName());

	private static final int DEF_BATCH_SIZE = 100;

	private static final String HASH_ALG = "SHA-256";
	private static final String[] MSG_BODY_PATH = {"message", "body"};
	private static final String MSGS_COLLECTION = "tig_ma_msgs";
	private static final String STORE_PLAINTEXT_BODY_KEY = "store-plaintext-body";
	private static final Charset UTF8 = Charset.forName("UTF-8");

	private static final SimpleParser parser = SingletonFactory.getParserInstance();
	@ConfigField(desc = "Batch size", alias = "batch-size")
	private int batchSize = DEF_BATCH_SIZE;
	private MongoDatabase db;
	private MongoCollection<Document> msgsCollection;
	@ConfigField(desc = "Store plaintext body in database", alias = STORE_PLAINTEXT_BODY_KEY)
	private boolean storePlaintextBody = true;

	private static byte[] calculateHash(String user) throws TigaseDBException {
		try {
			MessageDigest md = MessageDigest.getInstance(HASH_ALG);
			return md.digest(user.getBytes(UTF8));
		} catch (NoSuchAlgorithmException ex) {
			throw new TigaseDBException("Should not happen!!", ex);
		}
	}

	private static byte[] generateId(BareJID user) throws TigaseDBException {
		return calculateHash(user.toString().toLowerCase());
	}

	@Override
	protected void archiveMessage(BareJID ownerJid, BareJID buddyJid, Date timestamp, Element msg, String stableIdStr,
								  String stanzaId, String refStableId, Set<String> tags,
								  MongoDBAddMessageAdditionalDataProvider additionParametersProvider) {
		try {
			byte[] oid = generateId(ownerJid);
			byte[] bid = generateId(buddyJid);
			byte[] odid = calculateHash(ownerJid.getDomain());

			String type = msg.getAttributeStaticStr("type");
			Date date = new Date(timestamp.getTime() - (timestamp.getTime() % (24 * 60 * 60 * 1000)));

			UUID stableId = UUID.fromString(stableIdStr);

			Document crit = new Document("owner_id", oid).append("stable_id", stableId);

			Document dto = new Document("owner", ownerJid.toString()).append("owner_id", oid)
					.append("owner_domain_id", odid)
					.append("stable_id", stableId)
					.append("buddy", buddyJid.toString())
					.append("buddy_id", bid)
					// adding date for aggregation
					.append("date", date)
					.append("ts", timestamp)
					.append("msg", msg.toString());
			if (stableId != null) {
				dto.append("stanza_id", stableId);
			}
			if (refStableId != null) {
				dto.append("ref_stable_id", refStableId);
			}

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
			if (dateCrit == null) {
				dateCrit = new Document();
			}
			dateCrit.append("$gte", query.getStart());
		}
		if (query.getEnd() != null) {
			if (dateCrit == null) {
				dateCrit = new Document();
			}
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
				if (containsSb.length() > 0) {
					containsSb.append(" ");
				}
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
	public void archiveMessage(BareJID owner, JID buddy, Date timestamp, Element msg,
							   String stableId, Set tags) {
		super.archiveMessage(owner, buddy.getBareJID(), timestamp, msg, stableId, tags, null);
	}

	@Override
	public void deleteExpiredMessages(BareJID owner, LocalDateTime before) throws TigaseDBException {
		try {
			byte[] odid = calculateHash(owner.getDomain());
			long timestamp_long = before.toEpochSecond(ZoneOffset.UTC) * 1000;
			Document crit = new Document("owner_domain_id", odid).append("ts",
			                                                             new Document("$lt", new Date(timestamp_long)));

			msgsCollection.deleteMany(crit);
		} catch (Exception ex) {
			throw new TigaseDBException("Cound not remove expired messages", ex);
		}
	}

	@Override
	public String getStableId(BareJID owner, BareJID buddy, String stanzaId) throws TigaseDBException {
		return null;
	}

	private Integer getColletionPosition(String uid) {
		if (uid == null || uid.isEmpty()) {
			return null;
		}

		return Integer.parseInt(uid);
	}

	private Integer getItemPosition(String uid, QueryCriteria query, Document crit)
			throws TigaseDBException, ComponentException {
		if (uid == null || uid.isEmpty()) {
			return null;
		}

		System.out.println("getting position for " + uid);
		if (!query.getUseMessageIdInRsm()) {
			return Integer.parseInt(uid);
		}

		byte[] ownerId = generateId(query.getQuestionerJID().getBareJID());
		Bson idCrit = Filters.and(Filters.eq("owner_id", ownerId), Filters.eq("stable_id", UUID.fromString(uid)));

		FindIterable<Document> cursor = msgsCollection.find(idCrit).projection(Projections.include("ts"));
		Document doc = cursor.first();
		if (doc == null) {
			System.out.println("item with " + uid + " not found");
			return null;
		}
		
		Date ts = doc.getDate("ts");

		Document positionCrit = new Document(crit);
		positionCrit.append("ts", new Document("$lt", ts));

		long position = msgsCollection.count(positionCrit);

		System.out.println("got position " + position + " for " + uid);

		if (position < 0) {
			throw new ComponentException(Authorization.ITEM_NOT_FOUND, "Item with " + uid + " not found");
		}

		return (int) (position);
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
			calculateOffsetAndPosition(criteria, count, beforeStr == null ? null : Integer.parseInt(beforeStr),
			                           afterStr == null ? null : Integer.parseInt(afterStr));

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
				if (results != null && !results.isEmpty()) {
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

	@Override
	public void queryCollections(QueryCriteria query, CollectionHandler<QueryCriteria, Collection> collectionHandler)
			throws TigaseDBException {
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

				for (Document dto : cursor) {
					String buddy = (String) dto.get("buddy");
					Date ts = (Date) dto.get("ts");
					collectionHandler.collectionFound(query, new Collection() {
						@Override
						public Date getStartTs() {
							return ts;
						}

						@Override
						public String getWith() {
							return buddy;
						}
					});
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

	@Override
	public void queryItems(QueryCriteria query, ItemHandler<QueryCriteria, MAMRepository.Item> itemHandler)
			throws TigaseDBException {
		try {
			if (!query.getIds().isEmpty()) {
				BareJID owner = query.getQuestionerJID().getBareJID();
				byte[] oid = generateId(owner);
				Document crit = new Document("owner_id", oid);
				crit.append("stable_id", new Document("$in", query.getIds()
						.stream()
						.map(UUID::fromString)
						.collect(Collectors.toList())));

				FindIterable<Document> cursor = msgsCollection.find(crit);
				cursor = cursor.batchSize(batchSize).sort(new Document("ts", 1));
				query.getRsm().setIndex(0);

				handleQueryItemsResult(query, crit, cursor, itemHandler);
			} else {
				Document crit = createCriteriaDocument(query);
				int count = (int) msgsCollection.count(crit);

				Range range = MAMUtil.rangeFromPositions(getItemPosition(query.getAfterId(), query, crit),
														getItemPosition(query.getBeforeId(), query, crit));

				Integer afterPosRSM = getItemPosition(query.getRsm().getAfter(), query, crit);
				Integer beforePosRSM = getItemPosition(query.getRsm().getBefore(), query, crit);

				calculateOffsetAndPosition(query, count, beforePosRSM, afterPosRSM, range);
				
				FindIterable<Document> cursor = msgsCollection.find(crit);
				if (query.getRsm().getIndex() > 0 || range.getLowerBound() > 0) {
					cursor = cursor.skip(range.getLowerBound() + query.getRsm().getIndex());
				}
				cursor = cursor.batchSize(batchSize)
						.limit(Math.min(range.size(), query.getRsm().getMax()))
						.sort(new Document("ts", 1));

				handleQueryItemsResult(query, crit, cursor, itemHandler);
			}
		} catch (Exception ex) {
			throw new TigaseDBException("Cound not retrieve collections", ex);
		}
	}

	private void handleQueryItemsResult(QueryCriteria query, Document crit, FindIterable<Document> cursor, MAMRepository.ItemHandler itemHandler) {
		Iterator<Document> iter = cursor.iterator();
		if (iter.hasNext()) {
			int idx = query.getRsm().getIndex();
			int i = 0;
			Date startTimestamp = query.getStart();
			DomBuilderHandler domHandler = new DomBuilderHandler();
			while (iter.hasNext()) {
				Item item = new Item();
				item.owner = query.getQuestionerJID().getBareJID();
				Document dto = iter.next();

				String msgStr = (String) dto.get("msg");
				item.timestamp = (Date) dto.get("ts");

				item.with = (crit.containsKey("buddy")) ? null : (String) dto.get("buddy");
				if (query.getUseMessageIdInRsm()) {
					item.id = ((UUID) dto.get("stable_id")).toString();
				}

				parser.parse(domHandler, msgStr.toCharArray(), 0, msgStr.length());

				if (startTimestamp == null) {
					startTimestamp = item.timestamp;
				}

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
			if (query.getIds().isEmpty()) {
				query.setStart(startTimestamp);
			}
		}
	}

	@Override
	public void removeItems(BareJID owner, String with, Date start, Date end) throws TigaseDBException {
		try {
			byte[] oid = generateId(owner);

			List<Bson> crit = new ArrayList<>();
			crit.add(Filters.eq("owner_id", oid));

			if (with != null) {
				byte[] wid = calculateHash(with.toLowerCase());
				crit.add(Filters.eq("buddy_id", wid));
			}

			if (start != null) {
				crit.add(Filters.gte("ts", start));
			}
			if (end != null) {
				crit.add(Filters.lte("ts", end));
			}

			msgsCollection.deleteMany(Filters.and(crit));
		} catch (Exception ex) {
			throw new TigaseDBException("Cound not remove items", ex);
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
		msgsCollection.createIndex(new Document("owner_id", 1).append("ts", 1));
		msgsCollection.createIndex(new Document("body", "text"));
		msgsCollection.createIndex(new Document("owner_id", 1).append("tags", 1));
		msgsCollection.createIndex(new Document("owner_id", 1).append("stable_id", 1));
		msgsCollection.createIndex(new Document("owner_domain_id", 1).append("ts", 1));

		this.db = db;
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
		FindIterable<Document> msgsWithoutStableId = msgsCollection.find(Filters.exists("stable_id", false)).projection(Projections.include());
		for (Document doc: msgsWithoutStableId) {
			Document update = new Document("stable_id", UUID.randomUUID());
			msgsCollection.updateOne(doc, new Document("$set", update));
		}
		return SchemaLoader.Result.ok;
	}

	public static class Item<Q extends QueryCriteria>
			implements MessageArchiveRepository.Item {

		BareJID owner;
		String id;
		Element messageEl;
		Date timestamp;
		String with;

		@Override
		public Direction getDirection() {
			JID jid = JID.jidInstanceNS(getMessage().getAttributeStaticStr("to"));
			if (jid == null || !owner.equals(jid.getBareJID())) {
				return Direction.outgoing;
			} else {
				return Direction.incoming;
			}
		}

		@Override
		public String getId() {
			return id;
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

	public static class MongoDBAddMessageAdditionalDataProvider implements AbstractMessageArchiveRepository.AddMessageAdditionalDataProvider {

	}

}
