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
package tigase.mongodb.pubsub;

import com.mongodb.MongoException;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.*;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.Binary;
import org.bson.types.ObjectId;
import tigase.component.exceptions.ComponentException;
import tigase.component.exceptions.RepositoryException;
import tigase.db.Repository;
import tigase.db.util.RepositoryVersionAware;
import tigase.db.util.SchemaLoader;
import tigase.kernel.beans.config.ConfigField;
import tigase.mongodb.MongoDataSource;
import tigase.mongodb.MongoRepositoryVersionAware;
import tigase.pubsub.*;
import tigase.pubsub.modules.mam.Query;
import tigase.pubsub.repository.*;
import tigase.pubsub.repository.stateless.NodeMeta;
import tigase.pubsub.repository.stateless.UsersAffiliation;
import tigase.pubsub.repository.stateless.UsersSubscription;
import tigase.util.Version;
import tigase.util.stringprep.TigaseStringprepException;
import tigase.xml.Element;
import tigase.xmpp.Authorization;
import tigase.xmpp.jid.BareJID;
import tigase.xmpp.mam.MAMRepository;
import tigase.xmpp.rsm.RSM;

import java.nio.charset.Charset;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;

import static com.mongodb.client.model.Projections.include;
import static tigase.mongodb.Helper.collectionExists;
import static tigase.mongodb.Helper.indexCreateOrReplace;

/**
 * @author andrzej
 */
@Repository.Meta(supportedUris = {"mongodb:.*"})
@Repository.SchemaId(id = Schema.PUBSUB_SCHEMA_ID, name = Schema.PUBSUB_SCHEMA_NAME, external = false)
@RepositoryVersionAware.SchemaVersion
public class PubSubDAOMongo
		extends PubSubDAO<ObjectId, MongoDataSource, tigase.pubsub.modules.mam.Query>
		implements MongoRepositoryVersionAware {

	public static final String PUBSUB_AFFILIATIONS = "tig_pubsub_affiliations";
	public static final String PUBSUB_ITEMS = "tig_pubsub_items";
	public static final String PUBSUB_MAM = "tig_pubsub_mam";
	public static final String PUBSUB_NODES = "tig_pubsub_nodes";
	public static final String PUBSUB_SERVICE_JIDS = "tig_pubsub_service_jids";
	public static final String PUBSUB_SUBSCRIPTIONS = "tig_pubsub_subscriptions";
	private static final String JID_HASH_ALG = "SHA-256";
	private static final Charset UTF8 = Charset.forName("UTF-8");
	private static final int DEF_BATCH_SIZE = 100;
	private MongoCollection<Document> affiliationsCollection;
	@ConfigField(desc = "Batch size", alias = "batch-size")
	private int batchSize = DEF_BATCH_SIZE;
	private MongoDatabase db;
	private MongoCollection<Document> itemsCollecton;
	private MongoCollection<Document> nodesCollection;
	private MongoCollection<Document> serviceJidsCollection;
	private MongoCollection<Document> subscriptionsCollection;
	private MongoCollection<Document> mamCollection;

	public PubSubDAOMongo() {
	}
	
	private byte[] calculateHash(String in) throws RepositoryException {
		try {
			MessageDigest md = MessageDigest.getInstance(JID_HASH_ALG);
			return md.digest(in.getBytes(UTF8));
		} catch (NoSuchAlgorithmException ex) {
			throw new RepositoryException("Should not happen!!", ex);
		}
	}

	private Document createCrit(BareJID serviceJid, String nodeName) throws RepositoryException {
		byte[] serviceJidId = generateId(serviceJid);
		Document crit = new Document("service_jid_id", serviceJidId);
		if (nodeName != null) {
			byte[] nodeNameId = calculateHash(nodeName);
			crit.append("node_name_id", nodeNameId).append("node_name", nodeName);
		} else {
			crit.append("node_name", new Document("$exists", false));
		}
		return crit;
	}
	
	@Override
	public ObjectId createNode(BareJID serviceJid, String nodeName, BareJID ownerJid, AbstractNodeConfig nodeConfig,
	                           NodeType nodeType, ObjectId collectionId, boolean autocreate) throws RepositoryException {
		ensureServiceJid(serviceJid, autocreate);
		try {
			String serializedNodeConfig = null;
			if (nodeConfig != null) {
				nodeConfig.setNodeType(nodeType);
				serializedNodeConfig = nodeConfig.getFormElement().toString();
			}

			Document dto = createCrit(serviceJid, nodeName);
			dto.append("service_jid", serviceJid.toString())
					.append("owner", ownerJid.toString())
					.append("type", nodeType.name())
					.append("configuration", serializedNodeConfig)
					.append("creation_time", new Date());
			if (collectionId != null) {
				dto.append("collection", collectionId);
			}
			ObjectId id = new ObjectId();
			dto.append("_id", id);
			nodesCollection.insertOne(dto);
			return id;
		} catch (MongoException ex) {
			throw new RepositoryException("Error while adding node to repository", ex);
		}
	}

	@Override
	public void createService(BareJID serviceJid, boolean isPublic) throws RepositoryException {
		try {
			byte[] id = generateId(serviceJid);
			Document crit = new Document("_id", id).append("service_jid", serviceJid.toString())
					.append("domain", serviceJid.getDomain())
					.append("is_public", isPublic ? 1 : 0);
			serviceJidsCollection.insertOne(crit);
		} catch (MongoException ex) {
			throw new RepositoryException("Could not create service", ex);
		}
	}

	@Override
	public void deleteItem(BareJID serviceJid, ObjectId nodeId, String id) throws RepositoryException {
		try {
			Document crit = new Document("node_id", nodeId).append("item_id", id);
			itemsCollecton.deleteOne(crit);
		} catch (MongoException ex) {
			throw new RepositoryException("Error while deleting node from repository", ex);
		}
	}

	@Override
	public void deleteNode(BareJID serviceJid, ObjectId nodeId) throws RepositoryException {
		try {
			Document crit = new Document("node_id", nodeId);
			itemsCollecton.deleteMany(crit);
			affiliationsCollection.deleteMany(crit);
			subscriptionsCollection.deleteMany(crit);
			nodesCollection.deleteOne(new Document("_id", nodeId));
		} catch (MongoException ex) {
			throw new RepositoryException("Could not retrieve node id", ex);
		}
	}

	private void ensureServiceJid(BareJID serviceJid, boolean autoCreate) throws RepositoryException {
		try {
			byte[] id = generateId(serviceJid);
			if (serviceJidsCollection.count(Filters.and(Filters.eq("_id", id), Filters.eq("service_jid", serviceJid.toString()))) == 0) {
				// no service jid
				if (autoCreate) {
					this.createService(serviceJid, false);
				}
			}
		} catch (MongoException ex) {
			throw new RepositoryException("Could not create entry for service jid " + serviceJid, ex);
		}
	}

	private byte[] generateId(BareJID jid) throws RepositoryException {
		return calculateHash(jid.toString().toLowerCase());
	}

	@Override
	public String[] getAllNodesList(BareJID serviceJid) throws RepositoryException {
		try {
			byte[] serviceJidId = generateId(serviceJid);
			Document crit = new Document("service_jid_id", serviceJidId);
			//List<String> result = db.getCollection(PUBSUB_NODES).distinct("node_name", crit);
			List<String> result = readAllValuesForField(nodesCollection, "node_name", crit);
			return result.toArray(new String[result.size()]);
		} catch (MongoException ex) {
			throw new RepositoryException("Could not retrieve list of all nodes", ex);
		}
	}

	@Override
	public String[] getChildNodes(BareJID serviceJid, String nodeName) throws RepositoryException {
		return getNodesList(serviceJid, nodeName);
	}

	@Override
	public IItems.IItem getItem(BareJID serviceJid, ObjectId nodeId, String id) throws RepositoryException {
		try {
			Document crit = new Document("node_id", nodeId).append("item_id", id);
			Document dto = itemsCollecton.find(crit).first();
			if (dto == null) {
				return null;
			}

			return new IItems.Item(dto.getString("node"), id,
								   Optional.ofNullable((UUID) dto.get("uuid")).map(String::valueOf).orElse(null),
								   itemDataToElement(((String) dto.get("item")).toCharArray()));
		} catch (MongoException ex) {
			throw new RepositoryException("Error while retrieving item from repository", ex);
		}
	}

	@Override
	public List<IItems.IItem> getItems(BareJID serviceJid, List<ObjectId> nodeIds, Date afterDate, Date beforeDate, RSM rsm,
									   CollectionItemsOrdering collectionItemsOrdering) throws RepositoryException {
		try {
			List<Bson> filters = new ArrayList<>();
			filters.add(Filters.in("node_id", nodeIds));
			String timestampField =
					collectionItemsOrdering == CollectionItemsOrdering.byCreationDate ? "creation_date" : "update_date";
			if (afterDate != null) {
				filters.add(Filters.gte(timestampField, afterDate));
			}
			if (beforeDate != null) {
				filters.add(Filters.lte(timestampField, beforeDate));
			}

			Bson filter = Filters.and(filters);
			long count = itemsCollecton.count(filter);

			Long after = getItemPosition(rsm.getAfter(), filter, timestampField);
			Long before = getItemPosition(rsm.getBefore(), filter, timestampField);

			calculateOffsetAndPosition(rsm, (int) count, before == null ? null : before.intValue(),
									   after == null ? null : after.intValue());

			Map<ObjectId, String> nodes = new HashMap<>();
			for (Document dto : nodesCollection.find(Filters.in("_id", nodeIds)).batchSize(batchSize).projection(Projections.include("node_name"))) {
				nodes.put(dto.getObjectId("_id"), dto.getString("node_name"));
			}
			
			List<IItems.IItem> items = new ArrayList<>();
			Document order = new Document(timestampField, 1);
			FindIterable<Document> cursor = itemsCollecton.find(filter).sort(order).skip(rsm.getIndex()).limit(rsm.getMax());
			for (Document dto : cursor) {
				String node = nodes.get(dto.getObjectId("node_id"));
				String id = dto.getString("item_id");
				String uuid = Optional.ofNullable((UUID) dto.get("uuid")).map(UUID::toString).orElse(null);
				Element itemEl = itemDataToElement(dto.getString("data"));

				items.add(new IItems.Item(node, id, uuid, itemEl));
			}
			return items;
		} catch (MongoException ex) {
			throw new RepositoryException(ex.getMessage(), ex);
		} catch (ComponentException ex) {
			throw new RepositoryException(ex.getMessage(), ex);
		}
	}

	private Long getItemPosition(String msgId, Bson filter, String timestampField) throws ComponentException {
		if (msgId == null) {
			return null;
		}

		ObjectId id = new ObjectId(msgId);
		Document doc = itemsCollecton.find(Filters.eq("_id", id))
				.projection(Projections.include(timestampField))
				.first();
		if (doc == null) {
			throw new ComponentException(Authorization.ITEM_NOT_FOUND, "Not found item with id = " + msgId);
		}
		Date ts = doc.getDate(timestampField);

		return itemsCollecton.count(Filters.and(filter, Filters.lt(timestampField, ts)));
	}

	private Long getMAMItemPosition(String msgId, Bson filter) throws ComponentException {
		if (msgId == null) {
			return null;
		}

		ObjectId id = new ObjectId(msgId);
		Document doc = mamCollection.find(Filters.eq("_id", id))
				.projection(Projections.include("ts"))
				.first();
		if (doc == null) {
			throw new ComponentException(Authorization.ITEM_NOT_FOUND, "Not found item with id = " + msgId);
		}
		Date ts = doc.getDate("ts");

		return mamCollection.count(Filters.and(filter, Filters.lt("ts", ts)));
	}
	
	@Override
	public String[] getItemsIds(BareJID serviceJid, ObjectId nodeId, CollectionItemsOrdering order) throws RepositoryException {
		try {
			Bson filter = Filters.eq("node_id", nodeId);
			Bson sort = Sorts.ascending(order == CollectionItemsOrdering.byCreationDate ? "creation_date" : "update_date");

			List<String> ids = readAllValuesForField(itemsCollecton, "item_id", filter, sort);
			return ids.toArray(new String[ids.size()]);
		} catch (MongoException ex) {
			throw new RepositoryException("Error while retrieving item ids from repository", ex);
		}
	}

	@Override
	public String[] getItemsIdsSince(BareJID serviceJid, ObjectId nodeId, CollectionItemsOrdering order, Date since) throws RepositoryException {
		try {
			String orderField = order == CollectionItemsOrdering.byCreationDate ? "creation_date" : "update_date";
			Bson filter = Filters.and(Filters.eq("node_id", nodeId), Filters.gte(orderField, since));
			Bson sort = Sorts.ascending(orderField);
			List<String> ids = readAllValuesForField(itemsCollecton, "item_id", filter, sort);
			return ids.toArray(new String[ids.size()]);
		} catch (MongoException ex) {
			throw new RepositoryException("Error while retrieving item ids since timestamp from repository", ex);
		}
	}

	@Override
	public List<IItems.ItemMeta> getItemsMeta(BareJID serviceJid, ObjectId nodeId, String nodeName)
			throws RepositoryException {
		try {
			Document crit = new Document("node_id", nodeId);
			FindIterable<Document> cursor = itemsCollecton.find(crit)
					.projection(new Document("item_id", 1).append("creation_date", 1));
			List<IItems.ItemMeta> results = new ArrayList<IItems.ItemMeta>();
			for (Document it : cursor) {
				results.add(new IItems.ItemMeta(nodeName, (String) it.get("item_id"), (Date) it.get("creation_date"),
												(Date) it.get("update_date"), Optional.ofNullable((UUID) it.get("uuid"))
														.map(UUID::toString)
														.orElse(null)));
			}
			return results;
		} catch (MongoException ex) {
			throw new RepositoryException("Error while retrieving item ids from repository", ex);
		}
	}
	
	@Override
	public Map<BareJID, UsersAffiliation> getNodeAffiliations(BareJID serviceJid, ObjectId nodeId) throws RepositoryException {
		try {
			Document crit = new Document("node_id", nodeId);
			FindIterable<Document> cursor = affiliationsCollection.find(crit)
					.projection(new Document("jid", 1).append("affiliation", 1))
					.batchSize(batchSize);

			Map<BareJID, UsersAffiliation> data = new HashMap<>();
			for (Document it : cursor) {
				BareJID jid = BareJID.bareJIDInstanceNS((String) it.get("jid"));
				Affiliation affil = Affiliation.valueOf((String) it.get("affiliation"));
				data.put(jid, new UsersAffiliation(jid, affil));
			}
			return data;
		} catch (MongoException ex) {
			throw new RepositoryException("Could not retrieve node affiliations", ex);
		}
	}

	private ObjectId getNodeId(BareJID serviceJid, String nodeName) throws RepositoryException {
		try {
			Document crit = createCrit(serviceJid, nodeName);
			Document result = nodesCollection.find(crit).first();
			return result == null ? null : (ObjectId) result.get("_id");
		} catch (MongoException ex) {
			throw new RepositoryException("Could not retrieve node id", ex);
		}
	}

	@Override
	public INodeMeta<ObjectId> getNodeMeta(BareJID serviceJid, String nodeName) throws RepositoryException {
		try {
			Document crit = createCrit(serviceJid, nodeName);
			Document result = nodesCollection.find(crit).first();
			if (result == null) {
				return null;
			}

			AbstractNodeConfig nodeConfig = parseConfig(nodeName, (String) result.get("configuration"));
			return new NodeMeta((ObjectId) result.get("_id"), nodeConfig, result.get("owner") != null
			                                                              ? BareJID.bareJIDInstance(
					(String) result.get("owner")) : null, (Date) result.get("creation_time"));
		} catch (MongoException | TigaseStringprepException ex) {
			throw new RepositoryException("Could not retrieve node metadata", ex);
		}
	}

	@Override
	public Map<BareJID, UsersSubscription> getNodeSubscriptions(BareJID serviceJid, ObjectId nodeId) throws RepositoryException {
		try {
			Document crit = new Document("node_id", nodeId);
			FindIterable<Document> cursor = subscriptionsCollection.find(crit)
					.projection(new Document("jid", 1).append("subscription", 1).append("subscription_id", 1))
					.batchSize(batchSize);

			Map<BareJID, UsersSubscription> data = new HashMap<>();
			for (Document it : cursor) {
				BareJID jid = BareJID.bareJIDInstanceNS((String) it.get("jid"));
				Subscription subscr = Subscription.valueOf((String) it.get("subscription"));
				String subscr_id = (String) it.get("subscription_id");
				data.put(jid, new UsersSubscription(jid, subscr_id, subscr));
			}
			return data;
		} catch (MongoException ex) {
			throw new RepositoryException("Could not retrieve node affiliations", ex);
		}
	}

	@Override
	public long getNodesCount(BareJID serviceJid) throws RepositoryException {
		try {
			if (serviceJid == null) {
				return nodesCollection.count();
			} else {
				return nodesCollection.count(createCrit(serviceJid, null));
			}
		} catch (MongoException ex) {
			throw new RepositoryException("Could not count nodes", ex);
		}
	}

	@Override
	public String[] getNodesList(BareJID serviceJid, String nodeName) throws RepositoryException {
		ObjectId collectionId = nodeName == null ? null : getNodeId(serviceJid, nodeName);
		if (collectionId == null && nodeName != null) {
			return new String[0];
		}
		try {
			byte[] serviceJidId = generateId(serviceJid);
			Document crit = new Document("service_jid_id", serviceJidId);
			if (collectionId != null) {
				crit.append("collection", collectionId);
			} else {
				crit.append("collection", new Document("$exists", false));
			}
//			List<String> result = db.getCollection(PUBSUB_NODES).distinct("node_name", crit);
			List<String> result = readAllValuesForField(nodesCollection, "node_name", crit);
			return result.toArray(new String[result.size()]);
		} catch (MongoException ex) {
			throw new RepositoryException("Could not retrieve list of all nodes", ex);
		}
	}

	@Override
	public Map<String, UsersAffiliation> getUserAffiliations(BareJID serviceJid, BareJID jid)
			throws RepositoryException {
		try {
			byte[] serviceJidId = generateId(serviceJid);
			byte[] jidId = generateId(jid);
			Document crit = new Document("service_jid_id", serviceJidId).append("jid_id", jidId);
			FindIterable<Document> cursor = affiliationsCollection.find(crit)
					.projection(new Document("node_name", 1).append("affiliation", 1))
					.batchSize(batchSize);

			Map<String, UsersAffiliation> result = new HashMap<String, UsersAffiliation>();
			for (Document it : cursor) {
				String node = (String) it.get("node_name");
				Affiliation affil = Affiliation.valueOf((String) it.get("affiliation"));
				result.put(node, new UsersAffiliation(jid, affil));
			}
			return result;
		} catch (MongoException ex) {
			throw new RepositoryException("Could not retrieve user affiliations", ex);
		}
	}

	@Override
	public Map<String, UsersSubscription> getUserSubscriptions(BareJID serviceJid, BareJID jid)
			throws RepositoryException {
		try {
			byte[] serviceJidId = generateId(serviceJid);
			byte[] jidId = generateId(jid);
			Document crit = new Document("service_jid_id", serviceJidId).append("jid_id", jidId);
			FindIterable<Document> cursor = subscriptionsCollection.find(crit)
					.projection(new Document("node_name", 1).append("subscription", 1).append("subscription_id", 1))
					.batchSize(batchSize);

			Map<String, UsersSubscription> result = new HashMap<String, UsersSubscription>();
			for (Document it : cursor) {
				String node = (String) it.get("node_name");
				Subscription subscr = Subscription.valueOf((String) it.get("subscription"));
				String subscr_id = (String) it.get("subscription_id");
				result.put(node, new UsersSubscription(jid, subscr_id, subscr));
			}
			return result;
		} catch (MongoException ex) {
			throw new RepositoryException("Could not retrieve user affiliations", ex);
		}
	}

	@Override
	public void addMAMItem(BareJID serviceJid, ObjectId nodeId, String uuid, Element message, String itemId)
			throws RepositoryException {
		try {
			mamCollection.insertOne(new Document("node_id", nodeId).append("uuid", UUID.fromString(uuid))
											.append("data", message.toString())
											.append("item_id", itemId));
		} catch (MongoException ex) {
			throw new RepositoryException("Could not insert MAM entry", ex);
		}
	}

	@Override
	public void queryItems(Query query, ObjectId nodeId,
	                       MAMRepository.ItemHandler<Query, IPubSubRepository.Item> itemHandler)
			throws RepositoryException {
		try {
			List<Bson> filters = new ArrayList<>();
			filters.add(Filters.eq("node_id", nodeId));
			String timestampField = "ts";
			if (query.getStart() != null) {
				filters.add(Filters.gte(timestampField, query.getStart()));
			}
			if (query.getEnd() != null) {
				filters.add(Filters.lte(timestampField, query.getEnd()));
			}

			Bson filter = Filters.and(filters);
			long count = mamCollection.count(filter);

			Long after = getMAMItemPosition(query.getRsm().getAfter(), filter);
			Long before = getMAMItemPosition(query.getRsm().getBefore(), filter);

			calculateOffsetAndPosition(query.getRsm(), (int) count, before == null ? null : before.intValue(),
			                           after == null ? null : after.intValue());
			
			Document order = new Document(timestampField, 1);
			FindIterable<Document> cursor = mamCollection.find(filter)
					.sort(order)
					.skip(query.getRsm().getIndex())
					.limit(query.getRsm().getMax());
			for (Document dto : cursor) {
				UUID uuid = (UUID) dto.get("uuid");
				Date ts = dto.getDate("ts");
				Element itemEl = itemDataToElement(dto.getString("data"));

				itemHandler.itemFound(query, new MAMItem(uuid.toString(),  ts, itemEl));
			}
		} catch (Exception ex) {
			throw new RepositoryException(ex);
		}
	}

	protected <T> List<T> readAllValuesForField(MongoCollection<Document> collection, String field, Bson filter)
			throws MongoException {
		FindIterable<Document> cursor = collection.find(filter).projection(new Document(field, 1)).batchSize(batchSize);

		List<T> result = new ArrayList<>();
		for (Document item : cursor) {
			T val = (T) item.get(field);
			result.add(val);
		}

		return result;
	}

	protected <T> List<T> readAllValuesForField(MongoCollection<Document> collection, String field, Bson filter, Bson sort)
			throws MongoException {
		FindIterable<Document> cursor = collection.find(filter).sort(sort).projection(new Document(field, 1)).batchSize(batchSize);

		List<T> result = new ArrayList<>();
		for (Document item : cursor) {
			T val = (T) item.get(field);
			result.add(val);
		}

		return result;
	}

	public void removeAllFromRootCollection(BareJID serviceJid) throws RepositoryException {
		try {
			byte[] serviceJidId = generateId(serviceJid);
			Document crit = new Document("service_jid_id", serviceJidId);
			itemsCollecton.deleteMany(crit);
			affiliationsCollection.deleteMany(crit);
			subscriptionsCollection.deleteMany(crit);
			crit = new Document("service_jid_id", serviceJidId);
			nodesCollection.deleteMany(crit);
		} catch (MongoException ex) {
			throw new RepositoryException("Could not remove all nodes from root collection", ex);
		}
	}

	@Override
	public void removeNodeSubscription(BareJID serviceJid, ObjectId nodeId, BareJID jid) throws RepositoryException {
		try {
			byte[] serviceJidId = generateId(serviceJid);
			byte[] jidId = generateId(jid);
			Document crit = new Document("node_id", nodeId).append("service_jid_id", serviceJidId)
					.append("jid_id", jidId);

			subscriptionsCollection.deleteMany(crit);
		} catch (MongoException ex) {
			throw new RepositoryException("Could not remove user subscriptions", ex);
		}
	}

	@Override
	public void deleteService(BareJID serviceJid) throws RepositoryException {
		try {
			removeAllFromRootCollection(serviceJid);

			byte[] jidId = generateId(serviceJid);
			serviceJidsCollection.deleteOne(new Document("_id", jidId));
			Document crit = new Document("jid_id", jidId);
			affiliationsCollection.deleteMany(crit);
			subscriptionsCollection.deleteMany(crit);
		} catch (MongoException ex) {
			throw new RepositoryException("Could not remove service with jid = " + serviceJid, ex);
		}
	}

	@Override
	public List<BareJID> getServices(BareJID bareJID, Boolean isPublic) throws RepositoryException {
		 Bson filter = Filters.eq("domain", bareJID.getDomain());
		if (isPublic != null) {
			filter = Filters.and(filter, Filters.eq("is_public", isPublic ? 1 : 0));
		}
		List<BareJID> result = new ArrayList<>();
		for(Document doc : serviceJidsCollection.find(filter).batchSize(batchSize).projection(Projections.include("service_jid"))) {
			result.add(BareJID.bareJIDInstanceNS(doc.getString("service_jid")));
		}
		return result;
	}

	public void setDataSource(MongoDataSource dataSource) {
		db = dataSource.getDatabase();

		if (!collectionExists(db, PUBSUB_SERVICE_JIDS)) {
			db.createCollection(PUBSUB_SERVICE_JIDS);
		}
		serviceJidsCollection = db.getCollection(PUBSUB_SERVICE_JIDS);

		indexCreateOrReplace(serviceJidsCollection, new Document("service_jid", 1), new IndexOptions().unique(true));

		if (!collectionExists(db, PUBSUB_NODES)) {
			db.createCollection(PUBSUB_NODES);
		}
		nodesCollection = db.getCollection(PUBSUB_NODES);

		nodesCollection.createIndex(new Document("service_jid_id", 1).append("node_name_id", 1),
		                            new IndexOptions().unique(true));
		nodesCollection.createIndex(new Document("service_jid_id", 1).append("node_name_id", 1).append("collection", 1),
		                            new IndexOptions().unique(true));
		nodesCollection.createIndex(new Document("collection", 1));

		if (!collectionExists(db, PUBSUB_AFFILIATIONS)) {
			db.createCollection(PUBSUB_AFFILIATIONS);
		}
		affiliationsCollection = db.getCollection(PUBSUB_AFFILIATIONS);

		affiliationsCollection.createIndex(new Document("node_id", 1));
		affiliationsCollection.createIndex(new Document("node_id", 1).append("jid_id", 1),
		                                   new IndexOptions().unique(true));

		if (!collectionExists(db, PUBSUB_SUBSCRIPTIONS)) {
			db.createCollection(PUBSUB_SUBSCRIPTIONS);
		}
		subscriptionsCollection = db.getCollection(PUBSUB_SUBSCRIPTIONS);

		subscriptionsCollection.createIndex(new Document("node_id", 1));
		subscriptionsCollection.createIndex(new Document("node_id", 1).append("jid_id", 1),
		                                    new IndexOptions().unique(true));

		if (!collectionExists(db, PUBSUB_ITEMS)) {
			db.createCollection(PUBSUB_ITEMS);
		}
		itemsCollecton = db.getCollection(PUBSUB_ITEMS);

		itemsCollecton.createIndex(new Document("node_id", 1));
		itemsCollecton.createIndex(new Document("node_id", 1).append("item_id", 1), new IndexOptions().unique(true));
		itemsCollecton.createIndex(new Document("node_id", 1).append("creation_date", 1));

		if (!collectionExists(db, PUBSUB_MAM)) {
			db.createCollection(PUBSUB_MAM);
		}
		mamCollection = db.getCollection(PUBSUB_MAM);
	}

	@Override
	public void updateNodeAffiliation(BareJID serviceJid, ObjectId nodeId, String nodeName,
	                                  UsersAffiliation userAffiliation) throws RepositoryException {
		try {
			byte[] serviceJidId = generateId(serviceJid);
			byte[] jidId = generateId(userAffiliation.getJid());
			Document crit = new Document("node_id", nodeId).append("service_jid_id", serviceJidId)
					.append("jid_id", jidId);
			if (userAffiliation.getAffiliation() == Affiliation.none) {
				affiliationsCollection.deleteMany(crit);
			} else {
				affiliationsCollection.updateOne(crit, new Document("$setOnInsert", new Document("node_name", nodeName))
						.append("$set", new Document("affiliation", userAffiliation.getAffiliation().name()).append(
								"service_jid", serviceJid.toString())
								.append("jid", userAffiliation.getJid().toString())), new UpdateOptions().upsert(true));
			}
		} catch (MongoException ex) {
			throw new RepositoryException("Could not update user affiliations", ex);
		}
	}

	@Override
	public void updateNodeConfig(BareJID serviceJid, ObjectId nodeId, String serializedNodeConfig,
	                             ObjectId collectionId) throws RepositoryException {
		try {
			Document crit = new Document("_id", nodeId);
			Document set = new Document("configuration", serializedNodeConfig);
			Document dto = new Document("$set", set);
			if (collectionId != null) {
				set.append("collection", collectionId);
			} else {
				dto.append("$unset", new Document("collection", ""));
			}
			nodesCollection.updateOne(crit, dto);
		} catch (MongoException ex) {
			throw new RepositoryException("Error while updating node configuration in repository", ex);
		}
	}

	@Override
	public void updateNodeSubscription(BareJID serviceJid, ObjectId nodeId, String nodeName,
	                                   UsersSubscription userSubscription) throws RepositoryException {
		try {
			byte[] serviceJidId = generateId(serviceJid);
			byte[] jidId = generateId(userSubscription.getJid());
			Document crit = new Document("node_id", nodeId).append("service_jid_id", serviceJidId)
					.append("jid_id", jidId);
			if (userSubscription.getSubscription() == Subscription.none) {
				subscriptionsCollection.deleteMany(crit);
			} else {
				subscriptionsCollection.updateOne(crit, new Document("$setOnInsert",
				                                                     new Document("node_name", nodeName)).append("$set",
				                                                                                                 new Document(
						                                                                                                 "subscription",
						                                                                                                 userSubscription
								                                                                                                 .getSubscription()
								                                                                                                 .name())
						                                                                                                 .append("subscription_id",
						                                                                                                         userSubscription
								                                                                                                         .getSubid())
						                                                                                                 .append("service_jid",
						                                                                                                         serviceJid
								                                                                                                         .toString())
						                                                                                                 .append("jid",
						                                                                                                         userSubscription
								                                                                                                         .getJid()
								                                                                                                         .toString())),
				                                  new UpdateOptions().upsert(true));
			}
		} catch (MongoException ex) {
			throw new RepositoryException("Could not update user subscriptions", ex);
		}
	}

	@Override
	public SchemaLoader.Result updateSchema(Optional<Version> oldVersion, Version newVersion)
			throws RepositoryException {
		for (Document oldServiceDoc : serviceJidsCollection.find().batchSize(100)) {
			String serviceJid = (String) oldServiceDoc.get("service_jid");

			byte[] oldServiceId = ((Binary) oldServiceDoc.get("_id")).getData();
			byte[] newServiceId = calculateHash(serviceJid.toLowerCase());

			if (Arrays.equals(oldServiceId, newServiceId)) {
				continue;
			}

			Document updateQuery = new Document("service_jid_id", oldServiceId);
			Document updateValues = new Document("service_jid_id", newServiceId);

			Arrays.asList(nodesCollection, itemsCollecton, subscriptionsCollection, affiliationsCollection)
					.forEach((col) -> {
						col.updateMany(updateQuery, new Document("$set", updateValues));
					});

			Document newServiceDoc = new Document(oldServiceDoc).append("_id", newServiceId);

			serviceJidsCollection.findOneAndDelete(oldServiceDoc);
			serviceJidsCollection.insertOne(newServiceDoc);
		}

		for (Document oldAffil : affiliationsCollection.find().projection(include("jid", "jid_id")).batchSize(1000)) {
			String jid = (String) oldAffil.get("jid");

			byte[] oldJidId = ((Binary) oldAffil.get("jid_id")).getData();
			byte[] newJidId = calculateHash(jid.toLowerCase());

			if (Arrays.equals(oldJidId, newJidId)) {
				continue;
			}

			Document update = new Document("jid_id", newJidId);
			affiliationsCollection.updateOne(oldAffil, new Document("$set", update));
		}

		for (Document oldSubs : subscriptionsCollection.find().projection(include("jid", "jid_id")).batchSize(1000)) {
			String jid = (String) oldSubs.get("jid");

			byte[] oldJidId = ((Binary) oldSubs.get("jid_id")).getData();
			byte[] newJidId = calculateHash(jid.toLowerCase());

			if (Arrays.equals(oldJidId, newJidId)) {
				continue;
			}

			Document update = new Document("jid_id", newJidId);
			subscriptionsCollection.updateOne(oldSubs, new Document("$set", update));
		}
		return SchemaLoader.Result.ok;
	}

	@Override
	public void writeItem(BareJID serviceJid, ObjectId nodeId, long timeInMilis, String id, String publisher,
	                      Element item, String uuid) throws RepositoryException {
		try {
			byte[] serviceJidId = generateId(serviceJid);
			Document crit = new Document("service_jid_id", serviceJidId).append("service_jid", serviceJid.toString());
			crit.append("node_id", nodeId).append("item_id", id);
			Document dto = new Document("$set", new Document("update_date", new Date()).append("publisher", publisher)
					.append("item", item.toString()).append("uuid", uuid == null ? null : UUID.fromString(uuid)));
			dto.append("$setOnInsert", new Document("creation_date", new Date()));
			itemsCollecton.updateOne(crit, dto, new UpdateOptions().upsert(true));
		} catch (MongoException ex) {
			throw new RepositoryException("Could not write item to repository", ex);
		}
	}

}
