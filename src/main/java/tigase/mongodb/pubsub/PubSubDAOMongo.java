/*
 * PubSubDAOMongo.java
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
package tigase.mongodb.pubsub;

import com.mongodb.MongoException;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.UpdateOptions;
import org.bson.Document;
import org.bson.types.Binary;
import org.bson.types.ObjectId;
import tigase.component.exceptions.RepositoryException;
import tigase.db.Repository;
import tigase.kernel.beans.config.ConfigField;
import tigase.mongodb.MongoDataSource;
import tigase.mongodb.RepositoryVersionAware;
import tigase.pubsub.AbstractNodeConfig;
import tigase.pubsub.Affiliation;
import tigase.pubsub.NodeType;
import tigase.pubsub.Subscription;
import tigase.pubsub.repository.*;
import tigase.pubsub.repository.stateless.NodeMeta;
import tigase.pubsub.repository.stateless.UsersAffiliation;
import tigase.pubsub.repository.stateless.UsersSubscription;
import tigase.util.TigaseStringprepException;
import tigase.xml.Element;
import tigase.xmpp.BareJID;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;

import static com.mongodb.client.model.Projections.include;
import static tigase.mongodb.Helper.collectionExists;
import static tigase.mongodb.Helper.indexCreateOrReplace;

/**
 *
 * @author andrzej
 */
@Repository.Meta( supportedUris = { "mongodb:.*" } )
public class PubSubDAOMongo extends PubSubDAO<ObjectId,MongoDataSource> implements RepositoryVersionAware {
	
	private static final String JID_HASH_ALG = "SHA-256";

	private static final int DEF_BATCH_SIZE = 100;

	public static final String PUBSUB_AFFILIATIONS = "tig_pubsub_affiliations";
	public static final String PUBSUB_ITEMS = "tig_pubsub_items";
	public static final String PUBSUB_NODES = "tig_pubsub_nodes";
	public static final String PUBSUB_SERVICE_JIDS = "tig_pubsub_service_jids";
	public static final String PUBSUB_SUBSCRIPTIONS = "tig_pubsub_subscriptions";

	private MongoDatabase db;

	private MongoCollection<Document> serviceJidsCollection;
	private MongoCollection<Document> subscriptionsCollection;
	private MongoCollection<Document> nodesCollection;
	private MongoCollection<Document> itemsCollecton;
	private MongoCollection<Document> affiliationsCollection;

	@ConfigField(desc = "Batch size", alias = "batch-size")
	private int batchSize = DEF_BATCH_SIZE;

	public PubSubDAOMongo() {
	}

	@Override
	public void updateSchema() throws RepositoryException {
		for (Document oldServiceDoc : serviceJidsCollection.find().batchSize(100)) {
			String oldServiceJid = (String) oldServiceDoc.get("service_jid");
			String newServiceJid = oldServiceJid.toLowerCase();

			if (oldServiceJid.equals(newServiceJid)) {
				continue;
			}

			byte[] oldServiceId = ((Binary) oldServiceDoc.get("_id")).getData();
			byte[] newServiceId = generateId(newServiceJid);

			Document updateQuery = new Document("service_jid_id", oldServiceId).append("service_jid", oldServiceJid);
			Document updateValues = new Document("service_jid_id", newServiceId).append("service_jid", newServiceJid);

			Arrays.asList(nodesCollection, itemsCollecton, subscriptionsCollection, affiliationsCollection)
					.forEach((col) -> {
						col.updateMany(updateQuery, new Document("$set", updateValues));
					});

			Document newServiceDoc = new Document(oldServiceDoc).append("_id", newServiceId)
					.append("service_jid", newServiceJid);

			serviceJidsCollection.insertOne(newServiceDoc);
			serviceJidsCollection.findOneAndDelete(oldServiceDoc);
		}

		for (Document node : nodesCollection.find().projection(include("owner")).batchSize(1000)) {
			String oldOwner = (String) node.get("owner");
			String newOwner = oldOwner.toLowerCase();

			if (oldOwner.equals(newOwner)) {
				continue;
			}

			nodesCollection.updateOne(node, new Document("$set", new Document("owner", newOwner)));
		}

		for (Document oldAffil : affiliationsCollection.find().projection(include("jid")).batchSize(1000)) {
			String oldJid = (String) oldAffil.get("jid");
			String newJid = oldJid.toLowerCase();

			if (oldJid.equals(newJid))
				continue;;

			byte[] newJidId = generateId(newJid);

			Document update = new Document("jid_id", newJidId).append("jid", newJid);
			affiliationsCollection.updateOne(oldAffil, new Document("$set", update));
		}

		for (Document oldSubs : subscriptionsCollection.find().projection(include("jid")).batchSize(1000)) {
			String oldJid = (String) oldSubs.get("jid");
			String newJid = oldJid.toLowerCase();

			if (oldJid.equals(newJid))
				continue;;

			byte[] newJidId = generateId(newJid);

			Document update = new Document("jid_id", newJidId).append("jid", newJid);
			subscriptionsCollection.updateOne(oldSubs, new Document("$set", update));
		}
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

		nodesCollection.createIndex(new Document("service_jid_id", 1).append("node_name_id", 1), new IndexOptions().unique(true));
		nodesCollection.createIndex(new Document("service_jid_id", 1).append("node_name_id", 1).append("collection", 1), new IndexOptions().unique(true));
		nodesCollection.createIndex(new Document("collection", 1));

		if (!collectionExists(db, PUBSUB_AFFILIATIONS)) {
			db.createCollection(PUBSUB_AFFILIATIONS);
		}
		affiliationsCollection = db.getCollection(PUBSUB_AFFILIATIONS);

		affiliationsCollection.createIndex(new Document("node_id", 1));
		affiliationsCollection.createIndex(new Document("node_id", 1).append("jid_id", 1), new IndexOptions().unique(true));

		if (!collectionExists(db, PUBSUB_SUBSCRIPTIONS)) {
			db.createCollection(PUBSUB_SUBSCRIPTIONS);
		}
		subscriptionsCollection = db.getCollection(PUBSUB_SUBSCRIPTIONS);

		subscriptionsCollection.createIndex(new Document("node_id", 1));
		subscriptionsCollection.createIndex(new Document("node_id", 1).append("jid_id", 1), new IndexOptions().unique(true));

		if (!collectionExists(db, PUBSUB_ITEMS)) {
			db.createCollection(PUBSUB_ITEMS);
		}
		itemsCollecton = db.getCollection(PUBSUB_ITEMS);

		itemsCollecton.createIndex(new Document("node_id", 1));
		itemsCollecton.createIndex(new Document("node_id", 1).append("item_id", 1), new IndexOptions().unique(true));
		itemsCollecton.createIndex(new Document("node_id", 1).append("creation_date", 1));
	}

	private byte[] generateId(String in) throws RepositoryException {
		try {
			MessageDigest md = MessageDigest.getInstance(JID_HASH_ALG);
			return md.digest(in.getBytes());
		} catch (NoSuchAlgorithmException ex) {
			throw new RepositoryException("Should not happen!!", ex);
		}
	}	

	private void ensureServiceJid(BareJID serviceJid) throws RepositoryException {
		String serviceId = serviceJid.toString().toLowerCase();
		byte[] id = generateId(serviceId);
		try {
			Document crit = new Document("_id", id).append("service_jid", serviceId);
			serviceJidsCollection.updateOne(crit, new Document("$set", crit), new UpdateOptions().upsert(true));
		} catch (MongoException ex) {
			throw new RepositoryException("Could not create entry for service jid " + serviceJid, ex);
		}
	}
	
	private Document createCrit(BareJID serviceJid, String nodeName) throws RepositoryException {
		String serviceId = serviceJid.toString().toLowerCase();
		byte[] serviceJidId = generateId(serviceId);
		Document crit = new Document("service_jid_id", serviceJidId).append("service_jid", serviceId);
		if (nodeName != null) {
			byte[] nodeNameId = generateId(nodeName);
			crit.append("node_name_id", nodeNameId).append("node_name", nodeName);
		}
		else {
			crit.append("node_name", new Document("$exists", false));
		}
		return crit;
	}
	
	@Override
	public void addToRootCollection(BareJID serviceJid, String nodeName) throws RepositoryException {
	}

	@Override
	public ObjectId createNode(BareJID serviceJid, String nodeName, BareJID ownerJid, AbstractNodeConfig nodeConfig, NodeType nodeType, ObjectId collectionId) throws RepositoryException {
		ensureServiceJid(serviceJid);
		try {
			String serializedNodeConfig = null;
			if ( nodeConfig != null ){
				nodeConfig.setNodeType( nodeType );
				serializedNodeConfig = nodeConfig.getFormElement().toString();
			}
			
			Document dto = createCrit(serviceJid, nodeName);
			dto.append("owner", ownerJid.toString().toLowerCase()).append("type", nodeType.name())
					.append("configuration", serializedNodeConfig).append("creation_time", new Date());
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

	@Override
	public String[] getAllNodesList(BareJID serviceJid) throws RepositoryException {
		try {
			String serviceId = serviceJid.toString().toLowerCase();
			byte[] serviceJidId = generateId(serviceId);
			Document crit = new Document("service_jid_id", serviceJidId).append("service_jid", serviceId);
			//List<String> result = db.getCollection(PUBSUB_NODES).distinct("node_name", crit);
			List<String> result = readAllValuesForField(nodesCollection, "node_name", crit);
			return result.toArray(new String[result.size()]);
		} catch (MongoException ex) {
			throw new RepositoryException("Could not retrieve list of all nodes", ex);
		}
	}

	@Override
	public Element getItem(BareJID serviceJid, ObjectId nodeId, String id) throws RepositoryException {
		try {			
			Document crit = new Document("node_id", nodeId).append("item_id", id);
			Document dto = itemsCollecton.find(crit).first();
			if (dto == null)
				return null;
			return itemDataToElement(((String) dto.get("item")).toCharArray());
		} catch (MongoException ex) {
			throw new RepositoryException("Error while retrieving item from repository", ex);
		}
	}

	@Override
	public Date getItemCreationDate(BareJID serviceJid, ObjectId nodeId, String id) throws RepositoryException {
		try {			
			Document crit = new Document("node_id", nodeId).append("item_id", id);
			Document dto = itemsCollecton.find(crit).projection(new Document("creation_date", 1)).first();
			if (dto == null)
				return null;
			return (Date) dto.get("creation_date");
		} catch (MongoException ex) {
			throw new RepositoryException("Error while retrieving item creation date from repository", ex);
		}
	}

	@Override
	public String[] getItemsIds(BareJID serviceJid, ObjectId nodeId) throws RepositoryException {
		try {			
			Document crit = new Document("node_id", nodeId);
//			List<String> ids = (List<String>) db.getCollection(PUBSUB_ITEMS).distinct("item_id", crit);
			List<String> ids = readAllValuesForField(itemsCollecton, "item_id", crit);
			return ids.toArray(new String[ids.size()]);
		} catch (MongoException ex) {
			throw new RepositoryException("Error while retrieving item ids from repository", ex);
		}
	}

	@Override
	public String[] getItemsIdsSince(BareJID serviceJid, ObjectId nodeId, Date since) throws RepositoryException {
		try {			
			Document crit = new Document("node_id", nodeId).append("$gte", new Document("creation_date", since));
//			List<String> ids = (List<String>) db.getCollection(PUBSUB_ITEMS).distinct("item_id", crit);
			List<String> ids = readAllValuesForField(itemsCollecton, "item_id", crit);
			return ids.toArray(new String[ids.size()]);
		} catch (MongoException ex) {
			throw new RepositoryException("Error while retrieving item ids since timestamp from repository", ex);
		}
	}

	@Override
	public List<IItems.ItemMeta> getItemsMeta(BareJID serviceJid, ObjectId nodeId, String nodeName) throws RepositoryException {
		try {			
			Document crit = new Document("node_id", nodeId);
			FindIterable<Document> cursor = itemsCollecton.find(crit).projection(new Document("item_id", 1).append("creation_date", 1));
			List<IItems.ItemMeta> results = new ArrayList<IItems.ItemMeta>();
			for (Document it : cursor) {
				results.add(new IItems.ItemMeta(nodeName, (String) it.get("item_id"), (Date) it.get("creation_date")));
			}
			return results;
		} catch (MongoException ex) {
			throw new RepositoryException("Error while retrieving item ids from repository", ex);
		}
	}

	@Override
	public Date getItemUpdateDate(BareJID serviceJid, ObjectId nodeId, String id) throws RepositoryException {
		try {			
			Document crit = new Document("node_id", nodeId).append("item_id", id);
			Document dto = itemsCollecton.find(crit).projection(new Document("update_date", 1)).first();
			if (dto == null)
				return null;
			return (Date) dto.get("update_date");
		} catch (MongoException ex) {
			throw new RepositoryException("Error while retrieving item update date from repository", ex);
		}
	}

	@Override
	public NodeAffiliations getNodeAffiliations(BareJID serviceJid, ObjectId nodeId) throws RepositoryException {
		try {
			Document crit = new Document("node_id", nodeId);
			FindIterable<Document> cursor = affiliationsCollection.find(crit).projection(new Document("jid", 1).append("affiliation", 1)).batchSize(batchSize);
			
			Queue<UsersAffiliation> data = new ArrayDeque<UsersAffiliation>();
			for (Document it : cursor) {
				BareJID jid = BareJID.bareJIDInstanceNS((String) it.get("jid"));
				Affiliation affil = Affiliation.valueOf((String) it.get("affiliation"));
				data.offer(new UsersAffiliation(jid, affil));
			}
			return NodeAffiliations.create(data);
		} catch (MongoException ex) {
			throw new RepositoryException("Could not retrieve node affiliations", ex);
		}
	}

	@Override
	public String getNodeConfig(BareJID serviceJid, ObjectId nodeId) throws RepositoryException {
		try {
			Document crit = new Document("_id", nodeId);
			Document result = nodesCollection.find(crit).first();
			return result == null ? null : (String) result.get("configuration");
		} catch (MongoException ex) {
			throw new RepositoryException("Could not retrieve node configuration", ex);
		}
	}

	@Override
	public ObjectId getNodeId(BareJID serviceJid, String nodeName) throws RepositoryException {
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
			if (result == null)
				return null;

			AbstractNodeConfig nodeConfig = parseConfig(nodeName, (String) result.get("configuration"));
			return new NodeMeta((ObjectId)result.get("_id"), nodeConfig,
					result.get("owner") != null ? BareJID.bareJIDInstance((String) result.get("owner")) : null, (Date) result.get("creation_time"));
		} catch (MongoException|TigaseStringprepException ex) {
			throw new RepositoryException("Could not retrieve node metadata", ex);
		}
	}

	@Override
	public String[] getNodesList(BareJID serviceJid, String nodeName) throws RepositoryException {
		ObjectId collectionId = nodeName == null ? null : getNodeId(serviceJid, nodeName);
		if (collectionId == null && nodeName != null) {
			return new String[0];
		}
		try {
			String serviceId = serviceJid.toString().toLowerCase();
			byte[] serviceJidId = generateId(serviceId);
			Document crit = new Document("service_jid_id", serviceJidId).append("service_jid", serviceId);
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
	public NodeSubscriptions getNodeSubscriptions(BareJID serviceJid, ObjectId nodeId) throws RepositoryException {
		try {
			Document crit = new Document("node_id", nodeId);
			FindIterable<Document> cursor = subscriptionsCollection.find(crit).projection(new Document("jid", 1).append("subscription", 1).append("subscription_id", 1)).batchSize(batchSize);
			
			Queue<UsersSubscription> data = new ArrayDeque<UsersSubscription>();
			for (Document it : cursor) {
				BareJID jid = BareJID.bareJIDInstanceNS((String) it.get("jid"));
				Subscription subscr = Subscription.valueOf((String) it.get("subscription"));
				String subscr_id = (String) it.get("subscription_id");
				data.offer(new UsersSubscription(jid, subscr_id, subscr));
			}
			NodeSubscriptions result = NodeSubscriptions.create();
			result.init(data);
			return result;
		} catch (MongoException ex) {
			throw new RepositoryException("Could not retrieve node affiliations", ex);
		}
	}

	@Override
	public String[] getChildNodes(BareJID serviceJid, String nodeName) throws RepositoryException {
		return getNodesList( serviceJid, nodeName );
	}

	@Override
	public Map<String, UsersAffiliation> getUserAffiliations(BareJID serviceJid, BareJID jid) throws RepositoryException {
		try {
			String serviceId = serviceJid.toString().toLowerCase();
			byte[] serviceJidId = generateId(serviceId);
			String jidStr = jid.toString().toLowerCase();
			byte[] jidId = generateId(jidStr);
			Document crit = new Document("service_jid_id", serviceJidId).append("service_jid", serviceId)
					.append("jid_id", jidId).append("jid", jidStr);
			FindIterable<Document> cursor = affiliationsCollection.find(crit).projection(new Document("node_name", 1).append("affiliation", 1)).batchSize(batchSize);
			
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
	public Map<String, UsersSubscription> getUserSubscriptions(BareJID serviceJid, BareJID jid) throws RepositoryException {
		try {
			String serviceId = serviceJid.toString().toLowerCase();
			byte[] serviceJidId = generateId(serviceId);
			String jidStr = jid.toString().toLowerCase();
			byte[] jidId = generateId(jidStr);
			Document crit = new Document("service_jid_id", serviceJidId).append("service_jid", serviceId)
					.append("jid_id", jidId).append("jid", jidStr);
			FindIterable<Document> cursor = subscriptionsCollection.find(crit).projection(new Document("node_name", 1).append("subscription", 1).append("subscription_id", 1)).batchSize(batchSize);
			
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
	public void removeAllFromRootCollection(BareJID serviceJid) throws RepositoryException {
		try {
			String serviceId = serviceJid.toString().toLowerCase();
			byte[] serviceJidId = generateId(serviceId);
			Document crit = new Document("service_jid_id", serviceJidId).append("service_jid", serviceId);
			itemsCollecton.deleteMany(crit);
			affiliationsCollection.deleteMany(crit);
			subscriptionsCollection.deleteMany(crit);
			crit = new Document("service_jid_id", serviceJidId).append("service_jid", serviceId);
			nodesCollection.deleteMany(crit);
		} catch (MongoException ex) {
			throw new RepositoryException("Could not remove all nodes from root collection", ex);
		}
	}

	@Override
	public void removeFromRootCollection(BareJID serviceJid, ObjectId nodeId) throws RepositoryException {
	}

	@Override
	public void removeNodeSubscription(BareJID serviceJid, ObjectId nodeId, BareJID jid) throws RepositoryException {
		try {
			String serviceId = serviceJid.toString().toLowerCase();
			byte[] serviceJidId = generateId(serviceId);
			String jidStr = jid.toString().toLowerCase();
			byte[] jidId = generateId(jidStr);
			Document crit = new Document("node_id", nodeId).append("service_jid_id", serviceJidId).append("service_jid", serviceId)
					.append("jid_id", jidId).append("jid", jidStr);

			subscriptionsCollection.deleteMany(crit);
		} catch (MongoException ex) {
			throw new RepositoryException("Could not remove user subscriptions", ex);
		}
	}

	@Override
	public void removeService(BareJID serviceJid) throws RepositoryException {
		try {
			removeAllFromRootCollection(serviceJid);

			String serviceId = serviceJid.toString().toLowerCase();
			byte[] jidId = generateId(serviceId);
			serviceJidsCollection.deleteOne(new Document("_id", jidId).append("service_jid", serviceId));
			Document crit = new Document("jid_id", jidId).append("jid", serviceId);
			affiliationsCollection.deleteMany(crit);
			subscriptionsCollection.deleteMany(crit);
		} catch (MongoException ex) {
			throw new RepositoryException("Could not remove service with jid = " + serviceJid, ex);
		}
	}
	
	@Override
	public void updateNodeConfig(BareJID serviceJid, ObjectId nodeId, String serializedNodeConfig, ObjectId collectionId) throws RepositoryException {
		try {			
			Document crit = new Document("_id", nodeId);
			Document set = new Document("configuration", serializedNodeConfig);
			Document dto = new Document("$set", set);
			if (collectionId != null) {
				set.append("collection", collectionId);
			}
			else {
				dto.append("$unset", new Document("collection", ""));
			}
			nodesCollection.updateOne(crit, dto);
		} catch (MongoException ex) {
			throw new RepositoryException("Error while updating node configuration in repository", ex);
		}
	}

	@Override
	public void updateNodeAffiliation(BareJID serviceJid, ObjectId nodeId, String nodeName, UsersAffiliation userAffiliation) throws RepositoryException {
		try {
			String serviceId = serviceJid.toString().toLowerCase();
			byte[] serviceJidId = generateId(serviceId);
			String jid = userAffiliation.getJid().toString().toLowerCase();
			byte[] jidId = generateId(jid);
			Document crit = new Document("node_id", nodeId).append("service_jid_id", serviceJidId).append("service_jid", serviceId)
					.append("jid_id", jidId).append("jid", jid);
			if (userAffiliation.getAffiliation() == Affiliation.none) {
				affiliationsCollection.deleteMany(crit);
			} else {
				affiliationsCollection.updateOne(crit, new Document("$setOnInsert", new Document("node_name", nodeName))
					.append("$set", new Document("affiliation", userAffiliation.getAffiliation().name())), new UpdateOptions().upsert(true));
			}
		} catch (MongoException ex) {
			throw new RepositoryException("Could not update user affiliations", ex);
		}
	}

	@Override
	public void updateNodeSubscription(BareJID serviceJid, ObjectId nodeId, String nodeName, UsersSubscription userSubscription) throws RepositoryException {
		try {
			String serviceId = serviceJid.toString().toLowerCase();
			byte[] serviceJidId = generateId(serviceId);
			String jid = userSubscription.getJid().toString().toLowerCase();
			byte[] jidId = generateId(jid);
			Document crit = new Document("node_id", nodeId).append("service_jid_id", serviceJidId).append("service_jid", serviceId)
					.append("jid_id", jidId).append("jid", jid);
			if (userSubscription.getSubscription() == Subscription.none) {
				subscriptionsCollection.deleteMany(crit);
			} else {
				subscriptionsCollection.updateOne(crit, new Document("$setOnInsert", new Document("node_name", nodeName))
						.append("$set", new Document("subscription", userSubscription.getSubscription().name()).append("subscription_id", userSubscription.getSubid())),
						new UpdateOptions().upsert(true));
			}
		} catch (MongoException ex) {
			throw new RepositoryException("Could not update user subscriptions", ex);
		}
	}

	@Override
	public void writeItem(BareJID serviceJid, ObjectId nodeId, long timeInMilis, String id, String publisher, Element item) throws RepositoryException {
		try {
			String serviceId = serviceJid.toString().toLowerCase();
			byte[] serviceJidId = generateId(serviceId);
			Document crit = new Document("service_jid_id", serviceJidId).append("service_jid", serviceId);
			crit.append("node_id", nodeId).append("item_id", id);
			Document dto = new Document("$set", new Document("update_date", new Date()).append("publisher", publisher).append("item", item.toString()));
			dto.append("$setOnInsert", new Document("creation_date", new Date()));
			itemsCollecton.updateOne(crit, dto, new UpdateOptions().upsert(true));
		} catch (MongoException ex) {
			throw new RepositoryException("Could not write item to repository", ex);
		}
	}

	protected <T> List<T> readAllValuesForField(MongoCollection<Document> collection, String field, Document crit) throws MongoException {
		FindIterable<Document> cursor = collection.find(crit).projection(new Document(field, 1)).batchSize(batchSize);

		List<T> result = new ArrayList<>();
		for (Document item : cursor) {
			T val = (T) item.get(field);
			result.add(val);
		}

		return result;
	}

}
