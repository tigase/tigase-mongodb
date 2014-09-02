/*
 * PubSubDAOMongo.java
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
package tigase.mongodb.pubsub;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.MongoException;
import com.mongodb.WriteResult;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import org.bson.types.ObjectId;
import tigase.db.DBInitException;
import tigase.db.Repository;
import tigase.db.UserRepository;
import tigase.pubsub.AbstractNodeConfig;
import tigase.pubsub.Affiliation;
import tigase.pubsub.NodeType;
import tigase.pubsub.PubSubConfig;
import tigase.pubsub.Subscription;
import tigase.pubsub.repository.IItems;
import tigase.pubsub.repository.NodeAffiliations;
import tigase.pubsub.repository.NodeSubscriptions;
import tigase.pubsub.repository.PubSubDAO;
import tigase.pubsub.repository.RepositoryException;
import tigase.pubsub.repository.stateless.UsersAffiliation;
import tigase.pubsub.repository.stateless.UsersSubscription;
import tigase.xml.Element;
import tigase.xmpp.BareJID;

/**
 *
 * @author andrzej
 */
@Repository.Meta( supportedUris = { "mongodb:.*" } )
public class PubSubDAOMongo extends PubSubDAO<ObjectId> {
	
	private static final String JID_HASH_ALG = "SHA-256";
	
	private static final String PUBSUB_AFFILIATIONS = "tig_pubsub_affiliations";
	private static final String PUBSUB_ITEMS = "tig_pubsub_items";
	private static final String PUBSUB_NODES = "tig_pubsub_nodes";
	private static final String PUBSUB_SERVICE_JIDS = "tig_pubsub_service_jids";
	private static final String PUBSUB_SUBSCRIPTIONS = "tig_pubsub_subscriptions";

	private MongoClient mongo;
	private DB db;
	private String resourceUri;
	
	public PubSubDAOMongo() {
	}

	@Override
	public void initRepository(String resource_uri, Map<String, String> params) throws DBInitException {
		this.resourceUri = resource_uri;
		try {
			MongoClientURI uri = new MongoClientURI(resourceUri);
			//uri.get
			mongo = new MongoClient(uri);
			db = mongo.getDB(uri.getDatabase());
			
			DBCollection serviceJids = null;
			if (!db.collectionExists(PUBSUB_SERVICE_JIDS)) {
				serviceJids = db.createCollection(PUBSUB_SERVICE_JIDS, new BasicDBObject());
			} else {
				serviceJids = db.getCollection(PUBSUB_SERVICE_JIDS);
			}
			serviceJids.createIndex(new BasicDBObject("service_jid", 1));
			
			DBCollection nodes = null;
			if (!db.collectionExists(PUBSUB_NODES)) {
				nodes = db.createCollection(PUBSUB_NODES, new BasicDBObject());
			} else {
				nodes = db.getCollection(PUBSUB_NODES);
			}
			nodes.createIndex(new BasicDBObject("service_jid_id", 1).append("node_name_id", 1), new BasicDBObject("unique", true));
			nodes.createIndex(new BasicDBObject("collection", 1));
			
			DBCollection affiliations = null;
			if (!db.collectionExists(PUBSUB_AFFILIATIONS)) {
				affiliations = db.createCollection(PUBSUB_AFFILIATIONS, new BasicDBObject());
			} else {
				affiliations = db.getCollection(PUBSUB_AFFILIATIONS);
			}
			affiliations.createIndex(new BasicDBObject("node_id", 1), new BasicDBObject());
			affiliations.createIndex(new BasicDBObject("node_id", 1).append("jid_id", 1), new BasicDBObject("unique", true));
			
			DBCollection subscriptions = null;
			if (!db.collectionExists(PUBSUB_SUBSCRIPTIONS)) {
				subscriptions = db.createCollection(PUBSUB_SUBSCRIPTIONS, new BasicDBObject());
			} else {
				subscriptions = db.getCollection(PUBSUB_SUBSCRIPTIONS);
			}
			subscriptions.createIndex(new BasicDBObject("node_id", 1), new BasicDBObject());
			subscriptions.createIndex(new BasicDBObject("node_id", 1).append("jid_id", 1), new BasicDBObject("unique", true));			
			
			DBCollection items = null;
			if (!db.collectionExists(PUBSUB_ITEMS)) {
				items = db.createCollection(PUBSUB_ITEMS, new BasicDBObject());
			} else {
				items = db.getCollection(PUBSUB_ITEMS);
			}
			items.createIndex(new BasicDBObject("node_id", 1).append("item_id", 1), new BasicDBObject("unique", true));
		} catch (UnknownHostException ex) {
			throw new DBInitException("Could not connect to MongoDB server using URI = " + resourceUri, ex);
		}
	}

	private byte[] generateId(BareJID user) throws RepositoryException {
		try {
			MessageDigest md = MessageDigest.getInstance(JID_HASH_ALG);
			return md.digest(user.toString().getBytes());
		} catch (NoSuchAlgorithmException ex) {
			throw new RepositoryException("Should not happen!!", ex);
		}
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
		byte[] id = generateId(serviceJid);
		try {
			BasicDBObject crit = new BasicDBObject("_id", id).append("service_jid", serviceJid.toString());
			db.getCollection(PUBSUB_SERVICE_JIDS).update(crit, crit, true, false);
		} catch (MongoException ex) {
			throw new RepositoryException("Could not create entry for service jid " + serviceJid, ex);
		}
	}
	
	private BasicDBObject createCrit(BareJID serviceJid, String nodeName) throws RepositoryException {
		byte[] serviceJidId = generateId(serviceJid);	
		BasicDBObject crit = new BasicDBObject("service_jid_id", serviceJidId).append("service_jid", serviceJid.toString());
		if (nodeName != null) {
			byte[] nodeNameId = generateId(nodeName);
			crit.append("node_name_id", nodeNameId).append("node_name", nodeName);
		}
		else {
			crit.append("node_name", new BasicDBObject("$exists", false));
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
			
			BasicDBObject dto = createCrit(serviceJid, nodeName);
			dto.append("owner", ownerJid.toString()).append("type", nodeType.name()).append("configuration", serializedNodeConfig);
			if (collectionId != null) {
				dto.append("collection", collectionId);
			}
			ObjectId id = new ObjectId();
			dto.append("_id", id);
			WriteResult result = db.getCollection(PUBSUB_NODES).insert(dto);
			return id;
		} catch (MongoException ex) {
			throw new RepositoryException("Error while adding node to repository", ex);
		}
	}

	@Override
	public void deleteItem(BareJID serviceJid, ObjectId nodeId, String id) throws RepositoryException {
		try {			
			BasicDBObject crit = new BasicDBObject("node_id", nodeId).append("item_id", id);
			db.getCollection(PUBSUB_ITEMS).remove(crit);
		} catch (MongoException ex) {
			throw new RepositoryException("Error while deleting node from repository", ex);
		}
	}

	@Override
	public void deleteNode(BareJID serviceJid, ObjectId nodeId) throws RepositoryException {
		try {
			BasicDBObject crit = new BasicDBObject("node_id", nodeId);
			db.getCollection(PUBSUB_ITEMS).remove(crit);
			db.getCollection(PUBSUB_AFFILIATIONS).remove(crit);
			db.getCollection(PUBSUB_SUBSCRIPTIONS).remove(crit);
			db.getCollection(PUBSUB_NODES).remove(new BasicDBObject("_id", nodeId));
		} catch (MongoException ex) {
			throw new RepositoryException("Could not retrieve node id", ex);
		}
	}

	@Override
	public String[] getAllNodesList(BareJID serviceJid) throws RepositoryException {
		try {
			byte[] serviceJidId = generateId(serviceJid);
			BasicDBObject crit = new BasicDBObject("service_jid_id", serviceJidId).append("service_jid", serviceJid.toString());
			List<String> result = db.getCollection(PUBSUB_NODES).distinct("node_name", crit);
			return result.toArray(new String[result.size()]);
		} catch (MongoException ex) {
			throw new RepositoryException("Could not retrieve list of all nodes", ex);
		}
	}

	@Override
	public Element getItem(BareJID serviceJid, ObjectId nodeId, String id) throws RepositoryException {
		try {			
			BasicDBObject crit = new BasicDBObject("node_id", nodeId).append("item_id", id);
			DBObject dto = db.getCollection(PUBSUB_ITEMS).findOne(crit);
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
			BasicDBObject crit = new BasicDBObject("node_id", nodeId).append("item_id", id);
			DBObject dto = db.getCollection(PUBSUB_ITEMS).findOne(crit, new BasicDBObject("creation_date", 1));
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
			BasicDBObject crit = new BasicDBObject("node_id", nodeId);
			List<String> ids = (List<String>) db.getCollection(PUBSUB_ITEMS).distinct("item_id", crit);
			return ids.toArray(new String[ids.size()]);
		} catch (MongoException ex) {
			throw new RepositoryException("Error while retrieving item ids from repository", ex);
		}
	}

	@Override
	public String[] getItemsIdsSince(BareJID serviceJid, ObjectId nodeId, Date since) throws RepositoryException {
		try {			
			BasicDBObject crit = new BasicDBObject("node_id", nodeId).append("$gte", new BasicDBObject("creation_date", since));
			List<String> ids = (List<String>) db.getCollection(PUBSUB_ITEMS).distinct("item_id", crit);
			return ids.toArray(new String[ids.size()]);
		} catch (MongoException ex) {
			throw new RepositoryException("Error while retrieving item ids since timestamp from repository", ex);
		}
	}

	@Override
	public List<IItems.ItemMeta> getItemsMeta(BareJID serviceJid, ObjectId nodeId, String nodeName) throws RepositoryException {
		DBCursor cursor = null;
		try {			
			BasicDBObject crit = new BasicDBObject("node_id", nodeId);
			cursor = db.getCollection(PUBSUB_ITEMS).find(crit, new BasicDBObject("item_id", 1).append("creation_date", 1));
			List<IItems.ItemMeta> results = new ArrayList<IItems.ItemMeta>();
			while (cursor.hasNext()) {
				DBObject it = cursor.next();
				results.add(new IItems.ItemMeta(nodeName, (String) it.get("item_id"), (Date) it.get("creation_date")));
			}
			return results;
		} catch (MongoException ex) {
			throw new RepositoryException("Error while retrieving item ids from repository", ex);
		} finally {
			if (cursor != null) {
				cursor.close();
			}
		}
	}

	@Override
	public Date getItemUpdateDate(BareJID serviceJid, ObjectId nodeId, String id) throws RepositoryException {
		try {			
			BasicDBObject crit = new BasicDBObject("node_id", nodeId).append("item_id", id);
			DBObject dto = db.getCollection(PUBSUB_ITEMS).findOne(crit, new BasicDBObject("update_date", 1));
			if (dto == null)
				return null;
			return (Date) dto.get("update_date");
		} catch (MongoException ex) {
			throw new RepositoryException("Error while retrieving item update date from repository", ex);
		}
	}

	@Override
	public NodeAffiliations getNodeAffiliations(BareJID serviceJid, ObjectId nodeId) throws RepositoryException {
		DBCursor cursor = null;
		try {
			BasicDBObject crit = new BasicDBObject("node_id", nodeId);
			cursor = db.getCollection(PUBSUB_AFFILIATIONS).find(crit, new BasicDBObject("jid", 1).append("affiliation", 1));
			
			Queue<UsersAffiliation> data = new ArrayDeque<UsersAffiliation>();
			while (cursor.hasNext()) {
				DBObject it = cursor.next();
				BareJID jid = BareJID.bareJIDInstanceNS((String) it.get("jid"));
				Affiliation affil = Affiliation.valueOf((String) it.get("affiliation"));
				data.offer(new UsersAffiliation(jid, affil));
			}
			return NodeAffiliations.create(data);
		} catch (MongoException ex) {
			throw new RepositoryException("Could not retrieve node affiliations", ex);
		} finally {
			if (cursor != null) {
				cursor.close();
			}
		}
	}

	@Override
	public String getNodeConfig(BareJID serviceJid, ObjectId nodeId) throws RepositoryException {
		try {
			BasicDBObject crit = new BasicDBObject("_id", nodeId);
			DBObject result = db.getCollection(PUBSUB_NODES).findOne(crit, new BasicDBObject());
			return result == null ? null : (String) result.get("configuration");
		} catch (MongoException ex) {
			throw new RepositoryException("Could not retrieve node configuration", ex);
		}
	}

	@Override
	public ObjectId getNodeId(BareJID serviceJid, String nodeName) throws RepositoryException {
		try {
			BasicDBObject crit = createCrit(serviceJid, nodeName);
			DBObject result = db.getCollection(PUBSUB_NODES).findOne(crit, new BasicDBObject());
			return result == null ? null : (ObjectId) result.get("_id");
		} catch (MongoException ex) {
			throw new RepositoryException("Could not retrieve node id", ex);
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
			BasicDBObject crit = new BasicDBObject("service_jid_id", serviceJidId).append("service_jid", serviceJid.toString());
			if (collectionId != null) {
				crit.append("collection", collectionId);
			} else {
				crit.append("collection", new BasicDBObject("$exists", false));
			}
			List<String> result = db.getCollection(PUBSUB_NODES).distinct("node_name", crit);
			return result.toArray(new String[result.size()]);
		} catch (MongoException ex) {
			throw new RepositoryException("Could not retrieve list of all nodes", ex);
		}
	}

	@Override
	public NodeSubscriptions getNodeSubscriptions(BareJID serviceJid, ObjectId nodeId) throws RepositoryException {
		DBCursor cursor = null;
		try {
			BasicDBObject crit = new BasicDBObject("node_id", nodeId);
			cursor = db.getCollection(PUBSUB_SUBSCRIPTIONS).find(crit, new BasicDBObject("jid", 1).append("subscription", 1).append("subscription_id", 1));
			
			Queue<UsersSubscription> data = new ArrayDeque<UsersSubscription>();
			while (cursor.hasNext()) {
				DBObject it = cursor.next();
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
		} finally {
			if (cursor != null) {
				cursor.close();
			}
		}
	}

	@Override
	public String[] getChildNodes(BareJID serviceJid, String nodeName) throws RepositoryException {
		return getNodesList( serviceJid, nodeName );
	}

	@Override
	public Map<String, UsersAffiliation> getUserAffiliations(BareJID serviceJid, BareJID jid) throws RepositoryException {
		DBCursor cursor = null;
		try {
			byte[] serviceJidId = generateId(serviceJid);
			byte[] jidId = generateId(jid);
			BasicDBObject crit = new BasicDBObject("service_jid_id", serviceJidId).append("service_jid", serviceJid.toString())
					.append("jid_id", jidId).append("jid", jid.toString());
			cursor = db.getCollection(PUBSUB_AFFILIATIONS).find(crit, new BasicDBObject("node_name", 1).append("affiliation", 1));
			
			Map<String, UsersAffiliation> result = new HashMap<String, UsersAffiliation>();
			while (cursor.hasNext()) {
				DBObject it = cursor.next();
				String node = (String) it.get("node_name");
				Affiliation affil = Affiliation.valueOf((String) it.get("affiliation"));
				result.put(node, new UsersAffiliation(jid, affil));
			}
			return result;
		} catch (MongoException ex) {
			throw new RepositoryException("Could not retrieve user affiliations", ex);
		} finally {
			if (cursor != null) {
				cursor.close();
			}
		}
	}

	@Override
	public Map<String, UsersSubscription> getUserSubscriptions(BareJID serviceJid, BareJID jid) throws RepositoryException {
		DBCursor cursor = null;
		try {
			byte[] serviceJidId = generateId(serviceJid);
			byte[] jidId = generateId(jid);
			BasicDBObject crit = new BasicDBObject("service_jid_id", serviceJidId).append("service_jid", serviceJid.toString())
					.append("jid_id", jidId).append("jid", jid.toString());
			cursor = db.getCollection(PUBSUB_SUBSCRIPTIONS).find(crit, new BasicDBObject("node_name", 1).append("subscription", 1).append("subscription_id", 1));
			
			Map<String, UsersSubscription> result = new HashMap<String, UsersSubscription>();
			while (cursor.hasNext()) {
				DBObject it = cursor.next();
				String node = (String) it.get("node_name");
				Subscription subscr = Subscription.valueOf((String) it.get("subscription"));
				String subscr_id = (String) it.get("subscription_id");
				result.put(node, new UsersSubscription(jid, subscr_id, subscr));
			}
			return result;
		} catch (MongoException ex) {
			throw new RepositoryException("Could not retrieve user affiliations", ex);
		} finally {
			if (cursor != null) {
				cursor.close();
			}
		}
	}

	@Override
	public void removeAllFromRootCollection(BareJID serviceJid) throws RepositoryException {
		try {
			byte[] serviceJidId = generateId(serviceJid);
			BasicDBObject crit = new BasicDBObject("service_jid_id", serviceJidId).append("service_jid", serviceJid.toString());
			db.getCollection(PUBSUB_ITEMS).remove(crit);
			db.getCollection(PUBSUB_AFFILIATIONS).remove(crit);
			db.getCollection(PUBSUB_SUBSCRIPTIONS).remove(crit);
			crit = new BasicDBObject("service_jid_id", serviceJidId).append("service_jid", serviceJid.toString());
			db.getCollection(PUBSUB_NODES).remove(crit);			
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
			byte[] serviceJidId = generateId(serviceJid);
			byte[] jidId = generateId(jid);
			BasicDBObject crit = new BasicDBObject("node_id", nodeId).append("service_jid_id", serviceJidId).append("service_jid", serviceJid.toString())
					.append("jid_id", jidId).append("jid", jid.toString());

			db.getCollection(PUBSUB_SUBSCRIPTIONS).remove(crit);
		} catch (MongoException ex) {
			throw new RepositoryException("Could not remove user subscriptions", ex);
		}
	}

	@Override
	public void updateNodeConfig(BareJID serviceJid, ObjectId nodeId, String serializedNodeConfig, ObjectId collectionId) throws RepositoryException {
		try {			
			BasicDBObject crit = new BasicDBObject("_id", nodeId);
			BasicDBObject set = new BasicDBObject("configuration", serializedNodeConfig);
			BasicDBObject dto = new BasicDBObject("$set", set);
			if (collectionId != null) {
				set.append("collection", collectionId);
			}
			else {
				dto.append("$unset", new BasicDBObject("collection", ""));
			}
			db.getCollection(PUBSUB_NODES).update(crit, dto);
		} catch (MongoException ex) {
			throw new RepositoryException("Error while updating node configuration in repository", ex);
		}
	}

	@Override
	public void updateNodeAffiliation(BareJID serviceJid, ObjectId nodeId, String nodeName, UsersAffiliation userAffiliation) throws RepositoryException {
		try {
			byte[] serviceJidId = generateId(serviceJid);
			byte[] jidId = generateId(userAffiliation.getJid());
			BasicDBObject crit = new BasicDBObject("node_id", nodeId).append("service_jid_id", serviceJidId).append("service_jid", serviceJid.toString())
					.append("jid_id", jidId).append("jid", userAffiliation.getJid().toString());
			if (userAffiliation.getAffiliation() == Affiliation.none) {
				db.getCollection(PUBSUB_AFFILIATIONS).remove(crit);
			} else {
				db.getCollection(PUBSUB_AFFILIATIONS).update(crit, new BasicDBObject("$setOnInsert", new BasicDBObject("node_name", nodeName))
					.append("$set", new BasicDBObject("affiliation", userAffiliation.getAffiliation().name())), true, false);
			}
		} catch (MongoException ex) {
			throw new RepositoryException("Could not update user affiliations", ex);
		}
	}

	@Override
	public void updateNodeSubscription(BareJID serviceJid, ObjectId nodeId, String nodeName, UsersSubscription userSubscription) throws RepositoryException {
		try {
			byte[] serviceJidId = generateId(serviceJid);
			byte[] jidId = generateId(userSubscription.getJid());
			BasicDBObject crit = new BasicDBObject("node_id", nodeId).append("service_jid_id", serviceJidId).append("service_jid", serviceJid.toString())
					.append("jid_id", jidId).append("jid", userSubscription.getJid().toString());
			if (userSubscription.getSubscription() == Subscription.none) {
				db.getCollection(PUBSUB_SUBSCRIPTIONS).remove(crit);
			} else {
				db.getCollection(PUBSUB_SUBSCRIPTIONS).update(crit, new BasicDBObject("$setOnInsert", new BasicDBObject("node_name", nodeName))
						.append("$set", new BasicDBObject("subscription", userSubscription.getSubscription().name()).append("subscription_id", userSubscription.getSubid())),
						true, false);
			}
		} catch (MongoException ex) {
			throw new RepositoryException("Could not update user subscriptions", ex);
		}
	}

	@Override
	public void writeItem(BareJID serviceJid, ObjectId nodeId, long timeInMilis, String id, String publisher, Element item) throws RepositoryException {
		try {
			byte[] serviceJidId = generateId(serviceJid);
			BasicDBObject crit = new BasicDBObject("service_jid_id", serviceJidId).append("service_jid", serviceJid.toString());
			crit.append("node_id", nodeId).append("item_id", id);
			BasicDBObject dto = new BasicDBObject("$set", new BasicDBObject("update_date", new Date()).append("publisher", publisher).append("item", item.toString()));
			dto.append("$setOnInsert", new BasicDBObject("creation_date", new Date()));
			db.getCollection(PUBSUB_ITEMS).update(crit, dto, true, false);	
		} catch (MongoException ex) {
			throw new RepositoryException("Could not write item to repository", ex);
		}
	}
	
}
