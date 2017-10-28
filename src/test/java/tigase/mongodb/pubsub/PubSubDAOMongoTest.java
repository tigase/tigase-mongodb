/*
 * PubSubDAOMongoTest.java
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
package tigase.mongodb.pubsub;

import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;
import tigase.component.exceptions.RepositoryException;
import tigase.mongodb.MongoDataSource;
import tigase.mongodb.RepositoryVersionAware;
import tigase.pubsub.Affiliation;
import tigase.pubsub.NodeType;
import tigase.pubsub.Subscription;
import tigase.pubsub.repository.AbstractPubSubDAOTest;
import tigase.pubsub.repository.INodeMeta;
import tigase.pubsub.repository.stateless.UsersAffiliation;
import tigase.xmpp.jid.BareJID;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Date;
import java.util.UUID;

import static org.junit.Assert.*;

/**
 * @author andrzej
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class PubSubDAOMongoTest extends AbstractPubSubDAOTest {

	@Test
	public void testSchemaUpgrade_JidComparison() throws Exception {
		BareJID serviceJid = BareJID.bareJIDInstance("TeSt@example.com");
		byte[] serviceJidId = generateId(serviceJid.toString());
		getDatabase()
				.getCollection(PubSubDAOMongo.PUBSUB_SERVICE_JIDS)
				.insertOne(new Document("_id", serviceJidId).append("service_jid", serviceJid.toString()));

		String nodeName = "test-node-" + UUID.randomUUID().toString();
		byte[] nodeNameId = generateId(nodeName);

		ObjectId nodeId = new ObjectId();
		getDatabase()
				.getCollection(PubSubDAOMongo.PUBSUB_NODES)
				.insertOne(new Document("_id", nodeId).append("service_jid_id", serviceJidId)
								   .append("service_jid", serviceJid.toString())
								   .append("node_name_id", nodeNameId)
								   .append("node_name", nodeName)
								   .append("owner", serviceJid.toString())
								   .append("type", NodeType.leaf.name())
								   .append("creation_time", new Date()));

		String itemId = UUID.randomUUID().toString();

		getDatabase()
				.getCollection(PubSubDAOMongo.PUBSUB_ITEMS)
				.insertOne(new Document("service_jid_id", serviceJidId).append("service_jid", serviceJid.toString())
								   .append("node_id", nodeId)
								   .append("item_id", itemId)
								   .append("update_date", new Date())
								   .append("publisher", serviceJid.toString())
								   .append("item", "<dummy-item/>")
								   .append("creation_date", new Date()));

		getDatabase()
				.getCollection(PubSubDAOMongo.PUBSUB_AFFILIATIONS)
				.insertOne(new Document("node_id", nodeId).append("service_jid_id", serviceJidId)
								   .append("service_jid", serviceJid.toString())
								   .append("jid_id", serviceJidId)
								   .append("jid", serviceJid.toString())
								   .append("node_name", nodeName)
								   .append("affiliation", Affiliation.owner.name()));

		getDatabase()
				.getCollection(PubSubDAOMongo.PUBSUB_SUBSCRIPTIONS)
				.insertOne(new Document("node_id", nodeId).append("service_jid_id", serviceJidId)
								   .append("service_jid", serviceJid.toString())
								   .append("jid_id", serviceJidId)
								   .append("jid", serviceJid.toString())
								   .append("node_name", nodeName)
								   .append("subscription", Subscription.subscribed.name())
								   .append("subscription_id", UUID.randomUUID().toString()));

		assertNull(dao.getNodeMeta(serviceJid, nodeName));

		((RepositoryVersionAware) dao).updateSchema();

		assertNotNull(getDatabase()
							  .getCollection(PubSubDAOMongo.PUBSUB_SERVICE_JIDS)
							  .find(new Document("service_jid", serviceJid.toString().toLowerCase())));

		INodeMeta meta = dao.getNodeMeta(serviceJid, nodeName);
		assertNotNull(meta);
		assertEquals(serviceJid.toString(), meta.getCreator().toString());

		assertNotNull(dao.getItem(serviceJid, meta.getNodeId(), itemId));

		UsersAffiliation affil = dao.getNodeAffiliations(serviceJid, meta.getNodeId()).getSubscriberAffiliation(serviceJid);
		assertNotNull(affil);
		assertEquals(serviceJid.toString(), affil.getJid().toString());

		assertEquals(Subscription.subscribed, dao.getNodeSubscriptions(serviceJid, meta.getNodeId()).getSubscription(serviceJid));

		dao.removeService(serviceJid);
	}

	protected MongoDatabase getDatabase() {
		return ((MongoDataSource) dataSource).getDatabase();
	}

	@Override
	protected String getMAMID(Object nodeId, String itemId) {
		Document dto = getDatabase().getCollection("tig_pubsub_items")
				.find(Filters.and(Filters.eq("node_id", nodeId), Filters.eq("item_id", itemId)))
				.first();
		return dto.getObjectId("_id")
				.toString();
	}

	private byte[] generateId(String in) throws RepositoryException {
		try {
			MessageDigest md = MessageDigest.getInstance("SHA-256");
			return md.digest(in.getBytes());
		} catch (NoSuchAlgorithmException ex) {
			throw new RepositoryException("Should not happen!!", ex);
		}
	}

}