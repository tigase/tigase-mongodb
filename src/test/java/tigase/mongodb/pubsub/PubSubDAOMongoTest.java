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

import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.junit.Assume;
import org.junit.ClassRule;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.MethodSorters;
import org.junit.runners.model.Statement;
import tigase.component.exceptions.RepositoryException;
import tigase.db.util.RepositoryVersionAware;
import tigase.mongodb.MongoDataSource;
import tigase.pubsub.Affiliation;
import tigase.pubsub.NodeType;
import tigase.pubsub.Subscription;
import tigase.pubsub.repository.AbstractPubSubDAOTest;
import tigase.pubsub.repository.INodeMeta;
import tigase.util.Version;
import tigase.xmpp.jid.BareJID;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Date;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static org.junit.Assert.*;

/**
 * @author andrzej
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class PubSubDAOMongoTest
		extends AbstractPubSubDAOTest<MongoDataSource> {

	protected static String uri = System.getProperty("testDbUri");

	@ClassRule
	public static TestRule rule = new TestRule() {
		@Override
		public Statement apply(Statement stmnt, Description d) {
			if (uri == null) {
				return new Statement() {
					@Override
					public void evaluate() throws Throwable {
						Assume.assumeTrue("Ignored due to not passed DB URI!", false);
					}
				};
			}
			return stmnt;
		}
	};

	private byte[] generateId(String in) throws RepositoryException {
		try {
			MessageDigest md = MessageDigest.getInstance("SHA-256");
			return md.digest(in.getBytes());
		} catch (NoSuchAlgorithmException ex) {
			throw new RepositoryException("Should not happen!!", ex);
		}
	}

	protected MongoDatabase getDatabase() {
		return getDataSource().getDatabase();
	}
	
	@Test
	public void testSchemaUpgrade_JidComparison() throws Exception {
		BareJID serviceJid = BareJID.bareJIDInstance("TeSt@example.com");
		byte[] serviceJidId = generateId(serviceJid.toString());
		getDatabase().getCollection(PubSubDAOMongo.PUBSUB_SERVICE_JIDS)
				.insertOne(new Document("_id", serviceJidId).append("service_jid", serviceJid.toString()));

		String nodeName = "test-node-" + UUID.randomUUID().toString();
		byte[] nodeNameId = generateId(nodeName);

		ObjectId nodeId = new ObjectId();
		getDatabase().getCollection(PubSubDAOMongo.PUBSUB_NODES)
				.insertOne(new Document("_id", nodeId).append("service_jid_id", serviceJidId)
						           .append("service_jid", serviceJid.toString())
						           .append("node_name_id", nodeNameId)
						           .append("node_name", nodeName)
						           .append("owner", serviceJid.toString())
						           .append("type", NodeType.leaf.name())
						           .append("creation_time", new Date()));

		String itemId = UUID.randomUUID().toString();

		getDatabase().getCollection(PubSubDAOMongo.PUBSUB_ITEMS)
				.insertOne(new Document("service_jid_id", serviceJidId).append("service_jid", serviceJid.toString())
						           .append("node_id", nodeId)
						           .append("item_id", itemId)
						           .append("update_date", new Date())
						           .append("publisher", serviceJid.toString())
						           .append("item", "<dummy-item/>")
						           .append("creation_date", new Date()));

		getDatabase().getCollection(PubSubDAOMongo.PUBSUB_AFFILIATIONS)
				.insertOne(new Document("node_id", nodeId).append("service_jid_id", serviceJidId)
						           .append("service_jid", serviceJid.toString())
						           .append("jid_id", serviceJidId)
						           .append("jid", serviceJid.toString())
						           .append("node_name", nodeName)
						           .append("affiliation", Affiliation.owner.name()));

		getDatabase().getCollection(PubSubDAOMongo.PUBSUB_SUBSCRIPTIONS)
				.insertOne(new Document("node_id", nodeId).append("service_jid_id", serviceJidId)
						           .append("service_jid", serviceJid.toString())
						           .append("jid_id", serviceJidId)
						           .append("jid", serviceJid.toString())
						           .append("node_name", nodeName)
						           .append("subscription", Subscription.subscribed.name())
						           .append("subscription_id", UUID.randomUUID().toString()));

		assertNull(dao.getNodeMeta(serviceJid, nodeName));

		((RepositoryVersionAware) dao).updateSchema(Optional.of(Version.ZERO), Version.ZERO);

		assertNotNull(getDatabase().getCollection(PubSubDAOMongo.PUBSUB_SERVICE_JIDS)
				              .find(new Document("service_jid", serviceJid.toString().toLowerCase())));

		INodeMeta meta = dao.getNodeMeta(serviceJid, nodeName);
		assertNotNull(meta);
		assertEquals(serviceJid.toString(), meta.getCreator().toString());

		assertNotNull(dao.getItem(serviceJid, meta.getNodeId(), itemId));

		Map<BareJID, Affiliation> affil = dao.getNodeAffiliations(serviceJid, meta.getNodeId());
		assertNotNull(affil);
		assertNotNull(affil.get(serviceJid));

		assertEquals(Subscription.subscribed,
		             dao.getNodeSubscriptions(serviceJid, meta.getNodeId()).get(serviceJid));

		dao.deleteService(serviceJid);
	}

}