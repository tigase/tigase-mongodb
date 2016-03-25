/*
 * MongoMessageArchiveRepositoryTest.java
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
package tigase.mongodb;

import org.junit.*;
import org.junit.runners.MethodSorters;
import tigase.component.exceptions.RepositoryException;
import tigase.mongodb.pubsub.PubSubDAOMongo;
import tigase.pubsub.Affiliation;
import tigase.pubsub.LeafNodeConfig;
import tigase.pubsub.NodeType;
import tigase.pubsub.Subscription;
import tigase.pubsub.repository.*;
import tigase.pubsub.repository.stateless.UsersAffiliation;
import tigase.pubsub.repository.stateless.UsersSubscription;
import tigase.xml.Element;
import tigase.xmpp.BareJID;
import tigase.xmpp.JID;

import java.util.Arrays;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 *
 * @author andrzej
 */
@Ignore
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class PubSubDAOMongoTest {
	
	private PubSubDAO dao;
	
	private String nodeName = "test-node";
	private JID senderJid = JID.jidInstanceNS("owner@tigase/tigase-1");
	private BareJID serviceJid = BareJID.bareJIDInstanceNS("pubsub.tigase");
	private JID subscriberJid = JID.jidInstanceNS("subscriber@tigase/tigase-1");
	
	@Before
	public void setup() throws RepositoryException {
		dao = new PubSubDAOMongo();
		dao.init("mongodb://localhost/tigase_junit", null, null);
	}
	
	@After
	public void tearDown() {
		dao.destroy();
		dao = null;
	}
	
	@Test
	public void test1_createNode() throws RepositoryException {
		Object nodeId = dao.getNodeId(serviceJid, nodeName);
		if (nodeId != null) {
			dao.deleteNode(serviceJid, nodeId);
		}

		LeafNodeConfig nodeCfg = new LeafNodeConfig(nodeName);
		dao.createNode(serviceJid, nodeName, senderJid.getBareJID(), nodeCfg, NodeType.leaf, null);
		
		nodeId = dao.getNodeId(serviceJid, nodeName);
		Assert.assertNotNull("Could not retrieve nodeId for newly created node", nodeId);
	}	
	
	@Test
	public void test2_subscribeNode() throws RepositoryException {
		Object nodeId = dao.getNodeId(serviceJid, nodeName);
		Assert.assertNotNull("Could not fined nodeId", nodeId);
		UsersSubscription subscr = new UsersSubscription(subscriberJid.getBareJID(), "sub-1", Subscription.subscribed);
		dao.updateNodeSubscription(serviceJid, nodeId, nodeName, subscr);
		
		NodeSubscriptions nodeSubscr = dao.getNodeSubscriptions(serviceJid, nodeId);
		Assert.assertNotNull("Not found subscriptions for node", nodeSubscr);
		Subscription subscription = nodeSubscr.getSubscription(subscriberJid.getBareJID());
		Assert.assertEquals("Bad subscription type for user", Subscription.subscribed, subscription);
	}
	
	@Test
	public void test3_affiliateNode() throws RepositoryException {
		Object nodeId = dao.getNodeId(serviceJid, nodeName);
		Assert.assertNotNull("Could not fined nodeId", nodeId);
		UsersAffiliation affil = new UsersAffiliation(subscriberJid.getBareJID(), Affiliation.publisher);
		dao.updateNodeAffiliation(serviceJid, nodeId, nodeName, affil);
		
		NodeAffiliations nodeAffils = dao.getNodeAffiliations(serviceJid, nodeId);
		Assert.assertNotNull("Not found affiliations for node", nodeAffils);
		affil = nodeAffils.getSubscriberAffiliation(subscriberJid.getBareJID());
		Assert.assertNotNull("Not found affiliation for user", affil);
		Affiliation affiliation = affil.getAffiliation();
		Assert.assertEquals("Bad affiliation type for user", Affiliation.publisher, affiliation);
	}
	
	@Test
	public void test4_userSubscriptions() throws RepositoryException {
		Object nodeId = dao.getNodeId(serviceJid, nodeName);
		Assert.assertNotNull("Could not fined nodeId", nodeId);
		Map<String,UsersSubscription> map = dao.getUserSubscriptions(serviceJid, subscriberJid.getBareJID());
		Assert.assertNotNull("No subscriptions for user", map);
		UsersSubscription subscr = map.get(nodeName);
		Assert.assertNotNull("No subscription for user for node", subscr);
		Assert.assertEquals("Bad subscription for user for node", Subscription.subscribed, subscr.getSubscription());
	}
	
	@Test
	public void test5_userAffiliations() throws RepositoryException {
		Object nodeId = dao.getNodeId(serviceJid, nodeName);
		Assert.assertNotNull("Could not fined nodeId", nodeId);
		Map<String,UsersAffiliation> map = dao.getUserAffiliations(serviceJid, subscriberJid.getBareJID());
		Assert.assertNotNull("No affiliation for user", map);
		UsersAffiliation affil = map.get(nodeName);
		Assert.assertNotNull("No affiliation for user for node", affil);
		Assert.assertEquals("Bad affiliation for user for node", Affiliation.publisher, affil.getAffiliation());
	}	
	
	@Test
	public void test6_allNodes() throws RepositoryException {
		String[] allNodes = dao.getAllNodesList(serviceJid);
		Arrays.sort(allNodes);
		Assert.assertNotEquals("Node name not listed in list of all root nodes", -1, Arrays.binarySearch(allNodes, nodeName));
	}

	@Test
	public void test6_getNodeMeta() throws RepositoryException {
		INodeMeta meta = dao.getNodeMeta(serviceJid, nodeName);
		assertNotNull(meta);
		Object nodeId = dao.getNodeId(serviceJid, nodeName);
		assertEquals(nodeId, meta.getNodeId());
		assertEquals(nodeName, meta.getNodeConfig().getNodeName());
		assertEquals(senderJid.getBareJID(), meta.getCreator());
		assertNotNull(meta.getCreationTime());
	}
	
	@Test
	public void test7_nodeItems() throws RepositoryException {
		String itemId = "item-1";
		Element item = new Element("item", new String[] { "id" }, new String[] { itemId });
		item.addChild(new Element("payload", "test-payload", new String[] { "xmlns" }, new String[] { "test-xmlns" }));
		
		Object nodeId = dao.getNodeId(serviceJid, nodeName);
		Assert.assertNotNull("Could not fined nodeId", nodeId);
		dao.writeItem(serviceJid, nodeId, System.currentTimeMillis(), itemId, nodeName, item);
		
		String[] itemsIds = dao.getItemsIds(serviceJid, nodeId);
		Assert.assertArrayEquals("Added item id not listed in list of item ids", new String[] { itemId }, itemsIds);
		
		Element el = dao.getItem(serviceJid, nodeId, itemId);
		Assert.assertEquals("Element retrieved from store do not match to element added to store", item, el);
		
		dao.deleteItem(serviceJid, nodeId, itemId);
		el = dao.getItem(serviceJid, nodeId, itemId);
		Assert.assertNull("Element still available in store after removal", el);
	}
	
	@Test
	public void test8_subscribeNodeRemoval() throws RepositoryException {
		Object nodeId = dao.getNodeId(serviceJid, nodeName);
		Assert.assertNotNull("Could not fined nodeId", nodeId);
		UsersSubscription subscr = new UsersSubscription(subscriberJid.getBareJID(), "sub-1", Subscription.none);
		dao.updateNodeSubscription(serviceJid, nodeId, nodeName, subscr);
		
		NodeSubscriptions nodeSubscr = dao.getNodeSubscriptions(serviceJid, nodeId);
		Assert.assertNotNull("Not found subscriptions for node", nodeSubscr);
		Subscription subscription = nodeSubscr.getSubscription(subscriberJid.getBareJID());
		Assert.assertEquals("Bad subscription type for user", Subscription.none, subscription);
	}
	
	@Test
	public void test8_affiliateNodeRemoval() throws RepositoryException {
		Object nodeId = dao.getNodeId(serviceJid, nodeName);
		Assert.assertNotNull("Could not fined nodeId", nodeId);
		UsersAffiliation affil = new UsersAffiliation(subscriberJid.getBareJID(), Affiliation.none);
		dao.updateNodeAffiliation(serviceJid, nodeId, nodeName, affil);
		
		NodeAffiliations nodeAffils = dao.getNodeAffiliations(serviceJid, nodeId);
		Assert.assertNotNull("Not found affiliations for node", nodeAffils);
		affil = nodeAffils.getSubscriberAffiliation(subscriberJid.getBareJID());
		Assert.assertEquals("Bad affiliation for user", Affiliation.none, affil.getAffiliation());
	}

	@Test
	public void test9_nodeRemoval() throws RepositoryException {
		Object nodeId = dao.getNodeId(serviceJid, nodeName);
		dao.deleteNode(serviceJid, nodeId);		
		nodeId = dao.getNodeId(serviceJid, nodeName);
		Assert.assertNull("Node not removed", nodeId);
	}
}
