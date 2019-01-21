/**
 * Tigase MongoDB - Tigase MongoDB support library
 * Copyright (C) 2012 Tigase, Inc. (office@tigase.com)
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
package tigase.mongodb;

import tigase.db.DBInitException;
import tigase.db.TigaseDBException;

import tigase.xmpp.BareJID;

import tigase.util.TigaseStringprepException;

import java.util.HashMap;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

/**
 *
 * @author andrzej
 */
@Ignore
public class MongoRepositoryTest {
	
	private MongoRepository repo; 
	
	@Before
	public void setup() throws DBInitException {
		repo = new MongoRepository();
		repo.initRepository("mongodb://localhost/tigase_junit", new HashMap<String,String>());
	}
	
	@After
	public void tearDown() throws TigaseDBException {
		repo.removeUser(BareJID.bareJIDInstanceNS("test-1@example.com"));
		repo = null;
	}
	
	@Test
	public void testUser() throws TigaseStringprepException, TigaseDBException {
		BareJID jid = BareJID.bareJIDInstance("test-1@example.com");
		
		repo.addUser(jid);
		Assert.assertTrue("User creation failed", repo.userExists(jid));

		Assert.assertEquals(1, repo.getUsersCount());
		Assert.assertEquals(1, repo.getUsersCount("example.com"));
		Assert.assertEquals(0, repo.getUsersCount("test.com"));
		
		Assert.assertEquals(jid, repo.getUsers().get(0));
		
		repo.removeUser(jid);
		Assert.assertFalse("User removal failed", repo.userExists(jid));
	}
	
	@Test
	public void testData() throws TigaseStringprepException, TigaseDBException {
		BareJID jid = BareJID.bareJIDInstance("test-1@example.com");
		
		repo.addUser(jid);
		Assert.assertTrue("User creation failed", repo.userExists(jid));

		repo.setData(jid, "key1", "test value 1");
		String[] keys = repo.getKeys(jid);
		Assert.assertArrayEquals(new String[] { "key1" }, keys);

		repo.setData(jid, "test/node", "key2", "test value 2");
		keys = repo.getKeys(jid);
		Assert.assertArrayEquals(new String[] { "key1" }, keys);
		keys = repo.getKeys(jid, "test/node");
		Assert.assertArrayEquals(new String[] { "key2" }, keys);
		repo.setDataList(jid, "test/node2", "list", new String[] { "item1", "item2" });
		keys = repo.getKeys(jid, "test/node2");
		Assert.assertArrayEquals(new String[] { "list" }, keys);
		String[] subnodes = repo.getSubnodes(jid);
		Assert.assertArrayEquals(new String[] { "test" }, subnodes);
		subnodes = repo.getSubnodes(jid, "test");
		Assert.assertArrayEquals(new String[] { "node", "node2" }, subnodes);
		repo.setDataList(jid, "test/node3/subnode1", "list", new String[] { "item1", "item2" });
		subnodes = repo.getSubnodes(jid, "test/");
		Assert.assertArrayEquals(new String[] { "node", "node2", "node3" }, subnodes);
		
		// cleaning up
		repo.removeData(jid, "key1");
		keys = repo.getKeys(jid);
		Assert.assertArrayEquals(new String[] { }, keys);
		repo.removeData(jid, "test/node", "key2");
		subnodes = repo.getSubnodes(jid);
		Assert.assertArrayEquals(new String[] { "test" }, subnodes);
		subnodes = repo.getSubnodes(jid, "test");
		Assert.assertArrayEquals(new String[] { "node2", "node3" }, subnodes);
		repo.removeData(jid, "test/node2", "list");
		subnodes = repo.getSubnodes(jid, "test");
		Assert.assertEquals(new String[] { "node3" }, subnodes);
		repo.removeData(jid, "test/node3/subnode1", "list");
		subnodes = repo.getSubnodes(jid, "test");
		Assert.assertEquals(null, subnodes);
		
		repo.setDataList(jid, "test/node2", "list", new String[] { "item1", "item2" });
		subnodes = repo.getSubnodes(jid, "test");
		Assert.assertArrayEquals(new String[] { "node2" }, subnodes);
		repo.removeSubnode(jid, "test");
		subnodes = repo.getSubnodes(jid, "test");
		Assert.assertEquals(null, subnodes);

		// once more
		repo.setDataList(jid, "test/node2", "list", new String[] { "item1", "item2" });
		subnodes = repo.getSubnodes(jid, "test");
		Assert.assertArrayEquals(new String[] { "node2" }, subnodes);
		repo.removeSubnode(jid, "test/");
		subnodes = repo.getSubnodes(jid, "test");
		Assert.assertEquals(null, subnodes);

		// once more
		repo.setDataList(jid, "test/node2/", "list", new String[] { "item1", "item2" });
		subnodes = repo.getSubnodes(jid, "test");
		Assert.assertArrayEquals(new String[] { "node2" }, subnodes);
		repo.removeSubnode(jid, "test");
		subnodes = repo.getSubnodes(jid, "test");
		Assert.assertEquals(null, subnodes);


		repo.removeUser(jid);
		Assert.assertFalse("User removal failed", repo.userExists(jid));
	}
	
	@Test
	public void testExeutionTimes() throws Exception {	
		BareJID jid = BareJID.bareJIDInstance("test-1@example.com");
		
		repo.addUser(jid);
		
		int counts = 1000;
		
		for (int i=0; i<counts; i++) {
			repo.setData(jid, "rooms/test-" + i + "@test", "creation-date", "date-" + i);
			repo.setData(jid, "rooms/test-" + i + "@test", "value", "date-" + i);
		}
		
		long start = System.currentTimeMillis();
		
		for (int i=0; i<counts; i++) {
			repo.removeSubnode(jid, "rooms/test-" + i + "@test");
		}
		
		long end = System.currentTimeMillis();
		long time = end - start;
		System.out.println("executed in " + time + "ms for " + counts + " " + (time/counts) + " per execution");
		
		long timeLimit = counts * 2;
		
		Assert.assertTrue("Test should be executed in less than " + timeLimit + "ms", timeLimit > time);
	}
	
	private void prepareUserAutoCreateRepo() throws Exception {
		String uri = repo.getResourceUri();
		if (uri.contains("?")) {
			uri += "&";
		} else {
			uri += "?";
		}
		uri += "autoCreateUser=true";
		repo = new MongoRepository();
		repo.initRepository(uri, new HashMap<String,String>());		
	}
	
	@Test
	public void testAddDataListUserAutoCreate() throws Exception {
		prepareUserAutoCreateRepo();
		
		BareJID userJID = BareJID.bareJIDInstanceNS("test-1@example.com");
		String[] data = new String[] {
			"test1",
			"test2",
			"test3"
		};
		
		repo.addDataList(userJID, "test-node", "test-key", data);
		Assert.assertTrue("User autocreation failed", repo.userExists(userJID));
		Assert.assertArrayEquals(data, repo.getDataList(userJID, "test-node", "test-key"));
	}

	@Test
	public void testSetDataListUserAutoCreate() throws Exception {
		prepareUserAutoCreateRepo();
		
		BareJID userJID = BareJID.bareJIDInstanceNS("test-1@example.com");
		String[] data = new String[] {
			"test1",
			"test2",
			"test3"
		};
		
		repo.setDataList(userJID, "test-node", "test-key", data);
		Assert.assertTrue("User autocreation failed", repo.userExists(userJID));
		Assert.assertArrayEquals(data, repo.getDataList(userJID, "test-node", "test-key"));		
	}
	
	@Test
	public void testSetDataUserAutoCreate() throws Exception {
		prepareUserAutoCreateRepo();
		
		BareJID userJID = BareJID.bareJIDInstanceNS("test-1@example.com");
		String data = "test-data";
		
		repo.setData(userJID, "test-node", "test-key", data);
		Assert.assertTrue("User autocreation failed", repo.userExists(userJID));
		Assert.assertEquals(data, repo.getData(userJID, "test-node", "test-key"));
	}	
}
