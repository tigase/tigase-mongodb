/*
 * MongoRepositoryTest.java
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

import java.util.HashMap;
import org.junit.Assert;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import tigase.db.DBInitException;
import tigase.db.TigaseDBException;
import tigase.util.TigaseStringprepException;
import tigase.xmpp.BareJID;

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
		
		// cleaning up
		repo.removeData(jid, "key1");
		keys = repo.getKeys(jid);
		Assert.assertArrayEquals(new String[] { }, keys);
		repo.removeData(jid, "test/node", "key2");
		subnodes = repo.getSubnodes(jid);
		Assert.assertArrayEquals(new String[] { "test" }, subnodes);
		subnodes = repo.getSubnodes(jid, "test");
		Assert.assertArrayEquals(new String[] { "node2" }, subnodes);
		repo.removeData(jid, "test/node2", "list");
		subnodes = repo.getSubnodes(jid, "test");
		Assert.assertArrayEquals(new String[] { }, subnodes);
		
		repo.removeUser(jid);
		Assert.assertFalse("User removal failed", repo.userExists(jid));
	}
}
