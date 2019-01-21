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

import org.junit.*;
import org.junit.runners.MethodSorters;
import tigase.archive.AbstractCriteria;
import tigase.archive.db.MessageArchiveRepository;
import tigase.db.DBInitException;
import tigase.db.TigaseDBException;
import tigase.mongodb.archive.MongoMessageArchiveRepository;
import tigase.xml.Element;
import tigase.xmpp.JID;
import tigase.xmpp.StanzaType;

import java.text.SimpleDateFormat;
import java.util.*;

/**
 *
 * @author andrzej
 */
@Ignore
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class MongoMessageArchiveRepositoryTest {
	
	private final static SimpleDateFormat formatter2 = new SimpleDateFormat(
			"yyyy-MM-dd'T'HH:mm:ssXX");
	
	static {
		formatter2.setTimeZone(TimeZone.getTimeZone("UTC"));
	}	
	
	private String uri = System.getProperty("testDbUri");
	private MessageArchiveRepository repo;
	// this is static to pass date from first test to next one
	private static Date testStart = null;

	private JID owner = JID.jidInstanceNS("test1@zeus/tigase-1");
	private JID buddy = JID.jidInstanceNS("test2@zeus/tigase-2");	
	
	@Before
	public void setup() throws DBInitException {
		Assume.assumeNotNull(uri);
		
		repo = new MongoMessageArchiveRepository();
		repo.initRepository(uri, new HashMap<String,String>());
	}
	
	@After
	public void tearDown() {
		repo.destroy();
		repo = null;
	}

	@Test
	public void test1_archiveMessage1() throws TigaseDBException {
		Date date = new Date();
		testStart = date;
		String body = "Test 1";
		Element msg = new Element("message", new String[] { "from", "to", "type"}, new String[] { owner.toString(), buddy.toString(), StanzaType.chat.name()});
		msg.addChild(new Element("body", body));
		repo.archiveMessage(owner.getBareJID(), buddy, MessageArchiveRepository.Direction.outgoing, date, msg, null);
	
		AbstractCriteria crit = repo.newCriteriaInstance();
		crit.setWith(buddy.getBareJID().toString());
		crit.setStart(date);
		crit.setSize(1);
		List<Element> msgs = repo.getItems(owner.getBareJID(), crit);
		Assert.assertEquals("Incorrect number of message", 1, msgs.size());
		
		Element res = msgs.get(0);
		Assert.assertEquals("Incorrect direction", MessageArchiveRepository.Direction.outgoing.toElementName(), res.getName());
		Assert.assertEquals("Incorrect message body", body, res.getChildCData(res.getName()+"/body"));
	}
	
	@Test
	public void test2_archiveMessage2withTags() throws InterruptedException, TigaseDBException {
		Thread.sleep(2000);
		Date date = new Date();
		String body = "Test 2 with #Test123";
		Element msg = new Element("message", new String[] { "from", "to", "type"}, new String[] { owner.toString(), buddy.toString(), StanzaType.chat.name()});
		msg.addChild(new Element("body", body));
		Set<String> tags = new HashSet<String>();
		tags.add("#Test123");
		repo.archiveMessage(owner.getBareJID(), buddy, MessageArchiveRepository.Direction.incoming, date, msg, tags);
		
		AbstractCriteria crit = repo.newCriteriaInstance();
		crit.setWith(buddy.getBareJID().toString());
		crit.setStart(date);
		crit.setSize(1);
		List<Element> msgs = repo.getItems(owner.getBareJID(), crit);
		Assert.assertEquals("Incorrect number of message", 1, msgs.size());
		
		Element res = msgs.get(0);
		Assert.assertEquals("Incorrect direction", MessageArchiveRepository.Direction.incoming.toElementName(), res.getName());
		Assert.assertEquals("Incorrect message body", body, res.getChildCData(res.getName()+"/body"));
	}
	
	@Test
	public void test3_getCollections() throws TigaseDBException {
		AbstractCriteria crit = repo.newCriteriaInstance();
		crit.setWith(buddy.getBareJID().toString());
		crit.setStart(testStart);
		
		System.out.println("owner: " + owner + " buddy: " + buddy + " date: " + testStart);
		List<Element> chats = repo.getCollections(owner.getBareJID(), crit);
		Assert.assertEquals("Incorrect number of collections", 1, chats.size());
		
		Element chat = chats.get(0);
		Assert.assertEquals("Incorrect buddy", buddy.getBareJID().toString(), chat.getAttribute("with"));
		Assert.assertEquals("Incorrect timestamp", formatter2.format(testStart), chat.getAttribute("start"));
	}
		
	@Test
	public void test3_getCollectionsByTag() throws TigaseDBException {
		AbstractCriteria crit = repo.newCriteriaInstance();
		crit.setWith(buddy.getBareJID().toString());
		crit.setStart(testStart);
		crit.addTag("#Test123");
		
		System.out.println("owner: " + owner + " buddy: " + buddy + " date: " + testStart);
		List<Element> chats = repo.getCollections(owner.getBareJID(), crit);
		Assert.assertEquals("Incorrect number of collections", 1, chats.size());
		
		Element chat = chats.get(0);
		Assert.assertEquals("Incorrect buddy", buddy.getBareJID().toString(), chat.getAttribute("with"));
	}
		
	@Test
	public void test4_getItems() throws InterruptedException, TigaseDBException {
		AbstractCriteria crit = repo.newCriteriaInstance();
		crit.setWith(buddy.getBareJID().toString());
		crit.setStart(testStart);
		
		List<Element> msgs = repo.getItems(owner.getBareJID(), crit);
		Assert.assertEquals("Incorrect number of message", 2, msgs.size());
		
		Element res = msgs.get(0);
		Assert.assertEquals("Incorrect direction", MessageArchiveRepository.Direction.outgoing.toElementName(), res.getName());
		Assert.assertEquals("Incorrect message body", "Test 1", res.getChildCData(res.getName()+"/body"));
		
		res = msgs.get(1);
		Assert.assertEquals("Incorrect direction", MessageArchiveRepository.Direction.incoming.toElementName(), res.getName());
		Assert.assertEquals("Incorrect message body", "Test 2 with #Test123", res.getChildCData(res.getName()+"/body"));
	}
	
	@Test
	public void test4_getItemsWithTag() throws InterruptedException, TigaseDBException {
		AbstractCriteria crit = repo.newCriteriaInstance();
		crit.setWith(buddy.getBareJID().toString());
		crit.setStart(testStart);
		crit.addTag("#Test123");
		
		List<Element> msgs = repo.getItems(owner.getBareJID(), crit);
		Assert.assertEquals("Incorrect number of message", 1, msgs.size());
		
		Element res = msgs.get(0);
		Assert.assertEquals("Incorrect direction", MessageArchiveRepository.Direction.incoming.toElementName(), res.getName());
		Assert.assertEquals("Incorrect message body", "Test 2 with #Test123", res.getChildCData(res.getName()+"/body"));
	}
	
	//tests as MongoDB uses full text search which may return results which we cannot predict
	@Test
	public void test5_getCollectionsContains() throws TigaseDBException {
		AbstractCriteria crit = repo.newCriteriaInstance();
		crit.setWith(buddy.getBareJID().toString());
		crit.setStart(testStart);
		crit.addContains("Test 1");
		
		System.out.println("owner: " + owner + " buddy: " + buddy + " date: " + testStart);
		List<Element> chats = repo.getCollections(owner.getBareJID(), crit);
		Assert.assertNotEquals("Incorrect number of collections", 0, chats.size());
		
		Element chat = chats.get(0);
		Assert.assertEquals("Incorrect buddy", buddy.getBareJID().toString(), chat.getAttribute("with"));
		Assert.assertEquals("Incorrect timestamp", formatter2.format(testStart), chat.getAttribute("start"));		
		
		crit = repo.newCriteriaInstance();
		crit.setWith(buddy.getBareJID().toString());
		crit.setStart(testStart);
		crit.addContains("Test 123");
		
		System.out.println("owner: " + owner + " buddy: " + buddy + " date: " + testStart);
		chats = repo.getCollections(owner.getBareJID(), crit);
		Assert.assertNotEquals("Incorrect number of collections", 0, chats.size());		
	}
	
	//tests as MongoDB uses full text search which may return results which we cannot predict
	@Test
	public void test6_getItems() throws InterruptedException, TigaseDBException {
		AbstractCriteria crit = repo.newCriteriaInstance();
		crit.setWith(buddy.getBareJID().toString());
		crit.setStart(testStart);
		crit.addContains("Test 1");
		
		List<Element> msgs = repo.getItems(owner.getBareJID(), crit);
		Assert.assertNotEquals("Incorrect number of message", 0, msgs.size());
		
		Element res = msgs.get(0);
		Assert.assertEquals("Incorrect direction", MessageArchiveRepository.Direction.outgoing.toElementName(), res.getName());
		Assert.assertEquals("Incorrect message body", "Test 1", res.getChildCData(res.getName()+"/body"));
	}	
		
	
	@Test
	public void test7_removeItems() throws TigaseDBException {
		AbstractCriteria crit = repo.newCriteriaInstance();
		crit.setWith(buddy.getBareJID().toString());
		crit.setStart(testStart);
		
		List<Element> msgs = repo.getItems(owner.getBareJID(), crit);
		Assert.assertNotEquals("No messages in repository to execute test - we should have some already", 0, msgs.size());
		repo.removeItems(owner.getBareJID(), buddy.getBareJID().toString(), testStart, new Date());
		msgs = repo.getItems(owner.getBareJID(), crit);
		Assert.assertEquals("Still some messages, while in this duration all should be deleted", 0, msgs.size());
	}
}
