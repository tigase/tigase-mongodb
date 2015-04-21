/*
 * MongoHistoryProviderTest.java
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

import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import tigase.component.PacketWriter;
import tigase.component.responses.AsyncCallback;
import tigase.db.DBInitException;
import tigase.mongodb.muc.MongoHistoryProvider;
import tigase.muc.Room;
import tigase.muc.RoomConfig;
import tigase.server.Packet;
import tigase.xmpp.BareJID;
import tigase.xmpp.JID;

/**
 *
 * @author andrzej
 */
@Ignore
public class MongoHistoryProviderTest {

	private MongoHistoryProvider provider;
	private Room room;

	private JID test1 = JID.jidInstanceNS("test1@tigase/test");
	
	@Before
	public void setup() throws DBInitException {
		provider = new MongoHistoryProvider();
		provider.initRepository("mongodb://localhost/tigase_junit", new HashMap<String,String>());
		
		room = Room.newInstance(new RoomConfig(BareJID.bareJIDInstanceNS("test@muc.example")), new Date(), test1.getBareJID());
	}
	
	@After
	public void tearDown() {
		provider.destroy();
		provider = null;
	}

	@Test
	public void testProviderByLastMessages() {
		provider.addMessage(room, null, "Test message 1", test1, "Test 1", new Date());
		final AtomicInteger count = new AtomicInteger(0);
		provider.getHistoryMessages(room, test1, null, 1, null, null, new PacketWriter() {

			@Override
			public void write(Collection<Packet> packets) {
				for (Packet p : packets) {
					write(p);
				}
			}

			@Override
			public void write(Packet packet) {
				Assert.assertEquals("Retrieved incorrect messsage", "Test message 1", packet.getElement().getChildCDataStaticStr(new String[] { "message", "body" }));
				count.incrementAndGet();
			}

			@Override
			public void write(Packet packet, AsyncCallback callback) {
				throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
			}
			
		});
		Assert.assertEquals("Not retrieved correct number of messages", 1, count.get());
	}
	
	@Test
	public void testProviderByDate() {
		Date date = new Date();
		provider.addMessage(room, null, "Test message 2", test1, "Test 2", date);
		final AtomicInteger count = new AtomicInteger(0);
		provider.getHistoryMessages(room, test1, null, null, null, date, new PacketWriter() {

			@Override
			public void write(Collection<Packet> packets) {
				for (Packet p : packets) {
					write(p);
				}
			}

			@Override
			public void write(Packet packet) {
				Assert.assertEquals("Retrieved incorrect messsage", "Test message 2", packet.getElement().getChildCDataStaticStr(new String[] { "message", "body" }));
				count.incrementAndGet();
			}

			@Override
			public void write(Packet packet, AsyncCallback callback) {
				throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
			}
			
		});
		Assert.assertEquals("Not retrieved correct number of messages", 1, count.get());
	}	
	
	@Test
	public void testProviderRemoval() {
		provider.addMessage(room, null, "Test message 3", test1, "Test 3", new Date());
		provider.removeHistory(room);
		final AtomicInteger count = new AtomicInteger(0);
		provider.getHistoryMessages(room, test1, null, 1, null, null, new PacketWriter() {

			@Override
			public void write(Collection<Packet> packets) {
				for (Packet p : packets) {
					write(p);
				}
			}

			@Override
			public void write(Packet packet) {
				Assert.assertEquals("Retrieved incorrect messsage", "Test message 2", packet.getElement().getChildCDataStaticStr(new String[] { "message", "body" }));
				count.incrementAndGet();
			}

			@Override
			public void write(Packet packet, AsyncCallback callback) {
				throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
			}
			
		});
		Assert.assertEquals("Not retrieved correct number of messages", 0, count.get());
	}
	
}
