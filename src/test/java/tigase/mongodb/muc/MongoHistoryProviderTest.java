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
package tigase.mongodb.muc;

import org.bson.Document;
import org.junit.Assert;
import org.junit.Test;
import tigase.component.PacketWriter;
import tigase.component.responses.AsyncCallback;
import tigase.mongodb.MongoDataSource;
import tigase.muc.Affiliation;
import tigase.muc.Room;
import tigase.muc.RoomConfig;
import tigase.muc.history.AbstractHistoryProviderTest;
import tigase.server.Packet;
import tigase.util.Version;
import tigase.xmpp.jid.BareJID;
import tigase.xmpp.jid.JID;

import java.util.Collection;
import java.util.Date;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertTrue;

/**
 * @author andrzej
 */
public class MongoHistoryProviderTest
		extends AbstractHistoryProviderTest<MongoDataSource> {

	protected MongoHistoryProvider getProvider() {
		return (MongoHistoryProvider) historyProvider;
	}

	@Test
	public void testSchemaUpgrade_JidComparison() throws Exception {
		BareJID jid = BareJID.bareJIDInstance("TeSt@muc.example");
		byte[] rid = getProvider().calculateHash(jid.toString());

		JID creatorJID = JID.jidInstance("TeStX3@example");
		RoomConfig rc = new RoomConfig(jid);
		rc.setValue(RoomConfig.MUC_ROOMCONFIG_PERSISTENTROOM_KEY, Boolean.TRUE);
		creationDate = new Date();
		Room room = roomFactory.newInstance(null, rc, creationDate, creatorJID.getBareJID());
		room.addAffiliationByJid(creatorJID.getBareJID(), Affiliation.owner);

		String body = "Test JID Comparison";

		Document dto = new Document("room_jid_id", rid).append("room_jid", jid.toString())
				.append("event_type", 1)
				.append("sender_jid", creatorJID.getBareJID().toString())
				.append("sender_nickname", "Test 1")
				.append("body", body)
				.append("public_event", room.getConfig().isLoggingEnabled());
		dto.append("timestamp", new Date());
		getProvider().historyCollection.insertOne(dto);

		getProvider().getHistoryMessages(room, creatorJID, null, 1, null, null, new PacketWriter() {

			@Override
			public void write(Collection<Packet> packets) {
				for (Packet p : packets) {
					write(p);
				}
			}

			@Override
			public void write(Packet packet) {
				assertTrue("There should be no messages found!", false);
			}

			@Override
			public void write(Packet packet, AsyncCallback callback) {
				throw new UnsupportedOperationException(
						"Not supported yet."); //To change body of generated methods, choose Tools | Templates.
			}

		});

		getProvider().updateSchema(Optional.of(Version.ZERO), Version.ZERO);

		final AtomicInteger count = new AtomicInteger(0);
		getProvider().getHistoryMessages(room, creatorJID, null, 1, null, null, new PacketWriter() {

			@Override
			public void write(Collection<Packet> packets) {
				for (Packet p : packets) {
					write(p);
				}
			}

			@Override
			public void write(Packet packet) {
				Assert.assertEquals("Retrieved incorrect messsage", body,
				                    packet.getElement().getChildCDataStaticStr(new String[]{"message", "body"}));
				count.incrementAndGet();
			}

			@Override
			public void write(Packet packet, AsyncCallback callback) {
				throw new UnsupportedOperationException(
						"Not supported yet."); //To change body of generated methods, choose Tools | Templates.
			}

		});

		Assert.assertEquals("Not retrieved correct number of messages", 1, count.get());

		getProvider().removeHistory(room);
	}
}
