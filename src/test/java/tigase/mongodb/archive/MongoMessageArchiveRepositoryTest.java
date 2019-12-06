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
package tigase.mongodb.archive;

import com.mongodb.client.MongoCollection;
import org.bson.Document;
import org.junit.Assert;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;
import tigase.archive.QueryCriteria;
import tigase.archive.db.MessageArchiveRepository;
import tigase.component.exceptions.RepositoryException;
import tigase.db.util.RepositoryVersionAware;
import tigase.mongodb.MongoDataSource;
import tigase.util.Version;
import tigase.xml.Element;
import tigase.xmpp.jid.BareJID;
import tigase.xmpp.jid.JID;
import tigase.xmpp.mam.MAMRepository;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Optional;

/**
 * @author andrzej
 */
// Do not remove - it imports tests from Tigase Message Archiving project
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class MongoMessageArchiveRepositoryTest
		extends tigase.archive.db.AbstractMessageArchiveRepositoryTest<MongoDataSource,MessageArchiveRepository<QueryCriteria,MongoDataSource>> {

	private byte[] generateId(String in) throws RepositoryException {
		try {
			MessageDigest md = MessageDigest.getInstance("SHA-256");
			return md.digest(in.getBytes());
		} catch (NoSuchAlgorithmException ex) {
			throw new RepositoryException("Should not happen!!", ex);
		}
	}

	private MongoCollection<Document> getCollection(String name) {
		return ((MongoDataSource) dataSource).getDatabase().getCollection(name);
	}

	@Test
	public void testSchemaUpgrade_JidComparison() throws Exception {
		Date start = new Date();
		MongoCollection<Document> msgsCollection = getCollection("tig_ma_msgs");

		BareJID ownerJid = BareJID.bareJIDInstance("TeSt1@example.com");
		JID buddyJid = JID.jidInstance("TeSt12X@example.com");

		String owner = ownerJid.toString();
		String buddy = buddyJid.getBareJID().toString();
		byte[] oid = generateId(owner);
		byte[] bid = generateId(buddy);
		byte[] odid = generateId(ownerJid.getDomain());

		String type = "chat";
		Date timestamp = new Date();
		Date date = new Date(timestamp.getTime() - (timestamp.getTime() % (24 * 60 * 60 * 1000)));
		byte[] hash = "hash-dummy".getBytes();

		Document dto = new Document("owner", owner).append("owner_id", oid)
				.append("owner_domain_id", odid)
				.append("buddy", buddy)
				.append("buddy_id", bid)
				.append("buddy_res", buddyJid.getResource())
				// adding date for aggregation
				.append("date", date)
				.append("direction", MessageArchiveRepository.Direction.incoming.name())
				.append("ts", timestamp)
				.append("type", type)
				.append("msg", "<message from='" + buddyJid.toString() + "' to='" + ownerJid.toString() +
						"' type='chat'><body>Test body</body></message>")
				.append("hash", hash);

		msgsCollection.insertOne(dto);

		QueryCriteria crit = repo.newQuery();
		crit.setUseMessageIdInRsm(false);
		crit.setQuestionerJID(JID.jidInstance(owner));
		crit.setWith(buddyJid.copyWithoutResource());

		List<Element> msgs = new ArrayList<>();
		repo.queryItems(crit, (QueryCriteria qc, MAMRepository.Item item) -> {
			item.getMessage().setName(((MessageArchiveRepository.Item) item).getDirection().toElementName());
			msgs.add(item.getMessage());
			if (qc.getRsm().getFirst() == null) {
				qc.getRsm().setFirst(item.getId());
			}
			qc.getRsm().setLast(item.getId());
		});
		Assert.assertEquals("Incorrect number of message", 0, msgs.size());

		msgs.clear();

		((RepositoryVersionAware) repo).updateSchema(Optional.of(Version.ZERO), Version.ZERO);

		repo.queryItems(crit, (QueryCriteria qc, MAMRepository.Item item) -> {
			item.getMessage().setName(((MessageArchiveRepository.Item) item).getDirection().toElementName());
			msgs.add(item.getMessage());
			if (qc.getRsm().getFirst() == null) {
				qc.getRsm().setFirst(item.getId());
			}
			qc.getRsm().setLast(item.getId());
		});
		Assert.assertEquals("Incorrect number of message", 1, msgs.size());

		repo.removeItems(ownerJid, buddyJid.getBareJID().toString(), start, new Date());
	}

}
