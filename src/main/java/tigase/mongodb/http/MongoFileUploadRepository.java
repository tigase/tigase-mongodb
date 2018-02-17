/*
 * MongoFileUploadRepository.java
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
package tigase.mongodb.http;

import com.mongodb.MongoWriteException;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Projections;
import com.mongodb.client.model.Sorts;
import com.mongodb.client.model.Updates;
import org.bson.Document;
import tigase.db.Repository;
import tigase.db.TigaseDBException;
import tigase.http.db.Schema;
import tigase.http.upload.db.FileUploadRepository;
import tigase.mongodb.MongoDataSource;
import tigase.xmpp.jid.BareJID;
import tigase.xmpp.jid.JID;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import static tigase.mongodb.Helper.collectionExists;

/**
 * Created by andrzej on 14.03.2017.
 */
@Repository.Meta(supportedUris = {"mongodb:.*"})
@Repository.SchemaId(id = Schema.HTTP_UPLOAD_SCHEMA_ID, name = Schema.HTTP_UPLOAD_SCHEMA_NAME, external = false)
public class MongoFileUploadRepository
		implements FileUploadRepository<MongoDataSource> {

	private static final String SLOTS = "tig_hfu_slots";

	private MongoDatabase db;
	private MongoCollection<Document> slots;

	@Override
	public Slot allocateSlot(JID sender, String slotId, String filename, long filesize, String contentType)
			throws TigaseDBException {
		BareJID bareJid = sender.getBareJID();
		Date date = new Date();
		try {
			Document doc = new Document("_id", slotId).append("uploader", sender.toString())
					.append("domain", sender.getDomain())
					.append("resource", sender.getResource())
					.append("filename", filename)
					.append("filesize", filesize)
					.append("content_type", contentType)
					.append("status", 0)
					.append("ts", date);

			slots.insertOne(doc);
		} catch (MongoWriteException ex) {
			throw new TigaseDBException("Failed to allocate slot for file transfer", ex);
		}
		return new Slot(bareJid, slotId, filename, filesize, contentType, date);
	}

	@Override
	public Slot getSlot(BareJID sender, String slotId) throws TigaseDBException {
		Document doc = slots.find(Filters.eq("_id", slotId)).first();
		if (doc == null) {
			return null;
		}
		BareJID bareJID = BareJID.bareJIDInstanceNS(doc.getString("uploader"));
		String filename = doc.getString("filename");
		long filesize = doc.getLong("filesize");
		String contentType = doc.getString("content_type");
		Date ts = doc.getDate("ts");
		return new Slot(bareJID, slotId, filename, filesize, contentType, ts);
	}

	@Override
	public List<Slot> listExpiredSlots(BareJID domain, LocalDateTime before, int limit) throws TigaseDBException {
		long timestamp_long = before.toEpochSecond(ZoneOffset.UTC) * 1000;
		List<Slot> results = new ArrayList<>();
		for (Document doc : slots.find(Filters.lt("ts", new Date(timestamp_long)))
				.limit(limit)
				.sort(Sorts.ascending("ts"))) {
			String slotId = doc.getString("_id");
			BareJID bareJID = BareJID.bareJIDInstanceNS(doc.getString("uploader"));
			String filename = doc.getString("filename");
			long filesize = doc.getLong("filesize");
			String contentType = doc.getString("content_type");
			Date ts = doc.getDate("ts");
			results.add(new Slot(bareJID, slotId, filename, filesize, contentType, ts));
		}
		return results;
	}

	@Override
	public void removeExpiredSlots(BareJID domain, LocalDateTime before, int limit) throws TigaseDBException {
		long timestamp_long = before.toEpochSecond(ZoneOffset.UTC) * 1000;

		List<String> ids = new ArrayList<>();
		for (Document doc : slots.find(Filters.lt("ts", new Date(timestamp_long)))
				.limit(limit)
				.projection(Projections.include("_id"))) {
			ids.add(doc.getString("_id"));
		}
		slots.deleteMany(Filters.in("_id", ids));
	}

	@Override
	public void setDataSource(MongoDataSource dataSource) {
		db = dataSource.getDatabase();

		if (!collectionExists(db, SLOTS)) {
			db.createCollection(SLOTS);
		}
		slots = db.getCollection(SLOTS);
		slots.createIndex(new Document("ts", 1));
	}

	@Override
	public void updateSlot(BareJID userJid, String slotId) throws TigaseDBException {
		slots.updateOne(Filters.eq("_id", slotId), Updates.set("status", 1));
	}
}
