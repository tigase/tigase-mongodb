/*
 * Helper.java
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
package tigase.mongodb;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.IndexOptions;
import org.bson.Document;

import java.util.Map;

/**
 * Created by andrzej on 23.03.2016.
 */
public class Helper {

	public static boolean collectionExists(MongoDatabase db, String collection) {
		for (String name : db.listCollectionNames()) {
			if (collection.equals(name)) {
				return true;
			}
		}
		return false;
	}

	public static String createIndexName(Document index) {
		StringBuilder sb = new StringBuilder();
		for (Map.Entry<String, Object> e : index.entrySet()) {
			if (sb.length() > 0) {
				sb.append("_");
			}
			sb.append(e.getKey());
			sb.append("_");
			sb.append(e.getValue());
		}
		return sb.toString();
	}

	public static void indexCreateOrReplace(MongoCollection<Document> collection, Document index,
	                                        IndexOptions options) {
		String indexName = createIndexName(index);
		boolean drop = false;
		for (Document idx : collection.listIndexes()) {
			if (indexName.equals(idx.getString("name")) && (idx.getBoolean("unique", false) != options.isUnique())) {
				drop = true;
			}
		}
		if (drop) {
			collection.dropIndex(indexName);
		}
		collection.createIndex(index, options);
	}
}
