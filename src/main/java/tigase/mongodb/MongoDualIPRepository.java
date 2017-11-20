/*
 * MongoDualIPRepository.java
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

import com.mongodb.BasicDBObject;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import tigase.db.Repository;
import tigase.server.xmppclient.SeeOtherHostDualIP.DualIPRepository;
import tigase.util.stringprep.TigaseStringprepException;
import tigase.xmpp.jid.BareJID;

import java.sql.SQLException;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.logging.Level;
import java.util.logging.Logger;

import static tigase.mongodb.Helper.collectionExists;

/**
 * @author Wojtek
 */
@Repository.Meta(supportedUris = {"mongodb:.*"})
public class MongoDualIPRepository
		implements DualIPRepository<MongoDataSource> {

	private static final Logger log = Logger.getLogger(MongoDualIPRepository.class.getCanonicalName());

	private static final String CLUSTER_NODES = "tig_cluster_nodes";
	private MongoCollection<Document> clusterNodes;
	private MongoDatabase db;

	@Override
	public Map<BareJID, BareJID> queryAllDB() throws SQLException {

		Map<BareJID, BareJID> result = new ConcurrentSkipListMap<BareJID, BareJID>();

		try {
			for (Document dto : clusterNodes.find().batchSize(100)) {

				String user_jid = (String) dto.get(HOSTNAME_ID);
				String node_jid = (String) dto.get(SECONDARY_HOSTNAME_ID);
				try {
					BareJID hostname_hid = BareJID.bareJIDInstance(user_jid);
					BareJID secondary = BareJID.bareJIDInstance(node_jid);
					result.put(hostname_hid, secondary);
				} catch (TigaseStringprepException ex) {
					log.warning("Invalid host or secondary hostname JID: " + user_jid + ", " + node_jid);
				}
			}

		} catch (Exception ex) {
			log.log(Level.WARNING, "Problem getting elements from DB: ", ex);
		}

		log.info("Loaded " + result.size() + " redirect definitions from database.");
		return result;

	}

	@Override
	public void setDataSource(MongoDataSource dataSource) {
		db = dataSource.getDatabase();

		if (!collectionExists(db, CLUSTER_NODES)) {
			db.createCollection(CLUSTER_NODES);
		}
		clusterNodes = db.getCollection(CLUSTER_NODES);
		clusterNodes.createIndex(new BasicDBObject("hostname", 1));
	}

}
