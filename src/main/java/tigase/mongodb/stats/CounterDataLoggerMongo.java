/*
 * CounterDataLoggerMongo.java
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
package tigase.mongodb.stats;

import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import tigase.component.exceptions.RepositoryException;
import tigase.db.DBInitException;
import tigase.db.DataSource;
import tigase.db.Repository;
import tigase.mongodb.MongoDataSource;
import tigase.stats.CounterDataLogger;
import tigase.stats.db.CounterDataLoggerRepositoryIfc;

import java.util.Date;
import java.util.logging.Level;
import java.util.logging.Logger;

import static tigase.mongodb.Helper.collectionExists;

/**
 * @author Wojciech Kapcia
 */
@Repository.Meta(supportedUris = {"mongodb:.*"})
public class CounterDataLoggerMongo
		implements CounterDataLoggerRepositoryIfc<MongoDataSource> {

	private static final Logger log = Logger.getLogger(CounterDataLoggerMongo.class.getName());
	private MongoDatabase db;
	private MongoCollection<Document> tigaseStatsLogCollection;

	@Override
	public void addStatsLogEntry(String hostname, float cpu_usage, float mem_usage, long uptime, int vhosts,
	                             long sm_packets, long muc_packets, long pubsub_packets, long c2s_packets,
	                             long ws2s_packets, long s2s_packets, long ext_packets, long presences, long messages,
	                             long iqs, long registered, int c2s_conns, int ws2s_conns, int bosh_conns,
	                             int s2s_conns, int sm_sessions, int sm_connections) {

		try {
			Document dto = new Document().append("ts", new Date())
					.append(HOSTNAME_COL, hostname)
					.append(CPU_USAGE_COL, cpu_usage)
					.append(MEM_USAGE_COL, mem_usage)
					.append(UPTIME_COL, uptime)
					.append(VHOSTS_COL, vhosts)
					.append(SM_PACKETS_COL, sm_packets)
					.append(MUC_PACKETS_COL, muc_packets)
					.append(PUBSUB_PACKETS_COL, pubsub_packets)
					.append(C2S_PACKETS_COL, c2s_packets)
					.append(WS2S_PACKETS_COL, ws2s_packets)
					.append(S2S_PACKETS_COL, s2s_packets)
					.append(EXT_PACKETS_COL, ext_packets)
					.append(PRESENCES_COL, presences)
					.append(MESSAGES_COL, messages)
					.append(IQS_COL, iqs)
					.append(REGISTERED_COL, registered)
					.append(C2S_CONNS_COL, c2s_conns)
					.append(WS2S_CONNS_COL, ws2s_conns)
					.append(BOSH_CONNS_COL, bosh_conns)
					.append(S2S_CONNS_COL, s2s_conns)
					.append(SM_CONNECTIONS_COL, sm_connections)
					.append(SM_SESSIONS_COL, sm_sessions);
			tigaseStatsLogCollection.insertOne(dto);
		} catch (Exception ex) {
			log.log(Level.WARNING, "Problem setting element to DB: ", ex);
		}
	}

	@Override
	public void setDataSource(MongoDataSource dataSource) throws RepositoryException {
		db = dataSource.getDatabase();

		if (!collectionExists(db, STATS_TABLE)) {
			db.createCollection(STATS_TABLE);
		}
		tigaseStatsLogCollection = db.getCollection(STATS_TABLE);
		tigaseStatsLogCollection.createIndex(new BasicDBObject("hostname", 1));
	}
}
