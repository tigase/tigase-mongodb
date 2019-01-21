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
package tigase.mongo.stats;

import tigase.db.DBInitException;
import tigase.db.Repository;

import tigase.stats.CounterDataLogger;

import java.sql.SQLException;
import java.util.Date;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;

/**
 *
 * @author Wojciech Kapcia
 */
@Repository.Meta(supportedUris = { "mongodb:.*" })
public class CounterDataLoggerMongo extends CounterDataLogger {

	private static final Logger log = Logger.getLogger( CounterDataLoggerMongo.class.getName() );

	private String resourceUri;

	private MongoClient mongo;
	private DB db;
	private DBCollection tigaseStatsLogCollection;

	@Override
	public void addStatsLogEntry( float cpu_usage, float mem_usage, long uptime, int vhosts, long sm_packets, long muc_packets, long pubsub_packets, long c2s_packets, long s2s_packets, long ext_packets, long presences, long messages, long iqs, long registered, int c2s_conns, int s2s_conns, int bosh_conns ) {

		try {
			BasicDBObject dto = new BasicDBObject()
					.append( "ts", new Date() )
					.append( HOSTNAME_COL, defaultHostname )
					.append( CPU_USAGE_COL, cpu_usage )
					.append( MEM_USAGE_COL, mem_usage )
					.append( UPTIME_COL, uptime )
					.append( VHOSTS_COL, vhosts )
					.append( SM_PACKETS_COL, sm_packets )
					.append( MUC_PACKETS_COL, muc_packets )
					.append( PUBSUB_PACKETS_COL, pubsub_packets )
					.append( C2S_PACKETS_COL, c2s_packets )
					.append( S2S_PACKETS_COL, s2s_packets )
					.append( EXT_PACKETS_COL, ext_packets )
					.append( PRESENCES_COL, presences )
					.append( MESSAGES_COL, messages )
					.append( IQS_COL, iqs )
					.append( REGISTERED_COL, registered )
					.append( C2S_CONNS_COL, c2s_conns )
					.append( S2S_CONNS_COL, s2s_conns )
					.append( BOSH_CONNS_COL, bosh_conns );
			tigaseStatsLogCollection.insert( dto );
		} catch ( Exception ex ) {
			log.log( Level.WARNING, "Problem setting element to DB: ", ex );
		}
	}

	@Override
	public void init( Map<String, Object> archivizerConf ) {
		super.init( archivizerConf );
	}

	@Override
	public void initRepository( String conn_str, Map<String, String> map ) throws SQLException, ClassNotFoundException, IllegalAccessException, InstantiationException, DBInitException {
		try {
			resourceUri = conn_str;
			MongoClientURI uri = new MongoClientURI( resourceUri );
			mongo = new MongoClient( uri );
			db = mongo.getDB( uri.getDatabase() );

			tigaseStatsLogCollection = db.collectionExists( STATS_TABLE )
																 ? db.getCollection( STATS_TABLE )
																 : db.createCollection( STATS_TABLE, new BasicDBObject() );

		} catch ( Exception ex ) {
			throw new DBInitException( "Could not initialize MongoDB repository", ex );
		}
	}
}
