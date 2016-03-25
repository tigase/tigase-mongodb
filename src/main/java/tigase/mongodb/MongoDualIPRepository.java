/*
 * Tigase Jabber/XMPP Server
 * Copyright (C) 2004-2016 "Tigase, Inc." <office@tigase.com>
 * 
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, version 3 of the License,
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
 */
package tigase.mongodb;

import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import tigase.db.DBInitException;
import tigase.db.Repository;
import tigase.db.RepositoryFactory;
import tigase.server.xmppclient.SeeOtherHostDualIP.DualIPRepository;
import tigase.util.TigaseStringprepException;
import tigase.xmpp.BareJID;

import java.sql.SQLException;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.logging.Level;
import java.util.logging.Logger;

import static tigase.cluster.repo.ClusterRepoConstants.REPO_URI_PROP_KEY;
import static tigase.mongodb.Helper.collectionExists;

/**
 *
 * @author Wojtek
 */
@Repository.Meta(supportedUris = { "mongodb:.*" })
public class MongoDualIPRepository implements DualIPRepository {

	private static final Logger log = Logger.getLogger( MongoDualIPRepository.class.getCanonicalName() );

	private static final String CLUSTER_NODES = "cluster_nodes";

	private String resourceUri;

	private MongoClient mongo;
	private MongoDatabase db;
	private MongoCollection<Document> clusterNodes;

	@Override
	public void getDefaults( Map<String, Object> defs, Map<String, Object> params ) {
		String repo_uri = RepositoryFactory.DERBY_REPO_URL_PROP_VAL;

		if ( params.get( RepositoryFactory.GEN_USER_DB_URI ) != null ){
			repo_uri = (String) params.get( RepositoryFactory.GEN_USER_DB_URI );
		}
		defs.put( REPO_URI_PROP_KEY, repo_uri );

	}

	@Override
	public void initRepository( String resource_uri, Map<String, String> params ) throws DBInitException {

		try {
			resourceUri = resource_uri;
			MongoClientURI uri = new MongoClientURI( resource_uri );
			//uri.get
			mongo = new MongoClient( uri );
			db = mongo.getDatabase( uri.getDatabase() );

			if (!collectionExists(db, CLUSTER_NODES)) {
				db.createCollection(CLUSTER_NODES);
			}
			clusterNodes = db.getCollection( CLUSTER_NODES );
			clusterNodes.createIndex( new BasicDBObject( "hostname", 1 ) );

		} catch ( Exception ex ) {
			throw new DBInitException( "Could not initialize MongoDB repository", ex );
		}

	}

	@Override
	public Map<BareJID, BareJID> queryAllDB() throws SQLException {

		Map<BareJID, BareJID> result = new ConcurrentSkipListMap<BareJID, BareJID>();

		try {
			for ( Document dto : clusterNodes.find().batchSize(100) ) {

				String user_jid = (String) dto.get( HOSTNAME_ID );
				String node_jid = (String) dto.get( SECONDARY_HOSTNAME_ID );
				try {
					BareJID hostname_hid = BareJID.bareJIDInstance( user_jid );
					BareJID secondary = BareJID.bareJIDInstance( node_jid );
					result.put( hostname_hid, secondary );
				} catch ( TigaseStringprepException ex ) {
					log.warning( "Invalid host or secondary hostname JID: " + user_jid + ", " + node_jid );
				}
			}

		} catch ( Exception ex ) {
			log.log( Level.WARNING, "Problem getting elements from DB: ", ex );
		}

		log.info( "Loaded " + result.size() + " redirect definitions from database." );
		return result;

	}

	@Override
	public void setProperties( Map<String, Object> props ) {
	}

}
