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
package tigase.mongodb.cluster;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import java.sql.ResultSet;
import java.util.Date;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import tigase.cluster.repo.ClConConfigRepository;
import tigase.cluster.repo.ClusterRepoConstants;
import static tigase.cluster.repo.ClusterRepoConstants.REPO_URI_PROP_KEY;
import tigase.db.DBInitException;
import tigase.db.Repository;
import tigase.db.RepositoryFactory;

@Repository.Meta( supportedUris = { "mongodb:.*" } )
public class ClConMongoRepository extends ClConConfigRepository
				implements ClusterRepoConstants {

	private static final Logger log = Logger.getLogger(ClConMongoRepository.class.getCanonicalName());
	
	private static final String CLUSTER_NODES = "cluster_nodes";
	
	private String resourceUri;
	
	private MongoClient mongo;
	private DB db;
	
	@Override
	public void destroy() {
		// This implementation of ClConConfigRepository is using shared connection
		// pool to database which is cached by RepositoryFactory and maybe be used
		// in other places, so we can not destroy it.
		super.destroy();
		if (mongo != null) {
			mongo.close();
		}
	}
	

	@Override
	public void getDefaults(Map<String, Object> defs, Map<String, Object> params) {
		super.getDefaults(defs, params);

		String repo_uri = RepositoryFactory.DERBY_REPO_URL_PROP_VAL;

		if (params.get(RepositoryFactory.GEN_USER_DB_URI) != null) {
			repo_uri = (String) params.get(RepositoryFactory.GEN_USER_DB_URI);
		}
		defs.put(REPO_URI_PROP_KEY, repo_uri);
	}

	//~--- methods --------------------------------------------------------------

	@Override
	public void initRepository(String resource_uri, Map<String, String> params)
					throws DBInitException {
		super.initRepository(resource_uri, params);	
		try {
			resourceUri = resource_uri;
			MongoClientURI uri = new MongoClientURI(resource_uri);
			//uri.get
			mongo = new MongoClient(uri);
			db = mongo.getDB(uri.getDatabase());
			
			DBCollection clusterNodes = db.collectionExists(CLUSTER_NODES)
					? db.getCollection(CLUSTER_NODES)
					: db.createCollection(CLUSTER_NODES, new BasicDBObject());
			clusterNodes.createIndex(new BasicDBObject("hostname", 1));
			
		} catch (Exception ex) {
			throw new DBInitException("Could not initialize MongoDB repository", ex);
		}
	}

	@Override
	public void removeItem( String key ) {
		super.removeItem( key );

		try {
			BasicDBObject crit = new BasicDBObject("hostname", key);
			db.getCollection(CLUSTER_NODES).remove(crit);
		} catch (Exception ex) {
			log.log(Level.WARNING, "Problem removing element from DB: ", ex);
		}

	}



	@Override
	public void storeItem(tigase.cluster.repo.ClusterRepoItem item) {
		try {
			BasicDBObject crit = new BasicDBObject("hostname", item.getHostname());
			BasicDBObject dto = new BasicDBObject("password", item.getPassword())
					.append("secondary", item.getSecondaryHostname())
					.append("updated", new Date()).append("port", item.getPortNo())
					.append("cpu_usage", item.getCpuUsage()).append("mem_usage", item.getMemUsage());
			db.getCollection(CLUSTER_NODES).update(crit, new BasicDBObject("$set", dto), true, false);
		} catch (Exception ex) {
			log.log(Level.WARNING, "Problem setting element to DB: ", ex);
		}
	}	
	
	@Override
	public void reload() {
		if ( ( System.currentTimeMillis() - lastReloadTime ) <= ( autoreload_interval * lastReloadTimeFactor ) ){
			if ( log.isLoggable( Level.FINEST ) ){
				log.log( Level.FINEST, "Last reload performed in {0}, skipping: ", ( System.currentTimeMillis() - lastReloadTime ) );
			}
			return;
		}
		lastReloadTime = System.currentTimeMillis();

		super.reload();
		DBCursor cursor = null;
		try {
			cursor = db.getCollection(CLUSTER_NODES).find();
			
			while (cursor.hasNext()) {
				DBObject dto = cursor.next();
				
				ClusterRepoItem item = getItemInstance();
					item.setHostname((String) dto.get("hostname"));
					item.setSecondaryHostname((String) dto.get("secondary"));
					item.setPassword((String) dto.get("password"));
					item.setLastUpdate(((Date) dto.get("updated")).getTime());
					item.setPort((Integer) dto.get("port"));
					item.setCpuUsage((Double) dto.get("cpu_usage"));
					item.setMemUsage((Double) dto.get("mem_usage"));				
				itemLoaded(item);
			}
			
		} catch (Exception ex) {
			log.log(Level.WARNING, "Problem getting elements from DB: ", ex);
		} finally {
			if (cursor != null) {
				cursor.close();
			}
		}
	}
	
	@Override
	public ClusterRepoItem getItemInstance() {
		return new ClusterRepoItem();
	}
	
	@Override
	public void store() {
		// Do nothing everything is written on demand to DB
	}	
	
	private class ClusterRepoItem extends tigase.cluster.repo.ClusterRepoItem {

		protected void setCpuUsage(Double cpuUsage) {
			super.setCpuUsage(cpuUsage == null ? 0 : cpuUsage.floatValue());
		}
		
		protected void setMemUsage(Double memUsage) {
			super.setMemUsage(memUsage == null ? 0 : memUsage.floatValue());
		}		

		@Override
		protected void setCpuUsage(float cpuUsage) {
			super.setCpuUsage(cpuUsage);
		}
		
		@Override
		protected void setHostname(String hostname) {
			super.setHostname(hostname);
		}

		@Override
		protected void setLastUpdate(long update) {
			super.setLastUpdate(update);
		}

		@Override
		protected void setMemUsage(float memUsage) {
			super.setMemUsage(memUsage);
		}

		@Override
		protected void setPassword(String password) {
			super.setPassword(password);
		}

		@Override
		protected void setPort(int port) {
			super.setPort(port);
		}

		@Override
		protected void setSecondaryHostname( String secondaryHostname ) {
			super.setSecondaryHostname( secondaryHostname );
		}

	}
}
