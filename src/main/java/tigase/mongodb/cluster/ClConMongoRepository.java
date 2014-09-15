/*
 * ClConMongoRepository.java
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

/**
 *
 * @author andrzej
 */
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
	
	//~--- get methods ----------------------------------------------------------

	/**
	 * Method description
	 *
	 *
	 * @param defs
	 * @param params
	 */
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

	/**
	 * Method description
	 *
	 *
	 * @param conn_str
	 * @param params
	 * @throws tigase.db.DBInitException
	 */
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

	/**
	 * Method description
	 *
	 *
	 * @param item
	 */
	@Override
	public void storeItem(tigase.cluster.repo.ClusterRepoItem item) {
		try {
			BasicDBObject crit = new BasicDBObject("hostname", item.getHostname());
			BasicDBObject dto = new BasicDBObject("password", item.getPassword())
					.append("updated", new Date()).append("port", item.getPortNo())
					.append("cpu_usage", item.getCpuUsage()).append("mem_usage", item.getMemUsage());
			db.getCollection(CLUSTER_NODES).update(crit, new BasicDBObject("$set", dto), true, false);
		} catch (Exception ex) {
			log.log(Level.WARNING, "Problem setting element to DB: ", ex);
		}
	}	
	
	/**
	 * Method description
	 *
	 */
	@Override
	public void reload() {
		super.reload();
		DBCursor cursor = null;
		try {
			cursor = db.getCollection(CLUSTER_NODES).find();
			
			while (cursor.hasNext()) {
				DBObject dto = cursor.next();
				
				ClusterRepoItem item = getItemInstance();
					item.setHostname((String) dto.get("hostname"));
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
	
	/**
	 * Method description
	 *
	 */
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
		/**
		 * Method description
		 *
		 *
		 *
		 * @param cpuUsage
		 */
		@Override
		protected void setCpuUsage(float cpuUsage) {
			super.setCpuUsage(cpuUsage);
		}
		
		/**
		 * Method description
		 *
		 *
		 *
		 * @param hostname
		 */
		@Override
		protected void setHostname(String hostname) {
			super.setHostname(hostname);
		}

		/**
		 * Method description
		 *
		 *
		 *
		 * @param update
		 */
		@Override
		protected void setLastUpdate(long update) {
			super.setLastUpdate(update);
		}

		/**
		 * Method description
		 *
		 *
		 * @param memUsage
		 */
		@Override
		protected void setMemUsage(float memUsage) {
			super.setMemUsage(memUsage);
		}

		/**
		 * Method description
		 *
		 *
		 * @param password
		 */
		@Override
		protected void setPassword(String password) {
			super.setPassword(password);
		}

		/**
		 * Method description
		 *
		 *
		 * @param port
		 */
		@Override
		protected void setPort(int port) {
			super.setPort(port);
		}

	}
}
