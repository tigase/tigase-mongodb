/*
 * ClConMongoRepository.java
 *
 * Tigase Jabber/XMPP Server - MongoDB support
 * Copyright (C) 2004-2016 "Tigase, Inc." <office@tigase.com>
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

import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.UpdateOptions;
import org.bson.Document;
import tigase.cluster.repo.ClConConfigRepository;
import tigase.cluster.repo.ClusterRepoConstants;
import tigase.cluster.repo.ClusterRepoItem;
import tigase.db.DBInitException;
import tigase.db.Repository;
import tigase.db.comp.ComponentRepositoryDataSourceAware;
import tigase.kernel.beans.config.ConfigField;
import tigase.mongodb.MongoDataSource;

import java.util.Date;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import static tigase.mongodb.Helper.collectionExists;

@Repository.Meta( supportedUris = { "mongodb:.*" } )
public class ClConMongoRepository extends ClConConfigRepository
				implements ClusterRepoConstants, ComponentRepositoryDataSourceAware<ClusterRepoItem,MongoDataSource> {

	private static final Logger log = Logger.getLogger(ClConMongoRepository.class.getCanonicalName());

	private static final int DEF_BATCH_SIZE = 100;

	private static final String CLUSTER_NODES = "cluster_nodes";
	
	private MongoDatabase db;
	private MongoCollection<Document> clusterNodes;

	@ConfigField(desc = "Batch size", alias = "batch-size")
	private int batchSize = DEF_BATCH_SIZE;

	@Override
	public void destroy() {
		// This implementation of ClConConfigRepository is using shared connection
		// pool to database which is cached by RepositoryFactory and maybe be used
		// in other places, so we can not destroy it.
		super.destroy();
	}

	//~--- methods --------------------------------------------------------------

	@Override
	public void initRepository(String resource_uri, Map<String, String> params)
					throws DBInitException {
		super.initRepository(resource_uri, params);	
		try {
			if (db == null) {
				MongoDataSource ds = new MongoDataSource();
				ds.initRepository(resource_uri, params);
				setDataSource(ds);
			}
		} catch (Exception ex) {
			throw new DBInitException("Could not initialize MongoDB repository", ex);
		}
	}

	@Override
	public void removeItem( String key ) {
		super.removeItem( key );

		try {
			Document crit = new Document("hostname", key);
			db.getCollection(CLUSTER_NODES).deleteMany(crit);
		} catch (Exception ex) {
			log.log(Level.WARNING, "Problem removing element from DB: ", ex);
		}

	}

	@Override
	public void setDataSource(MongoDataSource dataSource) {
		db = dataSource.getDatabase();

		if (!collectionExists(db, CLUSTER_NODES)) {
			db.createCollection(CLUSTER_NODES);
		}
		clusterNodes = db.getCollection(CLUSTER_NODES);
		clusterNodes.createIndex(new Document("hostname", 1));
	}

	@Override
	public void storeItem(tigase.cluster.repo.ClusterRepoItem item) {
		try {
			Document crit = new Document("hostname", item.getHostname());
			Document dto = new Document("password", item.getPassword())
					.append("secondary", item.getSecondaryHostname())
					.append("updated", new Date()).append("port", item.getPortNo())
					.append("cpu_usage", item.getCpuUsage()).append("mem_usage", item.getMemUsage());
			db.getCollection(CLUSTER_NODES).updateOne(crit, new Document("$set", dto), new UpdateOptions().upsert(true));
		} catch (Exception ex) {
			log.log(Level.WARNING, "Problem setting element to DB: ", ex);
		}
	}	
	
	@Override
	public void reload() {
		if ( ( System.currentTimeMillis() - lastReloadTime ) <= ( autoReloadInterval * lastReloadTimeFactor ) ){
			if ( log.isLoggable( Level.FINEST ) ){
				log.log( Level.FINEST, "Last reload performed in {0}, skipping: ", ( System.currentTimeMillis() - lastReloadTime ) );
			}
			return;
		}
		lastReloadTime = System.currentTimeMillis();

		super.reload();
		try {
			FindIterable<Document> cursor = db.getCollection(CLUSTER_NODES).find().batchSize(batchSize);
			
			for (Document dto : cursor) {
				
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
