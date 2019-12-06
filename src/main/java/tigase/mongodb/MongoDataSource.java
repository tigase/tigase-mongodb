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
package tigase.mongodb;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.MongoException;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import tigase.db.DBInitException;
import tigase.db.DataSource;
import tigase.db.Repository;
import tigase.kernel.beans.UnregisterAware;
import tigase.kernel.beans.config.ConfigField;
import tigase.util.Version;

import java.util.Map;
import java.util.Optional;
import java.util.logging.Level;
import java.util.logging.Logger;

import static tigase.mongodb.MongoSchemaLoader.SCHEMA_VERSION;

/**
 * Created by andrzej on 04.10.2016.
 */
@Repository.Meta(isDefault = true, supportedUris = {"mongodb:.*"})
public class MongoDataSource
		implements DataSource, UnregisterAware {

	private static final Logger log = Logger.getLogger(MongoDataSource.class.getName());

	@ConfigField(desc = "Automatic schema management", alias = "schema-management")
	private boolean automaticSchemaManagement = true;

	private MongoDatabase db;
	private MongoClient mongo;
	private String resourceUri;

	@Override
	public boolean automaticSchemaManagement() {
		return automaticSchemaManagement;
	}

	@Override
	public void beforeUnregister() {
		mongo.close();
	}

	public MongoDatabase getDatabase() {
		return db;
	}

	@Override
	public String getResourceUri() {
		return resourceUri;
	}

	@Override
	public Optional<Version> getSchemaVersion(String component) {

		if (component == null || component.isEmpty()) {
			log.log(Level.WARNING, "Wrong component name passed: " + component);
			return Optional.empty();
		}

		try {
			MongoDatabase db = this.getDatabase();
			if (db != null) {
				Document crit = new Document("_id", component);
				final Document version = db.getCollection(SCHEMA_VERSION).find(new Document(crit)).first();
				if (version != null && version.get("version") != null) {
					return Optional.of(Version.of((String) version.get("version")));
				}
			}
		} catch (MongoException ex) {
			log.log(Level.WARNING, ex.getMessage());
		}

		return Optional.empty();
	}

	@Override
	@Deprecated
	public void initRepository(String resource_uri, Map<String, String> params) throws DBInitException {
		initialize(resource_uri);
	}

	@Override
	public void initialize(String resource_uri) throws DBInitException {
		resourceUri = resource_uri;
		MongoClientURI uri = new MongoClientURI(resource_uri);
		mongo = new MongoClient(uri);
		db = mongo.getDatabase(uri.getDatabase());
	}
}
