/*
 * MongoDataSource.java
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
package tigase.mongodb;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoDatabase;
import tigase.db.DBInitException;
import tigase.db.DataSource;
import tigase.db.Repository;
import tigase.kernel.beans.UnregisterAware;

import java.util.Map;

/**
 * Created by andrzej on 04.10.2016.
 */
@Repository.Meta( isDefault=true, supportedUris = {"mongodb:.*" } )
public class MongoDataSource implements DataSource, UnregisterAware {

	private MongoDatabase db;
	private MongoClient mongo;
	private String resourceUri;

	@Override
	public String getResourceUri() {
		return resourceUri;
	}

	@Override
	public void initRepository(String resource_uri, Map<String, String> params) throws DBInitException {
		resourceUri = resource_uri;
		MongoClientURI uri = new MongoClientURI(resource_uri);
		mongo = new MongoClient(uri);
		db = mongo.getDatabase(uri.getDatabase());
	}

	public MongoDatabase getDatabase() {
		return db;
	}

	@Override
	public void beforeUnregister() {
		mongo.close();
	}
}
