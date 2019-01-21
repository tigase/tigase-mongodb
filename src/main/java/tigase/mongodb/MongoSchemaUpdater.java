/**
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

import tigase.db.DataSourceAware;
import tigase.db.util.RepositoryVersionAware;
import tigase.mongodb.archive.MongoMessageArchiveRepository;
import tigase.mongodb.muc.MongoHistoryProvider;
import tigase.mongodb.pubsub.PubSubDAOMongo;
import tigase.util.Version;

import java.util.HashMap;
import java.util.Optional;

/**
 * Created by andrzej on 07.10.2016.
 */
public class MongoSchemaUpdater {

	public static void main(String[] args) throws Exception {
		String schema = null;
		String uri = null;

		for (int i = 0; i < args.length; i++) {
			switch (args[i]) {
				case "-schema":
					if (args.length > i + 1) {
						i++;
						schema = args[i];
					}
					break;
				case "-uri":
					if (args.length > i + 1) {
						i++;
						uri = args[i];
					}
					break;
				default:
					break;
			}
		}

		if (schema == null || uri == null) {
			String help = "Usage:" + "$ java -cp \"jars/*.jar\" tigase.mongodb.MongoSchemaUpdater" +
					"\n\t -schema schema type {user, offline-messages, muc, pubsub, message-archive} " +
					"\n\t -uri uri to connect to MongoDB";

			System.out.println(help);

			return;
		}

		MongoDataSource dataSource = new MongoDataSource();
		dataSource.initRepository(uri, new HashMap<>());

		Object repository = null;

		switch (schema) {
			case "user":
				repository = new MongoRepository();
				break;
			case "offline-messages":
				repository = new MongoMsgRepository();
				break;
			case "message-archive":
				repository = new MongoMessageArchiveRepository();
				break;
			case "muc":
				repository = new MongoHistoryProvider();
				break;
			case "pubsub":
				repository = new PubSubDAOMongo();
				break;
			default:
				System.out.println("Unknown schema type '" + schema + "' to upgrade");
				return;
		}

		if (!(repository instanceof DataSourceAware)) {
			System.out.println("Repository do not implement DataSourceAware" +
					                   " - can not initialize repository instance for schema update!");
			return;
		}
		if (!(repository instanceof RepositoryVersionAware)) {
			System.out.println("Repository do not implement RepositoryVersionAware" +
					                   " - can not update schema for repository which is unware of schema version!");
		}

		((DataSourceAware) repository).setDataSource(dataSource);

		((RepositoryVersionAware) repository).updateSchema(Optional.of(Version.ZERO), Version.ZERO);
	}

}
