/*
 * MongoRepositoryOld.java
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
import com.mongodb.ErrorCategory;
import com.mongodb.MongoException;
import com.mongodb.MongoWriteException;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.DeleteManyModel;
import com.mongodb.client.model.InsertOneModel;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.WriteModel;
import com.mongodb.client.result.UpdateResult;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.Binary;
import tigase.annotations.TigaseDeprecated;
import tigase.auth.credentials.Credentials;
import tigase.db.*;
import tigase.db.util.RepositoryVersionAware;
import tigase.db.util.SchemaLoader;
import tigase.kernel.beans.config.ConfigField;
import tigase.util.StringUtilities;
import tigase.util.Version;
import tigase.xmpp.jid.BareJID;

import java.nio.charset.Charset;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Pattern;

import static tigase.mongodb.Helper.collectionExists;

/**
 * MongoRepository is implementation of UserRepository and AuthRepository which supports MongoDB data store.
 *
 * @author andrzej
 */
@Repository.Meta(supportedUris = {"mongodb:.*"})
@Repository.SchemaId(id = Schema.SERVER_SCHEMA_ID, name = Schema.SERVER_SCHEMA_NAME)
@RepositoryVersionAware.SchemaVersion
@Deprecated
@TigaseDeprecated(since = "8.0.0")
public class MongoRepositoryOld
		implements AuthRepository, UserRepository, DataSourceAware<MongoDataSource>, MongoRepositoryVersionAware {

	protected static final String USERS_COLLECTION = "tig_users";
	protected static final String NODES_COLLECTION = "tig_nodes";
	protected static final String ID_KEY = "user_id";
	protected static final String DOMAIN_KEY = "domain";
	private static final Logger log = Logger.getLogger(MongoRepositoryOld.class.getCanonicalName());
	private static final String JID_HASH_ALG = "SHA-256";
	private static final int DEF_BATCH_SIZE = 100;
	private static final String AUTO_CREATE_USER_KEY = "autoCreateUser=";
	private static final Charset UTF8 = Charset.forName("UTF-8");
	@ConfigField(desc = "Auto create user", alias = AUTO_CREATE_USER_KEY)
	protected boolean autoCreateUser = false;
	private AuthRepository auth;
	@ConfigField(desc = "Batch size", alias = "batch-size")
	private int batchSize = DEF_BATCH_SIZE;
	private MongoDataSource dataSource;
	private MongoDatabase db;
	private MongoCollection<Document> nodesCollection;
	private MongoCollection<Document> usersCollection;

	@Override
	public void addDataList(BareJID user, String subnode, String key, String[] list)
			throws UserNotFoundException, TigaseDBException {
		subnode = normalizeSubnode(subnode);
		try {
			byte[] uid = generateId(user);
			Document dto = new Document("uid", uid).append("node", subnode)
					.append("key", key)
					.append("values", Arrays.asList(list));
			nodesCollection.insertOne(dto);
			if (autoCreateUser) {
				ensureUserExists(user, uid);
			}
		} catch (MongoException ex) {
			throw new TigaseDBException("Problem adding data list to repository", ex);
		}
	}

	@Override
	public void addUser(BareJID user) throws UserExistsException, TigaseDBException {
		addUserRepo(user);
	}

	@Override
	public void addUser(BareJID user, String password) throws UserExistsException, TigaseDBException {
		auth.addUser(user, password);
	}

	private Object addUserRepo(BareJID user) throws UserExistsException, TigaseDBException {
		try {
			byte[] id = generateId(user);
			Document userDto = new Document().append(ID_KEY, user.toString());
			userDto.append(DOMAIN_KEY, user.getDomain());
			userDto.append("_id", id);
			usersCollection.insertOne(userDto);
			return id;
		} catch (MongoWriteException ex) {
			if (ex.getError() != null) {
				if (ex.getError().getCategory() == ErrorCategory.DUPLICATE_KEY) {
					throw new UserExistsException("Error adding user to repository: ", ex);
				}
			}
			throw new TigaseDBException("Error adding user to repository: ", ex);
		} catch (MongoException ex) {
			throw new TigaseDBException("Error adding user to repository: ", ex);
		}
	}

	protected byte[] calculateHash(String user) throws TigaseDBException {
		try {
			MessageDigest md = MessageDigest.getInstance(JID_HASH_ALG);
			return md.digest(user.getBytes(UTF8));
		} catch (NoSuchAlgorithmException ex) {
			throw new TigaseDBException("Should not happen!!", ex);
		}
	}

	private Document createCrit(BareJID user, String subnode, String key) throws TigaseDBException {
		subnode = normalizeSubnode(subnode);
		byte[] uid = generateId(user);
		Document crit = new Document("uid", uid);
		if (key != null) {
			crit.append("key", key);
		}
		if (subnode == null) {
			crit.append("node", new Document("$exists", false));
		} else {
			crit.append("node", subnode);
		}
		return crit;
	}

	private void ensureUserExists(BareJID user, byte[] id) throws TigaseDBException {
		try {
			BasicDBObject userDto = new BasicDBObject();
			userDto.append(DOMAIN_KEY, user.getDomain());
			if (id == null) {
				id = generateId(user);
			}
			userDto.append("_id", id);
			usersCollection.updateOne(userDto,
			                          new Document("$set", new Document(userDto).append(ID_KEY, user.toString())),
			                          new UpdateOptions().upsert(true));
		} catch (MongoException ex) {
			throw new TigaseDBException("Error adding user to repository: ", ex);
		}
	}

	protected byte[] generateId(BareJID user) throws TigaseDBException {
		return calculateHash(user.toString().toLowerCase());
	}

	@Override
	public AccountStatus getAccountStatus(BareJID user) throws TigaseDBException {
		return auth.getAccountStatus(user);
	}

	@Override
	public Credentials getCredentials(BareJID user, String username) throws TigaseDBException {
		return auth.getCredentials(user, username);
	}

	@Override
	public String getData(BareJID user, String subnode, String key, String def)
			throws UserNotFoundException, TigaseDBException {
		String value = getData(user, subnode, key);
		if (value == null) {
			value = def;
		}
		return value;
	}

	@Override
	public String getData(BareJID user, String subnode, String key) throws UserNotFoundException, TigaseDBException {
		try {
			Document result = getDataInt(user, subnode, key);
			return (result != null) ? result.getString("value") : null;
		} catch (MongoException ex) {
			throw new TigaseDBException("Problem retrieving data from repository", ex);
		}
	}

	@Override
	public String getData(BareJID user, String key) throws UserNotFoundException, TigaseDBException {
		try {
			Document result = getDataInt(user, null, key);
			return (result != null) ? result.getString("value") : null;
		} catch (MongoException ex) {
			throw new TigaseDBException("Problem retrieving data from repository", ex);
		}
	}

	private Document getDataInt(BareJID user, String subnode, String key) throws TigaseDBException {
		Bson crit = createCrit(user, subnode, key);
		return nodesCollection.find(crit).first();
	}

	@Override
	public String[] getDataList(BareJID user, String subnode, String key)
			throws UserNotFoundException, TigaseDBException {
		try {
			List<String> values = new ArrayList<>();
			Document crit = createCrit(user, subnode, key);
			FindIterable<Document> cursor = nodesCollection.find(crit).batchSize(batchSize);
			for (Document it : cursor) {
				if (it.containsKey("values")) {
					values.addAll((List<String>) it.get("values"));
				} else if (it.containsKey("value")) {
					values.add((String) it.get("value"));
				}
			}
			return values.toArray(new String[values.size()]);
		} catch (MongoException ex) {
			throw new TigaseDBException("Problem retrieving data list from repository", ex);
		}
	}

	@Override
	public String[] getKeys(BareJID user, String subnode) throws UserNotFoundException, TigaseDBException {
		try {
			Document crit = createCrit(user, subnode, null);
			//List<String> result = db.getCollection(NODES_COLLECTION).distinct("key", crit);
			List<String> result = readAllDistinctValuesForField(nodesCollection, "key", crit);
			return result.toArray(new String[result.size()]);
		} catch (MongoException ex) {
			throw new TigaseDBException(
					"Problem retrieving keys for " + user + " and subnode " + subnode + " from repository", ex);
		}
	}

	@Override
	public String[] getKeys(BareJID user) throws UserNotFoundException, TigaseDBException {
		return getKeys(user, null);
	}

	@Override
	public String getPassword(BareJID user) throws UserNotFoundException, TigaseDBException {
		return auth.getPassword(user);
	}

	@Override
	public String getResourceUri() {
		return dataSource.getResourceUri();
	}

	@Override
	public String[] getSubnodes(BareJID user) throws UserNotFoundException, TigaseDBException {
		return getSubnodes(user, null);
	}

	@Override
	public String[] getSubnodes(BareJID user, String subnode) throws UserNotFoundException, TigaseDBException {
		subnode = normalizeSubnode(subnode);
		try {
			byte[] uid = generateId(user);
			Document crit = new Document("uid", uid);
			Pattern regex = Pattern.compile("^" + (subnode != null ? subnode + "/" : "") + "[^/]*");
			crit.append("node", regex);
			//List<String> result = (List<String>) db.getCollection(NODES_COLLECTION).distinct("node", crit);
			List<String> result = readAllDistinctValuesForField(nodesCollection, "node", crit);
			List<String> res = new ArrayList<>();
			for (String node : result) {
				if (subnode != null) {
					node = node.substring(subnode.length() + 1);
				}
				int idx = node.indexOf("/");
				if (idx > 0) {
					node = node.substring(0, idx);
				}
				if (!res.contains(node)) {
					res.add(node);
				}
			}
			return res.isEmpty() ? null : res.toArray(new String[res.size()]);
		} catch (MongoException ex) {
			throw new TigaseDBException("Error getting subnode from repository: ", ex);
		}
	}

	/**
	 * Should be removed an only relational DB are using this and it is not required by any other code
	 *
	 * @param user
	 *
	 * @return
	 *
	 * @throws TigaseDBException
	 * @deprecated
	 */
	@Override
	@Deprecated
	public long getUserUID(BareJID user) throws TigaseDBException {
		return 0;
	}

	@Override
	public List<BareJID> getUsers() throws TigaseDBException {
		List<BareJID> users = new ArrayList<>(1000);
		try {
			FindIterable<Document> cursor = usersCollection.find()
					.projection(new Document(ID_KEY, 1))
					.batchSize(batchSize);
			for (Document entry : cursor) {
				users.add(BareJID.bareJIDInstanceNS((String) entry.get(ID_KEY)));
			}
		} catch (MongoException ex) {
			throw new TigaseDBException("Problem loading user list from repository", ex);
		}
		return users;
	}

	@Override
	public long getUsersCount() {
		try {
			return usersCollection.count(new Document());
		} catch (MongoException ex) {
			return -1;
		}
	}

	@Override
	public long getUsersCount(String domain) {
		try {
			Document crit = new Document();
			// we can check domain field if we would use it or USER_ID field
			crit.append(DOMAIN_KEY, domain.toLowerCase());
			return usersCollection.count(crit);
		} catch (MongoException ex) {
			return -1;
		}
	}

	@Override
	@Deprecated
	public void initRepository(String resource_uri, Map<String, String> params) throws DBInitException {
		try {
			if (db == null) {
				MongoDataSource ds = new MongoDataSource();
				ds.initRepository(resource_uri, params);
				setDataSource(ds);
			}
		} catch (MongoException ex) {
			throw new DBInitException("Could not connect to MongoDB server using URI = " + resource_uri, ex);
		}
	}

	@Override
	public boolean isUserDisabled(BareJID user) throws UserNotFoundException, TigaseDBException {
		return auth.isUserDisabled(user);
	}

	@Override
	public void loggedIn(BareJID jid) throws TigaseDBException {
		auth.loggedIn(jid);
	}

	@Override
	public void logout(BareJID user) throws UserNotFoundException, TigaseDBException {
		auth.logout(user);
	}

	private String normalizeSubnode(String subnode) {
		// normalize subnode so it will always be without trailing slashes
		if (subnode != null) {
			String[] split = subnode.split("/");
			subnode = StringUtilities.stringArrayToString(split, "/");
		}
		return subnode;
	}

	@Override
	public boolean otherAuth(Map<String, Object> authProps)
			throws UserNotFoundException, TigaseDBException, AuthorizationException {
		return auth.otherAuth(authProps);
	}

	@Override
	public void queryAuth(Map<String, Object> authProps) {
		auth.queryAuth(authProps);
	}

	protected <T> List<T> readAllDistinctValuesForField(MongoCollection<Document> collection, String field,
	                                                    Document crit) throws MongoException {
		FindIterable<Document> cursor = collection.find(crit)
				.projection(new BasicDBObject(field, 1))
				.batchSize(batchSize);

		List<T> result = new ArrayList<>();
		for (Document item : cursor) {
			T val = (T) item.get(field);
			if (!result.contains(val)) {
				result.add(val);
			}
		}

		return result;
	}

	// TODO: add support for credentials!!
	@Override
	public void removeCredential(BareJID user, String username) throws TigaseDBException {

	}

	@Override
	public void removeData(BareJID user, String key) throws UserNotFoundException, TigaseDBException {
		removeData(user, null, key);
	}

	@Override
	public void removeData(BareJID user, String subnode, String key) throws UserNotFoundException, TigaseDBException {
		try {
			Document crit = createCrit(user, subnode, key);
			db.getCollection(NODES_COLLECTION).deleteMany(crit);
		} catch (MongoException ex) {
			throw new TigaseDBException("Error data from repository: ", ex);
		}
	}

	@Override
	public void removeSubnode(BareJID user, String subnode) throws UserNotFoundException, TigaseDBException {
		subnode = normalizeSubnode(subnode);

		try {
			byte[] uid = generateId(user);
			Document crit = new Document("uid", uid);
			Pattern regex = Pattern.compile("^" + (subnode != null ? subnode : "") + "[^/]*");
			crit.append("node", regex);
			nodesCollection.deleteMany(crit);
		} catch (MongoException ex) {
			throw new TigaseDBException("Error removing subnode from repository: ", ex);
		}
	}

	@Override
	public void removeUser(BareJID user) throws UserNotFoundException, TigaseDBException {
		try {
			Document userDto = new Document();
			byte[] id = generateId(user);
			userDto.append("_id", id);
			usersCollection.deleteOne(userDto);

			removeSubnode(user, null);
		} catch (MongoException e) {
			throw new TigaseDBException("Error removing user from repository: ", e);
		}
	}

	@Override
	public void setAccountStatus(BareJID user, AccountStatus status) throws TigaseDBException {
		auth.setAccountStatus(user, status);
	}

	@Override
	public void setData(BareJID user, String key, String value) throws UserNotFoundException, TigaseDBException {
		setData(user, null, key, value);
	}

	@Override
	public void setData(BareJID user, String subnode, String key, String value)
			throws UserNotFoundException, TigaseDBException {
		try {
			Document crit = createCrit(user, subnode, key);
			Document dto = new Document(crit).append("value", value);
			if (subnode == null) {
				dto.remove("node");
			}
			nodesCollection.updateOne(crit, new Document("$set", dto), new UpdateOptions().upsert(true));
			if (autoCreateUser) {
				ensureUserExists(user, null);
			}
		} catch (MongoException ex) {
			throw new TigaseDBException("Problem setting values in repository", ex);
		}
	}

	@Override
	public void setDataList(BareJID user, String subnode, String key, String[] list)
			throws UserNotFoundException, TigaseDBException {
		try {
			Document crit = createCrit(user, subnode, key);
			Document dto = new Document(crit).append("values", Arrays.asList(list));
			if (subnode == null) {
				dto.remove("node");
			}

			List<WriteModel<Document>> operation = new ArrayList<>();
			operation.add(new DeleteManyModel<>(crit));
			operation.add(new InsertOneModel<>(dto));
			nodesCollection.bulkWrite(operation);

			if (autoCreateUser) {
				ensureUserExists(user, null);
			}
		} catch (MongoException ex) {
			throw new TigaseDBException("Problem setting values in repository", ex);
		}
	}

	@Override
	public void setDataSource(MongoDataSource dataSource) {
		this.dataSource = dataSource;
		db = dataSource.getDatabase();

		MongoCollection users = null;
		if (!collectionExists(db, USERS_COLLECTION)) {
			db.createCollection(USERS_COLLECTION);
		}
		usersCollection = db.getCollection(USERS_COLLECTION);

		MongoCollection nodes = null;
		if (!collectionExists(db, NODES_COLLECTION)) {
			db.createCollection(NODES_COLLECTION);
		}
		nodesCollection = db.getCollection(NODES_COLLECTION);
		nodes = nodesCollection;
		nodes.createIndex(new BasicDBObject("uid", 1));
		nodes.createIndex(new BasicDBObject("node", 1));
		nodes.createIndex(new BasicDBObject("key", 1));
		nodes.createIndex(new BasicDBObject("uid", 1).append("node", 1).append("key", 1));

		// let's override AuthRepositoryImpl to store password inside objects in tig_users
		auth = new AuthRepositoryImpl(this) {
			@Override
			public String getPassword(BareJID user) throws TigaseDBException {
				try {
					byte[] id = generateId(user);
					Document userDto = usersCollection.find(new BasicDBObject("_id", id))
							.projection(new BasicDBObject(PASSWORD_KEY, 1))
							.first();
					if (userDto == null) {
						throw new UserNotFoundException("User " + user + " not found in repository");
					}
					return (String) userDto.get(PASSWORD_KEY);
				} catch (MongoException ex) {
					throw new TigaseDBException("Error retrieving password for user " + user, ex);
				}
			}

			@Override
			public void updatePassword(BareJID user, String password) throws TigaseDBException {
				try {
					byte[] id = generateId(user);
					UpdateResult result = usersCollection.updateOne(new BasicDBObject("_id", id),
					                                                new BasicDBObject("$set",
					                                                                  new BasicDBObject(PASSWORD_KEY,
					                                                                                    password)));
					if (result == null || result.getMatchedCount() <= 0) {
						throw new UserNotFoundException("User " + user + " not found in repository");
					}
				} catch (MongoException ex) {
					throw new TigaseDBException("Error retrieving password for user " + user, ex);
				}
			}
		};
	}

	@Override
	public void setUserDisabled(BareJID user, Boolean value) throws UserNotFoundException, TigaseDBException {
		auth.setUserDisabled(user, value);
	}

	@Override
	public void updateCredential(BareJID user, String username, String password) throws TigaseDBException {
		auth.updateCredential(user, username, password);
	}

	@Override
	public void updatePassword(BareJID user, String password) throws UserNotFoundException, TigaseDBException {
		auth.updatePassword(user, password);
	}

	@Override
	public SchemaLoader.Result updateSchema(Optional<Version> oldVersion, Version newVersion) throws TigaseDBException {
		long usersCount = getUsersCount();
		List<BareJID> users = getUsers();

		for (Document doc : usersCollection.find().batchSize(1000)) {
			try {
				byte[] oldUid = ((Binary) doc.get("_id")).getData();
				String user = (String) doc.get(ID_KEY);
				byte[] newUid = calculateHash(user.toLowerCase());

				if (Arrays.equals(oldUid, newUid)) {
					continue;
				}

				nodesCollection.updateMany(new Document("uid", oldUid),
				                           new Document("$set", new Document("uid", newUid)));

				Document oldUserFilter = new Document("_id", oldUid).append(ID_KEY, user);
				Document oldUserDocument = usersCollection.find(oldUserFilter).first();
				Document newUserDocument = new Document(oldUserDocument).append("_id", newUid);

				usersCollection.insertOne(newUserDocument);
				usersCollection.findOneAndDelete(oldUserDocument);
			} catch (TigaseDBException ex) {
				log.log(Level.SEVERE, "Schema update failed!", ex);
			}
		}
		;
		return SchemaLoader.Result.ok;
	}

	@Override
	public boolean userExists(BareJID user) {
		try {
			BasicDBObject userDto = new BasicDBObject();
			byte[] id = generateId(user);
			userDto.append("_id", id);
			return usersCollection.count(userDto) > 0;
		} catch (Exception e) {
			return false;
		}
	}
}
