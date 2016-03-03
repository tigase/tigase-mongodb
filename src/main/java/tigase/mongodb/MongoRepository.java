/*
 * MongoRepository.java
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
package tigase.mongodb;

import tigase.db.AuthRepository;
import tigase.db.AuthRepositoryImpl;
import tigase.db.AuthorizationException;
import tigase.db.DBInitException;
import tigase.db.Repository;
import tigase.db.TigaseDBException;
import tigase.db.UserExistsException;
import tigase.db.UserNotFoundException;
import tigase.db.UserRepository;

import tigase.xmpp.BareJID;

import tigase.util.StringUtilities;

import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import com.mongodb.BasicDBObject;
import com.mongodb.BulkWriteOperation;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.DuplicateKeyException;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.MongoException;
import com.mongodb.WriteResult;

/**
 * MongoRepository is implementation of UserRepository and AuthRepository which
 * supports MongoDB data store.
 *
 * @author andrzej
 */
@Repository.Meta( supportedUris = { "mongodb:.*" } )
public class MongoRepository implements AuthRepository, UserRepository {

	private static final String JID_HASH_ALG = "SHA-256";

	private static final String USERS_COLLECTION = "tig_users";
	private static final String NODES_COLLECTION = "tig_nodes";

	private static final String ID_KEY = "user_id";
	private static final String DOMAIN_KEY = "domain";
	private static final String AUTO_CREATE_USER_KEY = "autoCreateUser=";

	private String resourceUri;
	private MongoClient mongo;
	private DB db;
	private AuthRepository auth;
	private boolean autoCreateUser = false;

	private byte[] generateId(BareJID user) throws TigaseDBException {
		try {
			MessageDigest md = MessageDigest.getInstance(JID_HASH_ALG);
			return md.digest(user.toString().getBytes());
		} catch (NoSuchAlgorithmException ex) {
			throw new TigaseDBException("Should not happen!!", ex);
		}
	}

	@Override
	public void initRepository(String resource_uri, Map<String, String> params) throws DBInitException {
		try {
			resourceUri = resource_uri;
			int idx = resource_uri.indexOf(AUTO_CREATE_USER_KEY);
			if (idx > -1) {
				int valIdx = idx + AUTO_CREATE_USER_KEY.length();
				String val = resource_uri.substring(valIdx, valIdx + 4);
				if (resource_uri.length() > valIdx + 4) {
					resource_uri = resource_uri.substring(0, idx) + resource_uri.substring(valIdx + 5);
				} else {
					resource_uri = resource_uri.substring(0, idx-1);
				}
				autoCreateUser = Boolean.parseBoolean(val);
			}

			MongoClientURI uri = new MongoClientURI(resource_uri);
			
			mongo = new MongoClient(uri);
			db = mongo.getDB(uri.getDatabase());

			DBCollection users = null;
			if (!db.collectionExists(USERS_COLLECTION)) {
				users = db.createCollection(USERS_COLLECTION, new BasicDBObject());
			} else {
				users = db.getCollection(USERS_COLLECTION);
			}
			DBCollection nodes = db.collectionExists(NODES_COLLECTION)
					? db.getCollection(NODES_COLLECTION) : db.createCollection(NODES_COLLECTION, new BasicDBObject());
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
						DBObject userDto = db.getCollection(USERS_COLLECTION).findOne(new BasicDBObject("_id", id)
								.append(ID_KEY, user.toString()));
						if (userDto == null)
							throw new UserNotFoundException("User " + user + " not found in repository");
						return (String) userDto.get(PASSWORD_KEY);
					} catch (MongoException ex) {
						throw new TigaseDBException("Error retrieving password for user " + user, ex);
					}
				}

				@Override
				public void updatePassword(BareJID user, String password) throws TigaseDBException {
					try {
						byte[] id = generateId(user);
						WriteResult result = db.getCollection(USERS_COLLECTION).update(
								new BasicDBObject("_id", id).append(ID_KEY, user.toString()),
								new BasicDBObject("$set", new BasicDBObject(PASSWORD_KEY, password)));
						if (result == null || !result.isUpdateOfExisting())
							throw new UserNotFoundException("User " + user + " not found in repository");
					} catch (MongoException ex) {
						throw new TigaseDBException("Error retrieving password for user " + user, ex);
					}
				}
			};
		} catch (UnknownHostException ex) {
			throw new DBInitException("Could not connect to MongoDB server using URI = " + resource_uri, ex);
		}
	}

	private Object addUserRepo(BareJID user) throws UserExistsException, TigaseDBException {
		try {
			BasicDBObject userDto = new BasicDBObject().append(ID_KEY, user.toString());
			userDto.append(DOMAIN_KEY, user.getDomain());
			byte[] id = generateId(user);
			userDto.append("_id", id);
			return db.getCollection(USERS_COLLECTION).insert(userDto).getUpsertedId();
		} catch (DuplicateKeyException ex) {
			throw new UserExistsException("Error adding user to repository: ", ex);
		} catch (MongoException ex) {
			throw new TigaseDBException("Error adding user to repository: ", ex);
		}
	}

	private void ensureUserExists(BareJID user, byte[] id) throws TigaseDBException {
		try {
			BasicDBObject userDto = new BasicDBObject().append(ID_KEY, user.toString());
			userDto.append(DOMAIN_KEY, user.getDomain());
			if (id == null)
				id = generateId(user);
			userDto.append("_id", id);
			db.getCollection(USERS_COLLECTION).update(userDto, userDto, true, false);			
		} catch (MongoException ex) {
			throw new TigaseDBException("Error adding user to repository: ", ex);
		}
	}
	
	@Override
	public void addDataList(BareJID user, String subnode, String key, String[] list)
			throws UserNotFoundException, TigaseDBException {
		subnode = normalizeSubnode( subnode );
		try {
			byte[] uid = generateId(user);
			BasicDBObject dto = new BasicDBObject("uid", uid).append("node", subnode).append("key", key).append("values", list);
			db.getCollection(NODES_COLLECTION).insert(dto);
			if (autoCreateUser) {
				ensureUserExists(user, uid);
			}
		}
		catch (MongoException ex) {
			throw new TigaseDBException("Problem adding data list to repository", ex);
		}
	}

	@Override
	public void addUser(BareJID user) throws UserExistsException, TigaseDBException {
		addUserRepo(user);
	}

	private BasicDBObject createCrit(BareJID user, String subnode, String key) throws TigaseDBException {
		subnode = normalizeSubnode( subnode );
		byte[] uid = generateId(user);
		BasicDBObject crit = new BasicDBObject("uid", uid);
		if (key != null)
			crit.append("key", key);
		if (subnode == null) {
			crit.append("node", new BasicDBObject("$exists", false));
		} else {
			crit.append("node", subnode);
		}
		return crit;
	}

	private DBObject getDataInt(BareJID user, String subnode, String key) throws TigaseDBException {
		BasicDBObject crit = createCrit(user, subnode, key);
		return db.getCollection(NODES_COLLECTION).findOne(crit);
	}

	@Override
	public String getData(BareJID user, String subnode, String key, String def)
			throws UserNotFoundException, TigaseDBException {
		String value = getData(user, subnode, key);
		if (value == null)
			value = def;
		return value;
	}

	@Override
	public String getData(BareJID user, String subnode, String key)
			throws UserNotFoundException, TigaseDBException {
		try {
			DBObject result = getDataInt(user, subnode, key);
			return (result != null) ? (String) result.get("value") : null;
		}
		catch (MongoException ex) {
			throw new TigaseDBException("Problem retrieving data from repository", ex);
		}
	}

	@Override
	public String getData(BareJID user, String key) throws UserNotFoundException, TigaseDBException {
		try {
			DBObject result = getDataInt(user, null, key);
			return (result != null) ? (String) result.get("value") : null;
		}
		catch (MongoException ex) {
			throw new TigaseDBException("Problem retrieving data from repository", ex);
		}
	}

	@Override
	public String[] getDataList(BareJID user, String subnode, String key)
			throws UserNotFoundException, TigaseDBException {
		DBCursor cursor = null;
		try {
			List<String> values = new ArrayList<>();
			BasicDBObject crit = createCrit(user, subnode, key);
			cursor = db.getCollection(NODES_COLLECTION).find(crit);
			while (cursor.hasNext()) {
				DBObject it = cursor.next();
				if (it.containsField("values"))
					values.addAll((List<String>) it.get("values"));
				else if (it.containsField("value"))
					values.add((String) it.get("value"));
			}
			return values.toArray(new String[values.size()]);
		}
		catch (MongoException ex) {
			throw new TigaseDBException("Problem retrieving data list from repository", ex);
		}
		finally {
			if (cursor != null)
				cursor.close();
		}
	}

	@Override
	public String[] getKeys(BareJID user, String subnode) throws UserNotFoundException, TigaseDBException {
		try {
			BasicDBObject crit = createCrit(user, subnode, null);
			List<String> result = db.getCollection(NODES_COLLECTION).distinct("key", crit);
			return result.toArray(new String[result.size()]);
		} catch (MongoException ex) {
			throw new TigaseDBException("Problem retrieving keys for " + user
																	+ " and subnode " + subnode + " from repository", ex);
		}
	}

	@Override
	public String[] getKeys(BareJID user) throws UserNotFoundException, TigaseDBException {
		return getKeys(user, null);
	}

	@Override
	public String getResourceUri() {
		return resourceUri;
	}

	@Override
	public List<BareJID> getUsers() throws TigaseDBException {
		List<BareJID> users = new ArrayList<>(1000);
		DBCursor cursor = null;
		try {
			cursor = db.getCollection(USERS_COLLECTION).find(new BasicDBObject(), new BasicDBObject(ID_KEY, 1));
			while (cursor.hasNext()) {
				DBObject entry = cursor.next();
				users.add(BareJID.bareJIDInstanceNS((String) entry.get(ID_KEY)));
			}
		} catch (MongoException ex) {
			throw new TigaseDBException("Problem loading user list from repository", ex);
		} finally {
			if (cursor != null) {
				cursor.close();
			}
		}
		return users;
	}

	@Override
	public long getUsersCount() {
		try {
			return db.getCollection(USERS_COLLECTION).count();
		} catch (MongoException ex) {
			return -1;
		}
	}

	@Override
	public long getUsersCount(String domain) {
		try {
			BasicDBObject crit = new BasicDBObject();
			// we can check domain field if we would use it or USER_ID field
			if (DOMAIN_KEY == null) {
				crit.append(DOMAIN_KEY, domain);
			} else {
				Pattern regex = Pattern.compile(".+@" + domain.replace(".", "\\.") + "$");
				crit.append(ID_KEY, regex);
			}
			return db.getCollection(USERS_COLLECTION).count(crit);
		} catch (MongoException ex) {
			return -1;
		}
	}

	@Override
	public boolean userExists(BareJID user) {
		try {
			BasicDBObject userDto = new BasicDBObject();
			byte[] id = generateId(user);
			userDto.append("_id", id);
			userDto.append(ID_KEY, user.toString());
			return db.getCollection(USERS_COLLECTION).count(userDto) > 0;
		} catch (Exception e) {
			return false;
		}
	}

	@Override
	public void setDataList(BareJID user, String subnode, String key, String[] list)
			throws UserNotFoundException, TigaseDBException {
		try {

			BasicDBObject crit = createCrit( user, subnode, key );
			BasicDBObject dto = new BasicDBObject( crit ).append( "values", list );
			if ( subnode == null ) {
				dto.remove( "node" );
			}

			BulkWriteOperation builder = db.getCollection( NODES_COLLECTION ).initializeOrderedBulkOperation();
			builder.find( crit ).remove();
			builder.insert( dto );
			builder.execute();
			if (autoCreateUser) {
				ensureUserExists(user, null);
			}
		} catch ( MongoException ex ) {
			throw new TigaseDBException( "Problem setting values in repository", ex );
		}
	}

	@Override
	public void setData(BareJID user, String key, String value) throws UserNotFoundException, TigaseDBException {
		setData(user, null, key, value);
	}

	@Override
	public void setData(BareJID user, String subnode, String key, String value)
			throws UserNotFoundException, TigaseDBException {
		try {
			BasicDBObject crit = createCrit(user, subnode, key);
			BasicDBObject dto = new BasicDBObject(crit).append("value", value);
			if (subnode == null)
				dto.remove("node");
			db.getCollection(NODES_COLLECTION).update(crit, dto, true, false);
			if (autoCreateUser) {
				ensureUserExists(user, null);
			}			
		} catch (MongoException ex) {
			throw new TigaseDBException("Problem setting values in repository", ex);
		}
	}

	@Override
	public void removeUser(BareJID user) throws UserNotFoundException, TigaseDBException {
		try {
			BasicDBObject userDto = new BasicDBObject();
			byte[] id = generateId(user);
			userDto.append("_id", id);
			userDto.append(ID_KEY, user.toString());
			db.getCollection(USERS_COLLECTION).remove(userDto);

			removeSubnode(user, null);
		} catch (MongoException e) {
			throw new TigaseDBException("Error removing user from repository: ", e);
		}
	}

	@Override
	public void removeSubnode(BareJID user, String subnode) throws UserNotFoundException, TigaseDBException {
		subnode = normalizeSubnode( subnode );

		try {
			byte[] uid = generateId(user);
			BasicDBObject crit = new BasicDBObject("uid", uid);
			Pattern regex = Pattern.compile("^" + (subnode != null ? subnode : "") + "[^/]*");
			crit.append("node", regex);
			db.getCollection(NODES_COLLECTION).remove(crit);
		} catch (MongoException ex) {
			throw new TigaseDBException("Error removing subnode from repository: ", ex);
		}
	}

	@Override
	public void removeData(BareJID user, String key) throws UserNotFoundException, TigaseDBException {
		removeData(user, null, key);
	}

	@Override
	public void removeData(BareJID user, String subnode, String key) throws UserNotFoundException, TigaseDBException {
		try {
			BasicDBObject crit = createCrit(user, subnode, key);
			db.getCollection(NODES_COLLECTION).remove(crit);
		} catch (MongoException ex) {
			throw new TigaseDBException("Error data from repository: ", ex);
		}
	}

	/**
	 * Should be removed an only relational DB are using this and it is not
	 * required by any other code
	 *
	 * @param user
	 * @return
	 * @throws TigaseDBException
	 * @deprecated
	 */
	@Override
	@Deprecated
	public long getUserUID(BareJID user) throws TigaseDBException {
		return 0;
	}

	@Override
	public String[] getSubnodes(BareJID user) throws UserNotFoundException, TigaseDBException {
		return getSubnodes(user, null);
	}

	@Override
	public String[] getSubnodes(BareJID user, String subnode) throws UserNotFoundException, TigaseDBException {
		subnode = normalizeSubnode( subnode );
		try {
			byte[] uid = generateId(user);
			BasicDBObject crit = new BasicDBObject("uid", uid);
			Pattern regex = Pattern.compile("^" + (subnode != null ? subnode + "/" : "") + "[^/]*");
			crit.append("node", regex);
			List<String> result = (List<String>) db.getCollection(NODES_COLLECTION).distinct("node", crit);
			List<String> res = new ArrayList<>();
			for (String node : result) {
				if (subnode != null) {
					node = node.substring(subnode.length() + 1);
				}
				int idx = node.indexOf("/");
				if (idx > 0)
					node = node.substring(0, idx);
				if (!res.contains(node))
					res.add(node);
			}
			return res.isEmpty() ? null : res.toArray(new String[res.size()]);
		} catch (MongoException ex) {
			throw new TigaseDBException("Error getting subnode from repository: ", ex);
		}
	}

	private String normalizeSubnode( String subnode ) {
		// normalize subnode so it will always be without trailing slashes
		if ( subnode != null ){
			String[] split = subnode.split( "/" );
			subnode = StringUtilities.stringArrayToString( split, "/" );
		}
		return subnode;
	}

	@Override
	public void addUser(BareJID user, String password) throws UserExistsException, TigaseDBException {
		auth.addUser(user, password);
	}

	@Override
	public boolean digestAuth(BareJID user, String digest, String id, String alg) throws UserNotFoundException, TigaseDBException, AuthorizationException {
		return auth.digestAuth(user, digest, id, alg);
	}

	@Override
	public void logout(BareJID user)
			throws UserNotFoundException, TigaseDBException {
		auth.logout(user);
	}

	@Override
	public boolean otherAuth(Map<String, Object> authProps)
			throws UserNotFoundException, TigaseDBException, AuthorizationException {
		return auth.otherAuth(authProps);
	}

	@Override
	public boolean plainAuth(BareJID user, String password)
			throws UserNotFoundException, TigaseDBException, AuthorizationException {
		return auth.plainAuth(user, password);
	}

	@Override
	public void queryAuth(Map<String, Object> authProps) {
		auth.queryAuth(authProps);
	}

	@Override
	public String getPassword(BareJID user)
			throws UserNotFoundException, TigaseDBException {
		return null;
	}

	@Override
	public void updatePassword(BareJID user, String password)
			throws UserNotFoundException, TigaseDBException {
		auth.updatePassword(user, password);
	}
	
	@Override
	public boolean isUserDisabled(BareJID user) 
					throws UserNotFoundException, TigaseDBException {
		return false;
	}
	
	@Override
	public void setUserDisabled(BareJID user, Boolean value) 
					throws UserNotFoundException, TigaseDBException {
		throw new TigaseDBException("Feature not supported");
	}	
}
