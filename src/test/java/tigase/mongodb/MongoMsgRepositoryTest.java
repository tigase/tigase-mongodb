package tigase.mongodb;

import org.bson.types.ObjectId;
import tigase.server.amp.db.AbstractMsgRepositoryTest;

/**
 * Created by andrzej on 25.03.2017.
 */
public class MongoMsgRepositoryTest extends AbstractMsgRepositoryTest<MongoDataSource, ObjectId> {

	@Override
	protected ObjectId getMsgId(String msgIdStr) {
		return new ObjectId(msgIdStr);
	}
}
