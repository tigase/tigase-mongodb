package tigase.mongodb;

import tigase.db.util.RepositoryVersionAware;
import tigase.util.Version;

public interface MongoRepositoryVersionAware
		extends RepositoryVersionAware {

	@Override
	default Version getVersion() {
		if (this.getClass().isAnnotationPresent(SchemaVersion.class)) {
			SchemaVersion sv = this.getClass().getAnnotation(SchemaVersion.class);
			return Version.of(sv.version());
		} else {
			return Version.ZERO;
		}
	}

}
