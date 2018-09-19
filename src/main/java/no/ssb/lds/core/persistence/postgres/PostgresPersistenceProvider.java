package no.ssb.lds.core.persistence.postgres;

import javax.sql.DataSource;
import java.sql.Connection;

class PostgresPersistenceProvider {

    private final DataSource dataSource;

    PostgresPersistenceProvider(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    DataSource driver() {
        return dataSource;
    }

    PostgresPersistenceTransaction newTransaction() {
        return new PostgresPersistenceTransaction(dataSource);
    }

    PostgresPersistenceReader newReader(Connection client) {
        return new PostgresPersistenceReader(client);
    }

    PostgresPersistenceWriter newWriter(Connection client) {
        return new PostgresPersistenceWriter(client);
    }

}
