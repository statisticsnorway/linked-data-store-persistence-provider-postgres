package no.ssb.lds.core.persistence.postgres;

import no.ssb.lds.api.persistence.PersistenceException;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;

class PostgresPersistenceTransaction implements AutoCloseable {

    private final DataSource dataSource;
    private Connection connection;

    PostgresPersistenceTransaction(DataSource dataSource) {
        this.dataSource = dataSource;
        beginTransaction();
    }

    void beginTransaction() throws PersistenceException {
        try {
            connection = dataSource.getConnection();
            connection.beginRequest();
        } catch (SQLException e) {
            throw new PersistenceException(e);
        }
    }

    void endTransaction() throws PersistenceException {
        if (connection == null) {
            return;
        }

        try {
            connection.commit();
            connection.endRequest();
            connection.close();
            connection = null;
        } catch (SQLException e) {
            // TODO handle rollback here
            throw new PersistenceException(e);
        }
    }

    Connection getClient() {
        return connection;
    }

    @Override
    public void close() {
        endTransaction();
    }
}
