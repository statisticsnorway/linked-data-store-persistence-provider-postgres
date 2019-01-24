package no.ssb.lds.core.persistence.postgres;

import com.zaxxer.hikari.HikariDataSource;
import no.ssb.lds.api.persistence.PersistenceException;
import no.ssb.lds.api.persistence.TransactionFactory;

import java.sql.Connection;
import java.sql.SQLException;

public class PostgresTransactionFactory implements TransactionFactory {

    final HikariDataSource dataSource;

    public PostgresTransactionFactory(HikariDataSource dataSource) {
        this.dataSource = dataSource;
    }

    @Override
    public PostgresTransaction createTransaction(boolean readOnly) throws PersistenceException {
        try {
            Connection connection = dataSource.getConnection();
            connection.setAutoCommit(false);
            return new PostgresTransaction(connection);
        } catch (SQLException e) {
            throw new PersistenceException(e);
        }
    }

    @Override
    public void close() {
        dataSource.close();
    }
}
