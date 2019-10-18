package no.ssb.lds.core.persistence.postgres;

import org.reactivestreams.Publisher;
import org.reactivestreams.tck.PublisherVerification;
import org.reactivestreams.tck.TestEnvironment;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;
import java.util.Set;

public class ResultSetPublisherIntegrationTest extends PublisherVerification<ResultSet> {

    public static final long DEFAULT_TIMEOUT_MILLIS = 100;
    public static final long DEFAULT_NO_SIGNALS_TIMEOUT_MILLIS = 100;
    public static final long PUBLISHER_REFERENCE_CLEANUP_TIMEOUT_MILLIS = 500;

    final PostgresDbInitializer initializer;
    final PostgresPersistence postgresPersistence;

    PostgresTransaction tx;

    public ResultSetPublisherIntegrationTest() {
        super(new TestEnvironment(DEFAULT_TIMEOUT_MILLIS, DEFAULT_NO_SIGNALS_TIMEOUT_MILLIS), PUBLISHER_REFERENCE_CLEANUP_TIMEOUT_MILLIS);
        initializer = new PostgresDbInitializer();
        initializer.initialize("ns",
                Map.of(
                        "postgres.driver.host", "localhost",
                        "postgres.driver.port", "5432",
                        "postgres.driver.user", "lds",
                        "postgres.driver.password", "lds",
                        "postgres.driver.database", "lds"
                ),
                Set.of("A")
        );
        postgresPersistence = initializer.getPostgresPersistence();
    }

    @BeforeMethod
    public void beginTransaction() {
        tx = (PostgresTransaction) postgresPersistence.createTransaction(false);
    }

    @AfterMethod
    public void endTransaction() {
        tx.cancel();
    }

    @Override
    public Publisher<ResultSet> createPublisher(long elements) {
        rollbackExistingAndBeginNewTransaction();
        try {
            return new ResultSetPublisher(tx.connection.createStatement().executeQuery("SELECT * FROM GENERATE_SERIES(1, " + Math.min(elements, 1000) + ")"));
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Publisher<ResultSet> createFailedPublisher() {
        return new ResultSetPublisher(null);
    }

    private void rollbackExistingAndBeginNewTransaction() {
        tx.cancel();
        tx = (PostgresTransaction) postgresPersistence.createTransaction(false);
    }

    @Override
    public long boundedDepthOfOnNextAndRequestRecursion() {
        return 1;
    }
}
