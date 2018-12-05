package no.ssb.lds.core.persistence.postgres;

import no.ssb.lds.api.persistence.buffered.DefaultBufferedPersistence;
import no.ssb.lds.core.persistence.test.BufferedPersistenceIntegration;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;

import java.util.Map;
import java.util.Set;

public class PostgresIntegrationTest extends BufferedPersistenceIntegration {

    public PostgresIntegrationTest() {
        super("lds-provider-postgres-integration-test", Integer.MAX_VALUE);
    }

    @BeforeClass
    public void setup() {
        streaming = new PostgresDbInitializer().initialize(namespace,
                Map.of("postgres.driver.host", "localhost",
                        "postgres.driver.port", "5432",
                        "postgres.driver.user", "lds",
                        "postgres.driver.password", "lds",
                        "postgres.driver.database", "lds"
                ),
                Set.of("Person", "Address", "FunkyLongAddress"));
        persistence = new DefaultBufferedPersistence(streaming, Integer.MAX_VALUE);
    }

    @AfterClass
    public void teardown() {
        if (persistence != null) {
            persistence.close();
        }
    }
}
