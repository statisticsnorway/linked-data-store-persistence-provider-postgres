package no.ssb.lds.core.persistence.postgres;

import no.ssb.lds.core.persistence.test.PersistenceIntegrationTest;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;

import java.util.Map;
import java.util.Set;

public class PostgresIntegrationTest extends PersistenceIntegrationTest {

    public PostgresIntegrationTest() {
        super("lds-provider-postgres-integration-test");
    }

    @BeforeClass
    public void setup() {
        persistence = new PostgresDbInitializer().initialize(namespace,
                Map.of("postgres.driver.host", "localhost",
                        "postgres.driver.port", "5432",
                        "postgres.driver.user", "lds",
                        "postgres.driver.password", "lds",
                        "postgres.driver.database", "lds",
                        "persistence.fragment.capacity", String.valueOf(Integer.MAX_VALUE)
                ),
                Set.of("Person", "Address", "FunkyLongAddress"));
    }

    @AfterClass
    public void teardown() {
        if (persistence != null) {
            persistence.close();
        }
    }
}
