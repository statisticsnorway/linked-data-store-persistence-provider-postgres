package no.ssb.lds.core.persistence.postgres;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import no.ssb.lds.api.persistence.PersistenceInitializer;
import no.ssb.lds.api.persistence.ProviderName;
import no.ssb.lds.api.persistence.reactivex.RxJsonPersistence;
import no.ssb.lds.api.persistence.reactivex.RxJsonPersistenceBridge;
import no.ssb.lds.api.specification.Specification;

import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;
import java.util.Properties;
import java.util.Scanner;
import java.util.Set;
import java.util.logging.Level;

// https://jdbc.postgresql.org/documentation/head/index.html

@ProviderName("postgres")
public class PostgresDbInitializer implements PersistenceInitializer {

    static class JavaUtilLoggingInitializer {
        static {
            JavaUtilLoggerBridge.installJavaUtilLoggerBridgeHandler(Level.INFO);
        }

        static void initialize() {
        }

    }

    @Override
    public String persistenceProviderId() {
        return "postgres";
    }

    @Override
    public Set<String> configurationKeys() {
        return Set.of(
                "postgres.driver.host",
                "postgres.driver.port",
                "postgres.driver.user",
                "postgres.driver.password",
                "postgres.driver.database"
        );
    }

    private PostgresPersistence postgresPersistence;

    public PostgresPersistence getPostgresPersistence() {
        return postgresPersistence;
    }

    @Override
    public RxJsonPersistence initialize(String defaultNamespace, Map<String, String> configuration, Set<String> managedDomains, Specification specification) {
        int fragmentCapacityBytes = Integer.MAX_VALUE; // Postgres persistence-provider does not support fragmentation of document leaf-nodes.
        JavaUtilLoggingInitializer.initialize();
        HikariDataSource dataSource = openDataSource(configuration);
        postgresPersistence = new PostgresPersistence(new PostgresTransactionFactory(dataSource));
        return new RxJsonPersistenceBridge(postgresPersistence, fragmentCapacityBytes);
    }

    public static HikariDataSource openDataSource(Map<String, String> configuration) {
        String postgresDbDriverHost = configuration.get("postgres.driver.host");
        String postgresDbDriverPort = configuration.get("postgres.driver.port");
        HikariDataSource dataSource = PostgresDbInitializer.openDataSource(
                postgresDbDriverHost,
                postgresDbDriverPort,
                configuration.get("postgres.driver.user"),
                configuration.get("postgres.driver.password"),
                configuration.get("postgres.driver.database")
        );
        return dataSource;
    }

    static HikariDataSource openDataSource(String postgresDbDriverHost, String postgresDbDriverPort, String postgresDbDriverUser, String postgresDbDriverPassword, String postgresDbDriverDatabase) {
        Properties props = new Properties();
        props.setProperty("dataSourceClassName", "org.postgresql.ds.PGSimpleDataSource");
        props.setProperty("dataSource.serverName", postgresDbDriverHost);
        props.setProperty("dataSource.portNumber", postgresDbDriverPort);
        props.setProperty("dataSource.user", postgresDbDriverUser);
        props.setProperty("dataSource.password", postgresDbDriverPassword);
        props.setProperty("dataSource.databaseName", postgresDbDriverDatabase);
        props.put("dataSource.logWriter", new PrintWriter(System.out));

        HikariConfig config = new HikariConfig(props);
        config.setAutoCommit(false);
        config.setMaximumPoolSize(10);
        HikariDataSource datasource = new HikariDataSource(config);

        dropOrCreateDatabase(datasource);

        return datasource;
    }

    static void dropOrCreateDatabase(HikariDataSource datasource) {
        try {
            String initSQL = FileAndClasspathReaderUtils.readFileOrClasspathResource("postgres/init-db.sql");
            Connection conn = datasource.getConnection();
            conn.beginRequest();

            try (Scanner s = new Scanner(initSQL)) {
                s.useDelimiter("(;(\r)?\n)|(--\n)");
                try (Statement st = conn.createStatement()) {
                    try {
                        while (s.hasNext()) {
                            String line = s.next();
                            if (line.startsWith("/*!") && line.endsWith("*/")) {
                                int i = line.indexOf(' ');
                                line = line.substring(i + 1, line.length() - " */".length());
                            }

                            if (line.trim().length() > 0) {
                                st.execute(line);
                            }
                        }
                        conn.commit();
                    } finally {
                        st.close();
                    }
                }
            }
            conn.endRequest();

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
