import no.ssb.lds.api.persistence.PersistenceInitializer;
import no.ssb.lds.core.persistence.postgres.PostgresDbInitializer;

module no.ssb.lds.persistence.postgres {
    requires no.ssb.lds.persistence.api;
    requires java.sql;
    requires org.json;
    requires com.zaxxer.hikari;
    requires postgresql;
    requires java.logging;
    requires jul_to_slf4j;

    provides PersistenceInitializer with PostgresDbInitializer;
}
