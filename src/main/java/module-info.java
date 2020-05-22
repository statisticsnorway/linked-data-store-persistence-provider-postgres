import no.ssb.lds.api.persistence.PersistenceInitializer;
import no.ssb.lds.core.persistence.postgres.PostgresDbInitializer;

module no.ssb.lds.persistence.postgres {
    requires no.ssb.lds.persistence.api;
    requires java.sql;
    requires com.zaxxer.hikari;
    requires org.postgresql.jdbc;
    requires java.logging;
    requires jul_to_slf4j;
    requires io.reactivex.rxjava2;
    requires org.reactivestreams;

    opens postgres;

    provides PersistenceInitializer with PostgresDbInitializer;
}
