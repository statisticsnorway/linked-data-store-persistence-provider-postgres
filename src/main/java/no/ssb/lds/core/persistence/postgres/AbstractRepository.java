package no.ssb.lds.core.persistence.postgres;

import no.ssb.lds.api.persistence.PersistenceException;

import java.sql.Connection;

abstract class AbstractRepository<T,PK> {

    protected final Connection conn;

    AbstractRepository(Connection conn) {
        this.conn = conn;
    }

    abstract boolean createOrUpdate(T value) throws PersistenceException;

    abstract boolean delete(PK primaryKey) throws PersistenceException;

    abstract T find(PK primaryKey) throws PersistenceException;

}
