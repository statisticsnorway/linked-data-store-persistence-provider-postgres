package no.ssb.lds.core.persistence.postgres;

import no.ssb.lds.api.persistence.OutgoingLink;
import no.ssb.lds.api.persistence.Persistence;
import no.ssb.lds.api.persistence.PersistenceDeletePolicy;
import no.ssb.lds.api.persistence.PersistenceException;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Set;

class PostgresPersistence<T, R> implements Persistence {

    private final PostgresPersistenceProvider provider;

    PostgresPersistence(PostgresPersistenceProvider provider) {
        this.provider = provider;
    }

    @Override
    public boolean createOrOverwrite(String namespace, String entity, String id, JSONObject jsonObject, Set<OutgoingLink> links) throws PersistenceException {
        try (PostgresPersistenceTransaction transaction = provider.newTransaction()) {
            PostgresPersistenceReader reader = provider.newReader(transaction.getClient());
            PostgresPersistenceWriter writer = provider.newWriter(transaction.getClient());

            boolean created = writer.createOrOverwriteEntity(namespace, entity, id, jsonObject);

            Collection<OutgoingLink> existingOutgoingLinks;
            if (created) {
                existingOutgoingLinks = Collections.emptyList();
            } else {
                existingOutgoingLinks = reader.getOutgoingEntityLinks(namespace, entity, id);
            }
            writer.mergeOutgoingEntityLinks(links, existingOutgoingLinks);

            return created;
        } catch (Exception e) {
            throw new PersistenceException(e);
        }
    }

    @Override
    public JSONObject read(String namespace, String entity, String id) throws PersistenceException {
        try (PostgresPersistenceTransaction transaction = provider.newTransaction()) {
            PostgresPersistenceReader reader = provider.newReader(transaction.getClient());
            return reader.getEntity(namespace, entity, id);
        } catch (Exception e) {
            throw new PersistenceException(e);
        }
    }

    @Override
    public boolean delete(String namespace, String entity, String id, PersistenceDeletePolicy policy) throws PersistenceException {
        try (PostgresPersistenceTransaction transaction = provider.newTransaction()) {
            PostgresPersistenceWriter writer = provider.newWriter(transaction.getClient());
            return writer.deleteEntity(namespace, entity, id);
        } catch (Exception e) {
            throw new PersistenceException(e);
        }
    }

    @Override
    public JSONArray findAll(String namespace, String entity) throws PersistenceException {
        try (PostgresPersistenceTransaction transaction = provider.newTransaction()) {
            PostgresPersistenceReader reader = provider.newReader(transaction.getClient());
            return reader.getEntities(namespace, entity);
        } catch (Exception e) {
            throw new PersistenceException(e);
        }
    }

    @Override
    public void close() throws PersistenceException {
        if (provider.driver() instanceof Closeable) {
            try {
                ((Closeable) provider.driver()).close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

    }
}
