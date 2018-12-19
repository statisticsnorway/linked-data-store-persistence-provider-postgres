package no.ssb.lds.core.persistence.postgres;

import no.ssb.lds.api.persistence.DocumentKey;
import no.ssb.lds.api.persistence.PersistenceDeletePolicy;
import no.ssb.lds.api.persistence.PersistenceException;
import no.ssb.lds.api.persistence.Transaction;
import no.ssb.lds.api.persistence.TransactionFactory;
import no.ssb.lds.api.persistence.streaming.Fragment;
import no.ssb.lds.api.persistence.streaming.FragmentType;
import no.ssb.lds.api.persistence.streaming.Persistence;

import java.sql.Array;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

class PostgresPersistence implements Persistence {

    final PostgresTransactionFactory transactionFactory;

    PostgresPersistence(PostgresTransactionFactory transactionFactory) {
        this.transactionFactory = transactionFactory;
    }

    @Override
    public TransactionFactory transactionFactory() throws PersistenceException {
        return transactionFactory;
    }

    @Override
    public Transaction createTransaction(boolean readOnly) throws PersistenceException {
        return transactionFactory.createTransaction(readOnly);
    }

    @Override
    public CompletableFuture<Void> createOrOverwrite(Transaction transaction, Flow.Publisher<Fragment> publisher) throws PersistenceException {
        CompletableFuture<Void> result = new CompletableFuture<>();
        PostgresTransaction tx = (PostgresTransaction) transaction;
        publisher.subscribe(new Flow.Subscriber<>() {
            final Set<DocumentKey> deletedDocuments = new LinkedHashSet<>();
            PreparedStatement ps;
            Flow.Subscription subscription;

            @Override
            public void onSubscribe(Flow.Subscription subscription) {
                this.subscription = subscription;
                try {
                    ps = tx.connection.prepareStatement("INSERT INTO namespace(entity, id, version, path, indices, type, value) values(?, ?, ?, ?, ?, ?, ?)");
                } catch (SQLException e) {
                    throw new PersistenceException(e);
                }
                subscription.request(2);
            }

            @Override
            public void onNext(Fragment item) {
                if (item.offset() != 0) {
                    throw new IllegalStateException("Postgres fragments must have offset == 0, illegal offset: " + item.offset());
                }
                try {
                    Timestamp version = new Timestamp(item.timestamp().toInstant().toEpochMilli());
                    if (deletedDocuments.add(DocumentKey.from(item))) {
                        PreparedStatement deleteStatement = tx.connection.prepareStatement("DELETE FROM namespace WHERE entity = ? AND id = ? AND version = ?");
                        deleteStatement.setString(1, item.entity());
                        deleteStatement.setString(2, item.id());
                        deleteStatement.setTimestamp(3, version);
                        deleteStatement.executeUpdate();
                    }
                    ps.setString(1, item.entity());
                    ps.setString(2, item.id());
                    ps.setTimestamp(3, version);
                    ArrayList<Integer> indices = new ArrayList<>();
                    String indexUnawarePath = Fragment.computeIndexUnawarePath(item.path(), indices);
                    ps.setString(4, indexUnawarePath);
                    Integer[] elements = indices.toArray(new Integer[indices.size()]);
                    Array pathIndices = tx.connection.createArrayOf("integer", elements);
                    ps.setArray(5, pathIndices);
                    ps.setShort(6, (short) item.fragmentType().ordinal());
                    ps.setBytes(7, item.value());
                    ps.executeUpdate();
                } catch (SQLException e) {
                    throw new PersistenceException(e);
                }
                subscription.request(1);
            }

            @Override
            public void onError(Throwable throwable) {
                result.completeExceptionally(throwable);
            }

            @Override
            public void onComplete() {
                result.complete(null);
            }
        });

        return result;
    }

    @Override
    public Flow.Publisher<Fragment> read(Transaction transaction, ZonedDateTime snapshot, String namespace, String entity, String id) throws PersistenceException {
        return subscriber -> subscriber.onSubscribe(new Flow.Subscription() {
            final AtomicLong budget = new AtomicLong(0);
            final AtomicBoolean first = new AtomicBoolean(true);
            final PostgresTransaction tx = (PostgresTransaction) transaction;
            final AtomicBoolean cancelled = new AtomicBoolean(false);
            ResultSet resultSet;

            @Override
            public void request(long n) {
                if (budget.getAndAdd(n) > 0) {
                    // back-pressure arrived before budget was exhausted
                    return;
                }
                // budget was exhausted before back-pressure arrived
                if (first.compareAndSet(true, false)) {
                    // first request
                    try {
                        Timestamp snapshotVersion = new Timestamp(snapshot.toInstant().toEpochMilli());
                        PreparedStatement ps = tx.connection.prepareStatement("SELECT version, path, indices, type, value FROM namespace " +
                                "WHERE entity = ? AND id = ? AND version = (SELECT max(version) FROM namespace WHERE entity = ? AND id = ? AND version <= ?)");
                        ps.setString(1, entity);
                        ps.setString(2, id);
                        ps.setString(3, entity);
                        ps.setString(4, id);
                        ps.setTimestamp(5, snapshotVersion);
                        ps.setFetchSize(10);
                        resultSet = ps.executeQuery();
                    } catch (SQLException e) {
                        subscriber.onError(e);
                        return;
                    }
                } else {
                    // not-first requests
                }
                try {
                    while (budget.getAndDecrement() > 0) {
                        if (cancelled.get()) {
                            return;
                        }
                        if (!resultSet.next()) {
                            subscriber.onComplete();
                            return;
                        }
                        Timestamp version = resultSet.getTimestamp(1);
                        ZonedDateTime timestamp = ZonedDateTime.ofInstant(version.toInstant(), ZoneId.of("Etc/UTC"));
                        String indexUnawarePath = resultSet.getString(2);
                        Array arrayIndices = resultSet.getArray(3);
                        Integer[] integerArray = (Integer[]) arrayIndices.getArray();
                        List<Integer> indices = List.of(integerArray);
                        String path = Fragment.computePathFromIndexUnawarePathAndIndices(indexUnawarePath, indices);
                        short type = resultSet.getShort(4);
                        byte[] value = resultSet.getBytes(5);
                        subscriber.onNext(new Fragment(namespace, entity, id, timestamp, path, FragmentType.fromTypeCode((byte) type), 0, value));
                    }
                } catch (SQLException e) {
                    subscriber.onError(e);
                    return;
                }
            }

            @Override
            public void cancel() {
                if (cancelled.compareAndSet(false, true)) {
                    try {
                        resultSet.close();
                    } catch (SQLException e) {
                        e.printStackTrace();
                    }
                }
            }
        });
    }

    @Override
    public Flow.Publisher<Fragment> readVersions(Transaction transaction, ZonedDateTime snapshotFrom, ZonedDateTime snapshotTo, String namespace, String entity, String id, ZonedDateTime firstVersion, int limit) throws PersistenceException {
        return subscriber -> subscriber.onSubscribe(new Flow.Subscription() {
            final AtomicLong budget = new AtomicLong(0);
            final AtomicBoolean first = new AtomicBoolean(true);
            final PostgresTransaction tx = (PostgresTransaction) transaction;
            final AtomicBoolean cancelled = new AtomicBoolean(false);
            ResultSet resultSet;

            @Override
            public void request(long n) {
                if (budget.getAndAdd(n) > 0) {
                    // back-pressure arrived before budget was exhausted
                    return;
                }
                // budget was exhausted before back-pressure arrived
                if (first.compareAndSet(true, false)) {
                    // first request
                    try {
                        Timestamp snapshotFromVersion = new Timestamp(snapshotFrom.toInstant().toEpochMilli());
                        Timestamp snapshotToVersion = new Timestamp(snapshotTo.toInstant().toEpochMilli());
                        PreparedStatement ps = tx.connection.prepareStatement("SELECT version, path, indices, type, value FROM namespace " +
                                "WHERE entity = ? AND id = ? AND version < ? AND version >= (SELECT max (version) FROM namespace WHERE entity = ? AND id = ? AND version <= ? LIMIT 1)");
                        ps.setString(1, entity);
                        ps.setString(2, id);
                        ps.setTimestamp(3, snapshotToVersion);
                        ps.setString(4, entity);
                        ps.setString(5, id);
                        ps.setTimestamp(6, snapshotFromVersion);
                        ps.setMaxRows(limit);
                        ps.setFetchSize(10);
                        resultSet = ps.executeQuery();
                    } catch (SQLException e) {
                        subscriber.onError(e);
                        return;
                    }
                } else {
                    // not-first requests
                }
                try {
                    while (budget.getAndDecrement() > 0) {
                        if (cancelled.get()) {
                            return;
                        }
                        if (!resultSet.next()) {
                            subscriber.onComplete();
                            return;
                        }
                        Timestamp version = resultSet.getTimestamp(1);
                        ZonedDateTime timestamp = ZonedDateTime.ofInstant(version.toInstant(), ZoneId.of("Etc/UTC"));
                        String indexUnawarePath = resultSet.getString(2);
                        Array arrayIndices = resultSet.getArray(3);
                        Integer[] integerArray = (Integer[]) arrayIndices.getArray();
                        List<Integer> indices = List.of(integerArray);
                        String path = Fragment.computePathFromIndexUnawarePathAndIndices(indexUnawarePath, indices);
                        short type = resultSet.getShort(4);
                        byte[] value = resultSet.getBytes(5);
                        subscriber.onNext(new Fragment(namespace, entity, id, timestamp, path, FragmentType.fromTypeCode((byte) type), 0, value));
                    }
                } catch (SQLException e) {
                    subscriber.onError(e);
                    return;
                }
            }

            @Override
            public void cancel() {
                if (cancelled.compareAndSet(false, true)) {
                    try {
                        resultSet.close();
                    } catch (SQLException e) {
                        e.printStackTrace();
                    }
                }
            }
        });
    }

    @Override
    public Flow.Publisher<Fragment> readAllVersions(Transaction transaction, String namespace, String entity, String id, ZonedDateTime firstVersion, int limit) throws PersistenceException {
        return subscriber -> subscriber.onSubscribe(new Flow.Subscription() {
            final AtomicLong budget = new AtomicLong(0);
            final AtomicBoolean first = new AtomicBoolean(true);
            final PostgresTransaction tx = (PostgresTransaction) transaction;
            final AtomicBoolean cancelled = new AtomicBoolean(false);
            ResultSet resultSet;

            @Override
            public void request(long n) {
                if (budget.getAndAdd(n) > 0) {
                    // back-pressure arrived before budget was exhausted
                    return;
                }
                // budget was exhausted before back-pressure arrived
                if (first.compareAndSet(true, false)) {
                    // first request
                    try {
                        PreparedStatement ps = tx.connection.prepareStatement("SELECT version, path, indices, type, value FROM namespace WHERE entity = ? AND id = ?");
                        ps.setString(1, entity);
                        ps.setString(2, id);
                        ps.setMaxRows(limit);
                        ps.setFetchSize(10);
                        resultSet = ps.executeQuery();
                    } catch (SQLException e) {
                        subscriber.onError(e);
                        return;
                    }
                } else {
                    // not-first requests
                }
                try {
                    while (budget.getAndDecrement() > 0) {
                        if (cancelled.get()) {
                            return;
                        }
                        if (!resultSet.next()) {
                            subscriber.onComplete();
                            return;
                        }
                        Timestamp version = resultSet.getTimestamp(1);
                        ZonedDateTime timestamp = ZonedDateTime.ofInstant(version.toInstant(), ZoneId.of("Etc/UTC"));
                        String indexUnawarePath = resultSet.getString(2);
                        Array arrayIndices = resultSet.getArray(3);
                        Integer[] integerArray = (Integer[]) arrayIndices.getArray();
                        List<Integer> indices = List.of(integerArray);
                        String path = Fragment.computePathFromIndexUnawarePathAndIndices(indexUnawarePath, indices);
                        short type = resultSet.getShort(4);
                        byte[] value = resultSet.getBytes(5);
                        subscriber.onNext(new Fragment(namespace, entity, id, timestamp, path, FragmentType.fromTypeCode((byte) type), 0, value));
                    }
                } catch (SQLException e) {
                    subscriber.onError(e);
                    return;
                }
            }

            @Override
            public void cancel() {
                if (cancelled.compareAndSet(false, true)) {
                    try {
                        resultSet.close();
                    } catch (SQLException e) {
                        e.printStackTrace();
                    }
                }
            }
        });
    }

    @Override
    public CompletableFuture<Void> delete(Transaction transaction, String namespace, String entity, String id, ZonedDateTime version, PersistenceDeletePolicy policy) throws PersistenceException {
        CompletableFuture<Void> result = new CompletableFuture<>();
        try {
            PostgresTransaction tx = (PostgresTransaction) transaction;
            PreparedStatement ps = tx.connection.prepareStatement("DELETE FROM namespace WHERE entity = ? AND id = ? AND version = ?");
            ps.setString(1, entity);
            ps.setString(2, id);
            ps.setTimestamp(3, new Timestamp(version.toInstant().toEpochMilli()));
            ps.executeUpdate();
            result.complete(null);
        } catch (SQLException e) {
            result.completeExceptionally(e);
        }
        return result;
    }

    @Override
    public CompletableFuture<Void> deleteAllVersions(Transaction transaction, String namespace, String entity, String id, PersistenceDeletePolicy policy) throws PersistenceException {
        CompletableFuture<Void> result = new CompletableFuture<>();
        try {
            PostgresTransaction tx = (PostgresTransaction) transaction;
            PreparedStatement ps = tx.connection.prepareStatement("DELETE FROM namespace WHERE entity = ? AND id = ?");
            ps.setString(1, entity);
            ps.setString(2, id);
            ps.executeUpdate();
            result.complete(null);
        } catch (SQLException e) {
            result.completeExceptionally(e);
        }
        return result;
    }

    @Override
    public CompletableFuture<Void> markDeleted(Transaction transaction, String namespace, String entity, String id, ZonedDateTime timestamp, PersistenceDeletePolicy policy) throws PersistenceException {
        CompletableFuture<Void> result = new CompletableFuture<>();
        PostgresTransaction tx = (PostgresTransaction) transaction;
        try {
            Timestamp version = new Timestamp(timestamp.toInstant().toEpochMilli());
            PreparedStatement deleteStatement = tx.connection.prepareStatement("DELETE FROM namespace WHERE entity = ? AND id = ? AND version = ?");
            deleteStatement.setString(1, entity);
            deleteStatement.setString(2, id);
            deleteStatement.setTimestamp(3, version);
            deleteStatement.executeUpdate();
            PreparedStatement ps = tx.connection.prepareStatement("INSERT INTO namespace(entity, id, version, path, indices, type) values(?, ?, ?, ?, ?, ?)");
            ps.setString(1, entity);
            ps.setString(2, id);
            ps.setTimestamp(3, version);
            ps.setString(4, "");
            Array pathIndices = tx.connection.createArrayOf("integer", new Integer[0]);
            ps.setArray(5, pathIndices);
            ps.setShort(6, (short) FragmentType.DELETED.ordinal());
            ps.executeUpdate();
            result.complete(null);
        } catch (SQLException e) {
            result.completeExceptionally(e);
        }
        return result;
    }

    @Override
    public Flow.Publisher<Fragment> findAll(Transaction transaction, ZonedDateTime snapshot, String namespace, String entity, String firstId, int limit) throws PersistenceException {
        return subscriber -> subscriber.onSubscribe(new Flow.Subscription() {
            final AtomicLong budget = new AtomicLong(0);
            final AtomicBoolean first = new AtomicBoolean(true);
            final PostgresTransaction tx = (PostgresTransaction) transaction;
            final AtomicBoolean cancelled = new AtomicBoolean(false);
            ResultSet resultSet;

            @Override
            public void request(long n) {
                if (budget.getAndAdd(n) > 0) {
                    // back-pressure arrived before budget was exhausted
                    return;
                }
                // budget was exhausted before back-pressure arrived
                if (first.compareAndSet(true, false)) {
                    // first request
                    try {
                        PreparedStatement ps = tx.connection.prepareStatement("SELECT n.id, n.version, n.path, n.indices, n.type, n.value FROM namespace n " +
                                "JOIN (SELECT id, max(version) ver FROM namespace WHERE entity = ? AND version <= ? GROUP BY id ORDER BY id LIMIT ?) a ON (n.id = a.id AND n.version = a.ver) " +
                                "WHERE entity = ?");
                        ps.setString(1, entity);
                        Timestamp snapshotTime = new Timestamp(snapshot.toInstant().toEpochMilli());
                        ps.setTimestamp(2, snapshotTime);
                        ps.setInt(3, limit);
                        ps.setString(4, entity);
                        ps.setMaxRows(0); // number of resources returned limited by inner query
                        ps.setFetchSize(10);
                        resultSet = ps.executeQuery();
                    } catch (SQLException e) {
                        subscriber.onError(e);
                        return;
                    }
                } else {
                    // not-first requests
                }
                try {
                    while (budget.getAndDecrement() > 0) {
                        if (cancelled.get()) {
                            return;
                        }
                        if (!resultSet.next()) {
                            subscriber.onComplete();
                            return;
                        }
                        String id = resultSet.getString(1);
                        Timestamp version = resultSet.getTimestamp(2);
                        ZonedDateTime timestamp = ZonedDateTime.ofInstant(version.toInstant(), ZoneId.of("Etc/UTC"));
                        String indexUnawarePath = resultSet.getString(3);
                        Array arrayIndices = resultSet.getArray(4);
                        Integer[] integerArray = (Integer[]) arrayIndices.getArray();
                        List<Integer> indices = List.of(integerArray);
                        String path = Fragment.computePathFromIndexUnawarePathAndIndices(indexUnawarePath, indices);
                        short type = resultSet.getShort(5);
                        byte[] value = resultSet.getBytes(6);
                        subscriber.onNext(new Fragment(namespace, entity, id, timestamp, path, FragmentType.fromTypeCode((byte) type), 0, value));
                    }
                } catch (SQLException e) {
                    subscriber.onError(e);
                    return;
                }
            }

            @Override
            public void cancel() {
                if (cancelled.compareAndSet(false, true)) {
                    try {
                        resultSet.close();
                    } catch (SQLException e) {
                        e.printStackTrace();
                    }
                }
            }
        });
    }

    @Override
    public Flow.Publisher<Fragment> find(Transaction transaction, ZonedDateTime snapshot, String namespace, String entity, String path, byte[] value, String firstId, int limit) throws PersistenceException {
        return subscriber -> subscriber.onSubscribe(new Flow.Subscription() {
            final AtomicLong budget = new AtomicLong(0);
            final AtomicBoolean first = new AtomicBoolean(true);
            final PostgresTransaction tx = (PostgresTransaction) transaction;
            final AtomicBoolean cancelled = new AtomicBoolean(false);
            ResultSet resultSet;

            @Override
            public void request(long n) {
                if (budget.getAndAdd(n) > 0) {
                    // back-pressure arrived before budget was exhausted
                    return;
                }
                // budget was exhausted before back-pressure arrived
                if (first.compareAndSet(true, false)) {
                    // first request
                    try {
                        PreparedStatement ps = tx.connection.prepareStatement("SELECT n.id, n.version, n.path, n.indices, n.type, n.value FROM namespace n " +
                                "JOIN (SELECT id, max(version) ver FROM namespace WHERE entity = ? AND path = ? AND value = ? AND version <= ? GROUP BY id ORDER BY id LIMIT ?) a ON (n.id = a.id AND n.version = a.ver) " +
                                "WHERE entity = ?");
                        ps.setString(1, entity);
                        ps.setString(2, path);
                        ps.setBytes(3, value);
                        Timestamp snapshotTime = new Timestamp(snapshot.toInstant().toEpochMilli());
                        ps.setTimestamp(4, snapshotTime);
                        ps.setInt(5, limit);
                        ps.setString(6, entity);
                        ps.setMaxRows(0); // number of resources returned limited by inner query
                        ps.setFetchSize(10);
                        resultSet = ps.executeQuery();
                    } catch (SQLException e) {
                        subscriber.onError(e);
                        return;
                    }
                } else {
                    // not-first requests
                }
                try {
                    while (budget.getAndDecrement() > 0) {
                        if (cancelled.get()) {
                            return;
                        }
                        if (!resultSet.next()) {
                            subscriber.onComplete();
                            return;
                        }
                        String id = resultSet.getString(1);
                        Timestamp version = resultSet.getTimestamp(2);
                        ZonedDateTime timestamp = ZonedDateTime.ofInstant(version.toInstant(), ZoneId.of("Etc/UTC"));
                        String indexUnawarePath = resultSet.getString(3);
                        Array arrayIndices = resultSet.getArray(4);
                        Integer[] integerArray = (Integer[]) arrayIndices.getArray();
                        List<Integer> indices = List.of(integerArray);
                        String path = Fragment.computePathFromIndexUnawarePathAndIndices(indexUnawarePath, indices);
                        short type = resultSet.getShort(5);
                        byte[] value = resultSet.getBytes(6);
                        subscriber.onNext(new Fragment(namespace, entity, id, timestamp, path, FragmentType.fromTypeCode((byte) type), 0, value));
                    }
                } catch (SQLException e) {
                    subscriber.onError(e);
                    return;
                }
            }

            @Override
            public void cancel() {
                if (cancelled.compareAndSet(false, true)) {
                    try {
                        resultSet.close();
                    } catch (SQLException e) {
                        e.printStackTrace();
                    }
                }
            }
        });
    }

    @Override
    public void close() throws PersistenceException {
        transactionFactory.close();
    }
}