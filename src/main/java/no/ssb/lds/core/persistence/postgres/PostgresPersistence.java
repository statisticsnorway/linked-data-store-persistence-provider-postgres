package no.ssb.lds.core.persistence.postgres;

import io.reactivex.Completable;
import io.reactivex.Flowable;
import io.reactivex.Single;
import no.ssb.lds.api.persistence.DocumentKey;
import no.ssb.lds.api.persistence.PersistenceDeletePolicy;
import no.ssb.lds.api.persistence.PersistenceException;
import no.ssb.lds.api.persistence.Transaction;
import no.ssb.lds.api.persistence.TransactionFactory;
import no.ssb.lds.api.persistence.reactivex.Range;
import no.ssb.lds.api.persistence.reactivex.RxPersistence;
import no.ssb.lds.api.persistence.streaming.Fragment;
import no.ssb.lds.api.persistence.streaming.FragmentType;

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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.Optional.ofNullable;

class PostgresPersistence implements RxPersistence {

    private static final ZonedDateTime BEGINNING_OF_TIME = ZonedDateTime.of(1900, 1, 1, 0, 0, 0, 0, ZoneId.of("Etc/UTC"));
    private static final ZonedDateTime END_OF_TIME = ZonedDateTime.of(9999, 1, 1, 0, 0, 0, 0, ZoneId.of("Etc/UTC"));

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

    static class CreateSql {
        final PreparedStatement ps;
        final PreparedStatement deleteStatement;

        CreateSql(PreparedStatement ps, PreparedStatement deleteStatement) {
            this.ps = ps;
            this.deleteStatement = deleteStatement;
        }
    }

    @Override
    public Completable createOrOverwrite(Transaction transaction, Flowable<Fragment> fragments) {
        int batchSize = 100;
        final AtomicReference<CreateSql> createSqlRef = new AtomicReference<>();
        final AtomicInteger insertCount = new AtomicInteger();
        final AtomicInteger deleteCount = new AtomicInteger();
        final Set<DocumentKey> deletedDocuments = new LinkedHashSet<>();
        final PostgresTransaction tx = (PostgresTransaction) transaction;
        return Single.fromCallable(() -> new CreateSql(
                tx.connection.prepareStatement("INSERT INTO namespace(entity, id, version, path, indices, type, value) values(?, ?, ?, ?, ?, ?, ?)"),
                tx.connection.prepareStatement("DELETE FROM namespace WHERE entity = ? AND id = ? AND version = ?")))
                .flatMapPublisher(createSql -> fragments.doOnNext(item -> {
                    createSqlRef.set(createSql);
                    PreparedStatement ps = createSql.ps;
                    PreparedStatement deleteStatement = createSql.deleteStatement;
                    if (item.offset() != 0) {
                        throw new IllegalStateException("Postgres fragments must have offset == 0, illegal offset: " + item.offset());
                    }
                    try {
                        Timestamp version = new Timestamp(item.timestamp().toInstant().toEpochMilli());
                        if (deletedDocuments.add(DocumentKey.from(item))) {
                            deleteStatement.setString(1, item.entity());
                            deleteStatement.setString(2, item.id());
                            deleteStatement.setTimestamp(3, version);
                            deleteStatement.addBatch();
                            deleteCount.incrementAndGet();
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
                        ps.addBatch();
                        if (insertCount.incrementAndGet() % batchSize == 0) {
                            if (deleteCount.get() > 0) {
                                deleteStatement.executeBatch();
                                deleteStatement.clearBatch();
                                deleteCount.set(0);
                            }
                            ps.executeBatch();
                            ps.clearBatch();
                        }
                    } catch (SQLException e) {
                        throw new PersistenceException(e);
                    }
                }))
                .ignoreElements()
                .doOnComplete(() -> {
                    if (insertCount.get() % batchSize == 0) {
                        return; // batch was run by the very last fragment
                    }
                    if (createSqlRef.get() == null) {
                        return; // no fragments was processed
                    }
                    if (deleteCount.get() > 0) {
                        PreparedStatement deleteStatement = createSqlRef.get().deleteStatement;
                        deleteStatement.executeBatch();
                        deleteStatement.clearBatch();
                        deleteCount.set(0);
                    }
                    PreparedStatement ps = createSqlRef.get().ps;
                    ps.executeBatch();
                    ps.clearBatch();
                });
    }

    @Override
    public Flowable<Fragment> read(Transaction transaction, ZonedDateTime snapshot, String namespace, String entity, String id) {
        final PostgresTransaction tx = (PostgresTransaction) transaction;
        return Single.fromCallable(() -> {
            Timestamp snapshotVersion = new Timestamp(snapshot.toInstant().toEpochMilli());
            PreparedStatement ps = tx.connection.prepareStatement(
                    "SELECT id, version, path, indices, type, value " +
                            "FROM namespace " +
                            "WHERE entity = ? AND id = ? AND version = (SELECT max(version) FROM namespace WHERE entity = ? AND id = ? AND version <= ?) " +
                            "ORDER BY version, path, indices, type");
            ps.setString(1, entity);
            ps.setString(2, id);
            ps.setString(3, entity);
            ps.setString(4, id);
            ps.setTimestamp(5, snapshotVersion);
            ps.setFetchSize(10);
            ResultSet resultSet = ps.executeQuery();
            return resultSet;
        }).flatMapPublisher(resultSet -> Flowable.fromPublisher(new ResultSetPublisher(resultSet)).map(rs -> toFragment(rs, namespace, entity)));
    }

    private Fragment toFragment(ResultSet resultSet, String namespace, String entity) throws SQLException {
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
        return new Fragment(namespace, entity, id, timestamp, path, FragmentType.fromTypeCode((byte) type), 0, value);
    }

    @Override
    public Flowable<Fragment> readVersions(Transaction transaction, String namespace, String entity, String id, Range<ZonedDateTime> range) {
        final PostgresTransaction tx = (PostgresTransaction) transaction;
        return Single.fromCallable(() -> {
            Timestamp snapshotAfterVersion = new Timestamp(ofNullable(range.getAfter()).orElse(BEGINNING_OF_TIME).toInstant().toEpochMilli());
            Timestamp snapshotBeforeVersion = new Timestamp(ofNullable(range.getBefore()).orElse(END_OF_TIME).toInstant().toEpochMilli());
            PreparedStatement ps = tx.connection.prepareStatement("SELECT id, version, path, indices, type, value FROM namespace, " +
                    "(SELECT max(version) v FROM namespace WHERE entity = ? AND id = ? AND version <= ? LIMIT 1) a " +
                    "WHERE entity = ? AND id = ? AND version < ? AND (a.v IS NULL OR version >= a.v)");
            ps.setString(1, entity);
            ps.setString(2, id);
            ps.setTimestamp(3, snapshotAfterVersion);
            ps.setString(4, entity);
            ps.setString(5, id);
            ps.setTimestamp(6, snapshotBeforeVersion);
            if (range.getLimit() != null) {
                ps.setMaxRows(range.getLimit());
            }
            ps.setFetchSize(10);
            ResultSet resultSet = ps.executeQuery();
            return resultSet;
        }).flatMapPublisher(resultSet -> Flowable.fromPublisher(new ResultSetPublisher(resultSet)).map(rs -> toFragment(rs, namespace, entity)));
    }

    @Override
    public Flowable<Fragment> readAll(Transaction transaction, ZonedDateTime snapshot, String namespace, String entity, Range<String> range) {
        final PostgresTransaction tx = (PostgresTransaction) transaction;
        return Single.fromCallable(() -> {
            Timestamp snapshotVersion = new Timestamp(snapshot.toInstant().toEpochMilli());
            PreparedStatement ps = tx.connection.prepareStatement("SELECT n.id, n.version, n.path, n.indices, n.type, n.value FROM namespace n " +
                    "JOIN (SELECT id, max(version) as version FROM namespace WHERE entity = ? AND ? < id AND id < ? AND version <= ? GROUP BY id) a " +
                    "ON (n.id = a.id AND n.version = a.version) " +
                    "WHERE entity = ? " +
                    "ORDER BY n.id, n.version, n.path, n.indices, n.type");
            ps.setString(1, entity);
            ps.setString(2, ofNullable(range.getAfter()).orElse(" "));
            ps.setString(3, ofNullable(range.getBefore()).orElse("~"));
            ps.setTimestamp(4, snapshotVersion);
            ps.setString(5, entity);
            if (range.getLimit() != null) {
                ps.setMaxRows(range.getLimit());
            }
            ps.setFetchSize(10);
            ResultSet resultSet = ps.executeQuery();
            return resultSet;
        }).flatMapPublisher(resultSet -> Flowable.fromPublisher(new ResultSetPublisher(resultSet)).map(rs -> toFragment(rs, namespace, entity)));
    }

    @Override
    public Completable delete(Transaction transaction, String namespace, String entity, String id, ZonedDateTime version, PersistenceDeletePolicy policy) {
        return Completable.fromCallable(() -> {
            PostgresTransaction tx = (PostgresTransaction) transaction;
            PreparedStatement ps = tx.connection.prepareStatement("DELETE FROM namespace WHERE entity = ? AND id = ? AND version = ?");
            ps.setString(1, entity);
            ps.setString(2, id);
            ps.setTimestamp(3, new Timestamp(version.toInstant().toEpochMilli()));
            ps.executeUpdate();
            return null;
        });
    }

    @Override
    public Completable deleteAllVersions(Transaction transaction, String namespace, String entity, String id, PersistenceDeletePolicy policy) {
        return Completable.fromCallable(() -> {
            PostgresTransaction tx = (PostgresTransaction) transaction;
            PreparedStatement ps = tx.connection.prepareStatement("DELETE FROM namespace WHERE entity = ? AND id = ?");
            ps.setString(1, entity);
            ps.setString(2, id);
            ps.executeUpdate();
            return null;
        });
    }

    @Override
    public Completable markDeleted(Transaction transaction, String namespace, String entity, String id, ZonedDateTime timestamp, PersistenceDeletePolicy policy) {
        return Completable.fromCallable(() -> {
            PostgresTransaction tx = (PostgresTransaction) transaction;
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
            return null;
        });
    }

    @Override
    public Flowable<Fragment> find(Transaction transaction, ZonedDateTime snapshot, String namespace, String entity, String path, byte[] value, Range<String> range) {
        final PostgresTransaction tx = (PostgresTransaction) transaction;
        return Single.fromCallable(() -> {
            PreparedStatement ps = tx.connection.prepareStatement("SELECT n.id, n.version, n.path, n.indices, n.type, n.value FROM namespace n " +
                    "JOIN (SELECT id, max(version) ver FROM namespace WHERE entity = ? AND path = ? AND value = ? AND version <= ? GROUP BY id ORDER BY id LIMIT ?) a ON (n.id = a.id AND n.version = a.ver) " +
                    "WHERE entity = ?");
            ps.setString(1, entity);
            ps.setString(2, path);
            ps.setBytes(3, value);
            Timestamp snapshotTime = new Timestamp(snapshot.toInstant().toEpochMilli());
            ps.setTimestamp(4, snapshotTime);
            ps.setInt(5, ofNullable(range.getLimit()).orElse(100));
            ps.setString(6, entity);
            ps.setMaxRows(0); // number of resources returned limited by inner query
            ps.setFetchSize(10);
            ResultSet resultSet = ps.executeQuery();
            return resultSet;
        }).flatMapPublisher(resultSet -> Flowable.fromPublisher(new ResultSetPublisher(resultSet)).map(rs -> toFragment(rs, namespace, entity)));
    }

    @Override
    public Single<Boolean> hasPrevious(Transaction tx, ZonedDateTime snapshot, String namespace, String entityName, String id) {
        return readAll(tx, snapshot, namespace, entityName, Range.lastBefore(1, id)).isEmpty()
                .map(wasEmpty -> !wasEmpty);
    }

    @Override
    public Single<Boolean> hasNext(Transaction tx, ZonedDateTime snapshot, String namespace, String entityName, String id) {
        return readAll(tx, snapshot, namespace, entityName, Range.firstAfter(1, id)).isEmpty()
                .map(wasEmpty -> !wasEmpty);
    }

    @Override
    public void close() throws PersistenceException {
        transactionFactory.close();
    }
}