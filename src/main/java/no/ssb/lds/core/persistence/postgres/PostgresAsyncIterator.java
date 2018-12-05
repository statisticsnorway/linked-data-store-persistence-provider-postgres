package no.ssb.lds.core.persistence.postgres;

import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.async.AsyncIterator;

import java.util.Iterator;
import java.util.Map;
import java.util.NavigableMap;
import java.util.concurrent.CompletableFuture;

public class PostgresAsyncIterator implements AsyncIterator<KeyValue> {

    final NavigableMap<byte[], byte[]> map;
    final Iterator<Map.Entry<byte[], byte[]>> internalIterator;

    public PostgresAsyncIterator(NavigableMap<byte[], byte[]> map) {
        this.map = map;
        this.internalIterator = map.entrySet().iterator();
    }

    @Override
    public CompletableFuture<Boolean> onHasNext() {
        return CompletableFuture.completedFuture(hasNext());
    }

    @Override
    public boolean hasNext() {
        return internalIterator.hasNext();
    }

    @Override
    public KeyValue next() {
        Map.Entry<byte[], byte[]> nextInternal = internalIterator.next();
        return new KeyValue(nextInternal.getKey(), nextInternal.getValue());
    }

    @Override
    public void cancel() {
    }
}
