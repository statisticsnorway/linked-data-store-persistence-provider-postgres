package no.ssb.lds.core.persistence.postgres;

import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.async.AsyncIterable;
import com.apple.foundationdb.async.AsyncIterator;

import java.util.List;
import java.util.NavigableMap;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

public class PostgresAsyncIterable implements AsyncIterable<KeyValue> {

    final NavigableMap<byte[], byte[]> map;

    public PostgresAsyncIterable(NavigableMap<byte[], byte[]> map) {
        this.map = map;
    }

    @Override
    public AsyncIterator<KeyValue> iterator() {
        return new PostgresAsyncIterator(map);
    }

    @Override
    public CompletableFuture<List<KeyValue>> asList() {
        return CompletableFuture.completedFuture(map.entrySet().stream().map(e -> new KeyValue(e.getKey(), e.getValue())).collect(Collectors.toList()));
    }
}
