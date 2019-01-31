package no.ssb.lds.core.persistence.postgres;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import java.sql.ResultSet;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

public class ResultSetPublisher implements Publisher<ResultSet> {
    final ResultSet resultSet;

    public ResultSetPublisher(ResultSet resultSet) {
        this.resultSet = resultSet;
    }

    @Override
    public void subscribe(Subscriber<? super ResultSet> subscriber) {
        subscriber.onSubscribe(new ResultSetSubscription(subscriber));
        if (resultSet == null) {
            subscriber.onError(new NullPointerException("resultSet"));
        }
    }

    class ResultSetSubscription implements Subscription {
        final AtomicLong requested = new AtomicLong();
        final AtomicLong published = new AtomicLong();
        final AtomicBoolean cancelled = new AtomicBoolean();
        final AtomicReference<Subscriber<? super ResultSet>> subscriber = new AtomicReference<>();
        final AtomicBoolean publicationPending = new AtomicBoolean();

        ResultSetSubscription(Subscriber<? super ResultSet> subscriber) {
            this.subscriber.set(subscriber);
        }

        @Override
        public void request(long n) {
            try {
                if (n <= 0) {
                    throw new IllegalArgumentException("requested amount must be > 0");
                }
                if (cancelled.get()) {
                    return;
                }
                requested.addAndGet(n);
                queuePublicationRequest(() -> iterate());
            } catch (Throwable t) {
                Subscriber<? super ResultSet> subscriber = this.subscriber.get();
                if (subscriber == null) {
                    // asynchronously cancelled
                    return;
                }
                subscriber.onError(t);
            }
        }

        private void iterate() {
            Subscriber<? super ResultSet> subscriber = this.subscriber.get();
            if (subscriber == null) {
                // asynchronously cancelled
                return;
            }
            boolean next;
            try {
                next = resultSet.next();
            } catch (Throwable t) {
                subscriber.onError(t);
                return;
            }
            if (next) {
                try {
                    subscriber.onNext(resultSet);
                    published.incrementAndGet();
                } catch (Throwable t) {
                    subscriber.onError(t);
                } finally {
                    publicationPending.set(false);
                }
                queuePublicationRequest(() -> iterate());
            } else {
                subscriber.onComplete();
            }
        }

        @Override
        public void cancel() {
            cancelled.set(true);
            subscriber.set(null);
        }

        public void queuePublicationRequest(Runnable runnable) {
            if (cancelled.get()) {
                return;
            }
            // loop until stable. Instability is caused by concurrency
            for (; ; ) {
                if (!publicationPending.compareAndSet(false, true)) {
                    return; // a publication is already pending
                }
                long p = published.get();
                long r = requested.get();
                if (r > p) {
                    // available budget
                    runnable.run(); // runnable should eventually cause onNext publication
                    return;
                } else {
                    publicationPending.set(false);
                    // re-check whether requested count has increased while publicationPending flag was set
                    if (r == requested.get()) {
                        // requested count has not changed
                        return;
                    }
                }
            }
        }
    }
}
