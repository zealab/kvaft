package io.zealab.kvaft.core;

import com.google.common.collect.Lists;

import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * response confirm signal queue
 *
 * @author Leon Wong
 */
public class SignalQueue {

    /**
     * confirm signal
     */
    private List<String> signals = Lists.newArrayList();

    /**
     * signal in term
     */
    private long term = 0L;

    /**
     * queue rw lock
     */
    private ReadWriteLock rwLock = new ReentrantReadWriteLock();

    /**
     * this method use for updating term value and clearing signal queue
     *
     * @param term term
     */
    public void updateTerm(long term) {
        Lock wLock = rwLock.writeLock();
        try {
            wLock.lock();
            this.term = term;
            signals.clear();
        } finally {
            wLock.unlock();
        }
    }

    /**
     * add signal in this term if not existed
     *
     * @param endpoint responded endpoint
     * @param term     responded endpoint term value
     */
    public void addSignalIfNx(Endpoint endpoint, long term) {
        Lock wLock = rwLock.writeLock();
        try {
            wLock.lock();
            if (this.term == term && !signals.contains(endpoint.toString())) {
                signals.add(endpoint.toString());
            }
        } finally {
            wLock.unlock();
        }
    }

    /**
     * queue size
     *
     * @return queue size
     */
    public int size() {
        Lock rLock = rwLock.readLock();
        try {
            rLock.lock();
            return signals.size();
        } finally {
            rLock.unlock();
        }
    }
}
