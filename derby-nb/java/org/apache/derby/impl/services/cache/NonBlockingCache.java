/*

   Derby - Class org.apache.derby.impl.services.cache.NonBlockingCache

   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to you under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

 */

package org.apache.derby.impl.services.cache;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicReferenceArray;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.cache.CacheManager;
import org.apache.derby.iapi.services.cache.Cacheable;
import org.apache.derby.iapi.services.cache.CacheableFactory;
import org.apache.derby.iapi.services.daemon.DaemonService;
import org.apache.derby.iapi.services.sanity.SanityManager;
import org.apache.derby.iapi.util.Matchable;

/**
 * Non-blocking cache implementation based on Nb-GCLOCK.
 * 
 * The detail of Nb-GCLOCK is described in the following paper:
 * <pre>
 * Makoto Yui, Jun Miyazaki, Shunsuke Uemura and Hayato Yamana: 
 * ``Nb-GCLOCK: A Non-blocking Buffer Management based on the Generalized CLOCK'', 
 * pp. 745-756, Proc. ICDE, March 2010.
 * </pre>
 */
public final class NonBlockingCache extends BufferCache implements CacheManager {
    static final String CacheTrace = SanityManager.DEBUG ? "CacheTrace" : null;

    private final CacheableFactory holderFactory;
    private final String cacheName;

    private boolean attemptOptimistic = true;

    /**
     * Background cleaner which can be used to clean cached objects in a
     * separate thread to avoid blocking the user threads.
     */
    private BackgroundDirtyPageWriter cleaner;

    /**
     * Flag that indicates whether this cache instance has been shut down.
     */
    private volatile boolean stopped;

    private final BufferStatistics stat;

    public NonBlockingCache(CacheableFactory holderFactory, String name, int initialSize, int maximumSize) {
        super(nextPowerOfTwo(maximumSize));
        this.holderFactory = holderFactory;
        this.cacheName = name;
        this.stat = (CacheTrace == null) ? null : new BufferStatistics();
    }

    private static int nextPowerOfTwo(final int targetSize) {
        int i;
        for(i = 2; (1 << i) < targetSize; i++)
            ;
        return 1 << i;
    }

    public void attemptOptimistic(boolean optimistic) {
        this.attemptOptimistic = optimistic;
    }

    /**
     * Specify a daemon service that can be used to perform operations in
     * the background. Callers must provide enough synchronization so that
     * they have exclusive access to the cache when this method is called.
     *
     * @param daemon the daemon service to use
     */
    public void useDaemonService(DaemonService daemon) {
        if(cleaner != null) {
            cleaner.unsubscribe();
        }
        // Create a background cleaner that can queue up 1/10 of the elements
        // in the cache. Max purge unit is 100 pages.
        this.cleaner = new BackgroundDirtyPageWriter(daemon, Math.max(Math.min(poolsize / 10, 100), 1), stat);
        clockbuf.setBackgroundWriter(cleaner);
    }

    /**
     * Find an object in the cache. If it is not present, add it to the
     * cache. The returned object is kept until <code>release()</code> is
     * called.
     */
    public Cacheable find(final Object key) throws StandardException {
        if(stopped) {
            return null;
        }
        final BufferFrame slot = fixEntry(key);
        final Cacheable cachedItem = slot.getValue();
        if(cachedItem != null) {
            if(stat != null) {
                stat.hits.increment();
            }
            assert (cachedItem.getIdentity() != null);
            return cachedItem;
        }
        if(stat != null) {
            stat.misses.increment();
        }
        return attemptOptimistic ? allocateOpt(slot, key, null, false)
                : allocate(slot, key, null, false);
    }

    /**
     * Find an object in the cache. If it is not present, return
     * <code>null</code>. The returned object is kept until
     * <code>release()</code> is called.
     */
    public Cacheable findCached(final Object key) throws StandardException {
        if(stopped) {
            return null;
        }
        final BufferFrame slot = findEntry(key);
        Cacheable cached = null;
        if(slot != null) {
            cached = slot.getValue();
        }
        if(stat != null) {
            if(cached == null) {
                stat.misses.increment();
            } else {
                stat.hits.increment();
            }
        }
        return cached;
    }

    /**
     * Create an object in the cache. The object is kept until
     * <code>release()</code> is called.
     */
    public Cacheable create(final Object key, final Object createParameter)
            throws StandardException {
        if(stopped) {
            return null;
        }
        final BufferFrame slot = createNewEntry(key);
        if(slot == null) {
            throw StandardException.newException(SQLState.OBJECT_EXISTS_IN_CACHE, cacheName, key);
        }
        return attemptOptimistic ? allocateOpt(slot, key, createParameter, true)
                : allocate(slot, key, createParameter, true);
    }

    private Cacheable allocateOpt(final BufferFrame slot, final Object key, final Object createParameter, final boolean create)
            throws StandardException {
        final Cacheable cachedItem = slot.getValue();
        if(cachedItem != null) {
            return cachedItem;
        }
        // attempt optimistic IO for pread
        final Cacheable newItem = holderFactory.newCacheable(this);
        final Cacheable itemWithIdentity = create ? newItem.createIdentity(key, createParameter)
                : newItem.setIdentity(key);
        if(slot.casValue(itemWithIdentity)) {
            if(itemWithIdentity == null) {
                if(SanityManager.DEBUG) {
                    SanityManager.DEBUG(CacheTrace, newItem.getClass().getName()
                            + "#createIdentity(" + key + ',' + createParameter + ") returned null");
                }
            }
            return itemWithIdentity;
        } else {
            final Cacheable item = slot.getValue();
            if(item == null) {
                SanityManager.DEBUG(CacheTrace, "Slot was not filled");
            }
            return item;
        }
    }

    private Cacheable allocate(final BufferFrame slot, final Object key, final Object createParameter, final boolean create)
            throws StandardException {
        Cacheable cachedItem = slot.getValue();
        if(cachedItem == null) {
            final AtomicBackoffLock lock = slot.getLock();
            try {
                lock.lock();
                cachedItem = slot.getValue();
                if(cachedItem == null) {// double-checked locking
                    Cacheable newItem = holderFactory.newCacheable(this);
                    Cacheable itemWithIdentity = create ? newItem.createIdentity(key, createParameter)
                            : newItem.setIdentity(key);
                    slot.setValue(itemWithIdentity);
                    return itemWithIdentity;
                }
            } finally {
                lock.unlock();
            }
        }
        assert (cachedItem != null);
        return cachedItem;
    }

    /**
     * Release an object that has been fetched from the cache with
     * <code>find()</code>, <code>findCached()</code> or <code>create()</code>.
     */
    public void release(final Cacheable entry) {
        if(SanityManager.DEBUG) {
            SanityManager.ASSERT(entry != null);
        }
        final Object key = entry.getIdentity();
        final BufferFrame slot = getEntry(key);
        if(SanityManager.DEBUG) {
            SanityManager.ASSERT(slot != null, "Released entry does not exist");
            SanityManager.ASSERT(!slot.isEvicted(), "Entry is already evicted");
        }
        slot.unpin();
    }

    /**
     * Remove an object from the cache. The object must previously have been
     * fetched from the cache with <code>find()</code>,
     * <code>findCached()</code> or <code>create()</code>. The user of the
     * cache must make sure that only one caller executes this method on a
     * cached object. This method will wait until the object has been removed
     * (its keep count must drop to zero before it can be removed).
     */
    public void remove(Cacheable entry) throws StandardException {
        final Object key = entry.getIdentity();
        if(removeEntry(key) != null) {
            entry.clean(true);
        } else {
            SanityManager.DEBUG(CacheTrace, "Cannot be removed: " + key);
        }
    }

    /**
     * Clean all dirty objects in the cache.
     */
    public void cleanAll() throws StandardException {
        clean(null);
    }

    /**
     * Clean all dirty objects matching a partial key. 
     * All objects that existed in the cache at the time of the call will be cleaned. 
     * Objects added later may or may not be cleaned.
     * 
     * @param partialKey the partial (or exact) key to match, or
     * <code>null</code> to match all keys
     */
    public void clean(final Matchable partialKey) throws StandardException {
        final AtomicReferenceArray<BufferFrame> pool = clockbuf.getBufferPool();
        final int size = pool.length();
        for(int i = 0; i < size; i++) {
            final BufferFrame slot = pool.get(i);
            if(slot != null) {
                final Cacheable item = slot.getValue();
                if(item == null || !item.isDirty()) {
                    continue;
                }
                if(slot.isEvicted()) {
                    item.clean(false);
                } else {
                    final Object key = slot.getKey();
                    if(partialKey != null && !partialKey.match(key)) {
                        continue;
                    }
                    try {
                        if(slot.pin() && item.isDirty()) {
                            item.clean(false);
                        }
                    } finally {
                        slot.unpin();
                    }
                }
            }
        }
    }

    /**
     * Remove all objects that are not kept and not dirty.
     * Age as many objects as possible out of the cache.
     */
    public void ageOut() {
        final AtomicReferenceArray<BufferFrame> pool = clockbuf.getBufferPool();
        final int size = pool.length();
        for(int i = 0; i < size; i++) {
            final BufferFrame slot = pool.get(i);
            if(slot != null && !slot.isEvicted()) {
                final Cacheable item = slot.getValue();
                if(item == null || item.isDirty()) {
                    continue;
                }
                // non-dirty entry
                if(slot.tryEvict() && !item.isDirty()) {
                    final Object key = slot.getKey();
                    if(!tbl.remove(key, slot)) {
                        SanityManager.DEBUG(CacheTrace, "Cannot remove a mapping for a key: " + key);
                    }
                }
            }
        }
    }

    /**
     * Discard all unused objects that match a partial key. Dirty objects will
     * not be cleaned before their removal.
     */
    public boolean discard(final Matchable partialKey) {
        boolean allDisabled = true;
        final AtomicReferenceArray<BufferFrame> pool = clockbuf.getBufferPool();
        final int size = pool.length();
        for(int i = 0; i < size; i++) {
            final BufferFrame slot = pool.get(i);
            if(slot != null && !slot.isEvicted()) {
                final Cacheable item = slot.getValue();
                if(item == null) {
                    continue;
                }
                final Object key = slot.getKey();
                if(partialKey != null && !partialKey.match(key)) {
                    continue;
                }
                if(!slot.tryEvict()) {
                    allDisabled = false;
                }
            }
        }
        return allDisabled;
    }

    /**
     * Return a collection view of all the <code>Cacheable</code>s in the
     * cache.
     */
    public Collection<Cacheable> values() {
        final AtomicReferenceArray<BufferFrame> pool = clockbuf.getBufferPool();
        final int size = pool.length();
        final List<Cacheable> values = new ArrayList<Cacheable>(size);
        for(int i = 0; i < size; i++) {
            final BufferFrame slot = pool.get(i);
            if(slot != null) {
                final Cacheable item = slot.getValue();
                if(item != null) {
                    values.add(item);
                }
            }
        }
        return values;
    }

    /**
     * Shutdown the cache.
     */
    public void shutdown() throws StandardException {
        this.stopped = true;
        cleanAll(); // flush dirty pages
        ageOut(); // sweep non-dirty pages
    }

    void purgeEntry(final BufferFrame entry) throws StandardException {
        final Cacheable removedEntry = entry.getValue();
        if(removedEntry != null & removedEntry.isDirty()) {
            removedEntry.clean(false); // refinable to select a non-dirty page first.
        }
    }
}
