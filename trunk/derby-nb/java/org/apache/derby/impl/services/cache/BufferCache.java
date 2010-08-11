/*

   Derby - Class org.apache.derby.impl.services.cache.BufferCache

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

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.cache.Cacheable;

/**
 * Core operations of non-blocking cache are provided in this base class.
 * Derby specific operations are implemented in <code>NonBlockingCache</code>.
 */
abstract class BufferCache {

    protected final ConcurrentMap<Object, BufferFrame> tbl;
    protected final GClockBuffer clockbuf;
    protected final int poolsize;

    BufferCache(int size) {
        if(!isPowerOfTwo(size)) {
            throw new IllegalArgumentException("Not power of two: " + size);
        }
        this.tbl = new ConcurrentHashMap<Object, BufferFrame>(size, 0.75f, 32); // new org.cliffc.high_scale_lib.NonBlockingHashMap<Object, BufferFrame>(size);
        this.clockbuf = new GClockBuffer(size);
        this.poolsize = size;
    }

    final BufferFrame fixEntry(final Object key) throws StandardException {
        final BufferFrame entry = tbl.get(key);
        if(entry != null && entry.pin()) {
            entry.incrementRefCount();
            return entry;
        } else {
            return addEntry(key, null);
        }
    }

    private final BufferFrame addEntry(final Object key, final Cacheable value)
            throws StandardException {
        for(;;) {
            final BufferFrame newEntry = new BufferFrame(key, value);
            final BufferFrame removed = clockbuf.add(newEntry);
            if(removed != null) {
                assert (removed.isEvicted());
                if(tbl.remove(removed.getKey(), removed)) {
                    purgeEntry(removed);
                }
            }
            final BufferFrame prevEntry = tbl.putIfAbsent(key, newEntry);
            if(prevEntry != null) {
                if(prevEntry.pin()) {
                    newEntry.evictUnshared();
                    prevEntry.incrementRefCount();
                    return prevEntry;
                }
                if(tbl.replace(key, prevEntry, newEntry)) {
                    newEntry.setValue(prevEntry.getValue()); // actually not evicted
                    return newEntry;
                }
                newEntry.evictUnshared();
                continue;
            }
            return newEntry;
        }
    }

    abstract void purgeEntry(BufferFrame entry) throws StandardException;

    final BufferFrame findEntry(final Object key) {
        final BufferFrame entry = tbl.get(key);
        if(entry != null && entry.pin()) {
            entry.incrementRefCount();
            return entry;
        }
        return null;
    }

    final BufferFrame getEntry(final Object key) {
        return tbl.get(key);
    }

    final BufferFrame removeEntry(final Object key) {
        final BufferFrame slot = tbl.get(key);
        if(slot.unpin() == 0 && slot.tryEvict()) {
            return tbl.remove(key);
        }
        return null;
    }

    /**
     * @return null if the specified key is already associated.
     */
    final BufferFrame createNewEntry(final Object key) throws StandardException {
        final BufferFrame newEntry = new BufferFrame(key, null);
        final BufferFrame removed = clockbuf.add(newEntry);
        if(removed != null) {
            assert (removed.isEvicted());
            if(tbl.remove(removed.getKey(), removed)) {
                final Cacheable removedEntry = removed.getValue();
                if(removedEntry != null & removedEntry.isDirty()) {
                    removedEntry.clean(false); // refine to select a non-dirty page first.
                }
            }
        }
        final BufferFrame prevEntry = tbl.putIfAbsent(key, newEntry);
        if(prevEntry != null) {
            newEntry.evictUnshared();
            return null;
        }
        return newEntry;
    }

    private static boolean isPowerOfTwo(final int x) {
        if(x < 1) {
            return false;
        }
        return (x & (x - 1)) == 0;
    }

}
