/*

   Derby - Class org.apache.derby.impl.services.cache.GClockBuffer

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

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReferenceArray;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.cache.Cacheable;

/**
 * A class managing buffer frames as Generalized CLOCK (GCLOCK).
 * GCLOCK considers `frequency' as well as `recency' of cache accesses.
 */
public final class GClockBuffer implements ReplacementPolicy {

    private final AtomicReferenceArray<BufferFrame> pool;
    private final AtomicInteger _free;
    private final StripedCounter clockhand = new StripedCounter(0);
    private final int size;
    private final int mask;

    private BackgroundDirtyPageWriter bgWriter = null;
    
    public GClockBuffer(int size) {
        this.pool = new AtomicReferenceArray<BufferFrame>(size);
        this._free = new AtomicInteger(size);
        this.size = size;
        this.mask = size - 1;
    }
    
    public AtomicReferenceArray<BufferFrame> getBufferPool() {
        return pool;
    }

    public void insertEntry(CacheEntry entry) throws StandardException {
        throw new UnsupportedOperationException();
    }
    
    public void setBackgroundWriter(BackgroundDirtyPageWriter bgWriter) {
        this.bgWriter = bgWriter;
    }

    public void doShrink() {
        throw new UnsupportedOperationException();
    }

    public BufferFrame add(final BufferFrame entry) {
        do {
            final int free = _free.get();
            if(free == 0) {
                return swap(entry);
            }
            if(_free.compareAndSet(free, free - 1)) {
                break;
            }
        } while(true);
        int idx = clockhand.get();
        while(!pool.compareAndSet(idx & mask, null, entry)) {
            idx++;
        }
        clockhand.increment();
        return null;
    }

    private BufferFrame swap(final BufferFrame entry) {
        int numPinning = 0;
        final boolean bgWriterExists = (bgWriter != null);
        final int start = clockhand.get();
        for(int i = start & mask;; i = ((i + 1) & mask)) {
            final BufferFrame e = pool.get(i);
            if(e == null) {
                continue;
            }
            if(bgWriterExists) {
                if(i == mask) {// reached clock end
                    bgWriter.requestService();
                }
                final Cacheable item = e.getValue();
                if(item != null && item.isDirty()) {
                    if(!bgWriter.scheduleClean(item)) {
                        bgWriter.requestService();
                    }
                }
            }
            final int pincount = e.getPinCount();
            if(pincount == -1) {//evicted?
                if(pool.compareAndSet(i, e, entry)) {
                    moveClockHand(clockhand, size, i, start);
                    return e;
                }
                continue;
            }
            if(pincount > 0) {//pinned?
                if(++numPinning >= size) {
                    Thread.yield();
                }
                continue;
            }
            if(e.decrementRefCount() <= 0) {
                if(e.tryEvict() && pool.compareAndSet(i, e, entry)) {
                    moveClockHand(clockhand, size, i, start);
                    return e;
                }
            }
        }
    }

    private static void moveClockHand(final StripedCounter clockhand, final int capacity, final int curr, final int start) {
        final int delta = (curr < start) ? (curr + capacity - start + 1) : (curr - start + 1);
        clockhand.add(delta);
    }
}
