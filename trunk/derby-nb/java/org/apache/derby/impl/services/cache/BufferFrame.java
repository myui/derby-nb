/*

   Derby - Class org.apache.derby.impl.services.cache.BufferFrame

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
import java.util.concurrent.atomic.AtomicReference;

import org.apache.derby.iapi.services.cache.Cacheable;

/**
 * Class representing a slot for a cached object.
 */
public final class BufferFrame {

    private final Object key;
    private final AtomicReference<Cacheable> value;

    private final AtomicInteger weight = new AtomicInteger(1);
    private final AtomicInteger pinning = new AtomicInteger(1);

    private final AtomicBackoffLock lock = new AtomicBackoffLock(false);

    public BufferFrame(Object key, Cacheable value) {
        this.key = key;
        this.value = new AtomicReference<Cacheable>(value);
    }

    public Object getKey() {
        return key;
    }

    public Cacheable getValue() {
        return value.get();
    }

    public void setValue(Cacheable v) {
        value.set(v);
    }

    public boolean casValue(Cacheable update) {
        return value.compareAndSet(null, update);
    }

    public void incrementWeight() {
        weight.getAndIncrement();
    }

    public int decrementWeight() {
        return weight.decrementAndGet();
    }

    public boolean tryEvict() {
        return pinning.compareAndSet(0, -1);
    }

    public void evictUnshared() {
        pinning.compareAndSet(1, -1);
    }

    public boolean isEvicted() {
        return pinning.get() == -1;
    }

    public int getPinCount() {
        return pinning.get();
    }

    public boolean isPinned() {
        return pinning.get() > 0;
    }

    public boolean pin() {
        final AtomicInteger pin = this.pinning;
        int capa;
        do {
            capa = pin.get();
            if(capa == -1) {// do not pin when the entry is evicted
                return false;
            }
        } while(!pin.compareAndSet(capa, capa + 1));
        return true;
    }

    public int unpin() {
        return pinning.decrementAndGet();
    }

    public AtomicBackoffLock getLock() {
        return lock;
    }
}
