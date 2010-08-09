/*

   Derby - Class org.apache.derby.impl.services.cache.StripedCounter

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

/**
 * A high performance counter in which write operations are striped.
 */
public final class StripedCounter {

    private final int size;
    private final AtomicInteger[] counters;

    public StripedCounter() {
        this(0, Runtime.getRuntime().availableProcessors() + 1);
    }

    public StripedCounter(int initValue) {
        this(initValue, Runtime.getRuntime().availableProcessors() + 1);
    }

    public StripedCounter(int initValue, int nstripe) {
        this.size = nextPowerOfTwo(nstripe);
        this.counters = new AtomicInteger[size];
        counters[0] = new AtomicInteger(initValue);
        for(int i = 1; i < size; i++) {
            counters[i] = new AtomicInteger(0);
        }
    }

    public int get() {
        final AtomicInteger[] cnts = counters;
        int sum = 0;
        for(int i = 0; i < size; i++) {
            sum += cnts[i].get();
        }
        return sum;
    }

    public long getLong() {
        final AtomicInteger[] cnts = counters;
        long sum = 0L;
        for(int i = 0; i < size; i++) {
            sum += cnts[i].get();
        }
        return sum;
    }

    public void increment() {
        final int idx = hash(Thread.currentThread(), size - 1);
        counters[idx].addAndGet(1);
    }

    public void add(final int x) {
        final int idx = hash(Thread.currentThread(), size - 1);
        counters[idx].addAndGet(x);
    }

    private static int nextPowerOfTwo(final int targetSize) {
        int i;
        for(i = 2; (1 << i) < targetSize; i++)
            ;
        return 1 << i;
    }

    private static int hash(final Thread thread, final int max) {
        final long id = thread.getId();
        int hash = (((int) (id ^ (id >>> 32))) ^ 0x811c9dc5) * 0x01000193;
        final int nbits = (((0xfffffc00 >> max) & 4) | // Compute ceil(log2(m+1))
                ((0x000001f8 >>> max) & 2) | // The constants hold
        ((0xffff00f2 >>> max) & 1)); // a lookup table
        int index;
        while((index = hash & ((1 << nbits) - 1)) > max) {// May retry on
            hash = (hash >>> nbits) | (hash << (33 - nbits)); // non-power-2 m
        }
        return index;
    }
}
