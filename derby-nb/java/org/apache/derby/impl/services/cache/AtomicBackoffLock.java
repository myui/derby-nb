/*

   Derby - Class org.apache.derby.impl.services.cache.AtomicBackoffLock

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

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Test-and-Test-and-Set (TTAS) spinlock with exponential backoff.
 * 
 * The benefits of TTAS spinlock can be found in the following paper:
 * <pre>
 * Anderson, T. E:
 * ``The Performance of Spin Lock Alternatives for Shared-Memory Multiprocessors'',
 * IEEE Trans. Parallel Distrib. Syst. 1, 1, pp. 6-16, 1990.
 * </pre>
 */
public final class AtomicBackoffLock {

    private final int spinsBeforeYield;
    private final int spinsBeforeSleep;
    private final long initSleepTime;
    private final AtomicBoolean state;

    public AtomicBackoffLock() {
        this(128, 256, 1, false);
    }

    public AtomicBackoffLock(boolean lock) {
        this(128, 256, 1, lock);
    }

    public AtomicBackoffLock(int spinsInterval) {
        this(spinsInterval, spinsInterval << 1, 1, false);
    }

    public AtomicBackoffLock(int spinsInterval, boolean lock) {
        this(spinsInterval, spinsInterval << 1, 1, lock);
    }

    public AtomicBackoffLock(int spinsBeforeYield, int spinsBeforeSleep, long initSleepTime, boolean lock) {
        this.spinsBeforeSleep = spinsBeforeSleep;
        this.spinsBeforeYield = spinsBeforeYield;
        this.initSleepTime = initSleepTime;
        this.state = new AtomicBoolean(lock);
    }

    public boolean isLocked() {
        return state.get();
    }

    public void lock() {
        final int lspinsBeforeYield = spinsBeforeYield;
        final int l_spinsBeforeSleep = spinsBeforeSleep;
        long sleepTime = initSleepTime;
        int spins = 0;
        while(true) {
            if(!state.get()) { // test-and-test-and-set
                if(!state.getAndSet(true)) {
                    return;
                }
            }
            if(spins < lspinsBeforeYield) { // spin phase
                ++spins;
            } else if(spins < l_spinsBeforeSleep) { // yield phase
                ++spins;
                Thread.yield();
            } else { // back-off phase
                try {
                    Thread.sleep(sleepTime);
                } catch (InterruptedException e) {
                    throw new IllegalStateException(e);
                }
                sleepTime = (3 * sleepTime) >> 1 + 1;
            }
        }
    }

    public void unlock() {
        state.set(false);
    }

}
