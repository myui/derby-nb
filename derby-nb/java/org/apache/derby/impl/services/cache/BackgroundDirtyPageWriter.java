/*

   Derby - Class org.apache.derby.impl.services.cache.BackgroundDirtyPageWriter

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

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.cache.Cacheable;
import org.apache.derby.iapi.services.context.ContextManager;
import org.apache.derby.iapi.services.daemon.DaemonService;
import org.apache.derby.iapi.services.daemon.Serviceable;
import org.apache.derby.iapi.services.sanity.SanityManager;

/**
 * A background cleaner that {@code NonBlockingCache} can use to clean {@code
 * Cacheable}s asynchronously in a background instead of synchronously in the
 * user threads.
 */
public final class BackgroundDirtyPageWriter implements Serviceable {

    private final BlockingQueue<Cacheable> queue;
    private final int maxPurgeUnit;

    /** The service thread which performs the clean operations. */
    private final DaemonService daemonService;
    /** Subscription number for this <code>Serviceable</code>. */
    private final int clientNumber;

    private final BufferStatistics bufStats;

    /** write dirty pages every 30 seconds by the default. */
    private long interval = 30000L;

    private final AtomicBoolean scheduled = new AtomicBoolean(false);
    
    public BackgroundDirtyPageWriter(DaemonService daemon, int queueSize, BufferStatistics stat) {
        this.queue = new ArrayBlockingQueue<Cacheable>(queueSize); // new BoundedTransferQueue<Cacheable>(queueSize);
        this.maxPurgeUnit = Math.max(queueSize / 10, 100);
        this.daemonService = daemon;
        this.bufStats = stat;
        // subscribe with the onDemandOnly flag
        this.clientNumber = daemon.subscribe(this, false);
    }

    public void setInterval(long interval) {
        this.interval = interval;
    }

    boolean scheduleClean(final Cacheable entry) {
        return queue.offer(entry);
    }

    /**
     * Notify the daemon service that the cleaner needs to be serviced.
     */
    void requestService() {
        if(scheduled.compareAndSet(false, true)) {
            // Calling serviceNow() doesn't have any effect if we have already
            // called it and the request hasn't been processed yet. Therefore, we
            // only call serviceNow() if we can atomically change scheduled from
            // false to true. 
            synchronized(scheduled) {
                scheduled.notifyAll();
            }
            daemonService.serviceNow(clientNumber);
        }
    }

    /**
     * Stop subscribing to the daemon service.
     */
    void unsubscribe() {
        daemonService.unsubscribe(clientNumber);
    }

    /**
     * Clean the first entry in the queue. If there is more work, re-request
     * service from the daemon service.
     */
    public int performWork(ContextManager context) throws StandardException {
        Cacheable entry = queue.poll();
        if(entry != null) {
            int cleaned = 0;
            do {
                if(entry.isDirty()) {
                    entry.clean(false);
                    cleaned++;
                }
                if(cleaned >= maxPurgeUnit) {
                    break;
                }
            } while((entry = queue.poll()) != null);
            if(SanityManager.DEBUG) {
                SanityManager.DEBUG(NonBlockingCache.CacheTrace, "["
                        + Thread.currentThread().getName() + "] Wrote " + cleaned
                        + " dirty pages to disk");
                if(bufStats != null) {
                    SanityManager.DEBUG(NonBlockingCache.CacheTrace, bufStats.getStatistic());
                }
            }
        }
        scheduled.set(false);
        return sleep(scheduled, interval) ? Serviceable.REQUEUE : Serviceable.DONE;
    }

    public boolean serviceASAP() {
        return false;
    }

    public boolean serviceImmediately() {
        return false;
    }

    private static boolean sleep(final AtomicBoolean sync, final long mills) {
        try {
            synchronized(sync) {
                sync.wait(mills);
            }
            return true;
        } catch (InterruptedException e) {
            return false;
        }
    }

}
