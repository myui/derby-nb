/*
 * @(#)$Id$
 *
 * Copyright 2006-2008 Makoto YUI
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * 
 * Contributors:
 *     Makoto YUI - initial implementation
 */
package org.apache.derby.impl.services.cache;

/**
 * A class managing buffer statistics.
 */
final class BufferStatistics {

    final StripedCounter hits = new StripedCounter(0);
    final StripedCounter misses = new StripedCounter(0);

    BufferStatistics() {}

    double getHitRate() {
        long hit = hits.getLong();
        long miss = misses.getLong();
        long total = hit + miss;
        return ((double) hit / total);
    }

    String getStatistic() {
        long hit = hits.getLong();
        long miss = misses.getLong();
        long total = hit + miss;
        double hitrate = ((double) hit / total);
        return "total accesses: " + total + ", buffer hit rate: " + String.format("%.2f", hitrate)
                + '%';
    }
}
