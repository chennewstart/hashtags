/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.service;

import java.util.List;
import java.util.Map;
import java.util.Set;

public interface StorageProxyMBean
{
    /**
     * @see org.apache.cassandra.metrics.LatencyMetrics#lastOpCount
     */
    @Deprecated
    public long getReadOperations();
    /**
     * @see org.apache.cassandra.metrics.LatencyMetrics#totalLatencyHistogram
     */
    @Deprecated
    public long getTotalReadLatencyMicros();
    /**
     * @see org.apache.cassandra.metrics.LatencyMetrics#recentLatencyHistogram
     */
    @Deprecated
    public double getRecentReadLatencyMicros();
    /**
     * @see org.apache.cassandra.metrics.LatencyMetrics#totalLatencyHistogram
     */
    @Deprecated
    public long[] getTotalReadLatencyHistogramMicros();
    /**
     * @see org.apache.cassandra.metrics.LatencyMetrics#recentLatencyHistogram
     */
    @Deprecated
    public long[] getRecentReadLatencyHistogramMicros();

    @Deprecated
    public long getRangeOperations();
    @Deprecated
    public long getTotalRangeLatencyMicros();
    @Deprecated
    public double getRecentRangeLatencyMicros();
    @Deprecated
    public long[] getTotalRangeLatencyHistogramMicros();
    @Deprecated
    public long[] getRecentRangeLatencyHistogramMicros();

    @Deprecated
    public long getWriteOperations();
    @Deprecated
    public long getTotalWriteLatencyMicros();
    @Deprecated
    public double getRecentWriteLatencyMicros();
    @Deprecated
    public long[] getTotalWriteLatencyHistogramMicros();
    @Deprecated
    public long[] getRecentWriteLatencyHistogramMicros();

    public long getTotalHints();
    public boolean getHintedHandoffEnabled();
    public Set<String> getHintedHandoffEnabledByDC();
    public void setHintedHandoffEnabled(boolean b);
    public void setHintedHandoffEnabledByDCList(String dcs);
    public int getMaxHintWindow();
    public void setMaxHintWindow(int ms);
    public int getMaxHintsInProgress();
    public void setMaxHintsInProgress(int qs);
    public int getHintsInProgress();

    public Long getRpcTimeout();
    public void setRpcTimeout(Long timeoutInMillis);
    public Long getReadRpcTimeout();
    public void setReadRpcTimeout(Long timeoutInMillis);
    public Long getWriteRpcTimeout();
    public void setWriteRpcTimeout(Long timeoutInMillis);
    public Long getCasContentionTimeout();
    public void setCasContentionTimeout(Long timeoutInMillis);
    public Long getRangeRpcTimeout();
    public void setRangeRpcTimeout(Long timeoutInMillis);
    public Long getTruncateRpcTimeout();
    public void setTruncateRpcTimeout(Long timeoutInMillis);

    public void reloadTriggerClasses();

    public long getReadRepairAttempted();
    public long getReadRepairRepairedBlocking();
    public long getReadRepairRepairedBackground();

    /** Returns each live node's schema version */
    public Map<String, List<String>> getSchemaVersions();
}
