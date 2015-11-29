/**
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

namespace org.apache.hadoop.hive.ql.io.orc
{
    using System.Collections.Generic;
    using org.apache.hadoop.hive.ql.io.orc.external;

    /**
     * : a memory manager that keeps a global context of how many ORC
     * writers there are and manages the memory between them. For use cases with
     * dynamic partitions, it is easy to end up with many writers in the same task.
     * By managing the size of each allocation, we try to cut down the size of each
     * allocation and keep the task from running out of memory.
     * 
     * This class is not thread safe, but is re-entrant - ensure creation and all
     * invocations are triggered from the same thread.
     */
    public class MemoryManager
    {
        private static Log LOG = LogFactory.getLog(typeof(MemoryManager));

        /**
         * How often should we check the memory sizes? Measured in rows added
         * to all of the writers.
         */
        private const int ROWS_BETWEEN_CHECKS = 5000;
        private long totalMemoryPool;
        private Dictionary<string, WriterInfo> writerList = new Dictionary<string, WriterInfo>();
        private long totalAllocation = 0;
        private double currentScale = 1;
        private int rowsAddedSinceCheck = 0;

        private class WriterInfo
        {
            public long allocation;
            public Callback callback;

            public WriterInfo(long allocation, Callback callback)
            {
                this.allocation = allocation;
                this.callback = callback;
            }
        }

        public interface Callback
        {
            /**
             * The writer needs to check its memory usage
             * @param newScale the current scale factor for memory allocations
             * @return true if the writer was over the limit
             * @
             */
            bool checkMemory(double newScale);
        }

        /**
         * Create the memory manager.
         * @param poolSize the size of the pool.
         */
        public MemoryManager(long poolSize)
        {
#if false
            double maxLoad = OrcConf.MEMORY_POOL.getDouble(conf);
            totalMemoryPool = Math.Round(ManagementFactory.getMemoryMXBean().
                getHeapMemoryUsage().getMax() * maxLoad);
#endif
            totalMemoryPool = poolSize;
        }

        /**
         * Light weight thread-safety check for multi-threaded access patterns
         */
        private void checkOwner()
        {
#if false
            Preconditions.checkArgument(ownerLock.isHeldByCurrentThread(),
                "Owner thread expected %s, got %s",
                ownerLock.getOwner(),
                Thread.currentThread());
#endif
        }

        /**
         * Add a new writer's memory allocation to the pool. We use the path
         * as a unique key to ensure that we don't get duplicates.
         * @param path the file that is being written
         * @param requestedAllocation the requested buffer size
         */
        public virtual void addWriter(string path, long requestedAllocation, Callback callback)
        {
            checkOwner();
            WriterInfo oldVal = writerList.get(path);
            // this should always be null, but we handle the case where the memory
            // manager wasn't told that a writer wasn't still in use and the task
            // starts writing to the same path.
            if (oldVal == null)
            {
                oldVal = new WriterInfo(requestedAllocation, callback);
                writerList.Add(path, oldVal);
                totalAllocation += requestedAllocation;
            }
            else
            {
                // handle a new writer that is writing to the same path
                totalAllocation += requestedAllocation - oldVal.allocation;
                oldVal.allocation = requestedAllocation;
                oldVal.callback = callback;
            }
            updateScale(true);
        }

        /**
         * Remove the given writer from the pool.
         * @param path the file that has been closed
         */
        public virtual void removeWriter(string path)
        {
            checkOwner();
            WriterInfo val = writerList.get(path);
            if (val != null)
            {
                writerList.Remove(path);
                totalAllocation -= val.allocation;
                if (writerList.Count == 0)
                {
                    rowsAddedSinceCheck = 0;
                }
                updateScale(false);
            }
            else
            {
                System.Diagnostics.Debugger.Break();
            }
            if (writerList.Count == 0)
            {
                rowsAddedSinceCheck = 0;
            }
        }

        /**
         * Get the total pool size that is available for ORC writers.
         * @return the number of bytes in the pool
         */
        long getTotalMemoryPool()
        {
            return totalMemoryPool;
        }

        /**
         * The scaling factor for each allocation to ensure that the pool isn't
         * oversubscribed.
         * @return a fraction between 0.0 and 1.0 of the requested size that is
         * available for each writer.
         */
        double getAllocationScale()
        {
            return currentScale;
        }

        /**
         * Give the memory manager an opportunity for doing a memory check.
         * @
         */
        public void addedRow()
        {
            if (++rowsAddedSinceCheck >= ROWS_BETWEEN_CHECKS)
            {
                notifyWriters();
            }
        }

        /**
         * Notify all of the writers that they should check their memory usage.
         * @
         */
        void notifyWriters()
        {
            checkOwner();
            LOG.debug("Notifying writers after " + rowsAddedSinceCheck);
            foreach (WriterInfo writer in writerList.Values)
            {
                bool flushed = writer.callback.checkMemory(currentScale);
                if (LOG.isDebugEnabled() && flushed)
                {
                    LOG.debug("flushed " + writer.ToString());
                }
            }
            rowsAddedSinceCheck = 0;
        }

        /**
         * Update the currentScale based on the current allocation and pool size.
         * This also updates the notificationTrigger.
         * @param isAllocate is this an allocation?
         */
        private void updateScale(bool isAllocate)
        {
            if (totalAllocation <= totalMemoryPool)
            {
                currentScale = 1;
            }
            else
            {
                currentScale = (double)totalMemoryPool / totalAllocation;
            }
        }
    }
}
