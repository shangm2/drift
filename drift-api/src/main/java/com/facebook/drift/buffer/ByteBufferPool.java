/*
 * Copyright (C) 2012 ${project.organization.name}
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
 */
package com.facebook.drift.buffer;

import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class ByteBufferPool
{
    private static final int DEFAULT_BUFFER_SIZE = 4096; // 4KB
    private static final int DEFAULT_BUFFER_COUNT = 10 * 1024 * 1024; // 40GB max pool size
    private final ConcurrentLinkedQueue<ReusableByteBuffer> pool = new ConcurrentLinkedQueue<>();
    private final int bufferSize;
    private final int maxCount;
    private final int id;
    private final AtomicLong recycleCounting = new AtomicLong();
    private final AtomicLong reuseCounting = new AtomicLong();
    private final AtomicLong acquireCounting = new AtomicLong();
    private final AtomicLong newAllocateCounting = new AtomicLong();

    private static final AtomicInteger idGenerator = new AtomicInteger();

    public ByteBufferPool()
    {
        this(DEFAULT_BUFFER_SIZE, DEFAULT_BUFFER_COUNT);
    }

    public ByteBufferPool(int bufferSize, int maxCount)
    {
        this.bufferSize = bufferSize;
        this.maxCount = maxCount;
        this.id = idGenerator.incrementAndGet();

        System.out.println("===========> pre allocation");
        for (int i = 0; i < Math.min(maxCount, 1000); i++) {
            ReusableByteBuffer buffer = new ReusableByteBuffer(bufferSize, this);
            buffer.released.set(true);
            pool.offer(buffer);
        }
    }

    public ReusableByteBuffer acquire()
    {
        acquireCounting.getAndIncrement();
        ReusableByteBuffer buffer = pool.poll();
        if (buffer == null) {
            newAllocateCounting.getAndIncrement();
            buffer = new ReusableByteBuffer(bufferSize, this);
        }
        else {
            reuseCounting.getAndIncrement();
            buffer.released.set(false);
        }
        buffer.ownerThread = Thread.currentThread();
        buffer.clear();

        return buffer;
    }

    private void release(ReusableByteBuffer reusableByteBuffer)
    {
        reusableByteBuffer.clear();
        if (pool.size() < maxCount) {
            if (!pool.offer(reusableByteBuffer)) {
                System.out.println("Fail to reuse a buffer");
            }
            else {
                recycleCounting.getAndIncrement();
            }
        }
    }

    public int getId()
    {
        return id;
    }

    public static class ReusableByteBuffer
    {
        private final ByteBuffer buffer;
        private final ByteBufferPool pool;
        private final AtomicBoolean released = new AtomicBoolean(false);
        private volatile Thread ownerThread;

        public ReusableByteBuffer(int bufferSize, ByteBufferPool pool)
        {
            this.buffer = ByteBuffer.allocate(bufferSize);
            this.pool = pool;
        }

        public void release()
        {
            if (Thread.currentThread() != ownerThread) {
                return;
            }
            if (released.compareAndSet(false, true)) {
                ownerThread = null;
                pool.release(this);
            }
        }

        public void clear()
        {
            if (Thread.currentThread() != ownerThread) {
                return;
            }
            buffer.clear();
        }

        public int getBufferRemaining()
        {
            if (Thread.currentThread() != ownerThread) {
                throw new IllegalStateException("Buffer can only be accessed by the owning thread");
            }
            return buffer.remaining();
        }

        public boolean hasNoRemaining()
        {
            if (Thread.currentThread() != ownerThread) {
                throw new IllegalStateException("Buffer can only be accessed by the owning thread");
            }
            return !buffer.hasRemaining();
        }

        public byte get()
        {
            if (Thread.currentThread() != ownerThread) {
                throw new IllegalStateException("Buffer can only be accessed by the owning thread");
            }
            return buffer.get();
        }

        public ReusableByteBuffer get(byte[] dst, int offset, int length)
        {
            if (Thread.currentThread() != ownerThread) {
                throw new IllegalStateException("Buffer can only be accessed by the owning thread");
            }
            buffer.get(dst, offset, length);
            return this;
        }

        public ReusableByteBuffer put(byte b)
        {
            if (Thread.currentThread() != ownerThread) {
                throw new IllegalStateException("Buffer can only be accessed by the owning thread");
            }
            buffer.put(b);
            return this;
        }

        public ReusableByteBuffer put(byte[] src, int offset, int length)
        {
            if (Thread.currentThread() != ownerThread) {
                throw new IllegalStateException("Buffer can only be accessed by the owning thread");
            }
            buffer.put(src, offset, length);
            return this;
        }

        // Position and limit control - only methods actually used
        public int getPosition()
        {
            if (Thread.currentThread() != ownerThread) {
                throw new IllegalStateException("Buffer can only be accessed by the owning thread");
            }
            return buffer.position();
        }

        public void setPosition(int newPosition)
        {
            if (Thread.currentThread() != ownerThread) {
                throw new IllegalStateException("Buffer can only be accessed by the owning thread");
            }
            buffer.position(newPosition);
        }

        public void setLimit(int newLimit)
        {
            if (Thread.currentThread() != ownerThread) {
                throw new IllegalStateException("Buffer can only be accessed by the owning thread");
            }
            buffer.limit(newLimit);
        }

        public void flip()
        {
            if (Thread.currentThread() != ownerThread) {
                throw new IllegalStateException("Buffer can only be accessed by the owning thread");
            }
            buffer.flip();
        }

        // Direct array access for performance-critical operations
        public byte[] getArray()
        {
            if (Thread.currentThread() != ownerThread) {
                throw new IllegalStateException("Buffer can only be accessed by the owning thread");
            }
            return buffer.array();
        }

        public int getArrayOffset()
        {
            if (Thread.currentThread() != ownerThread) {
                throw new IllegalStateException("Buffer can only be accessed by the owning thread");
            }
            return buffer.arrayOffset();
        }
    }

    public int getPoolSize()
    {
        return pool.size();
    }

    public long getRecycle()
    {
        return recycleCounting.get();
    }

    public long getReuse()
    {
        return reuseCounting.get();
    }

    public long getAcquire()
    {
        return acquireCounting.get();
    }

    public long getNewAllocate()
    {
        return newAllocateCounting.get();
    }
}
