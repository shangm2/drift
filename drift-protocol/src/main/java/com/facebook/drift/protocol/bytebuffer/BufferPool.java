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
package com.facebook.drift.protocol.bytebuffer;

import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

public class BufferPool
{
    private static final int DEFAULT_BUFFER_SIZE = 4096; // 4KB
    private static final int DEFAULT_BUFFER_COUNT = 10 * 1024 * 1024; // 40GB max pool size
    private final ConcurrentLinkedQueue<ByteBuffer> pool = new ConcurrentLinkedQueue<>();
    private final int bufferSize;
    private final AtomicInteger counter = new AtomicInteger();
    private final int maxCount;

    public BufferPool()
    {
        this(DEFAULT_BUFFER_SIZE, DEFAULT_BUFFER_COUNT);
    }

    public BufferPool(int bufferSize, int maxCount)
    {
        this.bufferSize = bufferSize;
        this.maxCount = maxCount;
    }

    public ByteBuffer acquire()
    {
        ByteBuffer buffer = pool.poll();
        if (buffer == null) {
            buffer = ByteBuffer.allocate(bufferSize);
            counter.incrementAndGet();
        }
        buffer.clear();
        return buffer;
    }

    public void release(ByteBuffer buffer)
    {
        // We only reuse buffer with the same size
        if (buffer.capacity() == bufferSize && counter.get() < maxCount) {
            buffer.clear();
            pool.offer(buffer);
        }
    }

    public int getBufferSize()
    {
        return bufferSize;
    }
}
