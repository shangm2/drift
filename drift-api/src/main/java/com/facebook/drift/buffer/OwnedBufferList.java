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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class OwnedBufferList
        implements AutoCloseable
{
    private final List<BufferPool.OwnedBuffer> ownedBuffers = new ArrayList<>();
    private final List<ByteBuffer> buffers = new ArrayList<>();
    private final BufferPool pool;

    public OwnedBufferList(BufferPool pool)
    {
        this.pool = pool;
    }

    public ByteBuffer acquireBuffer()
    {
        BufferPool.OwnedBuffer ownedBuffer = pool.acquireOwned();
        ByteBuffer buffer = ownedBuffer.getBuffer();
        ownedBuffers.add(ownedBuffer);
        buffers.add(buffer);
        return buffer;
    }

    public List<ByteBuffer> getBuffers()
    {
        return Collections.unmodifiableList(buffers);
    }

    @Override
    public void close()
            throws Exception
    {
        for (BufferPool.OwnedBuffer ownedBuffer : ownedBuffers) {
            ownedBuffer.close();
        }
        ownedBuffers.clear();
        buffers.clear();
    }
}
