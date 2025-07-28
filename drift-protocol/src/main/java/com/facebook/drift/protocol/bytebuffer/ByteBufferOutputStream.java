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

import com.facebook.drift.buffer.ByteBufferPool;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.List;

import static java.util.Objects.requireNonNull;

public class ByteBufferOutputStream
        extends OutputStream
{
    private final ByteBufferPool pool;
    private final List<ByteBufferPool.ReusableByteBuffer> byteBufferList;
    private ByteBufferPool.ReusableByteBuffer currentBuffer;

    public ByteBufferOutputStream(ByteBufferPool pool, List<ByteBufferPool.ReusableByteBuffer> byteBufferList)
    {
        this.pool = requireNonNull(pool, "pool is null");
        this.byteBufferList = requireNonNull(byteBufferList, "byteBufferList is null");

        byteBufferList.clear();
        addNewBuffer();
    }

    @Override
    public void write(int b)
            throws IOException
    {
        if (currentBuffer.hasNoRemaining()) {
            addNewBuffer();
        }
        currentBuffer.put((byte) b);
    }

    @Override
    public void write(byte[] b, int off, int len)
            throws IOException
    {
        int remaining = len;
        int offset = off;

        while (remaining > 0) {
            if (currentBuffer.hasNoRemaining()) {
                addNewBuffer();
            }

            int bytesToWrite = Math.min(remaining, currentBuffer.getBufferRemaining());
            
            currentBuffer.put(b, offset, bytesToWrite);

            offset += bytesToWrite;
            remaining -= bytesToWrite;
        }
    }

    private void addNewBuffer()
    {
        finishLastBuffer();

        ByteBufferPool.ReusableByteBuffer next = pool.acquire();
        byteBufferList.add(next);
        currentBuffer = next;
    }

    public void finishLastBuffer()
    {
        if (currentBuffer != null) {
            currentBuffer.flip();
        }
    }

    @Override
    public void close()
            throws IOException
    {
        finishLastBuffer();
    }

    /**
     * Write data from a ByteBuffer directly to the output stream without intermediate copying.
     * This method provides zero-copy performance when both source and destination support
     * direct buffer operations.
     * 
     * @param source the ByteBuffer to write from
     * @throws IOException if write fails
     */
    public void writeFromByteBuffer(ByteBuffer source) throws IOException
    {
        while (source.hasRemaining()) {
            if (currentBuffer.hasNoRemaining()) {
                addNewBuffer();
            }
            
            int bytesToCopy = Math.min(source.remaining(), currentBuffer.getBufferRemaining());
            
            // Use DirectBufferUtil for optimal buffer-to-buffer copying
            DirectBufferUtil.copyBufferToBuffer(source, currentBuffer.getByteBuffer(), bytesToCopy);
        }
    }

    /**
     * Write data from a ReusableByteBuffer directly to the output stream without intermediate copying.
     * This method provides zero-copy performance for buffer pool operations.
     * 
     * @param sourceBuffer the ReusableByteBuffer to write from
     * @throws IOException if write fails
     */
    public void writeFromReusableByteBuffer(ByteBufferPool.ReusableByteBuffer sourceBuffer) throws IOException
    {
        ByteBuffer source = sourceBuffer.getByteBuffer();
        int originalPosition = source.position();
        int originalLimit = source.limit();
        
        // Set up source buffer for reading
        source.position(sourceBuffer.getPosition());
        source.limit(sourceBuffer.getPosition() + sourceBuffer.getBufferRemaining());
        
        try {
            writeFromByteBuffer(source);
        } finally {
            // Restore original position and limit
            source.position(originalPosition);
            source.limit(originalLimit);
        }
    }
}
