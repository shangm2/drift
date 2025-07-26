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
import com.facebook.drift.protocol.TTransport;
import com.facebook.drift.protocol.TTransportException;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

public class ByteBufferOutputTransport
        implements ByteBufferCapableTransport
{
    private final ByteBufferOutputStream outputStream;

    public ByteBufferOutputTransport(ByteBufferPool pool, List<ByteBufferPool.ReusableByteBuffer> byteBufferList)
    {
        outputStream = new ByteBufferOutputStream(pool, byteBufferList);
    }

    @Override
    public void read(byte[] buf, int off, int len)
            throws TTransportException
    {
        throw new UnsupportedOperationException("This is a write-only transport");
    }

    @Override
    public void write(byte[] buf, int off, int len)
            throws TTransportException
    {
        try {
            outputStream.write(buf, off, len);
        }
        catch (IOException e) {
            throw new TTransportException("Failed to write", e);
        }
    }

    public void finish()
    {
        outputStream.finishLastBuffer();
    }

    // ByteBufferCapableTransport interface methods for zero-copy operations

    @Override
    public void write(ByteBuffer buffer) throws TTransportException
    {
        try {
            outputStream.writeFromByteBuffer(buffer);
        }
        catch (IOException e) {
            throw new TTransportException("Failed to write ByteBuffer", e);
        }
    }

    @Override
    public void write(List<ByteBufferPool.ReusableByteBuffer> bufferList) throws TTransportException
    {
        try {
            for (ByteBufferPool.ReusableByteBuffer buffer : bufferList) {
                outputStream.writeFromReusableByteBuffer(buffer);
            }
        }
        catch (IOException e) {
            throw new TTransportException("Failed to write buffer list", e);
        }
    }

    @Override
    public int read(ByteBuffer destination) throws TTransportException
    {
        throw new TTransportException("This is a write-only transport");
    }

    @Override
    public List<ByteBufferPool.ReusableByteBuffer> readToBufferList(ByteBufferPool pool, int size) throws TTransportException
    {
        throw new TTransportException("This is a write-only transport");
    }
}
