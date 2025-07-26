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

import java.nio.ByteBuffer;
import java.util.List;

/**
 * Enhanced transport interface that supports direct ByteBuffer operations for zero-copy performance.
 * Transports implementing this interface can bypass byte[] arrays and work directly with ByteBuffers,
 * eliminating intermediate copying for better performance.
 */
public interface ByteBufferCapableTransport extends TTransport
{
    /**
     * Write a ByteBuffer directly to the transport without intermediate copying.
     * 
     * @param buffer the ByteBuffer to write
     * @throws TTransportException if write fails
     */
    void write(ByteBuffer buffer) throws TTransportException;

    /**
     * Write a list of ReusableByteBuffers directly to the transport without intermediate copying.
     * 
     * @param bufferList the list of buffers to write
     * @throws TTransportException if write fails
     */
    void write(List<ByteBufferPool.ReusableByteBuffer> bufferList) throws TTransportException;

    /**
     * Read data directly into a ByteBuffer without intermediate copying.
     * 
     * @param destination the ByteBuffer to read into
     * @return number of bytes read, or -1 if end of stream
     * @throws TTransportException if read fails
     */
    int read(ByteBuffer destination) throws TTransportException;

    /**
     * Read data directly into a list of pooled ByteBuffers without intermediate copying.
     * 
     * @param pool the ByteBufferPool to acquire buffers from
     * @param size the total number of bytes to read
     * @return list of buffers containing the read data
     * @throws TTransportException if read fails
     */
    List<ByteBufferPool.ReusableByteBuffer> readToBufferList(ByteBufferPool pool, int size) throws TTransportException;
}