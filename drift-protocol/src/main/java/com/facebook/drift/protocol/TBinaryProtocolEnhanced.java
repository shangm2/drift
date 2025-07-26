/*
 * Copyright (C) 2017 Facebook, Inc.
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
package com.facebook.drift.protocol;

import com.facebook.drift.TException;
import com.facebook.drift.buffer.ByteBufferPool;
import com.facebook.drift.protocol.bytebuffer.ByteBufferCapableTransport;

import java.nio.ByteBuffer;
import java.util.List;

/**
 * Enhanced binary protocol implementation that provides zero-copy operations when used with
 * ByteBufferCapableTransport implementations. Falls back to standard TBinaryProtocol behavior
 * for other transport types.
 */
public class TBinaryProtocolEnhanced extends TBinaryProtocol
{
    private final ByteBufferCapableTransport byteBufferTransport;

    /**
     * Constructor that automatically detects ByteBuffer-capable transports for zero-copy optimization.
     * 
     * @param transport the transport to use
     */
    public TBinaryProtocolEnhanced(TTransport transport)
    {
        super(transport);
        this.byteBufferTransport = (transport instanceof ByteBufferCapableTransport) ? 
            (ByteBufferCapableTransport) transport : null;
    }

    @Override
    public void writeBinary(ByteBuffer value) throws TException
    {
        int length = value.limit() - value.position();
        writeI32(length);
        
        if (byteBufferTransport != null) {
            // ZERO COPY path - write ByteBuffer directly to transport
            byteBufferTransport.write(value);
        } else {
            // Fallback to original implementation for non-enhanced transports
            super.writeBinary(value);
        }
    }

    @Override
    public void writeBinaryFromBufferList(List<ByteBufferPool.ReusableByteBuffer> byteBufferList) throws TException
    {
        int size = 0;
        for (ByteBufferPool.ReusableByteBuffer byteBuffer : byteBufferList) {
            size += byteBuffer.getBufferRemaining();
        }

        writeI32(size);
        
        if (byteBufferTransport != null) {
            // ZERO COPY path - write buffer list directly to transport
            byteBufferTransport.write(byteBufferList);
        } else {
            // Fallback to original implementation (with DirectBufferUtil fixes)
            super.writeBinaryFromBufferList(byteBufferList);
        }
    }

    @Override
    public ByteBuffer readBinary() throws TException
    {
        int size = checkSize(readI32());
        
        if (byteBufferTransport != null) {
            // ZERO COPY path - allocate target buffer and read directly into it
            ByteBuffer result = ByteBuffer.allocate(size);
            int bytesRead = byteBufferTransport.read(result);
            
            if (bytesRead != size) {
                throw new TTransportException("Failed to read expected bytes: expected " + size + ", got " + bytesRead);
            }
            
            result.flip();
            return result;
        } else {
            // Fallback to original implementation
            return super.readBinary();
        }
    }

    @Override
    public List<ByteBufferPool.ReusableByteBuffer> readBinaryToBufferList(ByteBufferPool pool) throws TException
    {
        int size = checkSize(readI32());
        
        if (byteBufferTransport != null) {
            // ZERO COPY path - read directly into buffer list!
            return byteBufferTransport.readToBufferList(pool, size);
        } else {
            // Fallback to original implementation (which now uses DirectBufferUtil)
            return super.readBinaryToBufferList(pool);
        }
    }

    /**
     * Check if this protocol is using zero-copy optimizations.
     * 
     * @return true if zero-copy paths are available, false otherwise
     */
    public boolean isZeroCopyEnabled()
    {
        return byteBufferTransport != null;
    }

    private static int checkSize(int length) throws TProtocolException
    {
        if (length < 0) {
            throw new TProtocolException("Negative length: " + length);
        }
        return length;
    }
}