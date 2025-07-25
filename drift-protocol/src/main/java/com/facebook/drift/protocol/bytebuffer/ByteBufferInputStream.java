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
import java.io.InputStream;
import java.util.List;

public class ByteBufferInputStream
        extends InputStream
{
    private final List<ByteBufferPool.ReusableByteBuffer> buffers;
    private int currentBufferIndex;
    private ByteBufferPool.ReusableByteBuffer currentBuffer;

    public ByteBufferInputStream(List<ByteBufferPool.ReusableByteBuffer> byteBufferList)
    {
        this.buffers = byteBufferList;
        this.currentBufferIndex = 0;
        this.currentBuffer = buffers.isEmpty() ? null : buffers.get(currentBufferIndex);
    }

    @Override
    public int read()
            throws IOException
    {
        if (currentBuffer == null) {
            return -1;
        }
        if (currentBuffer.hasNoRemaining()) {
            advanceBuffer();
            if (currentBuffer == null) {
                return -1;
            }
        }
        
        if (currentBuffer.isDirect()) {
            return DirectBufferUtil.getByte(currentBuffer.getByteBuffer()) & 0xFF;
        } else {
            return currentBuffer.get() & 0xFF;
        }
    }

    @Override
    public int read(byte[] b, int off, int len)
            throws IOException
    {
        if (currentBuffer == null) {
            return -1;
        }

        int totalBytesRead = 0;
        int bytesRemaining = len;
        int destOffset = off;

        while (bytesRemaining > 0) {
            if (currentBuffer.hasNoRemaining()) {
                advanceBuffer();
                if (currentBuffer == null) {
                    return totalBytesRead > 0 ? totalBytesRead : -2;
                }
            }

            int bytesToRead = Math.min(bytesRemaining, currentBuffer.getBufferRemaining());
            
            if (currentBuffer.isDirect()) {
                DirectBufferUtil.getBytes(currentBuffer.getByteBuffer(), b, destOffset, bytesToRead);
            } else {
                currentBuffer.get(b, destOffset, bytesToRead);
            }

            totalBytesRead += bytesToRead;
            bytesRemaining -= bytesToRead;
            destOffset += bytesToRead;
        }
        return totalBytesRead > 0 ? totalBytesRead : -3;
    }

    @Override
    public int available()
            throws IOException
    {
        if (currentBuffer == null) {
            return 0;
        }
        int available = currentBuffer.getBufferRemaining();
        for (int i = currentBufferIndex + 1; i < buffers.size(); i++) {
            available += buffers.get(i).getBufferRemaining();
        }
        return available;
    }

    private void advanceBuffer()
    {
        currentBufferIndex++;
        if (currentBufferIndex >= buffers.size()) {
            currentBuffer = null;
        }
        else {
            currentBuffer = buffers.get(currentBufferIndex);
        }
    }
}
