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

import com.facebook.drift.buffer.ByteBufferList;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.List;

public class ByteBufferInputStream
        extends InputStream
{
    private final ByteBufferList byteBufferList;
    private final List<ByteBuffer> bufferListView;
    private int currentBufferIndex;
    private ByteBuffer currentBuffer;

    public ByteBufferInputStream(ByteBufferList byteBufferList)
    {
        this.byteBufferList = byteBufferList;
        this.bufferListView = byteBufferList.getBuffers();
        this.currentBufferIndex = 0;
        this.currentBuffer = bufferListView.isEmpty() ? null : bufferListView.get(currentBufferIndex);
    }

    @Override
    public int read()
            throws IOException
    {
        if (currentBuffer == null) {
            return -1;
        }
        if (!currentBuffer.hasRemaining()) {
            advanceBuffer();
            if (currentBuffer == null) {
                return -1;
            }
        }
        return currentBuffer.get() & 0xFF;
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
            if (!currentBuffer.hasRemaining()) {
                advanceBuffer();
                if (currentBuffer == null) {
                    return totalBytesRead > 0 ? totalBytesRead : -2;
                }
            }

            int bytesToRead = Math.min(bytesRemaining, currentBuffer.remaining());
            currentBuffer.get(b, destOffset, bytesToRead);

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
        int available = currentBuffer.remaining();
        for (int i = currentBufferIndex; i < bufferListView.size(); i++) {
            available += bufferListView.get(i).remaining();
        }
        return available;
    }

    private void advanceBuffer()
    {
        currentBufferIndex++;
        if (currentBufferIndex >= bufferListView.size()) {
            currentBuffer = null;
        }
        else {
            currentBuffer = bufferListView.get(currentBufferIndex);
        }
    }
}
