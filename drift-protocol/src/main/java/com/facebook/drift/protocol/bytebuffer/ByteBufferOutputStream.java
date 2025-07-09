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

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.List;

import static java.lang.String.format;

public class ByteBufferOutputStream
        extends OutputStream
{
    private final BufferPool pool;
    private final List<ByteBuffer> buffers;
    private ByteBuffer currentBuffer;

    public ByteBufferOutputStream(BufferPool pool, List<ByteBuffer> buffers)
    {
        this.pool = pool;
        this.buffers = buffers;
        this.currentBuffer = pool.acquire();
        buffers.add(currentBuffer);
    }

    @Override
    public void write(int b)
            throws IOException
    {
        if (!currentBuffer.hasRemaining()) {
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
            if (!currentBuffer.hasRemaining()) {
                addNewBuffer();
            }

            int bytesToWrite = Math.min(remaining, currentBuffer.remaining());

            System.out.println(format("===> ready to write %d bytes with remaining %d and buffer remaining %d", bytesToWrite, remaining, currentBuffer.remaining()));
            currentBuffer.put(b, offset, bytesToWrite);

            offset += bytesToWrite;
            remaining -= bytesToWrite;
        }
    }

    private void addNewBuffer()
    {
        finishLastBuffer();
        currentBuffer = pool.acquire();
        buffers.add(currentBuffer);
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
}
