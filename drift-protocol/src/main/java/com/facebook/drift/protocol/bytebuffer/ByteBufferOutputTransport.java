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

import com.facebook.drift.buffer.BufferPool;
import com.facebook.drift.buffer.OwnedBufferList;
import com.facebook.drift.protocol.TTransport;
import com.facebook.drift.protocol.TTransportException;

import java.io.IOException;

public class ByteBufferOutputTransport
        implements TTransport
{
    private final ByteBufferOutputStream outputStream;

    public ByteBufferOutputTransport(BufferPool pool, OwnedBufferList ownedBufferList)
    {
        outputStream = new ByteBufferOutputStream(pool, ownedBufferList);
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
}
