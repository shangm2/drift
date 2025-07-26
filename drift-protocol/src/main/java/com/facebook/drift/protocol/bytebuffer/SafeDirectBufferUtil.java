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

/**
 * Safe fallback utility for direct ByteBuffer operations that works in all environments.
 * Uses standard ByteBuffer methods instead of Unsafe, but still provides zero-copy for heap buffers.
 */
public final class SafeDirectBufferUtil
{
    private SafeDirectBufferUtil() {}

    /**
     * Copy bytes from a byte array to a ByteBuffer.
     * For heap buffers, uses direct array access for zero-copy performance.
     * For direct buffers, falls back to ByteBuffer.put() which is still efficient.
     */
    public static void putBytes(ByteBuffer buffer, byte[] src, int srcOffset, int length)
    {
        if (!buffer.isDirect()) {
            // Heap buffer - use direct array access for maximum performance
            System.arraycopy(src, srcOffset, buffer.array(), 
                           buffer.arrayOffset() + buffer.position(), length);
            buffer.position(buffer.position() + length);
        }
        else {
            // Direct buffer - use standard ByteBuffer API
            // This is less optimal but still avoids extra temporary arrays
            buffer.put(src, srcOffset, length);
        }
    }

    /**
     * Copy bytes from a ByteBuffer to a byte array.
     * For heap buffers, uses direct array access for zero-copy performance.
     * For direct buffers, falls back to ByteBuffer.get() which is still efficient.
     */
    public static void getBytes(ByteBuffer buffer, byte[] dst, int dstOffset, int length)
    {
        if (!buffer.isDirect()) {
            // Heap buffer - use direct array access for maximum performance
            System.arraycopy(buffer.array(), buffer.arrayOffset() + buffer.position(),
                           dst, dstOffset, length);
            buffer.position(buffer.position() + length);
        }
        else {
            // Direct buffer - use standard ByteBuffer API
            buffer.get(dst, dstOffset, length);
        }
    }

    /**
     * Put a single byte into the buffer at current position
     */
    public static void putByte(ByteBuffer buffer, byte value)
    {
        if (!buffer.isDirect()) {
            buffer.array()[buffer.arrayOffset() + buffer.position()] = value;
            buffer.position(buffer.position() + 1);
        }
        else {
            buffer.put(value);
        }
    }

    /**
     * Get a single byte from the buffer at current position
     */
    public static byte getByte(ByteBuffer buffer)
    {
        if (!buffer.isDirect()) {
            byte value = buffer.array()[buffer.arrayOffset() + buffer.position()];
            buffer.position(buffer.position() + 1);
            return value;
        }
        else {
            return buffer.get();
        }
    }
}