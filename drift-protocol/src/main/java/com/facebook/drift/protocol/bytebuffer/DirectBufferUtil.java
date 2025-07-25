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

import sun.misc.Unsafe;

import java.lang.reflect.Field;
import java.nio.ByteBuffer;

/**
 * Utility class for high-performance operations on direct ByteBuffers using Unsafe.
 * This avoids temporary array allocations when working with direct memory.
 */
public final class DirectBufferUtil
{
    private static final Unsafe UNSAFE;
    private static final long ARRAY_BYTE_BASE_OFFSET;

    static {
        try {
            Field unsafeField = Unsafe.class.getDeclaredField("theUnsafe");
            unsafeField.setAccessible(true);
            UNSAFE = (Unsafe) unsafeField.get(null);
            ARRAY_BYTE_BASE_OFFSET = UNSAFE.arrayBaseOffset(byte[].class);
        }
        catch (Exception e) {
            throw new RuntimeException("Failed to initialize DirectBufferUtil", e);
        }
    }

    private DirectBufferUtil() {}

    /**
     * Get the native memory address of a direct ByteBuffer
     */
    public static long getDirectBufferAddress(ByteBuffer buffer)
    {
        if (!buffer.isDirect()) {
            throw new IllegalArgumentException("Buffer must be direct");
        }
        return ((sun.nio.ch.DirectBuffer) buffer).address();
    }

    /**
     * Copy bytes from a byte array to a direct ByteBuffer without creating temporary arrays.
     * For heap buffers, falls back to System.arraycopy for optimal performance.
     */
    public static void putBytes(ByteBuffer buffer, byte[] src, int srcOffset, int length)
    {
        if (buffer.isDirect()) {
            long address = getDirectBufferAddress(buffer) + buffer.position();
            UNSAFE.copyMemory(src, ARRAY_BYTE_BASE_OFFSET + srcOffset, null, address, length);
            buffer.position(buffer.position() + length);
        }
        else {
            // Heap buffer - use direct array access for maximum performance
            System.arraycopy(src, srcOffset, buffer.array(), 
                           buffer.arrayOffset() + buffer.position(), length);
            buffer.position(buffer.position() + length);
        }
    }

    /**
     * Copy bytes from a direct ByteBuffer to a byte array without creating temporary arrays.
     * For heap buffers, falls back to System.arraycopy for optimal performance.
     */
    public static void getBytes(ByteBuffer buffer, byte[] dst, int dstOffset, int length)
    {
        if (buffer.isDirect()) {
            long address = getDirectBufferAddress(buffer) + buffer.position();
            UNSAFE.copyMemory(null, address, dst, ARRAY_BYTE_BASE_OFFSET + dstOffset, length);
            buffer.position(buffer.position() + length);
        }
        else {
            // Heap buffer - use direct array access for maximum performance
            System.arraycopy(buffer.array(), buffer.arrayOffset() + buffer.position(),
                           dst, dstOffset, length);
            buffer.position(buffer.position() + length);
        }
    }

    /**
     * Put a single byte into the buffer at current position
     */
    public static void putByte(ByteBuffer buffer, byte value)
    {
        if (buffer.isDirect()) {
            long address = getDirectBufferAddress(buffer) + buffer.position();
            UNSAFE.putByte(address, value);
            buffer.position(buffer.position() + 1);
        }
        else {
            buffer.array()[buffer.arrayOffset() + buffer.position()] = value;
            buffer.position(buffer.position() + 1);
        }
    }

    /**
     * Get a single byte from the buffer at current position
     */
    public static byte getByte(ByteBuffer buffer)
    {
        if (buffer.isDirect()) {
            long address = getDirectBufferAddress(buffer) + buffer.position();
            byte value = UNSAFE.getByte(address);
            buffer.position(buffer.position() + 1);
            return value;
        }
        else {
            byte value = buffer.array()[buffer.arrayOffset() + buffer.position()];
            buffer.position(buffer.position() + 1);
            return value;
        }
    }
}