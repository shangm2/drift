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

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;

/**
 * Utility class for high-performance operations on direct ByteBuffers using Unsafe.
 * This avoids temporary array allocations when working with direct memory.
 * Uses reflection to work around Java 9+ module system restrictions.
 */
public final class DirectBufferUtil
{
    private static final Object UNSAFE;
    private static final Method COPY_MEMORY_METHOD;
    private static final Method PUT_BYTE_METHOD; 
    private static final Method GET_BYTE_METHOD;
    private static final Method ARRAY_BASE_OFFSET_METHOD;
    private static final long ARRAY_BYTE_BASE_OFFSET;
    private static final Method GET_ADDRESS_METHOD;

    static {
        try {
            // Get Unsafe instance via reflection
            Class<?> unsafeClass = Class.forName("sun.misc.Unsafe");
            Field unsafeField = unsafeClass.getDeclaredField("theUnsafe");
            unsafeField.setAccessible(true);
            UNSAFE = unsafeField.get(null);

            // Get Unsafe methods via reflection
            COPY_MEMORY_METHOD = unsafeClass.getMethod("copyMemory", 
                Object.class, long.class, Object.class, long.class, long.class);
            PUT_BYTE_METHOD = unsafeClass.getMethod("putByte", long.class, byte.class);
            GET_BYTE_METHOD = unsafeClass.getMethod("getByte", long.class);
            ARRAY_BASE_OFFSET_METHOD = unsafeClass.getMethod("arrayBaseOffset", Class.class);
            
            ARRAY_BYTE_BASE_OFFSET = (Long) ARRAY_BASE_OFFSET_METHOD.invoke(UNSAFE, byte[].class);

            // Get DirectBuffer.address() method via reflection
            Class<?> directBufferClass = Class.forName("sun.nio.ch.DirectBuffer");
            GET_ADDRESS_METHOD = directBufferClass.getMethod("address");
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
        try {
            return (Long) GET_ADDRESS_METHOD.invoke(buffer);
        }
        catch (Exception e) {
            throw new RuntimeException("Failed to get direct buffer address", e);
        }
    }

    /**
     * Copy bytes from a byte array to a direct ByteBuffer without creating temporary arrays.
     * For heap buffers, falls back to System.arraycopy for optimal performance.
     */
    public static void putBytes(ByteBuffer buffer, byte[] src, int srcOffset, int length)
    {
        if (buffer.isDirect()) {
            try {
                long address = getDirectBufferAddress(buffer) + buffer.position();
                COPY_MEMORY_METHOD.invoke(UNSAFE, src, ARRAY_BYTE_BASE_OFFSET + srcOffset, null, address, (long) length);
                buffer.position(buffer.position() + length);
            }
            catch (Exception e) {
                throw new RuntimeException("Failed to copy bytes to direct buffer", e);
            }
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
            try {
                long address = getDirectBufferAddress(buffer) + buffer.position();
                COPY_MEMORY_METHOD.invoke(UNSAFE, null, address, dst, ARRAY_BYTE_BASE_OFFSET + dstOffset, (long) length);
                buffer.position(buffer.position() + length);
            }
            catch (Exception e) {
                throw new RuntimeException("Failed to copy bytes from direct buffer", e);
            }
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
            try {
                long address = getDirectBufferAddress(buffer) + buffer.position();
                PUT_BYTE_METHOD.invoke(UNSAFE, address, value);
                buffer.position(buffer.position() + 1);
            }
            catch (Exception e) {
                throw new RuntimeException("Failed to put byte to direct buffer", e);
            }
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
            try {
                long address = getDirectBufferAddress(buffer) + buffer.position();
                byte value = (Byte) GET_BYTE_METHOD.invoke(UNSAFE, address);
                buffer.position(buffer.position() + 1);
                return value;
            }
            catch (Exception e) {
                throw new RuntimeException("Failed to get byte from direct buffer", e);
            }
        }
        else {
            byte value = buffer.array()[buffer.arrayOffset() + buffer.position()];
            buffer.position(buffer.position() + 1);
            return value;
        }
    }

    /**
     * Copy bytes from one ByteBuffer to another ByteBuffer without creating temporary arrays.
     * Optimally handles all combinations: direct-to-direct, heap-to-heap, and mixed.
     * 
     * @param source the source ByteBuffer to copy from
     * @param destination the destination ByteBuffer to copy to
     * @param length the number of bytes to copy
     */
    public static void copyBufferToBuffer(ByteBuffer source, ByteBuffer destination, int length)
    {
        if (source.isDirect() && destination.isDirect()) {
            // Both direct - use Unsafe memory copy for maximum performance
            try {
                long srcAddress = getDirectBufferAddress(source) + source.position();
                long dstAddress = getDirectBufferAddress(destination) + destination.position();
                COPY_MEMORY_METHOD.invoke(UNSAFE, null, srcAddress, null, dstAddress, (long) length);
            }
            catch (Exception e) {
                throw new RuntimeException("Failed to copy between direct buffers", e);
            }
        }
        else if (!source.isDirect() && !destination.isDirect()) {
            // Both heap - use System.arraycopy for maximum performance
            System.arraycopy(source.array(), source.arrayOffset() + source.position(),
                           destination.array(), destination.arrayOffset() + destination.position(),
                           length);
        }
        else {
            // Mixed (one direct, one heap) - use existing methods for conversion
            byte[] temp = new byte[length];
            getBytes(source, temp, 0, length);
            putBytes(destination, temp, 0, length);
            return; // positions already updated by getBytes/putBytes
        }
        
        // Update positions for direct paths (mixed path already handled above)
        source.position(source.position() + length);
        destination.position(destination.position() + length);
    }
}