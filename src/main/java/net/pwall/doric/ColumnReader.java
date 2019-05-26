/*
 * @(#) ColumnReader.java
 *
 * doric Column-oriented database system
 * Copyright (c) 2019 Peter Wall
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package net.pwall.doric;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import net.pwall.util.Strings;

public class ColumnReader {

    private BufferPool bufferPool;
    private RandomAccessFile raf;
    private FileChannel channel;
    private long fileSize;

    public ColumnReader(BufferPool bufferPool, String filename, long fileSize) throws FileNotFoundException {
        this(bufferPool, new File(filename), fileSize);
    }

    public ColumnReader(BufferPool bufferPool, File file, long fileSize) throws FileNotFoundException {
        this.bufferPool = bufferPool;
        raf = new RandomAccessFile(file, "r");
        channel = raf.getChannel();
        this.fileSize = fileSize;
    }

    public int readInt8(long offset) throws IOException {
        ByteBuffer buffer = findBuffer(offset);
        return buffer.get();
    }

    public int readInt16(long offset) throws IOException {
        ByteBuffer buffer = findBuffer(offset);
        if (buffer.remaining() >= 2)
            return buffer.getShort();
        int result = buffer.get();
        buffer = findBuffer(offset + 1);
        return result << 8 | (buffer.get() & 0xFF);
    }

    public int readInt32(long offset) throws IOException {
        ByteBuffer buffer = findBuffer(offset);
        if (buffer.remaining() >= 4)
            return buffer.getInt();
        int result = buffer.get();
        if (!buffer.hasRemaining())
            buffer = findBuffer(offset + 1);
        result = result << 8 | (buffer.get() & 0xFF);
        if (!buffer.hasRemaining())
            buffer = findBuffer(offset + 2);
        result = result << 8 | (buffer.get() & 0xFF);
        if (!buffer.hasRemaining())
            buffer = findBuffer(offset + 3);
        return result << 8 | (buffer.get() & 0xFF);
    }

    public long readInt64(long offset) throws IOException {
        return (long)readInt32(offset) << 32 | ((long)readInt32(offset + 4) & 0xFFFFFFFFL);
    }

    public double readFloat32(long offset) throws IOException {
        return Float.intBitsToFloat(readInt32(offset));
    }

    public double readFloat64(long offset) throws IOException {
        return Double.longBitsToDouble(readInt64(offset));
    }

    public String readBytes(long offset, int len) throws IOException {
        byte[] array = new byte[len];
        int arrayOffset = 0;
        int bytesLeft = len - arrayOffset;
        while (bytesLeft > 0) {
            ByteBuffer buffer = findBuffer(offset + arrayOffset);
            int remaining = buffer.remaining();
            if (bytesLeft <= remaining) {
                buffer.get(array, arrayOffset, bytesLeft);
                break;
            }
            buffer.get(array, arrayOffset, remaining);
            arrayOffset += remaining;
            bytesLeft -= remaining;
        }
        return Strings.fromUTF8(array);
    }

    private ByteBuffer findBuffer(long offset) throws IOException {
        return bufferPool.findBuffer(channel, fileSize, offset);
    }

    public void close() throws IOException {
        bufferPool.purge(channel);
        raf.close();
    }

}
