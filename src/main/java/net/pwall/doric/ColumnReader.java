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
    private byte[] byteArray;
    private ByteBuffer arrayByteBuffer;

    public ColumnReader(BufferPool bufferPool, String filename, long fileSize) throws FileNotFoundException {
        this(bufferPool, new File(filename), fileSize);
    }

    public ColumnReader(BufferPool bufferPool, File file, long fileSize) throws FileNotFoundException {
        this.bufferPool = bufferPool;
        raf = new RandomAccessFile(file, "r");
        channel = raf.getChannel();
        this.fileSize = fileSize;
        byteArray = new byte[8];
        arrayByteBuffer = ByteBuffer.wrap(byteArray);
    }

    public int readInt8(long offset) throws IOException {
        ByteBuffer buffer = findBuffer(offset);
        return buffer.get();
    }

    public int readInt16(long offset) throws IOException {
        ByteBuffer buffer = findBuffer(offset);
        if (buffer.remaining() >= 2)
            return buffer.getShort();
        return fillArray(buffer, 2, offset).getShort();
    }

    public int readInt32(long offset) throws IOException {
        ByteBuffer buffer = findBuffer(offset);
        if (buffer.remaining() >= 4)
            return buffer.getInt();
        return fillArray(buffer, 4, offset).getInt();
    }

    public long readInt64(long offset) throws IOException {
        ByteBuffer buffer = findBuffer(offset);
        if (buffer.remaining() > 8)
            return buffer.getLong();
        return fillArray(buffer, 8, offset).getLong();
    }

    public double readFloat32(long offset) throws IOException {
        ByteBuffer buffer = findBuffer(offset);
        if (buffer.remaining() > 4)
            return buffer.getFloat();
        return fillArray(buffer, 4, offset).getFloat();
    }

    public double readFloat64(long offset) throws IOException {
        ByteBuffer buffer = findBuffer(offset);
        if (buffer.remaining() > 8)
            return buffer.getDouble();
        return fillArray(buffer, 8, offset).getDouble();
    }

    public String readBytes(long offset, int len) throws IOException {
        byte[] array = new byte[len];
        int arrayOffset = 0;
        int bytesLeft = len;
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

    private ByteBuffer fillArray(ByteBuffer buffer, int len, long offset) throws IOException {
        int remaining = buffer.remaining();
        buffer.get(byteArray, 0, remaining);
        buffer = findBuffer(offset + remaining);
        buffer.get(byteArray, remaining, len - remaining);
        arrayByteBuffer.clear();
        return arrayByteBuffer;
    }

    private ByteBuffer findBuffer(long offset) throws IOException {
        return bufferPool.findBuffer(channel, fileSize, offset);
    }

    public void close() throws IOException {
        bufferPool.purge(channel);
        raf.close();
    }

}
