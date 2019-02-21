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

import net.pwall.util.Strings;

public class ColumnReader {

    private BufferPool bufferPool;
    private RandomAccessFile raf;
    private long fileSize;

    public ColumnReader(BufferPool bufferPool, String filename, long fileSize) throws FileNotFoundException {
        this(bufferPool, new File(filename), fileSize);
    }

    public ColumnReader(BufferPool bufferPool, File file, long fileSize) throws FileNotFoundException {
        this.bufferPool = bufferPool;
        raf = new RandomAccessFile(file, "r");
        this.fileSize = fileSize;
    }

    public int readInt8(long offset) throws IOException {
        int bufferSize = bufferPool.getBufferSize();
        long bufferOffset = roundDown(offset, bufferSize);
        byte[] buffer = findBuffer(bufferOffset);
        return buffer[(int)(offset - bufferOffset)]; // confirm that this sign-extends b
    }

    public int readInt16(long offset) throws IOException {
        int bufferSize = bufferPool.getBufferSize();
        long bufferOffset = roundDown(offset, bufferSize);
        byte[] buffer = findBuffer(bufferOffset);
        int internalOffset = (int)(offset - bufferOffset);
        int result = buffer[internalOffset++];
        if (internalOffset == bufferSize) {
            buffer = findBuffer(offset + 1);
            internalOffset = 0;
        }
        return result << 8 | (buffer[internalOffset] & 0xFF);
    }

    public int readInt32(long offset) throws IOException {
        int bufferSize = bufferPool.getBufferSize();
        long bufferOffset = roundDown(offset, bufferSize);
        byte[] buffer = findBuffer(bufferOffset);
        int internalOffset = (int)(offset - bufferOffset);
        int result = buffer[internalOffset++];
        if (internalOffset == bufferSize) {
            buffer = findBuffer(offset + 1);
            internalOffset = 0;
        }
        result = result << 8 | (buffer[internalOffset++] & 0xFF);
        if (internalOffset == bufferSize) {
            buffer = findBuffer(offset + 1);
            internalOffset = 0;
        }
        result = result << 8 | (buffer[internalOffset++] & 0xFF);
        if (internalOffset == bufferSize) {
            buffer = findBuffer(offset + 1);
            internalOffset = 0;
        }
        return result << 8 | (buffer[internalOffset] & 0xFF);
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
        int bufferSize = bufferPool.getBufferSize();
        byte[] array = new byte[len];
        int arrayOffset = 0;
        while (arrayOffset < len) {
            long bufferOffset = roundDown(offset + arrayOffset, bufferSize);
            byte[] buffer = findBuffer(bufferOffset);
            int internalOffset = (int)(offset - bufferOffset);
            while (internalOffset < bufferSize && arrayOffset < len)
                array[arrayOffset++] = buffer[internalOffset++];
        }
        return Strings.fromUTF8(array);
    }

    private byte[] findBuffer(long offset) throws IOException {
        long bytesLeft = fileSize - offset;
        int readLength = (int)Math.min(bytesLeft, bufferPool.getBufferSize());
        return bufferPool.findBuffer(raf, offset, readLength);
    }

    private long roundDown(long offset, int bufferSize) {
        return (offset / bufferSize) * bufferSize;
    }

    public void close() throws IOException {
        raf.close();
    }

}
