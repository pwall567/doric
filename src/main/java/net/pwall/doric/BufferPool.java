/*
 * @(#) BufferPool.java
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

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.List;

public class BufferPool {

    private int maxEntries;
    private int bufferSize;
    private List<Entry> pool;

    public BufferPool(int maxEntries, int bufferSize) {
        if (((bufferSize - 1) & bufferSize) != 0)
            throw new IllegalArgumentException("Buffer size must be power of 2");
        this.maxEntries = maxEntries;
        this.bufferSize = bufferSize;
        pool = new ArrayList<>(maxEntries);
    }

    public int getBufferSize() {
        return bufferSize;
    }

    public byte[] findBuffer(RandomAccessFile file, long offset, int length)
            throws IOException {
        for (int i = 0, n = pool.size(); i < n; i++) {
            Entry entry = pool.get(i);
            if (entry.getFile() == file && entry.getOffset() == offset) {
                if (pool.size() > 1) {
                    pool.remove(i);
                    pool.add(0, entry);
                }
                return entry.getBuffer();
            }
        }
        Entry entry = pool.size() == maxEntries ? pool.remove(maxEntries - 1) :
                new Entry(bufferSize);
        entry.setFile(file);
        entry.setOffset(offset);
        file.seek(offset);
        byte[] buffer = entry.getBuffer();
        file.readFully(buffer, 0, length);
        return buffer;
    }

    private static class Entry {

        private RandomAccessFile file;
        private long offset;
        private byte[] buffer;

        public Entry(int size) {
            file = null;
            offset = 0;
            buffer = new byte[size];
        }

        public RandomAccessFile getFile() {
            return file;
        }

        public void setFile(RandomAccessFile file) {
            this.file = file;
        }

        public long getOffset() {
            return offset;
        }

        public void setOffset(long offset) {
            this.offset = offset;
        }

        private byte[] getBuffer() {
            return buffer;
        }

    }

}
