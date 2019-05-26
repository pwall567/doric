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
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
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

    public ByteBuffer findBuffer(FileChannel channel, long fileSize, long offset) throws IOException {
        long bufferOffset = roundDown(offset, bufferSize);
        long bytesRemaining = fileSize - bufferOffset;
        for (int i = 0, n = pool.size(); i < n; i++) {
            Entry entry = pool.get(i);
            if (entry.getChannel() == channel && entry.getOffset() == bufferOffset) {
                if (pool.size() > 1 && i > 0) {
                    pool.remove(i);
                    pool.add(0, entry);
                }
                ByteBuffer buffer = entry.getBuffer();
                buffer.position((int)(offset - bufferOffset));
                return buffer;
            }
        }
        Entry entry = pool.size() == maxEntries ? pool.remove(maxEntries - 1) :
                new Entry(bufferSize);
        pool.add(0, entry);
        entry.setChannel(channel);
        entry.setOffset(bufferOffset);
        channel.position(bufferOffset);
        ByteBuffer buffer = entry.getBuffer();
        buffer.clear();
        if (bytesRemaining < bufferSize)
            buffer.limit((int)(bytesRemaining));
        do {
            channel.read(buffer);
        } while (buffer.hasRemaining());
        buffer.position((int)(offset - bufferOffset));
        return buffer;
    }

    public void purge(FileChannel channel) {
        int i = 0;
        while (i < pool.size()) {
            if (pool.get(i).getChannel() == channel)
                pool.remove(i);
            else
                i++;
        }
    }

    /**
     * Round down the offset to a multiple of the buffer size.  This technique relies on
     * restricting buffer sizes to powers of 2.
     *
     * @param   offset      the offset
     * @param   bufferSize  the buffer size
     * @return  the offset rounded down.
     */
    private static long roundDown(long offset, int bufferSize) {
//        return (offset / bufferSize) * bufferSize;
        return offset & -bufferSize;
    }

    private static class Entry {

        private FileChannel channel;
        private long offset;
        private ByteBuffer buffer;

        public Entry(int size) {
            channel = null;
            offset = 0;
            buffer = ByteBuffer.allocate(size); // TODO - optionally use allocateDirect()
        }

        public FileChannel getChannel() {
            return channel;
        }

        public void setChannel(FileChannel channel) {
            this.channel = channel;
        }

        public long getOffset() {
            return offset;
        }

        public void setOffset(long offset) {
            this.offset = offset;
        }

        private ByteBuffer getBuffer() {
            return buffer;
        }

    }

}
