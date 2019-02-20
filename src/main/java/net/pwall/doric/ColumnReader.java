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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;

public class ColumnReader {

    private BufferPool bufferPool;
    private RandomAccessFile file;
    private long fileSize;

    public ColumnReader(BufferPool bufferPool, String filename, long fileSize) throws FileNotFoundException {
        this.bufferPool = bufferPool;
        file = new RandomAccessFile(filename, "r");
        this.fileSize = fileSize;
    }

    public int readInt8(long offset) throws IOException {
        int bufferSize = bufferPool.getBufferSize();
        long bufferOffset = (offset % bufferSize) * bufferSize;
        int readLength = (int)(fileSize < bufferOffset + bufferSize ? fileSize - bufferOffset : bufferSize);
        byte[] buffer = bufferPool.findBuffer(file, bufferOffset, readLength);
        byte b = buffer[(int)(offset - bufferOffset)];
        return b; // check that this sign-extends b
    }

}
