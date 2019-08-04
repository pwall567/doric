/*
 * @(#) ColumnWriter.java
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
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import net.pwall.util.Strings;

public class ColumnWriter {

    private FileChannel channel;
    private ByteBuffer buffer;
    private byte[] byteArray;
    private ByteBuffer arrayByteBuffer;
    private long offset;

    // TODO do we want to allow partial buffer write, so that buffer can be released back to pool?

    public ColumnWriter(File directory, String name) throws FileNotFoundException {
        FileOutputStream out = new FileOutputStream(new File(directory, name));
        channel = out.getChannel();
        buffer = ByteBuffer.allocate(8192); // TODO parameterise this? Or steal buffer from BufferPool?
        byteArray = new byte[8];
        arrayByteBuffer = ByteBuffer.wrap(byteArray);
        offset = 0;
    }

    public long getOffset() {
        return offset;
    }

    public void writeInt8(int i) throws IOException {
        buffer.put((byte)i);
        checkWriteBuffer();
        offset++;
    }

    public void writeInt16(int i) throws IOException {
        if (buffer.remaining() >= 2) {
            buffer.putShort((short)i);
            checkWriteBuffer();
        }
        else {
            arrayByteBuffer.clear();
            arrayByteBuffer.putShort((short)i);
            flushBytes(2);
        }
        offset += 2;
    }

    public void writeInt32(int i) throws IOException {
        if (buffer.remaining() >= 4) {
            buffer.putInt(i);
            checkWriteBuffer();
        }
        else {
            arrayByteBuffer.clear();
            arrayByteBuffer.putInt(i);
            flushBytes(4);
        }
        offset += 4;
    }

    public void writeInt64(long i) throws IOException {
        if (buffer.remaining() >= 8) {
            buffer.putLong(i);
            checkWriteBuffer();
        }
        else {
            arrayByteBuffer.clear();
            arrayByteBuffer.putLong(i);
            flushBytes(8);
        }
        offset += 8;
    }

    public void writeFloat64(double d) throws IOException {
        if (buffer.remaining() >= 8) {
            buffer.putDouble(d);
            checkWriteBuffer();
        }
        else {
            arrayByteBuffer.clear();
            arrayByteBuffer.putDouble(d);
            flushBytes(8);
        }
        offset += 8;
    }

    public void writeFloat32(float f) throws IOException {
        if (buffer.remaining() >= 4) {
            buffer.putFloat(f);
            checkWriteBuffer();
        }
        else {
            arrayByteBuffer.clear();
            arrayByteBuffer.putFloat(f);
            flushBytes(4);
        }
        offset += 4;
    }

    public void writeBytes(String str) throws IOException {
        byte[] bytes = Strings.toUTF8(str);
        int len = bytes.length;
        int arrayOffset = 0;
        while (len > 0) {
            int remaining = buffer.remaining();
            if (len <= remaining) {
                buffer.put(bytes, arrayOffset, len);
                checkWriteBuffer();
                break;
            }
            buffer.put(bytes, arrayOffset, remaining);
            writeBuffer();
            arrayOffset += remaining;
            len -= remaining;
        }
        offset += bytes.length;
    }

    private void flushBytes(int len) throws IOException {
        int remaining = buffer.remaining();
        buffer.put(byteArray, 0, remaining);
        writeBuffer();
        buffer.put(byteArray, remaining, len - remaining);
    }

    private void checkWriteBuffer() throws IOException {
        if (!buffer.hasRemaining())
            writeBuffer();
    }

    private void writeBuffer() throws IOException {
        buffer.flip();
        while (buffer.hasRemaining())
            channel.write(buffer);
        buffer.clear();
    }

    public void close() throws IOException {
        if (buffer.position() > 0)
            writeBuffer();
        channel.close();
    }

}
