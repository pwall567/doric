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

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import net.pwall.util.Strings;

public class ColumnWriter {

    private OutputStream out;
    private long offset;

    public ColumnWriter(File directory, String name) throws FileNotFoundException {
        out = new BufferedOutputStream(new FileOutputStream(new File(directory, name)));
        offset = 0;
    }

    public long getOffset() {
        return offset;
    }

    public void writeInt8(int i) throws IOException {
        out.write(i);
        offset++;
    }

    public void writeInt16(int i) throws IOException {
        out.write(i >> 8);
        out.write(i);
        offset += 2;
    }

    public void writeInt32(int i) throws IOException {
        out.write(i >> 24);
        out.write(i >> 16);
        out.write(i >> 8);
        out.write(i);
        offset += 4;
    }

    public void writeInt64(long i) throws IOException {
        writeInt32((int)(i >> 32));
        writeInt32((int)i);
        offset += 8;
    }

    public void writeBytes(String str) throws IOException {
        out.write(Strings.toUTF8(str));
    }

    public void close() throws IOException {
        out.close();
    }

}
