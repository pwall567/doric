/*
 * @(#) ColumnOutputBytes8L8.java
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

package net.pwall.doric.columnoutput;

import java.io.File;
import java.io.IOException;
import java.util.Map;

public class ColumnOutputBytes8L8 extends ColumnOutputBytes {

    public ColumnOutputBytes8L8(File file, int columnNumber, Map<String, Long> uniqueValues) throws IOException {
        super(file, columnNumber, uniqueValues);
    }

    @Override
    public void putOffset(long offset) throws IOException {
        getColumnWriter().writeInt8((int)offset);
    }

    @Override
    public void putLength(int length) throws IOException {
        getColumnWriter().writeInt8(length);
    }

}
