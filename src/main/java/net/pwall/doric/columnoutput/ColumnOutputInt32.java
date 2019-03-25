/*
 * @(#) ColumnOutputInt32.java
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

import net.pwall.doric.Column;
import net.pwall.doric.ColumnWriter;
import net.pwall.util.Strings;

public class ColumnOutputInt32 implements ColumnOutput {

    private ColumnWriter columnWriter;
    private String filename;

    public ColumnOutputInt32(File file, int columnNumber) throws IOException {
        StringBuilder sb = new StringBuilder(3);
        Strings.append3Digits(sb, columnNumber);
        sb.append(".rows");
        filename = sb.toString();
        columnWriter = new ColumnWriter(file, filename);
    }

    @Override
    public void putLong(long value) throws IOException {
        columnWriter.writeInt32((int)value);
    }

    @Override
    public void putNumber(Number value) throws IOException {
        putLong(value.longValue());
    }

    @Override
    public void putString(String value) throws IOException {
        try {
            putLong(Long.valueOf(value));
        }
        catch (NumberFormatException nfe) {
            throw new IllegalArgumentException("Can't convert value to decimal: " + value);
        }
    }

    @Override
    public Column.FileData summariseAndClose() throws IOException {
        Column.FileData fileData = new Column.FileData();
        fileData.setRowData(new Column.FileDetails(filename, columnWriter.getOffset()));
        columnWriter.close();
        return fileData;
    }

}
