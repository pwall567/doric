/*
 * @(#) ColumnOutputBytes.java
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

import net.pwall.doric.Column;
import net.pwall.doric.ColumnWriter;
import net.pwall.util.Strings;

abstract public class ColumnOutputBytes implements ColumnOutput {

    private ColumnWriter columnWriter;
    private ColumnWriter dataWriter;
    private Map<String, Long> uniqueValues;
    private String filename;
    private String dataFilename;

    public ColumnOutputBytes(File file, int columnNumber, Map<String, Long> uniqueValues) throws IOException {
        StringBuilder sb = new StringBuilder(8);
        Strings.append3Digits(sb, columnNumber);
        sb.append(".rows");
        filename = sb.toString();
        columnWriter = new ColumnWriter(file, filename);
        sb.setLength(0);
        Strings.append3Digits(sb, columnNumber);
        sb.append(".data");
        dataFilename = sb.toString();
        dataWriter = new ColumnWriter(file, dataFilename);
        this.uniqueValues = uniqueValues;
    }

    public ColumnWriter getColumnWriter() {
        return columnWriter;
    }

    @Override
    public void putString(String value) throws IOException {
        // TODO - revise this to output unique values once at start of file
        long start;
        long length;
        if (uniqueValues != null && uniqueValues.containsKey(value)) {
            // note - the second test above should be superfluous - if the
            // uniqueValues map is present all values should be in it
            long uniqueValueCached = uniqueValues.get(value);
            if (uniqueValueCached == -1L) {
                start = dataWriter.getOffset();
                dataWriter.writeBytes(value);
                length = dataWriter.getOffset() - start;
                uniqueValues.put(value, (length << 32) + start);
            }
            else {
                start = uniqueValueCached;
                length = uniqueValueCached >> 32;
            }
        }
        else {
            start = dataWriter.getOffset();
            dataWriter.writeBytes(value);
            length = dataWriter.getOffset() - start;
        }
        putOffset(start);
        putLength((int)length);
    }

    abstract public void putOffset(long offset) throws IOException;

    abstract public void putLength(int length) throws IOException;

    @Override
    public Column.FileData summariseAndClose() throws IOException {
        Column.FileData fileData = new Column.FileData();
        fileData.setRowData(new Column.FileDetails(filename, columnWriter.getOffset()));
        fileData.setBytesData(new Column.FileDetails(dataFilename, dataWriter.getOffset()));
        columnWriter.close();
        dataWriter.close();
        return fileData;
    }

}
