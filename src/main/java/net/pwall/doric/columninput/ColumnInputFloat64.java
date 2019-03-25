/*
 * @(#) ColumnInputFloat64.java
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

package net.pwall.doric.columninput;

import java.io.File;
import java.io.IOException;

import net.pwall.doric.Column;
import net.pwall.doric.ColumnReader;
import net.pwall.doric.Doric;

class ColumnInputFloat64 implements ColumnInput {

    private ColumnReader columnReader;

    public ColumnInputFloat64(File file, Column.FileData fileData) throws IOException {
        Column.FileDetails fileDetails = fileData.getRowData();
        columnReader = new ColumnReader(Doric.getBufferPool(), new File(file, fileDetails.getName()),
                fileDetails.getSize());
    }

    @Override
    public boolean isNull(int rowNumber) {
        return false;
    }

    @Override
    public Number getNumber(int rowNumber) throws IOException {
        return columnReader.readInt64(rowNumber << 3);
    }

    @Override
    public long getLong(int rowNumber) {
        throw new IllegalStateException("Column can not return <long>");
    }

    @Override
    public String getString(int rowNumber) throws IOException {
        return String.valueOf(columnReader.readFloat64(rowNumber << 3));
    }

    @Override
    public void close() throws Exception {
        columnReader.close();
    }

}
