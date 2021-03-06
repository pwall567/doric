/*
 * @(#) Row.java
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

import net.pwall.doric.query.Query;

public class Row {

    private Query query;
    private int rowNumber;

    public Row(Query query, int rowNumber) {
        this.query = query;
        this.rowNumber = rowNumber;
    }

    public long getLong(String columnName) throws IOException {
        return getLong(query.getColumn(columnName));
    }

    public long getLong(int columnNumber) throws IOException {
        return getLong(query.getColumn(columnNumber));
    }

    public long getLong(Column column) throws IOException {
        return column.getColumnInput().getLong(rowNumber);
    }

    public String getString(String columnName) throws IOException {
        return getString(query.getColumn(columnName));
    }

    public String getString(int columnNumber) throws IOException {
        return getString(query.getColumn(columnNumber));
    }

    public String getString(Column column) throws IOException {
        return column.getColumnInput().getString(rowNumber);
    }

}
