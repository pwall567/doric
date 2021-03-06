/*
 * @(#) Query.java
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

package net.pwall.doric.query;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Spliterator;

import net.pwall.doric.Column;
import net.pwall.doric.Row;

public interface Query extends Iterable<Row> {

    boolean isNumRowsKnown();

    int getNumRows();

    int getNumColumns();

    Column getColumn(int i);

    default Column getColumn(String columnName) {
        for (int i = 0, n = getNumColumns(); i < n; i++) {
            Column column = getColumn(i);
            if (column.getName().equals(columnName))
                return column;
        }
        throw new IllegalArgumentException("Can't locate column: " + columnName);
    }

    default Row getRow(int rowNumber) {
        return new Row(this, rowNumber);
    }

    default Iterable<Column> getColumns() {
        return () -> new Iterator<Column>() {
            private int index = 0;
            @Override
            public boolean hasNext() {
                return index < getNumColumns();
            }
            @Override
            public Column next() {
                if (!hasNext())
                    throw new NoSuchElementException();
                return getColumn(index++);
            }
        };
    }

    @Override
    default Iterator<Row> iterator() {
        return new QueryIterator(this);
    }

    @Override
    default Spliterator<Row> spliterator() {
        return new QuerySpliterator(this, 0, getNumRows());
    }

}
