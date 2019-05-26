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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Spliterator;
import java.util.function.Consumer;

import net.pwall.doric.Column;
import net.pwall.doric.Row;

public class CSVQuery implements Query, AutoCloseable {

    private BufferedReader brdr;
    private String line;
    private boolean started;
    private boolean atEnd;
    private char delimiter;
    private char quote;
    private List<Column> columns;

    /**
     * Construct a {@code CSVQuery} from an {@link Reader}.
     *
     * @param   rdr     the  {@link Reader}
     */
    public CSVQuery(Reader rdr) {
        brdr = rdr instanceof BufferedReader ? (BufferedReader)rdr : new BufferedReader(rdr);
        line = null;
        started = false;
        atEnd = false;
        delimiter = ',';
        quote = '"';
        columns = new ArrayList<>();
    }

    /**
     * Construct a {@code CSVQuery} from an {@link InputStream}.
     *
     * @param   is      the  {@link InputStream}
     */
    public CSVQuery(InputStream is) {
        this(new InputStreamReader(is));
    }

    /**
     * Construct a {@code CSVQuery} from an {@link InputStream}, specifying the character set by name.
     *
     * @param   is          the  {@link InputStream}
     * @param   charsetName the character set name
     * @throws UnsupportedEncodingException if the named character set is not supported
     */
    public CSVQuery(InputStream is, String charsetName) throws UnsupportedEncodingException {
        this(new InputStreamReader(is, charsetName));
    }

    public CSVQuery(InputStream is, Charset cs) {
        this(new InputStreamReader(is, cs));
    }

    public char getDelimiter() {
        return delimiter;
    }

    public void setDelimiter(char delimiter) {
        if (started)
            throw new RuntimeException("Attempt to modify after reading started");
        this.delimiter = delimiter;
    }

    public char getQuote() {
        return quote;
    }

    public void setQuote(char quote) {
        if (started)
            throw new RuntimeException("Attempt to modify after reading started");
        this.quote = quote;
    }

    @Override
    public Column getColumn(int columnNumber) {
        return columns.get(columnNumber);
    }

    @Override
    public Column getColumn(String columnName) {
        for (int i = 0, n = columns.size(); i < n; i++) {
            Column column = columns.get(i);
            if (column.getName().equals(columnName))
                return column;
        }
        throw new IllegalArgumentException("Can't locate column: " + columnName);
    }

    @Override
    public boolean isNumRowsKnown() {
        return false;
    }

    @Override
    public int getNumRows() {
        return 0;
    }

    @Override
    public int getNumColumns() {
        return columns.size();
    }

    @Override
    public Iterator<Row> iterator() {
        return new CSVIterator();
    }

    @Override
    public Spliterator<Row> spliterator() {
        return new CSVSpliterator();
    }

    @Override
    public void close() throws IOException {
        brdr.close();
    }

    public class CSVIterator implements Iterator<Row> {

        @Override
        public boolean hasNext() {
            started = true;
            if (atEnd)
                return false;
            if (line != null)
                return true;
            try {
                line = brdr.readLine();
            }
            catch (IOException e) {
                throw new RuntimeException("Error reading CSV", e);
            }
            if (line == null) {
                atEnd = true;
                return false;
            }
            return true;
        }

        @Override
        public Row next() {
            return null; // TODO split line into columns
        }

    }

    public class CSVSpliterator implements Spliterator<Row> {

        private CSVIterator iterator = new CSVIterator();

        @Override
        public boolean tryAdvance(Consumer<? super Row> action) {
            Objects.requireNonNull(action);
            if (iterator.hasNext()) {
                action.accept(iterator.next());
                return true;
            }
            return false;
        }

        @Override
        public Spliterator<Row> trySplit() {
            return null;
        }

        @Override
        public long estimateSize() {
            return 1000; // arbitrary - is this useful?
        }

        @Override
        public int characteristics() {
            return ORDERED | IMMUTABLE;
        }

    }

    public class Options {

        private char separator;
        private char quote;
        private boolean headerPresent;

    }

}
