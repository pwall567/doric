/*
 * @(#) CSV.java
 *
 * javautil Java Utility Library
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

package net.pwall.util;

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
import java.util.NoSuchElementException;

public class CSV implements AutoCloseable, Iterator<CSV.Record>, Iterable<CSV.Record> {

    private BufferedReader brdr;
    private String line;
    private boolean started;
    private boolean atEnd;
    private char delimiter;
    private char quote;

    /**
     * Construct a {@code CSV} from an {@link Reader}.
     *
     * @param   rdr     the  {@link Reader}
     */
    public CSV(Reader rdr) {
        brdr = rdr instanceof BufferedReader ? (BufferedReader)rdr : new BufferedReader(rdr);
        line = null;
        started = false;
        atEnd = false;
        delimiter = ',';
        quote = '"';
    }

    /**
     * Construct a {@code CSV} from an {@link InputStream}.
     *
     * @param   is      the  {@link InputStream}
     */
    public CSV(InputStream is) {
        this(new InputStreamReader(is));
    }

    /**
     * Construct a {@code CSV} from an {@link InputStream}, specifying the character set by name.
     *
     * @param   is          the  {@link InputStream}
     * @param   charsetName the character set name
     * @throws  UnsupportedEncodingException if the named character set is not supported
     */
    public CSV(InputStream is, String charsetName) throws UnsupportedEncodingException {
        this(new InputStreamReader(is, charsetName));
    }

    public CSV(InputStream is, Charset cs) {
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
    public Iterator<Record> iterator() {
        return this;
    }

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
    public Record next() {
        if (!hasNext())
            throw new NoSuchElementException();
        Record record = new Record();
        // process line into Record
        ParseText pt = new ParseText(line);
        for (;;) {
            if (pt.match(quote)) {
                // quoted string
                StringBuilder sb = new StringBuilder();
                for (;;) {
                    pt.skipTo(quote);
                    pt.appendResultTo(sb);
                    if (pt.isExhausted())
                        throw new RuntimeException("Quotes not closed");
                    pt.skip(1); // step over quote
                    if (pt.isExhausted())
                        break;
                    if (pt.match(delimiter)) {
                        pt.back(1);
                        break;
                    }
                    if (!pt.match(quote))
                        throw new RuntimeException("Invalid quotes");
                    sb.append(quote);
                }
                record.addField(sb.toString());
            }
            else {
                pt.skipTo(delimiter);
                record.addField(pt.getResultString());
            }
            if (pt.isExhausted())
                break;
            pt.skip(1);
        }
        line = null;
        return record;
    }

    @Override
    public void close() throws IOException {
        brdr.close();
    }

    public class Record {

        private List<String> fields;

        public Record() {
            fields = new ArrayList<>();
        }

        public void addField(String s) {
            fields.add(s);
        }

        public List<String> getFields() {
            return fields;
        }

        public String getField(int i) {
            return fields.get(i);
        }

        public int getWidth() {
            return fields.size();
        }
    }

}
