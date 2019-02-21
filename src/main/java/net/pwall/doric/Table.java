/*
 * @(#) Table.java
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
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import net.pwall.json.JSON;
import net.pwall.json.JSONArray;
import net.pwall.json.JSONObject;
import net.pwall.util.CSV;

/**
 * Doric database table main class.
 *
 * @author  Peter Wall
 */
public class Table {

    public static final int defaultMaxUniqueValues = 20;
    public static final int maxDecimalShift = 4;
    public static final int[] decimalShifts = { 1, 10, 100, 1000, 10000 };

    private String source;
    private List<Column> columns;
    private int maxUniqueValues;
    private int rowCount;

    private BufferPool bufferPool;
    private ColumnReader[] columnReaders;
    private ColumnReader[] columnDataReaders;

    /**
     * Construct a table.
     */
    public Table() {
        source = null;
        maxUniqueValues = defaultMaxUniqueValues;
        columns = null;
        rowCount = 0;

        bufferPool = null;
        columnReaders = null;
        columnDataReaders = null;
    }

    public void open(String filename) throws IOException {
        open(new File(filename));
    }

    public void open(File file) throws IOException {
        if (!(file.exists() && file.isDirectory()))
            throw new IOException("Not found or not a directory: " + file);
        try {
            JSONObject json = JSON.parseObject(new File(file, "metadata.json"));
            source = json.getString("source");
            rowCount = json.getInt("rows");
            JSONArray jsonColumns = json.getArray("columns");
            int numColumns = jsonColumns.size();
            columns = new ArrayList<>(numColumns);
            bufferPool = new BufferPool(16, 8192); // TODO parameterise these values
            columnReaders = new ColumnReader[numColumns];
            columnDataReaders = new ColumnReader[numColumns];
            for (int i = 0; i < numColumns; i++) {
                JSONObject jsonColumn = jsonColumns.getObject(i);
                String columnName = jsonColumn.getString("name");
                Column column = Column.fromJSON(this, columnName, jsonColumn);
                columns.add(column);
                String filename = column.getFilename();
                if (filename != null)
                    columnReaders[i] = new ColumnReader(bufferPool, new File(file, filename),
                            column.getFileSize());
                filename = column.getDataFilename();
                if (filename != null)
                    columnDataReaders[i] = new ColumnReader(bufferPool, new File(file, filename),
                            column.getDataFileSize());
            }
        }
        catch (IOException ioe) {
            throw ioe;
        }
        catch (Exception e) {
            throw new IOException("Error reading metadata", e);
        }
    }

    public void close() throws IOException {
        for (int i = 0; i < columns.size(); i++) {
            if (columnReaders[i] != null)
                columnReaders[i].close();;
            if (columnDataReaders[i] != null)
                columnDataReaders[i].close();;
        }
    }

    public String getSource() {
        return source;
    }

    public void setSource(String source) {
        this.source = source;
    }

    public int getMaxUniqueValues() {
        return maxUniqueValues;
    }

    public void setMaxUniqueValues(int maxUniqueValues) {
        this.maxUniqueValues = maxUniqueValues;
    }

    public void analyse(CSV csv, boolean readHeader) {
        columns = new ArrayList<>();
        if (readHeader) {
            if (!csv.hasNext())
                throw new IllegalArgumentException("CSV Header line missing");
            CSV.Record header = csv.next();
            for (int i = 0, n = header.getWidth(); i < n; i++)
                columns.add(new Column(this, header.getField(i)));
        }
        while (csv.hasNext()) {
            CSV.Record record = csv.next();
            if (!readHeader && rowCount == 0) {
                for (int i = 0, n = record.getWidth(); i < n; i++)
                    columns.add(new Column(this, "col" + i));
            }

            int width = record.getWidth();
            if (width != columns.size())
                throw new IllegalArgumentException("CSV number of fields inconsistent, row " +
                        rowCount + "; expected " + columns.size() + ", was " + width);

            for (int i = 0; i < width; i++)
                columns.get(i).analyse(record.getField(i));

            rowCount++;
        }
        if (rowCount > 0) {
            for (Column column : columns)
                column.resolve();
        }
    }

    public int getRowCount() {
        return rowCount;
    }

    public int getColumnCount() {
        return columns.size();
    }

    public Column getColumn(int index) {
        return columns.get(index);
    }

    public Iterable<Column> getColumns() {
        return () -> new Iterator<Column>() {
            private int index = 0;
            @Override
            public boolean hasNext() {
                return index < columns.size();
            }
            @Override
            public Column next() {
                if (!hasNext())
                    throw new NoSuchElementException();
                return columns.get(index++);
            }
        };
    }

    public void setColumnName(int index, String name) {
        if (columns == null)
            throw new IllegalStateException("Columns not initialised");
        if (index < 0 || index >= columns.size())
            throw new IllegalArgumentException("Column number out of range: " + index);
        for (int i = 0, n = columns.size(); i < n; i++) {
            if (i != index && columns.get(i).getName().equals(name))
                throw new IllegalArgumentException("Duplicate name");
        }
        columns.get(index).setName(name);
    }

    public void setColumnName(String oldName, String newName) {
        if (columns == null)
            throw new IllegalStateException("Columns not initialised");
        for (int i = 0, n = columns.size(); i < n; i++) {
            if (columns.get(i).getName().equals(oldName)) {
                setColumnName(i, newName);
                return;
            }
        }
        throw new IllegalArgumentException("Column not found: " + oldName);
    }

    /**
     * Create a JSON representation of the {@code Table}.
     *
     * @return  the JSON
     */
    public JSONObject toJSON() {
        JSONObject json = new JSONObject();
        if (source != null)
            json.putValue("source", source);
        json.putValue("rows", rowCount);
        JSONArray array = new JSONArray();
        for (Column column : getColumns())
            array.add(column.toJSON());
        json.put("columns", array);
        return json;
    }

}
