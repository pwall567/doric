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

import net.pwall.doric.columninput.ColumnInput;
import net.pwall.doric.query.Query;
import net.pwall.json.JSON;
import net.pwall.json.JSONArray;
import net.pwall.json.JSONObject;
import net.pwall.util.CSV;

/**
 * Doric database table main class.
 *
 * @author  Peter Wall
 */
public class Table implements Query {

    public static final int defaultMaxUniqueValues = 200;
    public static final int maxDecimalShift = 4;
    public static final int[] decimalShifts = { 1, 10, 100, 1000, 10000 };

    private String name;
    private String source;
    private List<Column> columns;
    private int maxUniqueValues;
    private int numRows;

    /**
     * Construct a table.
     */
    Table(String name) {
        this.name = name;
        source = null;
        maxUniqueValues = defaultMaxUniqueValues;
        columns = null;
        numRows = 0;
    }

    @Override
    public boolean isNumRowsKnown() {
        return numRows > 0;
    }

    public String getName() {
        return name;
    }

    public void close() throws Exception {
        for (int i = 0; i < columns.size(); i++) {
            getColumn(i).getColumnInput().close();
        }
    }

    public String getSource() {
        return source;
    }

    public int getMaxUniqueValues() {
        return maxUniqueValues;
    }

    public void setMaxUniqueValues(int maxUniqueValues) {
        this.maxUniqueValues = maxUniqueValues;
    }

    public void analyse(CSV csv, boolean readHeader) {
        List<ColumnAnalysis> analyses = new ArrayList<>();
        if (readHeader) {
            if (!csv.hasNext())
                throw new IllegalArgumentException("CSV Header line missing");
            CSV.Record header = csv.next();
            for (int i = 0, n = header.getWidth(); i < n; i++)
                analyses.add(new ColumnAnalysis(header.getField(i), maxUniqueValues));
        }
        while (csv.hasNext()) {
            CSV.Record record = csv.next();
            if (!readHeader && numRows == 0) {
                for (int i = 0, n = record.getWidth(); i < n; i++)
                    analyses.add(new ColumnAnalysis("col" + i, maxUniqueValues));
            }

            int width = record.getWidth();
            if (width != analyses.size())
                throw new IllegalArgumentException("CSV number of fields inconsistent, row " +
                        numRows + "; expected " + analyses.size() + ", was " + width);

            for (int i = 0; i < width; i++)
                analyses.get(i).analyse(record.getField(i));

            numRows++;
        }
        columns = new ArrayList<>();
        for (ColumnAnalysis analysis : analyses) {
            columns.add(analysis.resolve());
        }
    }

    @Override
    public int getNumRows() {
        return numRows;
    }

    @Override
    public int getNumColumns() {
        return columns.size();
    }

    @Override
    public Column getColumn(int columnNumber) {
        return columns.get(columnNumber);
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
        if (name != null)
            json.putValue("name", name);
        if (source != null)
            json.putValue("source", source);
        json.putValue("rows", numRows);
        JSONArray array = new JSONArray();
        for (Column column : getColumns())
            array.add(column.toJSON());
        json.put("columns", array);
        return json;
    }

    void setSource(String source) {
        this.source = source;
    }

    private void setNumRows(int numRows) {
        this.numRows = numRows;
    }

    private void setColumns(List<Column> columns) {
        this.columns = columns;
    }

    public static Table open(String filename) throws IOException {
        return open(new File(filename));
    }

    public static Table open(File file) throws IOException {
        if (!(file.exists() && file.isDirectory()))
            throw new IOException("Not found or not a directory: " + file);
        try {
            JSONObject json = JSON.parseObject(new File(file, "metadata.json"));
            String name = json.getString("name");
            if (name == null)
                name = "table";
            Table table = new Table(name);
            if (json.containsKey("source"))
                table.setSource(json.getString("source"));
            if (json.containsKey("rows"))
                table.setNumRows(json.getInt("rows"));
            JSONArray jsonColumns = json.getArray("columns");
            int numColumns = jsonColumns.size();
            List<Column> columns = new ArrayList<>(numColumns);
            for (int i = 0; i < numColumns; i++) {
                Column column = Column.fromJSON(jsonColumns.getObject(i));
                columns.add(column);
                column.setColumnInput(ColumnInput.getExtendedColumnInputObject(file, column));
            }
            table.setColumns(columns);
            return table;
        }
        catch (IOException ioe) {
            throw ioe;
        }
        catch (Exception e) {
            throw new IOException("Error reading metadata", e);
        }
    }

}
