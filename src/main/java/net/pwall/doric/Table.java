/*
 * @(#) Table.java
 */

package net.pwall.doric;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;

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

    /**
     * Construct a table.
     */
    public Table() {
        source = null;
        maxUniqueValues = defaultMaxUniqueValues;
        columns = null;
        rowCount = 0;
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
                columns.add(new Column(header.getField(i)));
        }
        while (csv.hasNext()) {
            CSV.Record record = csv.next();
            if (!readHeader && rowCount == 0) {
                for (int i = 0, n = record.getWidth(); i < n; i++)
                    columns.add(new Column("col" + i));
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

    /**
     * Inner class to represent the metadata for the column.
     */
    public class Column {

        private String name;

        private int minWidth;
        private int maxWidth;

        private boolean integer;
        private boolean intInit;
        private boolean floating;
        private boolean floatInit;

        private long maxInt;
        private long minInt;
        private double maxFloat;
        private double minFloat;
        private int maxDecimals;

        private Set<String> uniqueValues;
        // Implementation note - once the maximum is reached, this variable is set to null.
        // That can be used to determine that the column has more than the maximum

        private StorageType storageType;
        private String value;
        private int decimalShift;

        private String filename;
        private String dataFilename;

        public Column(String name) {
            this.name = name;
            minWidth = Integer.MAX_VALUE;
            maxWidth = 0;
            integer = true;
            intInit = false;
            floating = true;
            floatInit = false;
            maxInt = 0;
            minInt = 0;
            maxFloat = 0.0;
            minFloat = 0.0;
            maxDecimals = 0;
            uniqueValues = new HashSet<>();
            storageType = StorageType.undetermined;
            value = null;
            decimalShift = 0;
            filename = null;
            dataFilename = null;
        }

        /**
         * Analyse the contents of the string and make appropriate changes to the metadata for
         * the column.
         *
         * @param   str     the contents of one row for this column
         */
        public void analyse(String str) {

            int strWidth = str.length();
            if (strWidth < minWidth)
                minWidth = strWidth;
            if (strWidth > maxWidth)
                maxWidth = strWidth;

            if (strWidth == 0)
                return; // empty column doesn't affect following analysis

            if (floating) {
                try {
                    double d = Double.valueOf(str);
                    if (floatInit) {
                        if (d < minFloat)
                            minFloat = d;
                        if (d > maxFloat)
                            maxFloat = d;
                    }
                    else {
                        minFloat = d;
                        maxFloat = d;
                        floatInit = true;
                    }
                    int i = str.indexOf('.');
                    if (i >= 0) {
                        int decimals = 0;
                        while (++i < str.length()) {
                            char ch = str.charAt(i);
                            if (ch < '0' || ch > '9')
                                break;
                            decimals++;
                        }
                        if (decimals > maxDecimals)
                            maxDecimals = decimals;
                    }
                }
                catch (NumberFormatException nfe) {
                    floating = false;
                }
            }

            if (integer) {
                try {
                    long i = Long.valueOf(str);
                    if (intInit) {
                        if (i < minInt)
                            minInt = i;
                        if (i > maxInt)
                            maxInt = i;
                    }
                    else {
                        minInt = i;
                        maxInt = i;
                        intInit = true;
                    }
                }
                catch (NumberFormatException nfe) {
                    integer = false;
                }
            }

            if (uniqueValues != null) {
                if (uniqueValues.size() <= maxUniqueValues)
                    uniqueValues.add(str);
                if (uniqueValues.size() > maxUniqueValues)
                    uniqueValues = null;
            }

            // At this point, we can check for other data types, like dates, times, currency
            // codes, IATA city codes, ...
        }

        /**
         * Work out the storage requirements for the column.
         */
        public void resolve() {
            if (maxWidth == 0) {
                storageType = StorageType.none;
            }
            if (getNumUniqueValues() == 1) {
                storageType = StorageType.fixed;
                value = uniqueValues.iterator().next();
            }
            else if (integer) {
                storageType = getIntStorageType(minInt, maxInt);
            }
            else if (floating) {
                if (maxDecimals <= maxDecimalShift) {
                    long minShifted = Math.round(minFloat * decimalShifts[maxDecimals]);
                    long maxShifted = Math.round(maxFloat * decimalShifts[maxDecimals]);
                    storageType = getIntStorageType(minShifted, maxShifted);
                    decimalShift = maxDecimals;
                }
                else
                    storageType = StorageType.float64;
            }
            else {
                storageType = StorageType.bytes;
            }
        }

        private StorageType getIntStorageType(long min, long max) {
            if (min >= Byte.MIN_VALUE && max <= Byte.MAX_VALUE)
                return StorageType.int8;
            if (min >= Short.MIN_VALUE && max <= Short.MAX_VALUE)
                return StorageType.int16;
            if (min >= Integer.MIN_VALUE && max <= Integer.MAX_VALUE)
                return StorageType.int32;
            return StorageType.int64;
        }

        public String getName() {
            return name;
        }

        public int getMinWidth() {
            return minWidth;
        }

        public void setName(String name) {
            this.name = name;
        }

        public int getMaxWidth() {
            return maxWidth;
        }

        public boolean isInteger() {
            return integer;
        }

        public long getMinInt() {
            return minInt;
        }

        public long getMaxInt() {
            return maxInt;
        }

        public boolean isFloating() {
            return floating;
        }

        public double getMinFloat() {
            return minFloat;
        }

        public double getMaxFloat() {
            return maxFloat;
        }

        public int getMaxDecimals() {
            return maxDecimals;
        }

        public int getDecimalShift() {
            return decimalShift;
        }

        public int getNumUniqueValues() {
            return uniqueValues == null ? 0 : uniqueValues.size();
        }

        public String getFilename() {
            return filename;
        }

        public void setFilename(String filename) {
            this.filename = filename;
        }

        public String getDataFilename() {
            return dataFilename;
        }

        public void setDataFilename(String dataFilename) {
            this.dataFilename = dataFilename;
        }

        public StorageType getStorageType() {
            return storageType;
        }

        public JSONObject toJSON() {
            JSONObject json = JSONObject.create().putValue("name", name);
            if (rowCount > 0) {
                json.putValue("minWidth", minWidth).putValue("maxWidth", maxWidth);
                if (isInteger()) {
                    json.putValue("type", "integer").putValue("minInt", minInt).
                            putValue("maxInt", maxInt);
                }
                else if (isFloating()) {
                    json.putValue("type", "float").putValue("minFloat", minFloat).
                            putValue("maxFloat", maxFloat).putValue("maxDecimals", maxDecimals);
                }
                else
                    json.putValue("type", "undetermined");
                int numUnique = getNumUniqueValues();
                if (numUnique > 0)
                    json.putValue("uniqueValues", numUnique);
                json.putValue("storageType", storageType.toString());
                if (storageType == StorageType.fixed)
                    json.putValue("value", value);
                if (decimalShift != 0)
                    json.putValue("decimalShift", decimalShift);
                if (filename != null)
                    json.putValue("filename", filename);
                if (dataFilename != null)
                    json.putValue("dataFilename", dataFilename);
            }
            return json;
        }

    }

    public enum StorageType { undetermined, none, fixed, int8, int16, int32, int64, float64, bytes }

}
