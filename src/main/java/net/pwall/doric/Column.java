/*
 * @(#) Column.java
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


import java.time.LocalDate;
import java.time.format.DateTimeParseException;
import java.util.HashSet;
import java.util.Set;

import net.pwall.json.JSONObject;

/**
 * A Column in the column-oriented database.
 */
public class Column {

    private Table table;
    private int number;
    private String name;

    private int minWidth;
    private int maxWidth;

    private boolean integer;
    private boolean intInit;
    private boolean floating;
    private boolean floatInit;
    private boolean date;
    private boolean dateInit;

    private long maxInt;
    private long minInt;
    private double maxFloat;
    private double minFloat;
    private int maxDecimals;

    private Set<String> uniqueValues;
    // Implementation note - once the maximum is reached, this variable is set to null.
    // That can be used to determine that the column has more than the maximum

    private StorageType storageType;
    private StorageType dataOffsetStorageType;
    private StorageType dataLengthStorageType;
    private String value;
    private int decimalShift;

    private String filename;
    private String dataFilename;
    private long fileSize;
    private long dataFileSize;

    public Column(Table table, int number, String name) {
        this.table = table;
        this.number = number;
        this.name = name;
        minWidth = Integer.MAX_VALUE;
        maxWidth = 0;
        integer = true;
        intInit = false;
        floating = true;
        floatInit = false;
        date = true;
        dateInit = false;
        maxInt = 0;
        minInt = 0;
        maxFloat = 0.0;
        minFloat = 0.0;
        maxDecimals = 0;
        uniqueValues = new HashSet<>();
        storageType = StorageType.undetermined;
        dataOffsetStorageType = StorageType.undetermined;
        dataLengthStorageType = StorageType.undetermined;

        value = null;
        decimalShift = 0;
        filename = null;
        dataFilename = null;
        fileSize = 0;
        dataFileSize = 0;
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

        if (date) {
            try {
                LocalDate localDate = LocalDate.parse(str);
                long epochDay = localDate.toEpochDay();
                if (dateInit) {
                    if (epochDay < minInt)
                        minInt = epochDay;
                    if (epochDay > maxInt)
                        maxInt = epochDay;
                }
                else {
                    minInt = epochDay;
                    maxInt = epochDay;
                    dateInit = true;
                    integer = false; // should be anyway, but just in case
                }
            }
            catch (DateTimeParseException e) {
                date = false;
            }
        }

        if (uniqueValues != null) {
            if (uniqueValues.size() <= table.getMaxUniqueValues())
                uniqueValues.add(str);
            if (uniqueValues.size() > table.getMaxUniqueValues())
                uniqueValues = null;
        }

        // At this point, we can check for other data types, like times, currency codes,
        // IATA city codes, ...
    }

    /**
     * Work out the storage requirements for the column.
     */
    public void resolve() {
        if (maxWidth == 0) {
            storageType = StorageType.none;
        }
        else if (getNumUniqueValues() == 1) {
            storageType = StorageType.constant;
            value = uniqueValues.iterator().next();
        }
        else if (integer || date) {
            storageType = getIntStorageType(minInt, maxInt);
        }
        else if (floating) {
            if (maxDecimals <= Table.maxDecimalShift) {
                long minShifted = Math.round(minFloat * Table.decimalShifts[maxDecimals]);
                long maxShifted = Math.round(maxFloat * Table.decimalShifts[maxDecimals]);
                storageType = getIntStorageType(minShifted, maxShifted);
                decimalShift = maxDecimals;
            }
            else
                storageType = StorageType.float64;
        }
        else {
            storageType = StorageType.bytes;
            int numOccurrences = uniqueValues == null ? table.getRowCount() :
                    uniqueValues.size();
            dataOffsetStorageType = getIntStorageType(0, numOccurrences * maxWidth);
            dataLengthStorageType = getIntStorageType(0, maxWidth);
        }
    }

    private static StorageType getIntStorageType(long min, long max) {
        if (min >= 0 && max <= 0xFF)
            return StorageType.uint8;
        if (min >= Byte.MIN_VALUE && max <= Byte.MAX_VALUE)
            return StorageType.int8;
        if (min >= 0 && max <= 0xFFFF)
            return StorageType.uint16;
        if (min >= Short.MIN_VALUE && max <= Short.MAX_VALUE)
            return StorageType.int16;
        if (min >= 0 && max <= 0xFFFFFFFFL)
            return  StorageType.uint32;
        if (min >= Integer.MIN_VALUE && max <= Integer.MAX_VALUE)
            return StorageType.int32;
        return StorageType.int64;
    }

    public int getNumber() {
        return number;
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

    public boolean isDate() {
        return date;
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

    public String getConstantValue() {
        return value;
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

    public long getFileSize() {
        return fileSize;
    }

    public void setFileSize(long fileSize) {
        this.fileSize = fileSize;
    }

    public long getDataFileSize() {
        return dataFileSize;
    }

    public void setDataFileSize(long dataFileSize) {
        this.dataFileSize = dataFileSize;
    }

    public StorageType getStorageType() {
        return storageType;
    }

    public StorageType getDataOffsetStorageType() {
        return dataOffsetStorageType;
    }

    public StorageType getDataLengthStorageType() {
        return dataLengthStorageType;
    }

    public JSONObject toJSON() {
        JSONObject json = JSONObject.create().putValue("name", name);
        if (table.getRowCount() > 0) {
            json.putValue("minWidth", minWidth).putValue("maxWidth", maxWidth);
            if (isInteger()) {
                json.putValue("type", "integer").putValue("minInt", minInt).
                        putValue("maxInt", maxInt);
            }
            else if (isFloating()) {
                json.putValue("type", "float").putValue("minFloat", minFloat).
                        putValue("maxFloat", maxFloat).putValue("maxDecimals", maxDecimals);
            }
            else if (isDate()) {
                json.putValue("type", "date").putValue("minInt", minInt).
                        putValue("maxInt", maxInt);
            }
            else
                json.putValue("type", "undetermined");
            int numUnique = getNumUniqueValues();
            if (numUnique > 0)
                json.putValue("uniqueValues", numUnique);
            json.putValue("storageType", storageType.toString());
            if (storageType == StorageType.bytes) {
                json.putValue("offsetStorageType", dataOffsetStorageType.toString());
                json.putValue("lengthStorageType", dataLengthStorageType.toString());
            }
            else if (storageType == StorageType.constant)
                json.putValue("value", value);
            if (decimalShift != 0)
                json.putValue("decimalShift", decimalShift);
            if (filename != null) {
                json.putValue("filename", filename);
                json.putValue("fileSize", fileSize);
            }
            if (dataFilename != null) {
                json.putValue("dataFilename", dataFilename);
                json.putValue("dataFileSize", dataFileSize);
            }
        }
        return json;
    }

    public static Column fromJSON(Table table, int number, String name, JSONObject json) {
        Column column = new Column(table, number, name);
        column.integer = false;
        column.floating = false;
        column.date = false;
        if (table.getRowCount() > 0) {
            column.minWidth = json.getInt("minWidth");
            column.maxWidth = json.getInt("maxWidth");
            String type = json.getString("type");
            if (type.equals("integer")) {
                column.integer = true;
                column.minInt = json.getLong("minInt");
                column.maxInt = json.getLong("maxInt");
            }
            else if (type.equals("float")) {
                column.floating = true;
                column.minFloat = json.getDouble("minFloat");
                column.maxFloat = json.getDouble("maxFloat");
                column.maxDecimals = json.getInt("maxDecimals");
            }
            else if (type.equals("date")) {
                column.date = true;
                column.minInt = json.getLong("minInt");
                column.maxInt = json.getLong("maxInt");
            }
            // TODO - if "uniqueValues" is in JSON, how do we make use of it?
            column.storageType = StorageType.valueOf(json.getString("storageType"));
            if (column.storageType == StorageType.bytes) {
                column.dataOffsetStorageType =
                        StorageType.valueOf(json.getString("offsetStorageType"));
                column.dataLengthStorageType =
                        StorageType.valueOf(json.getString("lengthStorageType"));
            }
            else if (column.storageType == StorageType.constant)
                column.value = json.getString("value");
            if (json.containsKey("decimalShift"))
                column.decimalShift = json.getInt("decimalShift");
            if (json.containsKey("filename")) {
                column.filename = json.getString("filename");
                column.fileSize = json.getLong("fileSize");
            }
            if (json.containsKey("dataFilename")) {
                column.dataFilename = json.getString("dataFilename");
                column.dataFileSize = json.getLong("dataFileSize");
            }
        }
        return column;
    }

    public enum StorageType {
        undetermined,
        none,
        constant,
        int8,
        int16,
        int32,
        int64,
        uint8,
        uint16,
        uint32,
        float64,
        bytes
    }

}
