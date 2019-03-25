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


import java.util.Map;

import net.pwall.doric.columninput.ColumnInput;
import net.pwall.json.JSONObject;

/**
 * A Column in the column-oriented database.
 */
public class Column {

    private String name;

    private int minWidth;
    private int maxWidth;

    private boolean nullable;

    private long maxInt;
    private long minInt;
    private double maxFloat;
    private double minFloat;
    private int maxDecimals;

    private Map<String, Long> uniqueValues;
    private Map<Long, Long> integerUniqueValues;
    private Map<Double, Long> floatUniqueValues;

    private Type type;
    private StorageType storageType;
    private StorageType dataOffsetStorageType;
    private StorageType dataLengthStorageType;
    private String value;
    private Long valueInt;
    private Double valueFloat;
    private int decimalShift;

    private FileData fileData;

    private ColumnInput columnInput;

    public Column(String name) {
        this.name = name;
        minWidth = Integer.MAX_VALUE;
        maxWidth = 0;
        nullable = false;
        maxInt = 0;
        minInt = 0;
        maxFloat = 0.0;
        minFloat = 0.0;
        maxDecimals = 0;

        uniqueValues = null;
        integerUniqueValues = null;
        floatUniqueValues = null;

        type = Type.undetermined;
        storageType = StorageType.undetermined;
        dataOffsetStorageType = StorageType.undetermined;
        dataLengthStorageType = StorageType.undetermined;

        value = null;
        valueInt = 0L;
        valueFloat = 0.0;
        decimalShift = 0;
        fileData = null;

        columnInput = null;
    }

    public Type getType() {
        return type;
    }

    void setType(Type type) {
        this.type = type;
    }

    public ColumnInput getColumnInput() {
        return columnInput;
    }

    public void setColumnInput(ColumnInput columnInput) {
        this.columnInput = columnInput;
    }

    public String getName() {
        return name;
    }

    void setName(String name) {
        this.name = name;
    }

    public boolean isNullable() {
        return nullable;
    }

    void setNullable(boolean nullable) {
        this.nullable = nullable;
    }

    public int getMinWidth() {
        return minWidth;
    }

    void setMinWidth(int minWidth) {
        this.minWidth = minWidth;
    }

    public int getMaxWidth() {
        return maxWidth;
    }

    void setMaxWidth(int maxWidth) {
        this.maxWidth = maxWidth;
    }

    public long getMinInt() {
        return minInt;
    }

    void setMinInt(long minInt) {
        this.minInt = minInt;
    }

    public long getMaxInt() {
        return maxInt;
    }

    void setMaxInt(long maxInt) {
        this.maxInt = maxInt;
    }

    public double getMinFloat() {
        return minFloat;
    }

    void setMinFloat(double minFloat) {
        this.minFloat = minFloat;
    }

    public double getMaxFloat() {
        return maxFloat;
    }

    void setMaxFloat(double maxFloat) {
        this.maxFloat = maxFloat;
    }

    public int getMaxDecimals() {
        return maxDecimals;
    }

    void setMaxDecimals(int maxDecimals) {
        this.maxDecimals = maxDecimals;
    }

    public int getDecimalShift() {
        return decimalShift;
    }

    void setDecimalShift(int decimalShift) {
        this.decimalShift = decimalShift;
    }

    public int getNumUniqueValues() {
        if (uniqueValues != null)
            return uniqueValues.size();
        if (integerUniqueValues != null)
            return integerUniqueValues.size();
        if (floatUniqueValues != null)
            return floatUniqueValues.size();
        return 0;
    }

    public Map<String, Long> getUniqueValues() {
        return uniqueValues;
    }

    void setUniqueValues(Map<String, Long> uniqueValues) {
        this.uniqueValues = uniqueValues;
    }

    public Map<Long, Long> getIntegerUniqueValues() {
        return integerUniqueValues;
    }

    void setIntegerUniqueValues(Map<Long, Long> integerUniqueValues) {
        this.integerUniqueValues = integerUniqueValues;
    }

    public Map<Double, Long> getFloatUniqueValues() {
        return floatUniqueValues;
    }

    void setFloatUniqueValues(Map<Double, Long> floatUniqueValues) {
        this.floatUniqueValues = floatUniqueValues;
    }

    public String getConstantValue() {
        return value;
    }

    void setConstantValue(String value) {
        this.value = value;
    }

    public Long getConstantValueInt() {
        return valueInt;
    }

    void setConstantValueInt(Long valueInt) {
        this.valueInt = valueInt;
    }

    public Double getConstantValueFloat() {
        return valueFloat;
    }

    void setConstantValueFloat(Double valueFloat) {
        this.valueFloat = valueFloat;
    }

    public StorageType getStorageType() {
        return storageType;
    }

    void setStorageType(StorageType storageType) {
        this.storageType = storageType;
    }

    public StorageType getDataOffsetStorageType() {
        return dataOffsetStorageType;
    }

    void setDataOffsetStorageType(StorageType dataOffsetStorageType) {
        this.dataOffsetStorageType = dataOffsetStorageType;
    }

    public StorageType getDataLengthStorageType() {
        return dataLengthStorageType;
    }

    void setDataLengthStorageType(StorageType dataLengthStorageType) {
        this.dataLengthStorageType = dataLengthStorageType;
    }

    public FileData getFileData() {
        return fileData;
    }

    public void setFileData(FileData fileData) {
        this.fileData = fileData;
    }

    public JSONObject toJSON() {
        JSONObject json = JSONObject.create().putValue("name", name);
        if (maxWidth == 0) {
            json.putValue("type", "null");
        }
        else {
            json.putValue("minWidth", minWidth).putValue("maxWidth", maxWidth);
            if (type == Type.integer) {
                json.putValue("type", "integer").putValue("minInt", minInt).
                        putValue("maxInt", maxInt);
            }
            else if (type == Type.floating) {
                json.putValue("type", "floating").putValue("minFloat", minFloat).
                        putValue("maxFloat", maxFloat).putValue("maxDecimals", maxDecimals);
            }
            else if (type == Type.date) {
                json.putValue("type", "date").putValue("minInt", minInt).
                        putValue("maxInt", maxInt);
            }
            else
                json.putValue("type", "undetermined");
            if (nullable)
                json.putValue("nullable", true);
            int numUnique = getNumUniqueValues();
            if (numUnique > 0)
                json.putValue("uniqueValues", numUnique);
            json.putValue("storageType", storageType.toString());
            if (storageType == StorageType.bytes) {
                json.putValue("offsetStorageType", dataOffsetStorageType.toString());
                json.putValue("lengthStorageType", dataLengthStorageType.toString());
            }
            else if (storageType == StorageType.constant) {
                if (type == Type.integer)
                    json.putValue("integerValue", valueInt);
                else if (type == Type.floating)
                    json.putValue("floatValue", valueFloat);
                else
                    json.putValue("value", value);
            }
            if (decimalShift != 0)
                json.putValue("decimalShift", decimalShift);
            if (fileData != null) {
                JSONObject fileDataObject = fileData.toJSON();
                if (fileDataObject != null)
                    json.put("files", fileDataObject);
            }
        }
        return json;
    }

    public static Column fromJSON(JSONObject json) {
        String name = json.getString("name");
        Column column = new Column(name);
        String type = json.getString("type");
        if (type.equals("null")) {
            column.minWidth = 0;
            column.maxWidth = 0;
            column.nullable = true;
            column.type = Type.undetermined;
            column.storageType = StorageType.none;
        }
        else {
            column.minWidth = json.getInt("minWidth");
            column.maxWidth = json.getInt("maxWidth");
            if (type.equals("integer")) {
                column.type = Type.integer;
                column.minInt = json.getLong("minInt");
                column.maxInt = json.getLong("maxInt");
            }
            else if (type.equals("floating")) {
                column.type = Type.floating;
                column.minFloat = json.getDouble("minFloat");
                column.maxFloat = json.getDouble("maxFloat");
                column.maxDecimals = json.getInt("maxDecimals");
            }
            else if (type.equals("date")) {
                column.type = Type.date;
                column.minInt = json.getLong("minInt");
                column.maxInt = json.getLong("maxInt");
            }
            if (json.containsKey("nullable"))
                column.nullable = json.getBoolean("nullable");
            // TODO - if "uniqueValues" is in JSON, how do we make use of it?
            column.storageType = StorageType.valueOf(json.getString("storageType"));
            if (column.storageType == StorageType.bytes) {
                column.dataOffsetStorageType =
                        StorageType.valueOf(json.getString("offsetStorageType"));
                column.dataLengthStorageType =
                        StorageType.valueOf(json.getString("lengthStorageType"));
            }
            else if (column.storageType == StorageType.constant) {
                if (column.type == Type.integer)
                    column.valueInt = json.getLong("integerValue");
                else if (column.type == Type.floating)
                    column.valueFloat = json.getDouble("floatValue");
                else
                    column.value = json.getString("value");
            }
            if (json.containsKey("decimalShift"))
                column.decimalShift = json.getInt("decimalShift");
            if (json.containsKey("files"))
                column.fileData = FileData.fromJSON(json.getObject("files"));
        }
        return column;
    }

    public static class FileData {

        private FileDetails rowData;
        private FileDetails bytesData;

        public FileDetails getRowData() {
            return rowData;
        }

        public void setRowData(FileDetails rowData) {
            this.rowData = rowData;
        }

        public FileDetails getBytesData() {
            return bytesData;
        }

        public void setBytesData(FileDetails bytesData) {
            this.bytesData = bytesData;
        }

        public JSONObject toJSON() {
            JSONObject result = new JSONObject();
            if (rowData != null)
                result.put("main", rowData.toJSON());
            if (bytesData != null)
                result.put("data", bytesData.toJSON());
            return result.size() == 0 ? null : result;
        }

        public static FileData fromJSON(JSONObject json) {
            FileData result = new FileData();
            if (json.containsKey("main"))
                result.setRowData(FileDetails.fromJSON(json.getObject("main")));
            if (json.containsKey("data"))
                result.setBytesData(FileDetails.fromJSON(json.getObject("data")));
            return result.getRowData() == null && result.getBytesData() == null ? null : result;
        }

    }

    public static class FileDetails {

        private String name;
        private long size;

        public FileDetails(String name, long size) {
            this.name = name;
            this.size = size;
        }

        public String getName() {
            return name;
        }

        public long getSize() {
            return size;
        }

        public JSONObject toJSON() {
            return JSONObject.create().putValue("name", name).putValue("size", size);
        }

        public static FileDetails fromJSON(JSONObject json) {
            return new FileDetails(json.getString("name"), json.getLong("size"));
        }

    }

    public enum Type {
        integer,
        floating,
        date,
        undetermined
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
