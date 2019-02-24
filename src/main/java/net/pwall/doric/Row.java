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
import java.time.LocalDate;

import net.pwall.util.Strings;

public class Row {

    private Table table;
    private int rowNumber;

    public Row(Table table, int rowNumber) {
        this.table = table;
        this.rowNumber = rowNumber;
    }

    public long getLong(String columnName) throws IOException {
        return getLong(table.getColumn(columnName));
    }

    public long getLong(int columnNumber) throws IOException {
        return getLong(table.getColumn(columnNumber));
    }

    public long getLong(Column column) throws IOException {
        if (column.getDecimalShift() == 0) {
            Column.StorageType storageType = column.getStorageType();
            ColumnReader columnReader = table.getColumnReader(column.getNumber());
            if (storageType == Column.StorageType.int8)
                return columnReader.readInt8(rowNumber);
            if (storageType == Column.StorageType.uint8)
                return columnReader.readInt8(rowNumber) & 0xFF;
            if (storageType == Column.StorageType.int16)
                return columnReader.readInt16(rowNumber << 1);
            if (storageType == Column.StorageType.uint16)
                return columnReader.readInt16(rowNumber << 1) & 0xFFFF;
            if (storageType == Column.StorageType.int32)
                return columnReader.readInt32(rowNumber << 2);
            if (storageType == Column.StorageType.uint32)
                return columnReader.readInt32(rowNumber << 2) & 0xFFFFFFFFL;
            if (storageType == Column.StorageType.int64)
                return columnReader.readInt64(rowNumber << 4);
        }
        throw new IllegalStateException("Can't convert column to long: " + column.getName());
    }

    public String getString(String columnName) throws IOException {
        return getString(table.getColumn(columnName));
    }

    public String getString(int columnNumber) throws IOException {
        return getString(table.getColumn(columnNumber));
    }

    public String getString(Column column) throws IOException {
        Column.StorageType storageType = column.getStorageType();
        if (storageType == Column.StorageType.none)
            return "";
        if (storageType == Column.StorageType.constant)
            return column.getConstantValue();
        if (column.isDate())
            return LocalDate.ofEpochDay(getLong(column)).toString();
        if (storageType == Column.StorageType.int8) {
            int value = table.getColumnReader(column.getNumber()).readInt8(rowNumber);
            return decimalShifted(value, column.getDecimalShift());
        }
        if (storageType == Column.StorageType.uint8) {
            int value = table.getColumnReader(column.getNumber()).readInt8(rowNumber);
            return decimalShifted(value & 0xFF, column.getDecimalShift());
        }
        if (storageType == Column.StorageType.int16) {
            int value = table.getColumnReader(column.getNumber()).readInt16(rowNumber << 1);
            return decimalShifted(value, column.getDecimalShift());
        }
        if (storageType == Column.StorageType.uint16) {
            int value = table.getColumnReader(column.getNumber()).readInt16(rowNumber << 1);
            return decimalShifted(value & 0xFFFF, column.getDecimalShift());
        }
        if (storageType == Column.StorageType.int32) {
            int value = table.getColumnReader(column.getNumber()).readInt32(rowNumber << 2);
            return decimalShifted(value, column.getDecimalShift());
        }
        if (storageType == Column.StorageType.uint32) {
            long value = table.getColumnReader(column.getNumber()).readInt32(rowNumber << 2);
            return decimalShifted(value & 0xFFFFFFFFL, column.getDecimalShift());
        }
        if (storageType == Column.StorageType.int64) {
            long value = table.getColumnReader(column.getNumber()).readInt64(rowNumber << 4);
            return decimalShifted(value, column.getDecimalShift());
        }
        if (storageType == Column.StorageType.float64) {
            double value = table.getColumnReader(column.getNumber()).readFloat64(rowNumber << 4);
            return String.valueOf(value);
        }
        if (storageType == Column.StorageType.bytes) {
            int offsetLength = storageLength(column.getDataOffsetStorageType());
            int lengthLength = storageLength(column.getDataLengthStorageType());
            long offset = rowNumber * (offsetLength + lengthLength);
            long dataOffset = readInt(offsetLength, column.getNumber(), offset);
            int dataLength = (int)readInt(lengthLength, column.getNumber(), offset + offsetLength);
            return table.getColumnDataReader(column.getNumber()).readBytes(dataOffset, dataLength);
        }
        throw new IllegalArgumentException("Unexpected storage type: " + storageType);
    }

    private long readInt(int intLength, int columnNumber, long offset) throws IOException {
        if (intLength == 1)
            return table.getColumnReader(columnNumber).readInt8(offset);
        if (intLength == 2)
            return table.getColumnReader(columnNumber).readInt16(offset);
        if (intLength == 4)
            return table.getColumnReader(columnNumber).readInt32(offset);
        return table.getColumnReader(columnNumber).readInt64(offset);
    }

    private static String decimalShifted(long value, int decimalShift) {
        StringBuilder sb = new StringBuilder();
        try {
            Strings.appendLong(sb, value);
        }
        catch (IOException ignore) {
            // can't happen
        }
        if (decimalShift > 0) {
            if (sb.charAt(0) == '-') {
                while (sb.length() < decimalShift + 2)
                    sb.insert(1, '0');
            }
            else {
                while (sb.length() < decimalShift + 1)
                    sb.insert(0, '0');
            }
            sb.insert(sb.length() - decimalShift, '.');
        }
        return sb.toString();
    }

    private static int storageLength(Column.StorageType storageType) {
        if (storageType == Column.StorageType.uint8)
            return 1;
        if (storageType == Column.StorageType.uint16)
            return 2;
        if (storageType == Column.StorageType.uint32)
            return 4;
        if (storageType == Column.StorageType.int64)
            return 8;
        throw new IllegalArgumentException("Unexpected storage type: " + storageType);
    }

}
