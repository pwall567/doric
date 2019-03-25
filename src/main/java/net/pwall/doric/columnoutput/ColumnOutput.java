/*
 * @(#) ColumnOutput.java
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

package net.pwall.doric.columnoutput;

import java.io.File;
import java.io.IOException;

import net.pwall.doric.Column;

/**
 * Interface for column output classes, including factory method to create the correct class.
 *
 * @author  Peter Wall
 */
public interface ColumnOutput {

    // TODO to be completed

    static ColumnOutput getExtendedColumnOutputObject(File file, Column column, int columnNumber) throws IOException {
        // the only extension so far is date (and decimal shift)
        if (column.getType() == Column.Type.date)
            return new ColumnOutputDate(getColumnOutputObject(file, column, columnNumber));
        if (column.getDecimalShift() != 0)
            return new ColumnOutputDecimalShift(getColumnOutputObject(file, column, columnNumber),
                    column.getDecimalShift());
        return getColumnOutputObject(file, column, columnNumber);
    }

    static ColumnOutput getColumnOutputObject(File file, Column column, int columnNumber) throws IOException {
        Column.StorageType storageType = column.getStorageType();
        if (storageType == Column.StorageType.none || storageType == Column.StorageType.constant)
            return new ColumnOutputNone();
        if (storageType == Column.StorageType.int8 || storageType == Column.StorageType.uint8)
            return new ColumnOutputInt8(file, columnNumber);
        if (storageType == Column.StorageType.int16 || storageType == Column.StorageType.uint16)
            return new ColumnOutputInt16(file, columnNumber);
        if (storageType == Column.StorageType.int32 || storageType == Column.StorageType.uint32)
            return new ColumnOutputInt32(file, columnNumber);
        if (storageType == Column.StorageType.int64)
            return new ColumnOutputInt64(file, columnNumber);
        if (storageType == Column.StorageType.float64)
            return new ColumnOutputFloat64(file, columnNumber);
        if (storageType == Column.StorageType.bytes) {
            Column.StorageType offsetStorageType = column.getDataOffsetStorageType();
            Column.StorageType lengthStorageType = column.getDataLengthStorageType();
            if (offsetStorageType == Column.StorageType.uint8) {
                if (lengthStorageType == Column.StorageType.uint8)
                    return new ColumnOutputBytes8L8(file, columnNumber, column.getUniqueValues());
            }
            else if (offsetStorageType == Column.StorageType.uint16) {
                if (lengthStorageType == Column.StorageType.uint8)
                    return new ColumnOutputBytes16L8(file, columnNumber, column.getUniqueValues());
            }
            else if (offsetStorageType == Column.StorageType.uint32) {
                if (lengthStorageType == Column.StorageType.uint8)
                    return new ColumnOutputBytes32L8(file, columnNumber, column.getUniqueValues());
                else if (lengthStorageType == Column.StorageType.uint16)
                    return new ColumnOutputBytes32L16(file, columnNumber, column.getUniqueValues());
            }
            // TODO complete these combinations
        }
        throw new IllegalStateException("Can't handle storage type " + storageType);
    }

    default void putNull() throws IOException {
        throw new IllegalStateException("Column can not take <null>");
    }

    default void putLong(long value) throws IOException {
        throw new IllegalStateException("Column can not take <long>");
    }

    default void putDouble(double value) throws IOException {
        throw new IllegalStateException("Column can not take <double>");
    }

    default void putNumber(Number value) throws IOException {
        throw new IllegalStateException("Column can not take <Number>");
    }

    default void putString(String value) throws IOException {
        throw new IllegalStateException("Column can not take <String>");
    }

    default Column.FileData summariseAndClose() throws IOException {
        return null;
    }

}
