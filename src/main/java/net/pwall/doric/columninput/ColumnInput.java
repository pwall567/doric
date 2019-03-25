/*
 * @(#) ColumnInput.java
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

package net.pwall.doric.columninput;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;

import net.pwall.doric.Column;

/**
 * Interface for column input classes, including factory method to create the correct class.
 *
 * @author  Peter Wall
 */
public interface ColumnInput extends AutoCloseable {

    boolean isNull(int rowNumber) throws IOException;

    Number getNumber(int rowNumber) throws IOException;

    long getLong(int rowNumber) throws IOException;

    String getString(int rowNumber) throws IOException;

    /**
     * Append the value to an {@link Appendable} (e.g. {@link StringBuilder}, {@link PrintWriter}.  This is a default
     * implementation; derived classes may override this to improve performance if a more efficient append function is
     * available.
     *
     * @param   a           the {@link Appendable}
     * @param   rowNumber   the row number
     * @throws  IOException if thrown by the {@link Appendable} or by {@link #getString(int)}
     */
    default void appendString(Appendable a, int rowNumber) throws IOException {
        a.append(getString(rowNumber));
    }

    static ColumnInput getExtendedColumnInputObject(File file, Column column) throws IOException {
        // the only extension so far is date (and decimal shift)
        if (column.getType() == Column.Type.date)
            return new ColumnInputDate(getColumnInputObject(file, column));
        if (column.getDecimalShift() != 0)
            return new ColumnInputDecimalShift(getColumnInputObject(file, column), column.getDecimalShift());
        return getColumnInputObject(file, column);
    }

    /**
     * Create an appropriate {@code ColumnInput} to read the nominated {@link Column}.  The code to select the correct
     * class must match exactly the code to select the output class.
     *
     * @param   file    the {@link File} object for the directory holding the files
     * @param   column  the {@link Column}
     * @return          the {@code ColumnInput}
     * @throws  IOException if thrown by the file open functions
     */
    static ColumnInput getColumnInputObject(File file, Column column) throws IOException {
        // TODO allow nullable columns
        Column.StorageType storageType = column.getStorageType();
        if (storageType == Column.StorageType.none)
            return new ColumnInputNone();
        if (storageType == Column.StorageType.constant) {
            if (column.getType() == Column.Type.integer)
                return new ColumnInputConstantInt(column.getConstantValueInt());
            if (column.getType() == Column.Type.floating)
                return new ColumnInputConstantFloat(column.getConstantValueFloat());
            return new ColumnInputConstantString(column.getConstantValue());
        }
        if (storageType == Column.StorageType.int8)
            return new ColumnInputInt8(file, column.getFileData());
        if (storageType == Column.StorageType.uint8)
            return new ColumnInputUint8(file, column.getFileData());
        if (storageType == Column.StorageType.int16)
            return new ColumnInputInt16(file, column.getFileData());
        if (storageType == Column.StorageType.uint16)
            return new ColumnInputUint16(file, column.getFileData());
        if (storageType == Column.StorageType.int32)
            return new ColumnInputInt32(file, column.getFileData());
        if (storageType == Column.StorageType.uint32)
            return new ColumnInputUint32(file, column.getFileData());
        if (storageType == Column.StorageType.int64)
            return new ColumnInputInt64(file, column.getFileData());
        if (storageType == Column.StorageType.float64)
            return new ColumnInputFloat64(file, column.getFileData());
        if (storageType == Column.StorageType.bytes) {
            Column.StorageType offsetStorageType = column.getDataOffsetStorageType();
            Column.StorageType lengthStorageType = column.getDataLengthStorageType();
            if (offsetStorageType == Column.StorageType.uint8) {
                if (lengthStorageType == Column.StorageType.uint8)
                    return new ColumnInputBytes8L8(file, column.getFileData());
            }
            else if (offsetStorageType == Column.StorageType.uint16) {
                if (lengthStorageType == Column.StorageType.uint8)
                    return new ColumnInputBytes16L8(file, column.getFileData());
            }
            else if (offsetStorageType == Column.StorageType.uint32) {
                if (lengthStorageType == Column.StorageType.uint8)
                    return new ColumnInputBytes32L8(file, column.getFileData());
                else if (lengthStorageType == Column.StorageType.uint16)
                    return new ColumnInputBytes32L16(file, column.getFileData());
            }
            // TODO complete these combinations
        }
        throw new IllegalStateException("Can't handle storage type " + storageType);
    }

}
