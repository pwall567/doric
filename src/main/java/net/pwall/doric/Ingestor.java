/*
 * @(#) Ingestor.java
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
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;

import net.pwall.json.JSONFormat;
import net.pwall.json.JSONObject;
import net.pwall.util.CSV;
import net.pwall.util.Strings;

public class Ingestor {

    private static final double[] decimalShifts = { 1, 10, 100, 1000, 10000 };

    public static void ingest(File csvFile, boolean headers, File outFile, boolean showMetadata, Integer maxUnique)
            throws IOException {
        CSV csv = new CSV(new FileInputStream(csvFile));
        Table table = new Table();
        table.setSource(csvFile.getCanonicalPath());
        if (maxUnique != null)
            table.setMaxUniqueValues(maxUnique);
        table.analyse(csv, headers);
        csv.close();

        // now write to files

        if (outFile != null) {
            int columnCount = table.getColumnCount();
            ColumnWriter[] columnWriters = new ColumnWriter[columnCount];
            ColumnWriter[] columnDataWriters = new ColumnWriter[columnCount];
            UniqueValueMap[] uniqueValueMaps = new UniqueValueMap[columnCount];

            // open files

            for (int i = 0; i < columnCount; i++) {
                Column column = table.getColumn(i);
                Column.StorageType storageType = column.getStorageType();
                if (!(storageType == Column.StorageType.none ||
                        storageType == Column.StorageType.fixed)) {
                    StringBuilder sb = new StringBuilder();
                    Strings.append3Digits(sb, i);
                    sb.append(".out");
                    String fileName = sb.toString();
                    column.setFilename(fileName);
                    columnWriters[i] = new ColumnWriter(outFile, fileName);
                    if (storageType == Column.StorageType.bytes) {
                        sb.setLength(0);
                        Strings.append3Digits(sb, i);
                        sb.append("a.out");
                        String dataFilename = sb.toString();
                        column.setDataFilename(dataFilename);
                        columnDataWriters[i] = new ColumnWriter(outFile, dataFilename);
                        if (column.getNumUniqueValues() > 0)
                            uniqueValueMaps[i] = new UniqueValueMap();
                    }
                }
            }

            csv = new CSV(new FileInputStream(csvFile));
            if (headers)
                csv.next();
            while (csv.hasNext()) {
                CSV.Record record = csv.next();
                for (int i = 0; i < columnCount; i++)
                    storeValue(columnWriters[i], columnDataWriters[i], table.getColumn(i),
                            record.getField(i), uniqueValueMaps[i]);
            }
            for (int i = 0; i < columnCount; i++) {
                Column column = table.getColumn(i);
                ColumnWriter writer = columnWriters[i];
                if (writer != null) {
                    column.setFileSize(writer.getOffset());
                    writer.close();
                }
                writer = columnDataWriters[i];
                if (writer != null) {
                    column.setDataFileSize(writer.getOffset());
                    writer.close();
                }
            }
            JSONObject json = table.toJSON();
            FileWriter metadata = new FileWriter(new File(outFile, "metadata.json"));
            json.appendJSON(metadata);
            metadata.close();

        }

        if (showMetadata) {
            JSONFormat format = new JSONFormat();
            System.out.println(format.format(table.toJSON()));
        }
    }

    private static void storeValue(ColumnWriter writer, ColumnWriter dataWriter, Column column,
            String str, UniqueValueMap uniqueValueMap) throws IOException {
        Column.StorageType storageType = column.getStorageType();
        int decimalShift = column.getDecimalShift();
        if (storageType == Column.StorageType.int8 || storageType == Column.StorageType.uint8) {
            if (decimalShift > 0) {
                double d = Double.valueOf(str) * decimalShifts[decimalShift];
                writer.writeInt8((int)Math.round(d));
            }
            else {
                writer.writeInt8(Integer.valueOf(str));
            }
        }
        else if (storageType == Column.StorageType.int16 ||
                storageType == Column.StorageType.uint16) {
            if (decimalShift > 0) {
                double d = Double.valueOf(str) * decimalShifts[decimalShift];
                writer.writeInt16((int)Math.round(d));
            }
            else {
                writer.writeInt16(Integer.valueOf(str));
            }
        }
        else if (storageType == Column.StorageType.int32 ||
                storageType == Column.StorageType.uint32) {
            if (decimalShift > 0) {
                double d = Double.valueOf(str) * decimalShifts[decimalShift];
                writer.writeInt32((int)Math.round(d));
            }
            else {
                writer.writeInt32(Integer.valueOf(str));
            }
        }
        else if (storageType == Column.StorageType.int64) {
            if (decimalShift > 0) {
                double d = Double.valueOf(str) * decimalShifts[decimalShift];
                writer.writeInt64(Math.round(d));
            }
            else {
                writer.writeInt64(Long.valueOf(str));
            }
        }
        else if (storageType == Column.StorageType.float64) {
            writer.writeFloat64(Double.valueOf(str));
        }
        else if (storageType == Column.StorageType.bytes) {
            long start;
            long length;
            if (column.getNumUniqueValues() > 0) {
                Long uniqueValueCached = uniqueValueMap.get(str);
                if (uniqueValueCached != null) {
                    start = uniqueValueCached;
                    length = uniqueValueCached >> 32;
                }
                else {
                    start = dataWriter.getOffset();
                    dataWriter.writeBytes(str);
                    length = dataWriter.getOffset() - start;
                    uniqueValueMap.put(str, (length << 32) + start);
                }
            }
            else {
                start = dataWriter.getOffset();
                dataWriter.writeBytes(str);
                length = dataWriter.getOffset() - start;
            }
            storeValueInt(writer, column.getDataOffsetStorageType(), start);
            storeValueInt(writer, column.getDataLengthStorageType(), length);
        }
        else if (!(storageType == Column.StorageType.none ||
                storageType == Column.StorageType.fixed)) {
            throw new RuntimeException("Unhandled data type");
        }
    }

    private static void storeValueInt(ColumnWriter writer, Column.StorageType storageType,
            long value) throws IOException {
        if (storageType == Column.StorageType.int8 || storageType == Column.StorageType.uint8)
            writer.writeInt8((int)value);
        else if (storageType == Column.StorageType.int16 ||
                storageType == Column.StorageType.uint16)
            writer.writeInt16((int)value);
        else if (storageType == Column.StorageType.int32 ||
                storageType == Column.StorageType.uint32)
            writer.writeInt32((int)value);
        else
            writer.writeInt64(value);
    }

    private static class UniqueValueMap extends HashMap<String, Long> {

    }

}
