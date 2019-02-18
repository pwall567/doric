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
                for (int i = 0; i < columnCount; i++) {
                    String str = record.getField(i);
                    Column column = table.getColumn(i);
                    ColumnWriter writer = columnWriters[i];
                    Column.StorageType storageType = column.getStorageType();
                    int decimalShift = column.getDecimalShift();
                    if (storageType == Column.StorageType.int8) {
                        if (decimalShift > 0) {
                            double d = Double.valueOf(str) * decimalShifts[decimalShift];
                            writer.writeInt8((int)Math.round(d));
                        }
                        else {
                            writer.writeInt8(Integer.valueOf(str));
                        }
                    }
                    else if (storageType == Column.StorageType.int16) {
                        if (decimalShift > 0) {
                            double d = Double.valueOf(str) * decimalShifts[decimalShift];
                            writer.writeInt16((int)Math.round(d));
                        }
                        else {
                            writer.writeInt16(Integer.valueOf(str));
                        }
                    }
                    else if (storageType == Column.StorageType.int32) {
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
                    else if (storageType == Column.StorageType.bytes) {
                        // TODO confirm size of offset and length written to writer
                        long start;
                        long length;
                        if (column.getNumUniqueValues() > 0) {
                            Long uniqueValueCached = uniqueValueMaps[i].get(str);
                            if (uniqueValueCached != null) {
                                start = uniqueValueCached;
                                length = uniqueValueCached >> 32;
                            }
                            else {
                                ColumnWriter dataWriter = columnDataWriters[i];
                                start = dataWriter.getOffset();
                                dataWriter.writeBytes(str);
                                length = dataWriter.getOffset() - start;
                                uniqueValueMaps[i].put(str, (length << 32) + start);
                            }
                        }
                        else {
                            ColumnWriter dataWriter = columnDataWriters[i];
                            start = dataWriter.getOffset();
                            dataWriter.writeBytes(str);
                            length = dataWriter.getOffset() - start;
                        }
                        writer.writeInt32((int)start);
                        writer.writeInt16((int)length);
                    }
                    else if (!(storageType == Column.StorageType.none ||
                            storageType == Column.StorageType.fixed)) {
                        throw new RuntimeException("Unhandled data type");
                    }
                }
            }
            for (int i = 0; i < columnCount; i++) {
                ColumnWriter writer = columnWriters[i];
                if (writer != null)
                    writer.close();
                writer = columnDataWriters[i];
                if (writer != null)
                    writer.close();
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

    private static class UniqueValueMap extends HashMap<String, Long> {

    }

}