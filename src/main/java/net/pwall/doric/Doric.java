/*
 * @(#) Doric.java
 */

package net.pwall.doric;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.util.HashMap;

import net.pwall.json.JSONFormat;
import net.pwall.json.JSONObject;
import net.pwall.util.CSV;
import net.pwall.util.Strings;
import net.pwall.util.UserError;

/**
 * Doric database executor class.
 *
 * @author  Peter Wall
 */
public class Doric {

    private static final double[] decimalShifts = { 1, 10, 100, 1000, 10000 };

    public static void main(String[] args) {
        try {
            File csvFile = null;
            File outFile = null;
            Integer maxUnique = null;
            Boolean headers = null;
            Boolean showMetadata = null;
            for (int i = 0, n = args.length; i< n; i++) {
                String arg = args[i];
                switch (arg) {
                    case "--csv":
                        if (csvFile != null)
                            throw new UserError("Duplicate --csv switch");
                        csvFile = new File(getArg(args, ++i, "--csv with no pathname"));
                        if (!csvFile.exists() || csvFile.isDirectory())
                            throw new UserError("--csv file does not exist");
                        break;
                    case "--out":
                        if (outFile != null)
                            throw new UserError("Duplicate --out switch");
                        outFile = new File(getArg(args, ++i, "--out with no pathname"));
                        if (outFile.exists()) {
                            if (!outFile.isDirectory())
                                throw new UserError("--out exists but is not directory");
                        }
                        break;
                    case "--hdr":
                        if (headers != null)
                            throw new UserError("Duplicate --hdr switch");
                        headers = Boolean.TRUE;
                        break;
                    case "--show":
                        if (showMetadata != null)
                            throw new UserError("Duplicate --show switch");
                        showMetadata = Boolean.TRUE;
                        break;
                    case "--maxUnique":
                        if (maxUnique != null)
                            throw new UserError("Duplicate --maxUnique switch");
                        try {
                            maxUnique = Integer.valueOf(getArg(args, ++i,
                                    "--maxUnique with no value"));
                        }
                        catch (NumberFormatException nfe) {
                            throw new UserError("--maxUnique invalid value");
                        }
                        break;
                    default:
                        throw new UserError("Unrecognised argument - " + arg);
                }
            }
            if (csvFile == null)
                throw new UserError("--csv not specified");
            if (headers == null)
                headers = Boolean.FALSE;
            if (showMetadata == null)
                showMetadata = Boolean.FALSE;
            CSV csv = new CSV(new FileInputStream(csvFile));
            Table table = new Table();
            table.setSource(csvFile.getCanonicalPath());
            if (maxUnique != null)
                table.setMaxUniqueValues(maxUnique);
            table.analyse(csv, headers);

            // now write to files

            if (outFile != null) {
                int columnCount = table.getColumnCount();
                ColumnWriter[] columnWriters = new ColumnWriter[columnCount];
                ColumnWriter[] columnDataWriters = new ColumnWriter[columnCount];
                UniqueValueMap[] uniqueValueMaps = new UniqueValueMap[columnCount];
                if (!outFile.exists() && !outFile.mkdirs())
                    throw new UserError("Error creating output directory");

                // open files

                for (int i = 0; i < columnCount; i++) {
                    Table.Column column = table.getColumn(i);
                    Table.StorageType storageType = column.getStorageType();
                    if (!(storageType == Table.StorageType.none ||
                            storageType == Table.StorageType.fixed)) {
                        StringBuilder sb = new StringBuilder();
                        Strings.append3Digits(sb, i);
                        sb.append(".out");
                        String fileName = sb.toString();
                        column.setFilename(fileName);
                        columnWriters[i] = new ColumnWriter(outFile, fileName);
                        if (storageType == Table.StorageType.bytes) {
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
                        Table.Column column = table.getColumn(i);
                        ColumnWriter writer = columnWriters[i];
                        Table.StorageType storageType = column.getStorageType();
                        int decimalShift = column.getDecimalShift();
                        if (storageType == Table.StorageType.int8) {
                            if (decimalShift > 0) {
                                double d = Double.valueOf(str) * decimalShifts[decimalShift];
                                writer.writeInt8((int)Math.round(d));
                            }
                            else {
                                writer.writeInt8(Integer.valueOf(str));
                            }
                        }
                        else if (storageType == Table.StorageType.int16) {
                            if (decimalShift > 0) {
                                double d = Double.valueOf(str) * decimalShifts[decimalShift];
                                writer.writeInt16((int)Math.round(d));
                            }
                            else {
                                writer.writeInt16(Integer.valueOf(str));
                            }
                        }
                        else if (storageType == Table.StorageType.int32) {
                            if (decimalShift > 0) {
                                double d = Double.valueOf(str) * decimalShifts[decimalShift];
                                writer.writeInt32((int)Math.round(d));
                            }
                            else {
                                writer.writeInt32(Integer.valueOf(str));
                            }
                        }
                        else if (storageType == Table.StorageType.int64) {
                            if (decimalShift > 0) {
                                double d = Double.valueOf(str) * decimalShifts[decimalShift];
                                writer.writeInt64(Math.round(d));
                            }
                            else {
                                writer.writeInt64(Long.valueOf(str));
                            }
                        }
                        else if (storageType == Table.StorageType.bytes) {
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
                        else if (!(storageType == Table.StorageType.none ||
                                storageType == Table.StorageType.fixed)) {
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
        catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    private static String getArg(String[] args, int index, String msg) {
        if (index >= args.length)
            throw new UserError(msg);
        String result = args[index];
        if (result.startsWith("-"))
            throw new UserError(msg);
        return result;
    }

    private static class UniqueValueMap extends HashMap<String, Long> {

    }

}
