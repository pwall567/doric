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

import net.pwall.doric.columnoutput.ColumnOutput;
import net.pwall.json.JSONFormat;
import net.pwall.json.JSONObject;
import net.pwall.util.CSV;

public class Ingestor {

    public static void ingest(String name, File csvFile, boolean headers, File outFile, boolean showMetadata,
            Integer maxUnique) throws IOException {
        Table table = new Table(name);
        table.setSource(csvFile.getCanonicalPath());
        if (maxUnique != null)
            table.setMaxUniqueValues(maxUnique);
        try (CSV csv = new CSV(new FileInputStream(csvFile))) {
            table.analyse(csv, headers);
        }

        // now write to files

        if (outFile != null) {

            int columnCount = table.getNumColumns();
            ColumnOutput[] columnOutputs = new ColumnOutput[columnCount];
            for (int i = 0; i < columnCount; i++)
                columnOutputs[i] = ColumnOutput.getExtendedColumnOutputObject(outFile, table.getColumn(i), i);

            try (CSV csv = new CSV(new FileInputStream(csvFile))) {
                if (headers)
                    csv.next();
                while (csv.hasNext()) {
                    CSV.Record record = csv.next();
                    for (int i = 0; i < columnCount; i++)
                        columnOutputs[i].putString(record.getField(i));
                }
            }

            for (int i = 0; i < columnCount; i++) {
                table.getColumn(i).setFileData(columnOutputs[i].summariseAndClose());
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

}
