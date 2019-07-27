/*
 * @(#) TryColumnAnalysis.java
 */

package net.pwall.doric;

import java.io.File;
import java.io.FileInputStream;

import net.pwall.json.JSONArray;
import net.pwall.json.JSONFormat;
import net.pwall.util.CSV;

public class TryColumnAnalysis {

    public static void main(String[] args) {
        try {
            File csvFile = new File(args[0]);
            CSV csv = new CSV(new FileInputStream(csvFile));
            if (!csv.hasNext())
                throw new IllegalArgumentException("CSV Header line missing");
            CSV.Record record = csv.next(); // skip header line
            int width = record.getWidth();
            ColumnAnalysis[] columns = new ColumnAnalysis[width];
            for (int i = 0; i < width; i++)
                columns[i] = new ColumnAnalysis(record.getField(i));
            int rowCount = 0;
            while (csv.hasNext()) {
                record = csv.next();
                rowCount++;
                if (width != record.getWidth())
                    throw new IllegalArgumentException("CSV number of fields inconsistent, row " +
                            rowCount + "; expected " + width + ", was " + record.getWidth());
                for (int i = 0; i < width; i++)
                    columns[i].analyse(record.getField(i));
            }
            csv.close();
            JSONArray array = new JSONArray(width);
            for (int i = 0; i < width; i++)
                array.add(columns[i].resolveJSON());
            JSONFormat format = new JSONFormat();
            System.out.println(format.format(array));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
