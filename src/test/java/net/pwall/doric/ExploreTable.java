/*
 * @(#) ExploreTable.java
 */

package net.pwall.doric;

import java.io.IOException;

import net.pwall.util.Strings;

public class ExploreTable {

    public static void main(String[] args) {
        try {
            Table table = new Table();
            table.open("./target/super");
            for (int i = 0; i < 9; i++)
                System.out.println(gettRow(table, i));
            System.out.println();
            System.out.println();
            System.out.println("... and the last 10");
            System.out.println();
            System.out.println();
            for (int i = table.getRowCount() - 19; i < table.getRowCount(); i++)
                System.out.println(gettRow(table, i));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static String gettRow(Table table, int rowNumber) throws IOException {
        Row row = table.getRow(rowNumber);
        int n = table.getColumnCount();
        String[] cols = new String[table.getColumnCount()];
        for (int j = 0; j < n; j++)
            cols[j] = row.getString(j);
        return Strings.join(cols, ',');
    }

}
