/*
 * @(#) ExploreTable.java
 */

package net.pwall.doric;

import java.io.IOException;

import net.pwall.util.Strings;

public class ExploreTable {

    public static void main(String[] args) {
        try {
            Table table = Table.open("./target/super");
            for (int i = 0; i < 9; i++)
                System.out.println(getRow(table, i));
            System.out.println();
            System.out.println();
            System.out.println("... and the last 10");
            System.out.println();
            System.out.println();
            for (int i = table.getNumRows() - 19; i < table.getNumRows(); i++)
                System.out.println(getRow(table, i));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static String getRow(Table table, int rowNumber) throws IOException {
        Row row = table.getRow(rowNumber);
        int n = table.getNumColumns();
        String[] cols = new String[table.getNumColumns()];
        for (int j = 0; j < n; j++)
            cols[j] = row.getString(j);
        return Strings.join(cols, ',');
    }

}
