/*
 * @(#) TestCSV.java
 */

package net.pwall.util;

import java.io.FileInputStream;
import java.io.IOException;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

public class TestCSV {

    @Test
    public void testCreateCSV() throws IOException {
        try (CSV csv = new CSV(new FileInputStream("./src/test/resources/test1.csv"))) {
            assertTrue(csv.hasNext());
            CSV.Record record = csv.next();
            assertEquals(4, record.getWidth());
            assertEquals("col1", record.getField(0));
            assertEquals("col2", record.getField(1));
            assertEquals("col3", record.getField(2));
            assertEquals("col4", record.getField(3));
            assertTrue(csv.hasNext());
            record = csv.next();
            assertEquals(4, record.getWidth());
            assertEquals("1", record.getField(0));
            assertEquals("Andy", record.getField(1));
            assertEquals("Who knows?", record.getField(2));
            assertEquals("25.5", record.getField(3));
            assertTrue(csv.hasNext());
            record = csv.next();
            assertEquals(4, record.getWidth());
            assertEquals("2", record.getField(0));
            assertEquals("Brian", record.getField(1));
            assertEquals("Whatever", record.getField(2));
            assertEquals("100", record.getField(3));
            assertTrue(csv.hasNext());
            record = csv.next();
            assertEquals(4, record.getWidth());
            assertEquals("3", record.getField(0));
            assertEquals("Chris", record.getField(1));
            assertEquals("Running out", record.getField(2));
            assertEquals("12.25", record.getField(3));
            assertTrue(csv.hasNext());
            record = csv.next();
            assertEquals(4, record.getWidth());
            assertEquals("4", record.getField(0));
            assertEquals("Dave", record.getField(1));
            assertEquals("of ideas", record.getField(2));
            assertEquals("6", record.getField(3));
            assertTrue(csv.hasNext());
            record = csv.next();
            assertEquals(4, record.getWidth());
            assertEquals("5", record.getField(0));
            assertEquals("Ed", record.getField(1));
            assertEquals("embedded, comma", record.getField(2));
            assertEquals("27", record.getField(3));
            assertTrue(csv.hasNext());
            record = csv.next();
            assertEquals(4, record.getWidth());
            assertEquals("6", record.getField(0));
            assertEquals("Frank", record.getField(1));
            assertEquals("embedded \"quotes\"", record.getField(2));
            assertEquals("9", record.getField(3));
            assertFalse(csv.hasNext());
        }
    }

}
