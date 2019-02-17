/*
 * @(#) ColumnWriter.java
 */

package net.pwall.doric;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import net.pwall.util.Strings;

public class ColumnWriter {

    private OutputStream out;
    private long offset;

    public ColumnWriter(File directory, String name) throws FileNotFoundException {
        out = new BufferedOutputStream(new FileOutputStream(new File(directory, name)));
        offset = 0;
    }

    public long getOffset() {
        return offset;
    }

    public void writeInt8(int i) throws IOException {
        out.write(i);
        offset++;
    }

    public void writeInt16(int i) throws IOException {
        out.write(i >> 8);
        out.write(i);
        offset += 2;
    }

    public void writeInt32(int i) throws IOException {
        out.write(i >> 24);
        out.write(i >> 16);
        out.write(i >> 8);
        out.write(i);
        offset += 4;
    }

    public void writeInt64(long i) throws IOException {
        writeInt32((int)(i >> 32));
        writeInt32((int)i);
        offset += 8;
    }

    public void writeBytes(String str) throws IOException {
        out.write(Strings.toUTF8(str));
    }

    public void close() throws IOException {
        out.close();
    }

}
