/*
 * @(#) Doric.java
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
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import net.pwall.doric.columninput.ColumnInput;
import net.pwall.json.JSON;
import net.pwall.json.JSONArray;
import net.pwall.json.JSONObject;
import net.pwall.util.UserError;

/**
 * Doric database executor class.
 *
 * @author  Peter Wall
 */
public class Doric {

    private static BufferPool bufferPool = null;

    public static void main(String[] args) {
        try {
            String name = null;
            File csvFile = null;
            File outFile = null;
            Integer maxUnique = null;
            Boolean headers = null;
            Boolean showMetadata = null;
            for (int i = 0, n = args.length; i< n; i++) {
                String arg = args[i];
                switch (arg) {
                case "--name":
                    if (name != null)
                        throw new UserError("Duplicate --name switch");
                    name = getArg(args, ++i, "--name with no name");
                    break;
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
            if (name == null)
                throw new UserError("--name not specified");
            if (csvFile == null)
                throw new UserError("--csv not specified");
            if (headers == null)
                headers = Boolean.FALSE;
            if (showMetadata == null)
                showMetadata = Boolean.FALSE;
            if (outFile != null && !outFile.exists() && !outFile.mkdirs())
                throw new UserError("Error creating output directory");
            Ingestor.ingest(name, csvFile, headers, outFile, showMetadata, maxUnique);
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

    public static synchronized BufferPool getBufferPool() {
        if (bufferPool == null)
            bufferPool = new BufferPool(40, 8192, false); // TODO find a way to parameterise these values
        return bufferPool;
    }

}
