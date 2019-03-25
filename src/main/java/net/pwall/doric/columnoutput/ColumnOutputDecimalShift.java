/*
 * @(#) ColumnInputDecimalShift.java
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

package net.pwall.doric.columnoutput;

import java.io.IOException;

import net.pwall.doric.Column;

public class ColumnOutputDecimalShift implements ColumnOutput {

    private static final int[] intShift = { 1, 10, 100, 1000, 10000 };
    private static final double[] doubleShift = { 1.0, 10.0, 100.0, 1000.0, 10000.0 };

    private ColumnOutput intColumnOutput;
    private int decimalShift;

    public ColumnOutputDecimalShift(ColumnOutput intColumnOutput, int decimalShift) {
        this.intColumnOutput = intColumnOutput;
        this.decimalShift = decimalShift;
    }

    @Override
    public void putNull() throws IOException {
        intColumnOutput.putNull();
    }

    @Override
    public void putLong(long value) throws IOException {
        long longValue = value * intShift[decimalShift];
        intColumnOutput.putLong(longValue);
    }

    @Override
    public void putDouble(double value) throws IOException {
        long longValue = Math.round(value * doubleShift[decimalShift]); // round???
        intColumnOutput.putLong(longValue);
    }

    @Override
    public void putNumber(Number value) throws IOException {
        long longValue = Math.round(value.doubleValue() * doubleShift[decimalShift]); // round???
        intColumnOutput.putLong(longValue);
    }

    @Override
    public void putString(String value) throws IOException {
        try {
            putDouble(Double.valueOf(value));
        }
        catch (NumberFormatException nfe) {
            throw new IllegalArgumentException("Can't convert value to decimal: " + value);
        }
    }

    @Override
    public Column.FileData summariseAndClose() throws IOException {
        return intColumnOutput.summariseAndClose();
    }

}
