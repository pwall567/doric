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

package net.pwall.doric.columninput;

import java.io.IOException;

import net.pwall.util.Strings;

class ColumnInputDecimalShift implements ColumnInput {

    private static final double[] shift = { 1.0, 0.1, 0.01, 0.001, 0.0001 };

    private ColumnInput intColumnInput;
    private int decimalShift;

    public ColumnInputDecimalShift(ColumnInput intColumnInput, int decimalShift) {
        if (decimalShift < 1 || decimalShift > 4)
            throw new IllegalArgumentException("decimal shift must be 1-4");
        this.intColumnInput = intColumnInput;
        this.decimalShift = decimalShift;
    }

    @Override
    public boolean isNull(int rowNumber) throws IOException {
        return intColumnInput.isNull(rowNumber);
    }

    @Override
    public Number getNumber(int rowNumber) throws IOException {
        return (double)intColumnInput.getLong(rowNumber) * shift[decimalShift];
    }

    @Override
    public long getLong(int rowNumber) {
        throw new IllegalStateException("Column can not return <long>");
    }

    @Override
    public String getString(int rowNumber) throws IOException {
        StringBuilder sb = new StringBuilder();
        Strings.appendLong(sb, intColumnInput.getLong(rowNumber));
        if (sb.charAt(0) == '-') {
            while (sb.length() < decimalShift + 2)
                sb.insert(1, '0');
        }
        else {
            while (sb.length() < decimalShift + 1)
                sb.insert(0, '0');
        }
        sb.insert(sb.length() - decimalShift, '.');
        return sb.toString();
    }

    @Override
    public void close() throws Exception {
        intColumnInput.close();
    }

}
