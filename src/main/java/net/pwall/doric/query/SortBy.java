/*
 * @(#) SortBy.java
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

package net.pwall.doric.query;

import net.pwall.doric.Column;

public class SortBy implements Query {

    private Query source;
    private SortKey[] sortKeys;

    public SortBy(Query source, SortKey ... sortKeys) {
        this.source = source;
        this.sortKeys = sortKeys;
    }

    public void execute() {
        // TODO - to be completed...

        // first - check global options for multithreading; if so spawn threads
        // (defer this until later?)

        // check memory and decide on initial sorted block size

        // then merge the blocks


        // sorted blocks will be arrays of indexes into source; comparisons will
        // lookup each time (rely on buffer mechanism to optimise reads)

    }

    @Override
    public boolean isNumRowsKnown() {
        return source.isNumRowsKnown();
    }

    @Override
    public int getNumRows() {
        return source.getNumRows();
    }

    @Override
    public int getNumColumns() {
        return source.getNumColumns();
    }

    @Override
    public Column getColumn(int i) {
        return null; // TODO - to be completed...
    }

    public static class SortKey {

        private Expression expression;
        private boolean descending;

        public SortKey(Expression expression, boolean descending) {
            this.expression = expression;
            this.descending = descending;
        }

        public SortKey(Expression expression) {
            this(expression, false);
        }

        public Expression getExpression() {
            return expression;
        }

        public boolean isDescending() {
            return descending;
        }

    }

}
