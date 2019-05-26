/*
 * @(#) QuerySpliterator.java
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

import java.util.Objects;
import java.util.Spliterator;
import java.util.function.Consumer;

import net.pwall.doric.Row;

public class QuerySpliterator implements Spliterator<Row> {

    private Query query;
    private int index;
    private int limit;

    public QuerySpliterator(Query query, int index, int limit) {
        this.query = query;
        this.index = index;
        this.limit = limit;
    }

    @Override
    public boolean tryAdvance(Consumer<? super Row> action) {
        Objects.requireNonNull(action);
        if (index < limit) {
            action.accept(query.getRow(index++));
            return true;
        }
        return false;
    }

    @Override
    public Spliterator<Row> trySplit() {
        if (!query.isNumRowsKnown())
            return null; // TODO review this
        int size = limit - index;
        if (size < 2)
            return null;
        int oldIndex = index;
        index += size >>> 1;
        return new QuerySpliterator(query, oldIndex, index);
    }

    @Override
    public long estimateSize() {
        return limit - index; // TODO what to do if number of rows not known?
    }

    @Override
    public int characteristics() {
        return ORDERED | IMMUTABLE | (query.isNumRowsKnown() ? (SIZED | SUBSIZED) : 0);
    }

    @Override
    public void forEachRemaining(Consumer<? super Row> action) {
        Objects.requireNonNull(action);
        while (index < limit)
            action.accept(query.getRow(index++));
    }

}
