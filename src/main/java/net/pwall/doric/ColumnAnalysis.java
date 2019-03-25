/*
 * @(#) ColumnAnalysis.java
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

import java.time.LocalDate;
import java.time.format.DateTimeParseException;
import java.util.Map;
import java.util.function.Consumer;

import net.pwall.json.JSONArray;
import net.pwall.json.JSONObject;
import net.pwall.util.SortedListMap;

public class ColumnAnalysis {

    public static final int defaultMaxUniqueValues = 256;

    private String name;

    private int itemCount;
    private boolean nullable;

    private MinMax<Integer> widthMinMax;

    private boolean couldBeFloat;
    private MinMax<Double> floatMinMax;
    private Max<Integer> decimalMax;

    private boolean couldBeInt;
    private MinMax<Long> intMinMax;

    private boolean couldBeDate;

    private int maxUniqueValues;
    private Map<String, Long> uniqueValues;
    // Implementation note - once the maximum is reached, this variable is set to null.
    // That can be used to determine that the column has more than the maximum

    public ColumnAnalysis(String name, int maxUniqueValues) {

        this.name = name;

        itemCount = 0;
        nullable = false;

        widthMinMax = new MinMax<>();

        couldBeFloat = true;
        floatMinMax = new MinMax<>();
        decimalMax = new Max<>();

        couldBeInt = true;
        intMinMax = new MinMax<>();

        couldBeDate = true;

        this.maxUniqueValues = maxUniqueValues;
        uniqueValues = new SortedListMap<>(maxUniqueValues);

    }

    public ColumnAnalysis(String name) {
        this(name, defaultMaxUniqueValues);
    }

    public void analyse(String value) {

        itemCount++;

        if (value.length() == 0) { // note - should we use null as indicator of null?
            nullable = true;
            return;
        }

        widthMinMax.accept(value.length());

        if (couldBeFloat) {
            try {
                floatMinMax.accept(Double.valueOf(value));
                int decimals = 0;
                int i = value.indexOf('.');
                if (i >= 0) {
                    while (++i < value.length()) {
                        char ch = value.charAt(i);
                        if (ch < '0' || ch > '9')
                            break;
                        decimals++;
                    }
                }
                decimalMax.accept(decimals);
                couldBeDate = false;
            }
            catch (NumberFormatException nfe) {
                couldBeFloat = false;
            }
        }

        if (couldBeInt) {
            try {
                intMinMax.accept(Long.valueOf(value));
                couldBeDate = false;
            }
            catch (NumberFormatException nfe) {
                couldBeInt = false;
            }
        }

        if (couldBeDate) {
            try {
                LocalDate localDate = LocalDate.parse(value);
                intMinMax.accept(localDate.toEpochDay());
                couldBeInt = false;
                couldBeFloat = false;
            }
            catch (DateTimeParseException e) {
                couldBeDate = false;
            }
        }

        if (uniqueValues != null && !uniqueValues.containsKey(value)) {
            if (uniqueValues.size() < maxUniqueValues)
                uniqueValues.put(value, -1L);
            else
                uniqueValues = null;
        }

        // At this point, we can check for other data types, like times, currency codes,
        // IATA city codes, ...

    }

    public Column resolve() {
        Column column = new Column(name);
        if (itemCount == 0 || !widthMinMax.isInitialised()) {
            column.setType(Column.Type.undetermined);
            column.setStorageType(Column.StorageType.none);
        }
        else if (couldBeInt) {
            column.setType(Column.Type.integer);
            column.setStorageType(getIntStorageType(intMinMax.getMinimum(), intMinMax.getMaximum(), nullable));
            // TODO - do we need minimum and maximum values?
            column.setMinInt(intMinMax.getMinimum());
            column.setMaxInt(intMinMax.getMaximum());
        }
        else if (couldBeFloat) {
            column.setType(Column.Type.floating);
            int maxDecimals = decimalMax.getMaximum();
            if (maxDecimals <= Table.maxDecimalShift) {
                long minShifted = Math.round(floatMinMax.getMinimum() * Table.decimalShifts[maxDecimals]);
                long maxShifted = Math.round(floatMinMax.getMaximum() * Table.decimalShifts[maxDecimals]);
                column.setStorageType(getIntStorageType(minShifted, maxShifted, nullable));
                column.setDecimalShift(maxDecimals);
            }
            else {
                column.setStorageType(Column.StorageType.float64);
            }
            // TODO - do we need minimum and maximum values, and maxDecimals?
            column.setMinFloat(floatMinMax.getMinimum());
            column.setMaxFloat(floatMinMax.getMaximum());
            column.setMaxDecimals(maxDecimals);
        }
        else if (couldBeDate) {
            column.setType(Column.Type.date);
            column.setStorageType(getIntStorageType(intMinMax.getMinimum(), intMinMax.getMaximum(), nullable));
            // TODO - do we need minimum and maximum values?
            column.setMinInt(intMinMax.getMinimum());
            column.setMaxInt(intMinMax.getMaximum());
        }
        else {
            column.setType(Column.Type.undetermined);
            column.setStorageType(Column.StorageType.bytes);
            int numOccurrences = uniqueValues == null ? itemCount : uniqueValues.size();
            int maxWidth = widthMinMax.getMaximum();
            column.setDataOffsetStorageType(getIntStorageType(0, numOccurrences * maxWidth, nullable));
            column.setDataLengthStorageType(getIntStorageType(0, maxWidth, nullable));
        }
        checkUnique(column);
        // TODO - do we need minimum and maximum lengths?
        column.setMinWidth(widthMinMax.getMinimum());
        column.setMaxWidth(widthMinMax.getMaximum());
        column.setNullable(nullable);
        return column;
    }

    public JSONObject resolveJSON() {
        JSONObject result = JSONObject.create().putValue("name", name);
        if (itemCount == 0 || !widthMinMax.isInitialised()) {
            result.putValue("type", "null");
            result.putValue("storageType", "none");
        }
        else if (couldBeInt) {
            result.putValue("type", "integer");
            result.putValue("storageType",
                    getIntStorageType(intMinMax.getMinimum(), intMinMax.getMaximum(), nullable).toString());
            result.putValue("minInt", intMinMax.getMinimum());
            result.putValue("maxInt", intMinMax.getMaximum());
        }
        else if (couldBeFloat) {
            result.putValue("type", "floating");
            int maxDecimals = decimalMax.getMaximum();
            result.putValue("maxDecimals", maxDecimals);
            if (maxDecimals <= Table.maxDecimalShift) {
                long minShifted = Math.round(floatMinMax.getMinimum() * Table.decimalShifts[maxDecimals]);
                long maxShifted = Math.round(floatMinMax.getMaximum() * Table.decimalShifts[maxDecimals]);
                result.putValue("storageType", getIntStorageType(minShifted, maxShifted, nullable).toString());
                result.putValue("decimalShift", maxDecimals);
            }
            else {
                result.putValue("storageType", "float64");
            }
            result.putValue("minFloat", floatMinMax.getMinimum());
            result.putValue("maxFloat", floatMinMax.getMaximum());
        }
        else if (couldBeDate) {
            result.putValue("type", "date");
            result.putValue("storageType",
                    getIntStorageType(intMinMax.getMinimum(), intMinMax.getMaximum(), nullable).toString());
            result.putValue("minInt", intMinMax.getMinimum());
            result.putValue("maxInt", intMinMax.getMaximum());
        }
        else {
            result.putValue("type", "undetermined");
            result.putValue("storageType", "bytes");
            int numOccurrences = uniqueValues == null ? itemCount : uniqueValues.size();
            int maxWidth = widthMinMax.getMaximum();
            result.putValue("offsetStorageType",
                    getIntStorageType(0, numOccurrences * maxWidth, nullable).toString());
            result.putValue("lengthStorageType", getIntStorageType(0, maxWidth, nullable).toString());
        }
        result.putValue("minWidth", widthMinMax.getMinimum());
        result.putValue("maxWidth", widthMinMax.getMaximum());
        if (nullable)
            result.putValue("nullable", true);
        checkUnique(result);
        return result;
    }

    private static Column.StorageType getIntStorageType(long min, long max, boolean nullable) {
        if (nullable) {
            min <<= 1;
            max <<= 1;
        }
        if (min >= 0 && max <= 0xFF)
            return Column.StorageType.uint8;
        if (min >= Byte.MIN_VALUE && max <= Byte.MAX_VALUE)
            return Column.StorageType.int8;
        if (min >= 0 && max <= 0xFFFF)
            return Column.StorageType.uint16;
        if (min >= Short.MIN_VALUE && max <= Short.MAX_VALUE)
            return Column.StorageType.int16;
        if (min >= 0 && max <= 0xFFFFFFFFL)
            return  Column.StorageType.uint32;
        if (min >= Integer.MIN_VALUE && max <= Integer.MAX_VALUE)
            return Column.StorageType.int32;
        return Column.StorageType.int64;
    }

    private void checkUnique(Column column) {
        int uniqueValueCount = uniqueValues == null ? 0 : uniqueValues.size();
        if (uniqueValueCount == 1) {
            column.setStorageType(Column.StorageType.constant);
            String constantValue = uniqueValues.keySet().iterator().next();
            if (column.getType() == Column.Type.integer)
                column.setConstantValueInt(Long.valueOf(constantValue));
            else if (column.getType() == Column.Type.floating)
                column.setConstantValueFloat(Double.valueOf(constantValue));
            else
                column.setConstantValue(constantValue);
        }
        else if (uniqueValueCount > 0 && uniqueValueCount <= maxUniqueValues) {
            if (column.getType() == Column.Type.integer) {
                Map<Long, Long> integerUniqueValues = new SortedListMap<>(uniqueValueCount);
                for (Map.Entry<String, Long> entry : uniqueValues.entrySet())
                    integerUniqueValues.put(Long.valueOf(entry.getKey()), entry.getValue());
                column.setIntegerUniqueValues(integerUniqueValues);
            }
            else if (column.getType() == Column.Type.floating) {
                Map<Double, Long> floatUniqueValues = new SortedListMap<>(uniqueValueCount);
                for (Map.Entry<String, Long> entry : uniqueValues.entrySet())
                    floatUniqueValues.put(Double.valueOf(entry.getKey()), entry.getValue());
                column.setFloatUniqueValues(floatUniqueValues);
            }
            else
                column.setUniqueValues(uniqueValues);
        }
    }

    private void checkUnique(JSONObject result) {
        int uniqueValueCount = uniqueValues == null ? 0 : uniqueValues.size();
        if (uniqueValueCount == 1) {
            result.putValue("storageType", "constant");
            String constantValue = uniqueValues.keySet().iterator().next();
            if ("integer".equals(result.getString("type")))
                result.putValue("integerValue", Long.valueOf(constantValue));
            else if ("floating".equals(result.getString("type")))
                result.putValue("floatValue", Double.valueOf(constantValue));
            else
                result.putValue("value", constantValue);
        }
        else if (uniqueValueCount > 0 && uniqueValueCount <= maxUniqueValues) {
            result.putValue("uniqueValues", uniqueValueCount);
            JSONArray array = new JSONArray(uniqueValueCount);
            if ("integer".equals(result.getString("type"))) {
                Map<Long, Long> integerUniqueValues = new SortedListMap<>(uniqueValueCount);
                for (Map.Entry<String, Long> entry : uniqueValues.entrySet())
                    integerUniqueValues.put(Long.valueOf(entry.getKey()), entry.getValue());
                for (Long key : integerUniqueValues.keySet())
                    array.addValue(key);
                result.put("integerValues", array);
            }
            else if ("floating".equals(result.getString("type"))) {
                Map<Double, Long> floatUniqueValues = new SortedListMap<>(uniqueValueCount);
                for (Map.Entry<String, Long> entry : uniqueValues.entrySet())
                    floatUniqueValues.put(Double.valueOf(entry.getKey()), entry.getValue());
                for (Double key : floatUniqueValues.keySet())
                    array.addValue(key);
                result.put("floatValues", array);
            }
            else {
                for (String key : uniqueValues.keySet())
                    array.addValue(key);
                result.put("values", array);
            }
        }
    }

    public static class Max<T extends Comparable<T>> implements Consumer<T> {

        private T maximum;

        public Max() {
            maximum = null;
        }

        @Override
        public void accept(T t) {
            if (maximum == null) {
                maximum = t;
            }
            else {
                if (t.compareTo(maximum) > 0)
                    maximum = t;
            }
        }

        public boolean isInitialised() {
            return maximum != null;
        }

        public T getMaximum() {
            return maximum;
        }

    }

    public static class MinMax<T extends Comparable<T>> implements Consumer<T> {

        private T minimum;
        private T maximum;

        public MinMax() {
            minimum = null;
            maximum = null;
        }

        @Override
        public void accept(T t) {
            if (minimum == null) {
                minimum = t;
                maximum = t;
            }
            else {
                if (t.compareTo(minimum) < 0)
                    minimum = t;
                if (t.compareTo(maximum) > 0)
                    maximum = t;
            }
        }

        public boolean isInitialised() {
            return minimum != null;
        }

        public T getMinimum() {
            return minimum;
        }

        public T getMaximum() {
            return maximum;
        }

    }

}
