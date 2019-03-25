/*
 * @(#) ColumnStorageType.java
 */

package net.pwall.doric;

import net.pwall.doric.columninput.ColumnInput;
import net.pwall.doric.columnoutput.ColumnOutput;

/**
 * The {@code ColumnStorageType} is a form of {@code enum} the allows the creation of the appropriate
 * {@link ColumnInput} and {@link ColumnOutput} objects.
 */
public abstract class ColumnStorageType {

    public static final Bytes8L8 bytes = new Bytes8L8();

    /*
    undetermined,
        none,
        constant,
        int8,
        int16,
        int32,
        int64,
        uint8,
        uint16,
        uint32,
        float64,
        bytes
     */

    private static class Bytes8L8 extends ColumnStorageType {

    }

    // TODO - complete this or abandon it

}
