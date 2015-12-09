package ch.ethz;

import ch.ethz.tell.ScanQuery;

/**
 * Created by braunl on 04.12.15.
 */
public class TScanQuery extends ScanQuery {

    private final int scanMemoryManagerIndex;

    public TScanQuery(String tableName, int partitionKey, int scanMemoryManagerIndex) {
        super(tableName, partitionKey);
        this.scanMemoryManagerIndex = scanMemoryManagerIndex;
    }

    public TScanQuery(int partitionValue, TScanQuery other) {
        super(partitionValue, other);
        this.scanMemoryManagerIndex = other.getScanMemoryManagerIndex();
    }

    public final int getScanMemoryManagerIndex() {
        return scanMemoryManagerIndex;
    }
}
