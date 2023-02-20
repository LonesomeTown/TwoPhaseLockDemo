package com.smu;

/**
 * com.smu.RollbackLog
 *
 * @author T.W 12/4/22
 */
public class RollbackLog {
    /**
     * kth numbers in database
     */
    private int k;
    private int originalValue;

    public RollbackLog(int k, int originalValue) {
        this.k = k;
        this.originalValue = originalValue;
    }

    public int getK() {
        return k;
    }

    public void setK(int k) {
        this.k = k;
    }

    public int getOriginalValue() {
        return originalValue;
    }

    public void setOriginalValue(int originalValue) {
        this.originalValue = originalValue;
    }
}
