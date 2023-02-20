package com.smu;

/**
 * com.smu.LockInfo
 *
 * @author T.W 11/27/22
 */
public class LockInfo {
    /**
     * id of the transaction
     */
    private int tid;
    /**
     * index requested in database
     */
    private int k;
    /**
     * whether it is a s-lock, true: s-lock, false: x-lock
     */
    private boolean isSLock;

    public int getTid() {
        return tid;
    }

    public void setTid(int tid) {
        this.tid = tid;
    }

    public int getK() {
        return k;
    }

    public void setK(int k) {
        this.k = k;
    }

    public boolean isSLock() {
        return isSLock;
    }

    public void setSLock(boolean SLock) {
        isSLock = SLock;
    }
}
