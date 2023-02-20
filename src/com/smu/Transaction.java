package com.smu;

import java.util.ArrayList;
import java.util.List;

/**
 * com.smu.Transaction
 *
 * @author T.W 11/27/22
 */
public class Transaction {
    private Integer tid;
    private final List<Integer> local;
    private boolean finished;
    private boolean blocked;
    private List<String> commandLines;
    private int currentCommandId;
    private List<RollbackLog> rollbackLogs;

    /**
     * Create a transaction with k local variables (reference from 0 to k-1)
     *
     * @param k local variables
     */
    public Transaction(int k) {
        //Initialization
        this.currentCommandId = 1;
        local = new ArrayList<>();
        rollbackLogs = new ArrayList<>();
        for (int i = 0; i < k; i++) {
            local.add(0);
        }
    }

    /**
     * Read the source-th number from the database and copy it local[dest]
     *
     * @param db     database
     * @param source index of database
     * @param dest   index of local
     */
    public void read(Database db, int source, int dest) {
        try {
            local.set(dest, db.read(source));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Write the value of the local[source] from the to the dest-th number in the database.
     *
     * @param db     database
     * @param source local value
     * @param dest   index of database
     */
    public void write(Database db, int source, int dest) {
        try {
            db.write(dest, local.get(source));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void add(int source, int v) {
        local.set(source, local.get(source) + v);
    }

    public void mult(int source, int v) {
        local.set(source, local.get(source) * v);
    }

    public void copy(int s1, int s2) {
        local.set(s1, local.get(s2));
    }

    public void combine(int s1, int s2) {
        local.set(s1, local.get(s1) + local.get(s2));
    }

    public void display() {
        for (Integer integer : local) {
            System.out.println(integer + " ");
        }
    }

    public boolean isBlocked() {
        return blocked;
    }

    public void setBlocked(boolean blocked) {
        this.blocked = blocked;
    }

    public boolean isFinished() {
        return finished;
    }

    public void setFinished(boolean finished) {
        this.finished = finished;
    }

    public List<String> getCommandLines() {
        return commandLines;
    }

    public void setCommandLines(List<String> commandLines) {
        this.commandLines = commandLines;
    }

    public Integer getTid() {
        return tid;
    }

    public void setTid(Integer tid) {
        this.tid = tid;
    }

    public int getCurrentCommandId() {
        return currentCommandId;
    }

    public void setCurrentCommandId(int currentCommandId) {
        this.currentCommandId = currentCommandId;
    }

    public List<RollbackLog> getRollbackLogs() {
        return rollbackLogs;
    }

    public void setRollbackLogs(List<RollbackLog> rollbackLogs) {
        this.rollbackLogs = rollbackLogs;
    }

    public List<Integer> getLocal() {
        return local;
    }
}
