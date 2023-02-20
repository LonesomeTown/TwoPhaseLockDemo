package com.smu;

import java.util.*;

/**
 * com.smu.LockManager
 *
 * @author T.W 11/27/22
 */
public class LockManager {
    private final List<LockInfo> locks;

    /**
     * Constructor
     */
    public LockManager() {
        this.locks = new ArrayList<>();
    }

    /**
     * Request Lock
     *
     * @param tid     transaction Id
     * @param k       the k-th integer of the database where the lock is requested
     * @param isSLock true S-Lock, false X-Lock
     * @return int 1 granted,  0 not granted, -1 rollback
     */
    public int request(int tid, int k, boolean isSLock) {
        // find the first S-Lock which is holding on k but not the same tid
        Optional<LockInfo> firstSLock = locks.stream().filter(lockInfo ->
                lockInfo.getK() == k && lockInfo.getTid() != tid && lockInfo.isSLock()
        ).max(Comparator.comparing(LockInfo::getTid));
        // find the first X-Lock which is holding on k
        Optional<LockInfo> firstXLock = locks.stream().filter(lockInfo ->
                lockInfo.getK() == k && !lockInfo.isSLock()
        ).max(Comparator.comparing(LockInfo::getTid));
        if (!isSLock) {
            if (firstXLock.isPresent()) {
                //if tid is requesting an X-Lock, and there is already an X-Lock on k, then compare them
                if (tid == firstXLock.get().getTid()) {
                    //if tid already has an X-Lock, then grant
                    System.out.println("T" + tid + " request " + "X-Lock" + " on item " + k + " :G");
                    return 1;
                } else if (tid < firstXLock.get().getTid()) {
                    //if tid is ahead, then wait
                    System.out.println("T" + tid + " request " + "X-Lock" + " on item " + k + " :D");
                    return 0;
                } else {
                    //otherwise abort and rollback
                    return -1;
                }
            } else if (firstSLock.isPresent()) {
                //if tid is requesting an X-Lock, and there is already an other S-Lock on k, then compare them
                if (tid < firstSLock.get().getTid()) {
                    //if tid is ahead, then wait
                    System.out.println("T" + tid + " request " + "X-Lock" + " on item " + k + " :D");
                    return 0;
                } else {
                    //otherwise abort and rollback
                    System.out.println("T" + tid + " request " + "X-Lock" + " on item " + k + " :D");
                    return -1;
                }
            } else {
                //if there is no S-Lock or X-Lock on k, then add a new lock
                LockInfo lockInfo = new LockInfo();
                lockInfo.setTid(tid);
                lockInfo.setK(k);
                lockInfo.setSLock(false);
                locks.add(lockInfo);
                System.out.println("T" + tid + " request " + "X-Lock" + " on item " + k + " :G");
                return 1;
            }
        } else {
            if (firstXLock.isPresent()) {
                //if tid is requesting an S-Lock, and there is already an X-Lock on k, then compare them
                if (tid == firstXLock.get().getTid()) {
                    //if tid already has an X-Lock, then grant
                    System.out.println("T" + tid + " request " + "X-Lock" + " on item " + k + " :G");
                    return 1;
                } else if (tid < firstXLock.get().getTid()) {
                    //if tid is ahead, then wait
                    System.out.println("T" + tid + " request " + "S-Lock" + " on item " + k + " :D");
                    return 0;
                } else {
                    //otherwise abort and rollback
                    System.out.println("T" + tid + " request " + "S-Lock" + " on item " + k + " :D");
                    return -1;
                }
            } else {
                LockInfo lockInfo = new LockInfo();
                lockInfo.setTid(tid);
                lockInfo.setK(k);
                lockInfo.setSLock(true);
                locks.add(lockInfo);
                System.out.println("T" + tid + " request " + "S-Lock" + " on item " + k + " :G");
                return 1;
            }
        }

    }

    /**
     * Release all the locks that is held by transaction tid
     *
     * @param tid transaction Id
     * @return int the number of locks released
     */
    public int releaseAll(int tid) {
        int removeNums = 0;
        Iterator<LockInfo> iterator = locks.iterator();
        while (iterator.hasNext()) {
            if (tid == iterator.next().getTid()) {
                iterator.remove();
                removeNums++;
            }
        }
        return removeNums;
    }

    /**
     * Return all the locks that is being held by transaction tid.
     *
     * @param tid transaction Id
     * @return {@link List}<{@link Map}<{@link Integer}, {@link Boolean}>>
     */
    public List<Map<Integer, Boolean>> showLocks(int tid) {
        List<Map<Integer, Boolean>> results = new ArrayList<>();
        for (LockInfo lock : locks) {
            if (tid == lock.getTid()) {
                Map<Integer, Boolean> map = new HashMap<>();
                map.put(lock.getK(), lock.isSLock());
                results.add(map);
            }
        }
        return results;
    }

}
