package com.smu;

import java.util.ArrayList;
import java.util.List;

/**
 * com.smu.Database
 *
 * @author T.W 11/27/22
 */
public class Database {
    private final List<Integer> data;

    Database(int k, boolean nonzero) {
        data = new ArrayList<>();
        for (int i = 0; i < k; i++)
            if (nonzero) {
                data.add(i + 1);
            } else {
                data.add(0);
            }
    }

    public int read(int k) {
        return data.get(k);
    }

    public void write(int k, int w) {
        data.set(k, w);
    }

    public void print() {
        System.out.println(data);
    }
}
