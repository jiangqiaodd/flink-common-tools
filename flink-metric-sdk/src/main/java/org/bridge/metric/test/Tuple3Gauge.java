package org.bridge.metric.test;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.metrics.Gauge;

import java.util.HashMap;

public class Tuple3Gauge implements Gauge<HashMap<Tuple2<String, Integer>, Long>> {
    HashMap<Tuple2<String, Integer>, Long> map;

    public Tuple3Gauge(HashMap<Tuple2<String, Integer>, Long> map) {
        this.map = map;
    }

    public Tuple3Gauge() {
        map = new HashMap<Tuple2<String, Integer>, Long>();
    }

    @Override
    public HashMap<Tuple2<String, Integer>, Long> getValue() {
        return map;
    }

    public void inc(String tag, Integer date, Long value) {
        long old = 0L;
        Tuple2 tuple2 = Tuple2.of(tag, date);
        if (map.containsKey(Tuple2.of(tag, date))) {
            old = map.get(tuple2);
        }
        map.put(tuple2, old + value);
    }

    public void rm(String tag, Integer date) {
        map.remove(Tuple2.of(tag, date));
    }


    public void clear() {
        map.clear();
    }
}
