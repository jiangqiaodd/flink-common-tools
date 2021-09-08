package org.bridge.metric.test;

import org.apache.flink.metrics.Gauge;

import java.util.HashMap;
import java.util.Map;

public class MyGauge implements Gauge<Map<String, Map<String, Long>>> {
    Map<String, Map<String, Long>> totalMap = new HashMap<>();
    public MyGauge() {

    }

    @Override
    public Map<String, Map<String, Long>> getValue() {
        return totalMap;
    }

    public void add(String date, String key, Long value) {
        totalMap.computeIfAbsent(date, k -> new HashMap<String, Long>(8));
        totalMap.get(date).put(key, value);
    }
}
