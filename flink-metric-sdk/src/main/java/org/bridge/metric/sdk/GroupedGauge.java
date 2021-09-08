package org.bridge.metric.sdk;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.metrics.Gauge;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * 自定义 Gauge 指标，结构为 Map<String, Map<String, Long>>。
 * key: groupName + identifiedName
 * value: String -> Long 格式的 map, 代表 metric 的 name 与 value 的映射;
 **/
public class GroupedGauge implements Gauge<String> {
    private final String FORMAT = "%s.%s";
    HashMap<String, GaugeElement> totalMap = new HashMap<>(8);

    /**
     * getValue 被 flink metric 框架调用，这里使用 toJsonString() 使用 json 格式数据，便于 rest API 获取时便于解析
     */
    @Override
    public String getValue() {
        return JSONObject.toJSONString(totalMap.values());
    }

    public void add(String groupName, String metricName, Long value) {
        add(groupName, metricName, value, null);
    }

    public void add(GaugeElement others) {
        add(others.getGroupName(), others.getMetricName(), others.getValue(), others.getTags());
    }

    public void add(String group, Map<String, Long> keyValues) {
        keyValues.forEach(
                (k, v) -> {
                    GaugeElement element = GaugeElement.fromIdentifiedNameAndValue(group, k, v);
                    if (element != null) {
                        add(element);
                    }
                }
        );
    }

    public void add(String groupName, String metricName, Long value, Map<String, String> tags) {
        String key = String.format(FORMAT, groupName, GaugeElement.concatAsIdentifiedName(metricName, tags));
        totalMap.computeIfAbsent(key, k -> new GaugeElement(groupName, metricName, 0, tags));
        totalMap.get(key).inc(value);
    }

    public void add(String group, Map<String, Long> keyValues, Map<String, String> tags) {
        keyValues.forEach(
                (k, v) -> {
                    add(group, k, v, tags);
                }
        );
    }


    public void remove(String group) {
        Iterator<Map.Entry<String, GaugeElement>> iterator = totalMap.entrySet().iterator();
        while (iterator.hasNext()) {
            String key = iterator.next().getKey();
            if (key.startsWith(group)) {
                iterator.remove();
            }
        }
    }

    public void remove(String groupName, String metricName) {
        String key = String.format(groupName, metricName);
        if (totalMap.get(key) != null) {
            totalMap.remove(key);
        }
    }

    @Override
    public String toString() {
        return JSONObject.toJSONString(totalMap);
    }
}
