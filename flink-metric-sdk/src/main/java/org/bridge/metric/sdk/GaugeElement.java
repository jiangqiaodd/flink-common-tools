package org.bridge.metric.sdk;

import com.alibaba.fastjson.JSONObject;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class GaugeElement implements Serializable {
    public static final String DOT = ".";
    public static final String DOT_SPLITTER = "\\.";
    private String groupName;
    private String metricName;
    private long value;
    private Map<String, String> tags;


    public GaugeElement(String groupName, String metricName, long value) {
        this.groupName = groupName;
        this.metricName = metricName;
        this.value = value;
    }

    public GaugeElement(String groupName, String metricName, long value, Map<String, String> tags) {
        this(groupName, metricName, value);
        if (tags != null) {
            this.tags = new HashMap<String, String>(8);
            tags.forEach(
                    this.tags::put
            );
        }
    }

    // 针对将 tag 内容以 $name.$value 格式拼接到 metricName 后的 identifiedName
    public static GaugeElement fromIdentifiedNameAndValue(String groupName, String identifiedName, long value) {
        String[] arr = identifiedName.split(DOT_SPLITTER);
        String metricName = arr[0];
        int length = arr.length;
        if (metricName.trim().isEmpty()) {
            return null;
        }

        if (length == 1) {
            return new GaugeElement(groupName, metricName, value);
        }

        HashMap<String, String> tag = new HashMap<String, String>(8);
        for (int index = 1; index + 1 < length; index += 2) {
            tag.put(arr[index], arr[index + 1]);
        }
        return new GaugeElement(groupName, metricName, value, tag);
    }

    // 将 tag 的键值内容，追加到 metricName 中，形成组内唯一标识
    public static String concatAsIdentifiedName(String metricName, Map<String, String> tags) {
        StringBuilder builder = new StringBuilder(metricName);
        if (tags != null) {
            tags.forEach(
                    (k, v) -> {
                        builder.append(DOT).append(k).append(DOT).append(v);
                    }
            );
        }
        return builder.toString();
    }

    public void inc(long value) {
        this.value += value;
    }

    public String toString() {
        return JSONObject.toJSONString(this);
    }

    public String getGroupName() {
        return groupName;
    }

    public void setGroupName(String groupName) {
        this.groupName = groupName;
    }

    public String getMetricName() {
        return metricName;
    }

    public void setMetricName(String metricName) {
        this.metricName = metricName;
    }

    public long getValue() {
        return value;
    }

    public void setValue(long value) {
        this.value = value;
    }

    public Map<String, String> getTags() {
        return tags;
    }

    public void setTags(Map<String, String> tags) {
        this.tags = tags;
    }
}
