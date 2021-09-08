import com.alibaba.fastjson.JSONObject;
import org.bridge.metric.sdk.GaugeElement;

import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class TestMap {
    public static void main(String[] args) {
        ConcurrentHashMap<String, Long> map = new ConcurrentHashMap<String, Long>();
        Set<String> treeSet = new TreeSet<String>();

        map.put("name", 100L);
        map.put("xjq", 510L);
        map.put("xmf", 520L);
        map.put("cccc", 520L);
        System.out.println("map:" + map);
        System.out.println(String.format("%s.%s", "xx", "jj"));

        Iterator<Map.Entry<String, Long>> iterable = map.entrySet().iterator();
        while (iterable.hasNext()) {
            Map.Entry<String, Long> entry = iterable.next();
            if (entry.getKey() == "xjq") {
                iterable.remove();
            }
        }
        System.out.println("map:" + map);

        Iterator<String> iterator = map.keySet().iterator();
        while (iterator.hasNext()) {
            if (iterator.next().equals("name")) {
                iterator.remove();
            }
        }
        System.out.println("map:" + map);

        for (String name : map.keySet()) {
            if (name == "xmf") {
                map.remove("xmf");
            }
        }

        System.out.println("map:" + map);

        System.out.println(LocalDateTime.now());

        JSONObject object = new JSONObject();
        JSONObject sonObj = new JSONObject();
        sonObj.put("name", "xxx");
        sonObj.put("value", 1);
        sonObj.put("tag1", "keyword");
        object.put("20210830", sonObj);

        System.out.println(object.toString());

        GaugeElement element = new GaugeElement("20210903", "record",
        100);

        System.out.println(JSONObject.toJSONString(element));

        System.out.println(element);

        System.out.println(element.toString());

        HashMap tag = new HashMap<String, String>();
        tag.put("tag1", "value1");
        System.out.println(new GaugeElement("20210910", "rps", 400, tag).toString());

        String a ="\\.";
        String[] arr = "ddd".split(a);
        String[] arr1 = "ddd.xx".split(a);
        System.out.println(arr.length + arr[0]);
        System.out.println(arr1.length + arr1[0]);

        System.out.println(map);
        map.merge("xxx", 12L, Long::sum);
        System.out.println(map);
        map.merge("xxx", 12L, Long::sum);
        System.out.println(map);


    }


}
