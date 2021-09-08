package org.bridge.metric;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.HashMap;

/**
 * 测试: 将 exception 的 trace 信息输出到一个 StringWriter, 变成字符串数据便于输出
 */
public class PrintExceptionTrace {
    public static Logger LOG = LoggerFactory.getLogger("main");
    public static void main(String[] args) {
        try {
            int a = 0;
            HashMap map = null;
            System.out.println(map.get("sss"));
        } catch (Exception e) {
            e.printStackTrace();

            System.out.println("----");

            StringWriter stringWriter = new StringWriter();
            PrintWriter printWriter = new PrintWriter(stringWriter);
            e.printStackTrace(printWriter);
            printWriter.flush();
            System.out.println(stringWriter.toString());

            LOG.warn("exception: {}", stringWriter.toString());

        }
    }
}
