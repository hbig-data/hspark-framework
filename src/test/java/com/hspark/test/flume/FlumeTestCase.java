package com.hspark.test.flume;

import org.apache.flume.Event;
import org.apache.flume.api.RpcClient;
import org.apache.flume.api.RpcClientFactory;
import org.apache.flume.event.EventBuilder;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * @author Rayn
 * @email liuwei412552703@163.com
 * Created by Rayn on 2016/8/22 14:22.
 */
public class FlumeTestCase {

    private RpcClient client;

    private Properties props = new Properties();

    private String location = "flume.properties";

    @Before
    public void setUp() throws Exception {
        try {
            props.load(Thread.currentThread().getContextClassLoader().getResourceAsStream(location));
            client = RpcClientFactory.getInstance(props);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @After
    public void tearDown() throws Exception {
        this.client.close();

    }

    /**
     * 发送数据
     *
     * @throws Exception
     */
    @Test
    public void testSendData() throws Exception {

        String data = "测试数据发送";
        try {
            final Event event = EventBuilder.withBody(data.getBytes(Charset.forName("UTF-8")));

            Map<String, String> headers = new HashMap<String, String>();
            headers.put("Hello", "word");
            headers.put("timestamp", System.currentTimeMillis() + "");
            event.setHeaders(headers);

            client.append(event);

        } catch (Exception e) {
            client.close();
            client = null;
            client = RpcClientFactory.getInstance(props);
        }
    }
}
