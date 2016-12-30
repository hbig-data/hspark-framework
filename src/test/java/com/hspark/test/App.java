package com.hspark.test;

import com.alibaba.fastjson.JSONObject;
import kafka.metrics.KafkaCSVMetricsReporter;
import kafka.metrics.KafkaMetricsConfig;
import kafka.metrics.KafkaMetricsReporter;
import kafka.metrics.KafkaMetricsReporterMBean;
import kafka.server.KafkaConfig;
import kafka.utils.Utils;
import net.sf.json.JSON;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.Iterator;
import scala.collection.Seq;
import scala.collection.mutable.StringBuilder;

import java.io.Serializable;
import java.util.Properties;

/**
 * @author Rayn on 2016/7/2.
 * @email liuwei412552703@163.com.
 */
public class App {
    private static final Logger log = LoggerFactory.getLogger(App.class);

    @Test
    public void testApp() throws Exception {


        Person person = new Person();
        person.setAge(10);
        person.setUname("ryan");
        person.setUpass("123456");

        String json = JSONObject.toJSONString(person);
        log.info("{}", json);

    }
}


class Person implements Serializable{
    private String uname;
    private String upass;

    private Integer age;

    public Person() {
    }

    public String getUname() {
        return uname;
    }

    public void setUname(String uname) {
        this.uname = uname;
    }

    public String getUpass() {
        return upass;
    }

    public void setUpass(String upass) {
        this.upass = upass;
    }

    public Integer getAge() {
        return age;
    }

    public void setAge(Integer age) {
        this.age = age;
    }
}
