package com.hspark.test;

import com.alibaba.fastjson.JSONObject;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.Serializable;

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

        File file = new File("E:/testtest.txt:js");
        file.createNewFile();

    }
}


class Person implements Serializable {
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
