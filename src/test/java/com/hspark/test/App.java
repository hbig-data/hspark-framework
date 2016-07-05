package com.hspark.test;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Rayn on 2016/7/2.
 * @email liuwei412552703@163.com.
 */
public class App {
    private static final Logger log = LoggerFactory.getLogger(App.class);

    @Test
    public void testApp() throws Exception {

        for (int i = 0; i < 1_000_000; i++) {
            log.info("app log test:{}", i);
        }


    }
}
