package com.hspark.test;

import kafka.metrics.KafkaCSVMetricsReporter;
import kafka.metrics.KafkaMetricsConfig;
import kafka.metrics.KafkaMetricsReporter;
import kafka.metrics.KafkaMetricsReporterMBean;
import kafka.server.KafkaConfig;
import kafka.utils.Utils;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.Iterator;
import scala.collection.Seq;
import scala.collection.mutable.StringBuilder;

import java.util.Properties;

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
