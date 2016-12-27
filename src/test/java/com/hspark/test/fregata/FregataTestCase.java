package com.hspark.test.fregata;

import breeze.linalg.Vector;
import fregata.spark.data.LibSvmReader;
import fregata.spark.model.classification.LogisticRegression;
import fregata.spark.model.classification.LogisticRegressionModel;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.rdd.RDD;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

/**
 * @Author Rayn
 * @Vendor liuwei412552703@163.com
 * Created by Rayn on 2016/12/21 10:45.
 */
public class FregataTestCase {
    private static final Logger LOG = LoggerFactory.getLogger(FregataTestCase.class);


    @Before
    public void setUp() throws Exception {
//        LibSvmReader.read(sc, trainPath, numFeatures.toInt);
//        LibSvmReader.read(sc, testPath, numFeatures.toInt);
    }

    @After
    public void tearDown() throws Exception {

    }

    @Test
    public void testName() throws Exception {

        final JavaSparkContext sparkContext = new JavaSparkContext("local[2]", "test");

        final Tuple2<Object, RDD<Tuple2<Vector<Object>, Object>>> trainData = LibSvmReader.read(sparkContext.sc(), "", 7, 10);

        /**
         * 针对训练样本训练Logsitic Regression 模型
         */
        LogisticRegressionModel regressionModel = LogisticRegression.run(trainData._2(), 10, 10);



    }
}
