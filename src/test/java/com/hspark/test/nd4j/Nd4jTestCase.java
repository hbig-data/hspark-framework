package com.hspark.test.nd4j;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.nd4j.jita.conf.CudaEnvironment;
import org.nd4j.linalg.api.iter.NdIndexIterator;
import org.nd4j.linalg.api.ndarray.INDArray;
import org.nd4j.linalg.api.ops.impl.transforms.Tanh;
import org.nd4j.linalg.api.rng.Random;
import org.nd4j.linalg.factory.Nd4j;
import org.nd4j.linalg.indexing.NDArrayIndex;
import org.nd4j.linalg.util.ArrayUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Rayn on 2016/8/28.
 * @email liuwei412552703@163.com.
 */
public class Nd4jTestCase {

    private static final Logger log = LoggerFactory.getLogger(Nd4jTestCase.class);

    @Before
    public void setUp() throws Exception {
        //If you have several GPUs, but your system is forcing you to use just one, there’s a solution.
        //just add it, as first line of your main() method.

//        try {
//            CudaEnvironment.getInstance().getConfiguration().allowMultiGPU(false);
//        } catch (Throwable e) {
//            e.printStackTrace();
//        }

    }

    @After
    public void tearDown() throws Exception {

    }

    /**
     * NdArray
     * <p/>
     * 创建2x2多维数组
     *
     * @throws Exception
     */
    @Test
    public void testNDArray() throws Exception {
        INDArray arr1 = Nd4j.create(new float[]{1, 2, 3, 4}, new int[]{2, 2});
        log.info("{}", arr1.toString());

        /**
         * 通过就地运算新增标量：
         */
        arr1.addi(1);
        log.info("{}", arr1);

        /**
         * 创建第二个数组（arr2）并将其加入第一个（arr1）
         */
        INDArray arr2 = Nd4j.create(new float[]{5, 6, 7, 8}, new int[]{2, 2});
        arr1.addi(arr2);
        log.info("{}", arr1);


    }

    /**
     * @throws Exception
     */
    @Test
    public void testTanh() throws Exception {

        INDArray x = Nd4j.rand(3, 2);    //input
        INDArray z = Nd4j.create(3, 2); //output
        Nd4j.getExecutioner().exec(new Tanh(x, z));
        Nd4j.getExecutioner().exec(Nd4j.getOpFactory().createTransform("tanh", x, z));

        log.info("Input Values :{}", x);
        log.info("Result Values : {}", z);


        double sum = x.sumNumber().doubleValue();
        log.info("Sum : {}", sum);


        //10 rows, 3 columns
        INDArray tenBy3 = Nd4j.ones(10, 3);
        INDArray sumRows = tenBy3.sum(0);

        //Output: [ 10.00, 10.00, 10.00]
        log.info("{}", sumRows);


        INDArray tens = Nd4j.zeros(3, 5).addi(10);
        log.info("{}", tens);
    }

    /**
     * Subset Operations on Arrays
     *
     * @throws Exception
     */
    @Test
    public void testSubset() throws Exception {
        INDArray random = Nd4j.rand(3, 3);
        log.info("{}", random);

        INDArray lastTwoRows = random.get(NDArrayIndex.interval(1, 3), NDArrayIndex.all());
        log.info("{}", lastTwoRows);

        INDArray twoValues = random.get(NDArrayIndex.point(1), NDArrayIndex.interval(0, 2));
        log.info("{}", twoValues);


        twoValues.addi(5.0);
        log.info("{}", twoValues);

    }

    /**
     * Random Array
     *
     * @throws Exception
     */
    @Test
    public void testRandmArray() throws Exception {
        //Random random = Nd4j.getRandom();
        //random.setSeed(10);

        INDArray array = Nd4j.create(5, 5).addi(1);
        log.info(" 5 × 5    : {}", array);

        double[] flat = ArrayUtil.flattenDoubleArray(array);
        log.info("{}", flat);

        INDArray diag = Nd4j.diag(array);
        log.info("{}", diag);


        INDArray eye = Nd4j.eye(5);
        log.info("{}", eye);

        /**
         * Nd4j.linspace(a, b, b-a+1)
         */
        INDArray linspace = Nd4j.linspace(1, 10, 1);
        INDArray reshape = Nd4j.linspace(1, 25, 25).reshape(5, 5);
        log.info("Linspace : {}, Reshape : {} ", linspace, reshape);



        NdIndexIterator iter = new NdIndexIterator(3, 3);
        while (iter.hasNext()) {
            int[] nextIndex = iter.next();
            double nextVal = linspace.getDouble(nextIndex);
            //do something with the value
        }



    }
}
