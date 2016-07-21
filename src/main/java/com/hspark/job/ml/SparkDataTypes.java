package com.hspark.job.ml;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.linalg.*;
import org.apache.spark.mllib.linalg.distributed.*;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.util.MLUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Rayn
 * @email liuwei412552703@163.com
 * Created by Rayn on 2016/7/20 17:29.
 */
public class SparkDataTypes {

    private static final Logger logger = LoggerFactory.getLogger(SparkDataTypes.class);


    public static void main(String[] args) {

        //行列式（3 × 2)
        // Create a dense matrix ((1.0, 2.0), (3.0, 4.0), (5.0, 6.0))
        Matrix dm = Matrices.dense(3, 2, new double[]{1.0, 3.0, 5.0, 2.0, 4.0, 6.0});

        // Create a sparse matrix ((9.0, 0.0), (0.0, 8.0), (0.0, 6.0))
        Matrix sm = Matrices.sparse(3, 2, new int[]{0, 1, 3}, new int[]{0, 2, 1}, new double[]{9, 6, 8});


        double[] doubles = {1.2, 3.2, 23.2};
        Vector dense = Vectors.dense(doubles);


        // Create a labeled point with a positive label and a dense feature vector.
        LabeledPoint pos = new LabeledPoint(1.0, Vectors.dense(1.0, 0.0, 3.0));

        // Create a labeled point with a negative label and a sparse feature vector.
        LabeledPoint neg = new LabeledPoint(0.0, Vectors.sparse(3, new int[]{0, 2}, new double[]{1.0, 3.0}));


        /**
         * Labeled point（标记点）
         */
        SparkConf sparkConf = new SparkConf().setMaster("local[1]").setAppName("ml_Test");
        JavaSparkContext jsc = new JavaSparkContext();
        JavaRDD<LabeledPoint> examples = MLUtils.loadLibSVMFile(jsc.sc(), "data/mllib/sample_libsvm_data.txt").toJavaRDD();


        /**
         * RowMatrix(行矩阵）
         */
        JavaRDD<Vector> examples2 = MLUtils.loadVectors(jsc.sc(), "data/mllib/sample_libsvm_data.txt").toJavaRDD();
        RowMatrix mat = new RowMatrix(examples2.rdd());

        // Get its size.
        long m = mat.numRows();
        long n = mat.numCols();

        // QR decomposition
        QRDecomposition<RowMatrix, Matrix> result = mat.tallSkinnyQR(true);


        /**
         * IndexedRowMatrix(索引行矩阵)
         */
        // a JavaRDD of indexed rows
        JavaRDD<IndexedRow> rows = MLUtils.loadLabeledPoints(jsc.sc(), "data/mllib/sample_libsvm_data.txt").toJavaRDD().map(new Function<LabeledPoint, IndexedRow>() {
            @Override
            public IndexedRow call(LabeledPoint v1) throws Exception {


                return null;
            }
        });
        // Create an IndexedRowMatrix from a JavaRDD<IndexedRow>.
        IndexedRowMatrix mat2 = new IndexedRowMatrix(rows.rdd());

        // Get its size.
        m = mat2.numRows();
        n = mat2.numCols();

        // Drop its row indices.
        RowMatrix rowMat = mat2.toRowMatrix();


        /**
         * a JavaRDD of matrix entries(矩阵元素)
         */
        JavaRDD<MatrixEntry> entries = jsc.textFile("", 10).map(new Function<String, MatrixEntry>() {
            @Override
            public MatrixEntry call(String v1) throws Exception {
                return null;
            }
        });


        // Create a CoordinateMatrix from a JavaRDD<MatrixEntry>.
        CoordinateMatrix mat3 = new CoordinateMatrix(entries.rdd());

        // Get its size.
        m = mat3.numRows();
        n = mat3.numCols();

        // Convert it to an IndexRowMatrix whose rows are sparse vectors.
        IndexedRowMatrix indexedRowMatrix = mat3.toIndexedRowMatrix();


        /**
         * BlockMatrix
         * // a JavaRDD of (i, j, v) Matrix Entries
         */
        JavaRDD<MatrixEntry> entries1 = MLUtils.loadLabeledPoints(jsc.sc(), "data/mllib/sample_libsvm_data.txt").toJavaRDD().map(new Function<LabeledPoint, MatrixEntry>() {
            @Override
            public MatrixEntry call(LabeledPoint v1) throws Exception {
                return null;
            }
        });

        // Create a CoordinateMatrix from a JavaRDD<MatrixEntry>.
        CoordinateMatrix coordMat = new CoordinateMatrix(entries1.rdd());
        // Transform the CoordinateMatrix to a BlockMatrix
        BlockMatrix matA = coordMat.toBlockMatrix().cache();

        // Validate whether the BlockMatrix is set up properly. Throws an Exception when it is not valid.
        // Nothing happens if it is valid.
        matA.validate();

        // Calculate A^T A.
        BlockMatrix ata = matA.transpose().multiply(matA);

    }
}
