package com.hspark.job.tachyon;

import alluxio.AlluxioURI;
import alluxio.client.lineage.AlluxioLineage;
import alluxio.exception.AlluxioException;
import alluxio.job.CommandLineJob;
import alluxio.job.JobConf;
import com.google.common.collect.Lists;

import java.io.IOException;
import java.util.List;

/**
 * @author Rayn
 * @email liuwei412552703@163.com
 * Created by Rayn on 2016/8/22 14:59.
 */
public class LineageClient {

    /**
     * 读数据
     */
    public void readData() {


    }

    /**
     * 写文件
     */
    public void writeData() throws IOException, AlluxioException {
        AlluxioLineage tl = AlluxioLineage.get();

        // input file paths
        AlluxioURI input1 = new AlluxioURI("/inputFile1");
        AlluxioURI input2 = new AlluxioURI("/inputFile2");
        List<AlluxioURI> inputFiles = Lists.newArrayList(input1, input2);

        // output file paths
        AlluxioURI output = new AlluxioURI("/outputFile");
        List<AlluxioURI> outputFiles = Lists.newArrayList(output);

        // command-line job
        JobConf conf = new JobConf("/tmp/recompute.log");
        CommandLineJob job = new CommandLineJob("my-spark-job.sh", conf);

        long lineageId = tl.createLineage(inputFiles, outputFiles, job);
    }

    public static void main(String[] args) {

    }
}
