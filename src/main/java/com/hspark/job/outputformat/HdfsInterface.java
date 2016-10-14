package com.hspark.job.outputformat;

/**
 * @author Rayn on 2016/9/23.
 * @email liuwei412552703@163.com.
 */
public interface HdfsInterface {


    /**
     * 默认或缓冲区大小
     */
    public static final int BUFFER_SIZE = 1024 * 1024;

    /**
     * 文件名称
     */
    public static final String DATA_FILE_NAME = "data";

    /**
     * 数据文件输出目录
     */
    public static final String OUTDIR = "mapreduce.output.fileoutputformat.outputdir";

    /**
     * 输出文件的名称
     */
    public static final String BASE_OUTPUT_FILE_NAME = "mapreduce.output.basename";
}
