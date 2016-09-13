package com.hspark.job.video;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.opencv.core.Core;

import java.io.IOException;

/**
 * @author Rayn
 * @email liuwei412552703@163.com
 * Created by Rayn on 2016/9/13 9:27.
 */
public class VideoInputFormat extends FileInputFormat<Text, HBMat> {


    @Override
    public RecordReader<Text, HBMat> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        System.loadLibrary(Core.NATIVE_LIBRARY_NAME);
        return (RecordReader) new VideoRecordReader(split, context);
    }

}
