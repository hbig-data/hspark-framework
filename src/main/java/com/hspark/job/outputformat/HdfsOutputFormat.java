package com.hspark.job.outputformat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * @author Rayn on 2016/9/23.
 * @email liuwei412552703@163.com.
 */
public class HdfsOutputFormat extends FileOutputFormat<String, String> {

    private static final Logger LOGGER = LoggerFactory.getLogger(HdfsOutputFormat.class);

    @Override
    public RecordWriter<String, String> getRecordWriter(TaskAttemptContext context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();


        return new HdfsRecordWriter(conf);
    }

    /**
     * Check for validity of the output-specification for the job.
     * <p/>
     * <p>This is to validate the output specification for the job when it is
     * a job is submitted.  Typically checks that it does not already exist,
     * throwing an exception when it already exists, so that output is not
     * overwritten.</p>
     *
     * @param context information about the job
     * @throws IOException when output should not be attempted
     */
    @Override
    public void checkOutputSpecs(JobContext context) throws IOException {

    }

    /**
     * Get the output committer for this output format. This is responsible
     * for ensuring the output is committed correctly.
     *
     * @param context the task context
     * @return an output committer
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public OutputCommitter getOutputCommitter(TaskAttemptContext context) throws IOException {
        return new OutputCommitter() {
            @Override
            public void setupJob(JobContext jobContext) throws IOException {

            }

            @Override
            public void setupTask(TaskAttemptContext taskContext) throws IOException {

            }

            @Override
            public boolean needsTaskCommit(TaskAttemptContext taskContext) throws IOException {
                return false;
            }

            @Override
            public void commitTask(TaskAttemptContext taskContext) throws IOException {

            }

            @Override
            public void abortTask(TaskAttemptContext taskContext) throws IOException {

            }
        };
    }
}
