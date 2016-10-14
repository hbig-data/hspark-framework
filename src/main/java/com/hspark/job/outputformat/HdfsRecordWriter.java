package com.hspark.job.outputformat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;

/**
 * 输出到 HDFS 文件中
 * @author Rayn on 2016/9/23.
 * @email liuwei412552703@163.com.
 */
public class HdfsRecordWriter extends RecordWriter<String, String> implements HdfsInterface {

    private static final Logger LOGGER = LoggerFactory.getLogger(HdfsRecordWriter.class);

    private FileSystem fs = null;
    private OutputStream out = null;

    /**
     *
     * @param configuration
     */
    public HdfsRecordWriter(Configuration configuration) {
        String outputPath = configuration.get(OUTDIR, "/");
        String baseName = configuration.get(BASE_OUTPUT_FILE_NAME, "part_0000");
        try {

            fs = FileSystem.get(configuration);
            Path path = new Path(outputPath);

            if (!fs.exists(path) && !fs.mkdirs(path)) {
                throw new IOException("Mkdirs failed to create directory " + path);
            }

            /**
             * 创建文件
             */
            Path dataFile = new Path(path, baseName);
            if (fs.isFile(dataFile) && fs.exists(dataFile)) {
                throw new IllegalArgumentException("该目录下存在该文件 " + path);
            } else if (!fs.createNewFile(dataFile)) {
                throw new IllegalArgumentException("创建文件失败 " + path);
            }
            out = fs.append(path, configuration.getInt("BUFFER_SIZE", BUFFER_SIZE));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * @param key
     * @param value
     * @throws IOException
     */
    public void write(String key, String value) throws IOException {
        out.write(value.getBytes());
    }

    /**
     * @param context
     * @throws IOException
     */
    public void close(TaskAttemptContext context) throws IOException {
        if (null != fs) {
            fs.close();
        }
        if (null != out) {
            out.flush();
            out.close();
        }
    }
}