package com.hspark.job.tachyon;

import alluxio.AlluxioURI;
import alluxio.Constants;
import alluxio.client.WriteType;
import alluxio.client.file.FileInStream;
import alluxio.client.file.FileOutStream;
import alluxio.client.file.FileSystem;
import alluxio.client.file.options.CreateFileOptions;
import alluxio.client.keyvalue.KeyValueStoreWriter;
import alluxio.client.keyvalue.KeyValueSystem;
import alluxio.exception.AlluxioException;

import java.io.IOException;

/**
 * @author Rayn
 * @email liuwei412552703@163.com
 * Created by Rayn on 2016/8/22 14:38.
 */
public class AlluxioClient {

    /**
     * 读文件
     *
     * @throws IOException
     * @throws AlluxioException
     */
    public void readFile() throws IOException, AlluxioException {
        FileSystem fs = FileSystem.Factory.get();
        AlluxioURI path = new AlluxioURI("/myFile");

        // Open the file for reading and obtains a lock preventing deletion
        FileInStream in = fs.openFile(path);

        byte[] bytes = new byte[1024];
        // Read data
        in.read(bytes);

        // Close file relinquishing the lock
        in.close();
    }

    /**
     *
     * @throws IOException
     * @throws AlluxioException
     */
    public void keyvalueSystem() throws IOException, AlluxioException {

        KeyValueSystem kvs = KeyValueSystem.Factory.create();
        KeyValueStoreWriter writer = kvs.createStore(new AlluxioURI("alluxio://path/my-kvstore"));
        // Insert key-value pair ("100", "foo")
        writer.put("100".getBytes(), "foo".getBytes());

        // Insert key-value pair ("200", "bar")
        writer.put("200".getBytes(), "bar".getBytes());

        // Close and complete the store
        writer.close();
    }

    public static void main(String[] args) throws IOException, AlluxioException {

        FileSystem fs = FileSystem.Factory.get();
        AlluxioURI path = new AlluxioURI("/myFile");

        CreateFileOptions options = CreateFileOptions.defaults();
        options.setBlockSizeBytes(5 * Constants.MB);
        options.setRecursive(true);
        options.setTtl(500 * 1000);
        options.setWriteType(WriteType.ASYNC_THROUGH);

        // Create a file and get its output stream
        FileOutStream out = fs.createFile(path, options);

        // Write data
        out.write("Write data".getBytes());

        // Close and complete file
        out.close();
    }
}
