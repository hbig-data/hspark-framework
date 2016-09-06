package com.hspark.job.tachyon;

import alluxio.AlluxioURI;
import alluxio.Constants;
import alluxio.client.WriteType;
import alluxio.client.file.FileInStream;
import alluxio.client.file.FileOutStream;
import alluxio.client.file.FileSystem;
import alluxio.client.file.options.CreateFileOptions;
import alluxio.client.keyvalue.*;
import alluxio.exception.AlluxioException;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * @author Rayn
 * @email liuwei412552703@163.com
 * Created by Rayn on 2016/8/22 14:38.
 */
public class KeyValueStoreClient {

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
     * 键值存储库
     * @throws IOException
     * @throws AlluxioException
     */
    public void writeKVSystem() throws IOException, AlluxioException {

        KeyValueSystem kvs = KeyValueSystem.Factory.create();
        KeyValueStoreWriter writer = kvs.createStore(new AlluxioURI("alluxio://path/my-kvstore"));
        // Insert key-value pair ("100", "foo")
        writer.put("100".getBytes(), "foo".getBytes());

        // Insert key-value pair ("200", "bar")
        writer.put("200".getBytes(), "bar".getBytes());

        // Close and complete the store
        writer.close();
    }

    /**
     * 键值存储库 读数据
     * @throws IOException
     * @throws AlluxioException
     */
    public void readKVSystem() throws IOException, AlluxioException {

        KeyValueSystem kvs = KeyValueSystem.Factory.create();

        KeyValueStoreReader reader = kvs.openStore(new AlluxioURI("alluxio://path/kvstore/"));
        // Return "foo"
        reader.get("100".getBytes());
        // Return null as no value associated with "300"
        reader.get("300".getBytes());


        /**
         * 第二种方式
         */
        KeyValueIterator iterator = reader.iterator();
        while (iterator.hasNext()) {
            KeyValuePair pair = iterator.next();
            ByteBuffer key = pair.getKey();
            ByteBuffer value = pair.getValue();
        }

        // Close the reader on the store
        reader.close();
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
