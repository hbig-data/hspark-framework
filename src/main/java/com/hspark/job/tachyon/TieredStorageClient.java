package com.hspark.job.tachyon;

import alluxio.AlluxioURI;
import alluxio.client.file.FileSystem;
import alluxio.client.file.options.SetAttributeOptions;
import alluxio.exception.AlluxioException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * 分层存储
 *
 * MEM (内存)
 SSD (固态硬盘)
 HDD (硬盘驱动器)
 *
 * @author Rayn
 * @email liuwei412552703@163.com
 * Created by Rayn on 2016/9/6 17:31.
 */
public class TieredStorageClient {

    public static final Logger LOG = LoggerFactory.getLogger(TieredStorageClient.class);

    /**
     *
     * @throws IOException
     * @throws AlluxioException
     */
    public void setPinned() throws IOException, AlluxioException {

        FileSystem fs = FileSystem.Factory.get();
        AlluxioURI uri = new AlluxioURI("/myFile");
        SetAttributeOptions pinOpt = SetAttributeOptions.defaults().setPinned(true);

        //SetAttributeOptions pinOpt = SetAttributeOptions.defaults().setPinned(false);

        fs.setAttribute(uri, pinOpt);


    }



    public static void main(String[] args) {



    }
}
