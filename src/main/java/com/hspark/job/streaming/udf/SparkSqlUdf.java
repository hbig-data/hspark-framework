package com.hspark.job.streaming.udf;

import org.apache.spark.sql.api.java.UDF1;
import scala.collection.Iterator;
import scala.collection.immutable.List;


/**
 * @author Rayn
 * @email liuwei412552703@163.com
 * Created by Rayn on 2016/11/28 17:19.
 */
public class SparkSqlUdf implements UDF1<List<String>,String> {

    /*//判断一条数据是否通过了所有节点的测试条件
    public static boolean isAllFlowNodePass(FlowData data) {
        boolean isPass = true;
        if( data == null ){
            isPass = false;
        }else{
            java.util.List<String> meta = data.getMeta();
            if(!meta.isEmpty()){
                Iterator<String> iterator = meta.iterator();
                while (iterator.hasNext() && isPass){
                    isPass = "Y".equals(iterator.next().split(":")[1]) ? true :false;
                }
            }
        }
        return  isPass;
    }

    public static boolean isAllFlowNodePass(java.util.List<String> data) {
        boolean isPass = true;
        if( data == null || data.isEmpty()){
            isPass = false;
        }else{
            Iterator<String> iterator = data.iterator();
            while (iterator.hasNext() && isPass){
                isPass = "Y".equals(iterator.next().split(":")[1]) ? true :false;
            }
        }
        return  isPass;
    }*/


    /**
     * 判断某一节点是否通过所有的节点测试
     * 集合转换建议优先使用scala集合类
     * @param data
     * @return
     * @throws Exception
     */
    @Override
    public String call(List<String> data) throws Exception {
        boolean isPass = true;
        if( data == null || data.isEmpty() ){
            isPass = false;
        }else{
            Iterator<String> iterator1 = data.iterator();
            while (iterator1.hasNext() && isPass){
                isPass = "Y".equals(iterator1.next().split(":")[1]) ? true :false;
            }
        }
        return  isPass?"Yes":"No";
    }
}
