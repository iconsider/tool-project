package org.iconsider.highway.driver;

import org.apache.spark.SparkConf;
import org.iconsider.highway.job.MyJob;

/**
 * Created by liuzhenxing on 2017-9-23.
 */
public class MyDriver {
    public static void main(String[] args) {
        MyDriver myDriver = new MyDriver();
        //默认是deploy mode是client
        SparkConf conf = new SparkConf()
                .setAppName("highway-report-analyze")
//                .setMaster("spark://host1:7077")  //表示是standalone
                .setMaster("local[2]")  //表示是local
                .set("spark.executor.memory", "300m")
                .set("spark.total.executor.cores", "1")
                .setJars(new String[]{"Z:\\remote-highway-analyze\\wg-highway-analyze.jar"});

        myDriver.startJob(conf);
    }


    public void startJob(SparkConf conf) {
        MyJob myJob = new MyJob();
        myJob.execute(conf);
    }
}