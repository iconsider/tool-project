package org.iconsider.job;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;

/**
 * Created by liuzhenxing on 2017-10-24.
 */
public class SparkJob implements Serializable {
    private static final long serialVersionUID = 5199871376840325353L;
    private static final Logger LOG = LoggerFactory.getLogger(SparkJob.class);
    private String path = "";

    public void setPath(String path) {
        this.path = path;
    }

    public void execute(SparkConf sparkConf) {
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
        JavaRDD<String> fiveMinRecordRDD = javaSparkContext.textFile(path);
        fiveMinRecordRDD.foreach(new VoidFunction<String>() {
            @Override
            public void call(String s) throws Exception {
                System.out.println(String.format("my lines: %s", s));
                LOG.info("log lines: {}", s);
            }
        });
        System.out.println(String.format("spark end"));
    }
}
