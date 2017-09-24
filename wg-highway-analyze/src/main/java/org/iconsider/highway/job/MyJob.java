package org.iconsider.highway.job;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.iconsider.highway.model.Record;
import scala.Tuple2;

import java.io.Serializable;
import java.util.*;

/**
 * Created by liuzhenxing on 2017-9-23.
 */
public class MyJob implements Serializable {
    private static final long serialVersionUID = 1546386160228769866L;

    public void execute(SparkConf conf) {
        this.task1(conf);
    }

    public void task1(SparkConf sparkConf) {
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
        JavaRDD<String> fiveMinRecordRDD = javaSparkContext.textFile("hdfs://host1:9000/user/hanxinnil/highwayInfo/201709221400");

        fiveMinRecordRDD.mapPartitions(new FlatMapFunction<Iterator<String>, Record>() {
            public Iterable<Record> call(Iterator<String> lines) throws Exception {
                List<Record> recordListWithoutResident = new ArrayList<Record>();

                //剔除常驻用户
                Set<String> residentUserSet = new HashSet<String>();
                residentUserSet.add("18820075738");
                residentUserSet.add("18814371663");
                residentUserSet.add("18813800322");

                while(lines.hasNext()) {
                    Record record = new Record(lines.next());
                    if("unknown".equals(record.getMsisdn())) {
                        continue;
                    } else if(!residentUserSet.contains(record.getMsisdn())) {
                        recordListWithoutResident.add(record);
                    }
                }

                return recordListWithoutResident;
            }
        }).foreach(new VoidFunction<Record>() {
            public void call(Record record) throws Exception {
                System.out.println(record);
            }
        });
    }



}
