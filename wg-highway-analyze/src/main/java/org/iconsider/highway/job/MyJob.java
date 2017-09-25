package org.iconsider.highway.job;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import org.iconsider.highway.dao.HighwayDao;
import org.iconsider.highway.model.Cell;
import org.iconsider.highway.model.Record;
import org.iconsider.highway.model.UserReport;
import scala.Tuple2;

import java.io.Serializable;
import java.util.*;

/**
 * Created by liuzhenxing on 2017-9-23.
 */
public class MyJob implements Serializable {
    private static final long serialVersionUID = 1546386160228769866L;
    Map<String, Cell> cellInfoMap = null;  //key,value -> cgi,cell

    public void execute(SparkConf conf) {
        this.task0(conf);
//        this.task1(conf);
    }

    public void task0(SparkConf sparkConf) {
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
        JavaRDD<String> fiveMinRecordRDD = javaSparkContext.textFile("hdfs://host1:9000/user/hanxinnil/highwayInfo/201709221400");


        fiveMinRecordRDD.mapPartitionsToPair(new PairFlatMapFunction<Iterator<String>, String, List<Record>>() {
            public Iterable<Tuple2<String, List<Record>>> call(Iterator<String> lines) throws Exception {
                List<Tuple2<String, List<Record>>> list = new ArrayList<Tuple2<String, List<Record>>>();

                //剔除常驻用户
                Set<String> residentUserSet = new HashSet<String>();
                residentUserSet.add("18820075738");
                residentUserSet.add("18814371663");
                residentUserSet.add("18813800322");

                while (lines.hasNext()) {
                    Record record = new Record(lines.next());
                    if ("unknown".equals(record.getMsisdn())) {
                        continue;
                    } else if (!residentUserSet.contains(record.getMsisdn())) {  //TODO，加上！
                        List<Record> tmp = new ArrayList<Record>();
                        tmp.add(record);
                        list.add(new Tuple2<String, List<Record>>(record.getMsisdn(), tmp));
                    }
                }
                return list;
            }
        }).reduceByKey(new Function2<List<Record>, List<Record>, List<Record>>() {  //按msisdn聚合所有Record对象
            public List<Record> call(List<Record> records1, List<Record> records2) throws Exception {
                records1.addAll(records2);
                return records1;
            }
        }).filter(new Function<Tuple2<String, List<Record>>, Boolean>() {           //把Record对象数量小于n的msisdn过滤
            public Boolean call(Tuple2<String, List<Record>> tuple) throws Exception {
                if (tuple._2.size() <= 1) {
                    return false;
                } else {
                    return true;
                }
            }
        }).mapPartitions(new FlatMapFunction<Iterator<Tuple2<String, List<Record>>>, Tuple2<String, List<Record>>>() {   //根据lastTime进行升序排序
            public Iterable<Tuple2<String, List<Record>>> call(Iterator<Tuple2<String, List<Record>>> iterator) throws Exception {
                List<Tuple2<String, List<Record>>> list = new ArrayList<Tuple2<String, List<Record>>>();

                while (iterator.hasNext()) {
                    Tuple2<String, List<Record>> tuple = iterator.next();

                    Collections.sort(tuple._2, new Comparator<Record>() {  //升序排列
                        public int compare(Record o1, Record o2) {
                            if (o1.getLasttime() - o2.getLasttime() > 0) {
                                return 1;
                            } else if (o1.getLasttime() - o2.getLasttime() < 0) {
                                return -1;
                            }
                            return 0;
                        }
                    });
                    list.add(new Tuple2<String, List<Record>>(tuple._1, tuple._2));
                }
                return list;
            }
        }).map(new Function<Tuple2<String, List<Record>>, UserReport>() {
            public UserReport call(Tuple2<String, List<Record>> tuple2) throws Exception {
                System.out.println(tuple2._1);
                for (Record record : tuple2._2) {
                    System.out.println(record);
                }
                System.out.println("-----------------------------");
                return userAnalyze(tuple2);
            }
        }).foreach(new VoidFunction<UserReport>() {
            public void call(UserReport userReport) throws Exception {
                double speed = userReport.getDistance()/userReport.getTime();
                if(speed > 80 && speed < 120) {
                    System.out.println(String.format("speed:%.2f, modle:%s", speed, userReport));
                }
            }
        });



    }


    public UserReport userAnalyze(Tuple2<String, List<Record>> tuple) {
        if(null == cellInfoMap) {
            HighwayDao dao = new HighwayDao();
            cellInfoMap = dao.getAllCell();
        }

        double distance = 0D;
        double time = ((tuple._2.size() - 1) * 5) / 60D;
        for (Record record : tuple._2) {
            Cell cell = cellInfoMap.get(record.getCgi());
            distance += cell.getDistance();
        }
        return new UserReport("unknown_sectionId", "unknown_direction", distance/1000D, time);
    }

    public boolean isInSameSection(String beginCgi, String endCgi) {
        if(null == cellInfoMap) {
            HighwayDao dao = new HighwayDao();
            cellInfoMap = dao.getAllCell();
        }

        int beginSectionId = cellInfoMap.get(beginCgi).getSectionId();
        int endSectionId = cellInfoMap.get(endCgi).getSectionId();

        if(beginSectionId == endSectionId) {
            return true;
        } else {
            return false;
        }
    }

}
