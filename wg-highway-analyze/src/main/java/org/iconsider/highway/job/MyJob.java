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
    Map<String, Cell> cgiCellMap = null;  //key,value -> cgi,cell
    Map<Integer, Double> cellIdDistanceMap = null;  //key,value -> cellId,distance

    public void execute(SparkConf conf) {
        this.task0(conf);
//        this.task1(conf);
    }

    public void task0(SparkConf sparkConf) {
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
//        JavaRDD<String> fiveMinRecordRDD = javaSparkContext.textFile("hdfs://host1:9000/user/hanxinnil/highwayInfo/201709221400");
        JavaRDD<String> fiveMinRecordRDD = javaSparkContext.textFile("hdfs://host1:9000/user/hanxinnil/highwayInfo/201709221420");
//        JavaRDD<String> fiveMinRecordRDD = javaSparkContext.textFile("hdfs://host1:9000/user/hanxinnil/highwayInfo/201709221440");


        fiveMinRecordRDD.mapPartitionsToPair(new PairFlatMapFunction<Iterator<String>, String, List<Record>>() {
            public Iterable<Tuple2<String, List<Record>>> call(Iterator<String> lines) throws Exception {
                List<Tuple2<String, List<Record>>> list = new ArrayList<Tuple2<String, List<Record>>>();

                //剔除常驻用户
                Set<String> residentUserSet = new HashSet<String>();
//                residentUserSet.add("18820075738");
//                residentUserSet.add("18814371663");
//                residentUserSet.add("18813800322");

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
        }).filter(new Function<Tuple2<String, List<Record>>, Boolean>() {           //把Record对象数量小于1的msisdn过滤
            public Boolean call(Tuple2<String, List<Record>> tuple) throws Exception {
                if (tuple._2.size() <= 1 || tuple._1.length() != 11) {  //过滤用户记录cgi小于等于1，电话号码长度不是11位
                    return false;
                } else if (isAllCgiSame(tuple._2)) {
                    return false;
                } else {
                    return true;
                }
            }
        }).mapToPair(new PairFunction<Tuple2<String, List<Record>>, Integer, Integer>() {
            public Tuple2<Integer, Integer> call(Tuple2<String, List<Record>> tuple2) throws Exception {
                UserReport userReport = userAnalyze(tuple2._2);
                int speed = (int) ((userReport.getDistance()/1000)/(userReport.getTime()/60));

                return new Tuple2<Integer, Integer>(speed/10, 1);
            }
        }).reduceByKey(new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer+integer2;
            }
        }).foreach(new VoidFunction<Tuple2<Integer, Integer>>() {
            public void call(Tuple2<Integer, Integer> tuple2) throws Exception {
                System.out.println(String.format("speed:%04d, number:%s", tuple2._1, tuple2._2));
            }
        });



    }


    public UserReport userAnalyze(List<Record> userRecordList) {
        if(null == cgiCellMap) {
            HighwayDao dao = new HighwayDao();
            cgiCellMap = dao.getAllCell();
        }
        if(null == cellIdDistanceMap) {
            HighwayDao dao = new HighwayDao();
            cellIdDistanceMap = dao.getCellDistance();
        }

        double time = ((userRecordList.size()) * 5);

        Set<Integer> cellIdSet = new HashSet<Integer>();
        double distance = 0D;
        for (Record record : userRecordList) {
            cellIdSet.add(cgiCellMap.get(record.getCgi()).getCellId());
        }
        int minCellId = Collections.min(cellIdSet);
        int maxCellId = Collections.max(cellIdSet);
        if (maxCellId - minCellId < 152) {
            for (int i = minCellId; i < maxCellId; i++) {
                distance += cellIdDistanceMap.get(i);
            }
        }

        return new UserReport("unknown_sectionId", "unknown_direction", distance, time);
    }





    /**
     * @param list
     * @return
     * 判断一个用户的记录是否都属于同一个cgi
     * 所有cgi一样则返回true
     * 只要有一个cgi不一样，则返回false
     *
     */
    public boolean isAllCgiSame(List<Record> list) {
        Set<String> cgis = new HashSet<String>();
        for (Record record : list) {
            cgis.add(record.getCgi());
        }
        if(cgis.size() > 1) {
            return false;
        } else {
            return true;
        }
    }

    public boolean isInSameSection(String beginCgi, String endCgi) {
        if(null == cgiCellMap) {
            HighwayDao dao = new HighwayDao();
            cgiCellMap = dao.getAllCell();
        }

        int beginSectionId = cgiCellMap.get(beginCgi).getSectionId();
        int endSectionId = cgiCellMap.get(endCgi).getSectionId();

        if(beginSectionId == endSectionId) {
            return true;
        } else {
            return false;
        }
    }

}
