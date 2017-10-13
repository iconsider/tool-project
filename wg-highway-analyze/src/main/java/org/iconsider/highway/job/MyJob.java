package org.iconsider.highway.job;

import org.apache.avro.generic.GenericData;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import org.datanucleus.store.types.backed.*;
import org.iconsider.highway.dao.HighwayDao;
import org.iconsider.highway.model.*;
import scala.Tuple2;

import java.io.Serializable;
import java.util.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Created by liuzhenxing on 2017-9-23.
 */
public class MyJob implements Serializable {
    private static final long serialVersionUID = 1546386160228769866L;
    Map<String, List<Cell>> cgiCellMap = null;  //key,value -> cgi,cell
    Map<Integer, Double> cellIdDistanceMap = null;  //key,value -> cellId,distance
    Map<String, List<String>> sectionIdMsisdnMap = null;    //key,value -> sectionId,msisdnList

    public void execute(SparkConf conf) {
        this.task0(conf);
//        this.task1(conf);
    }

    public void task0(SparkConf sparkConf) {
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
        JavaRDD<String> fiveMinRecordRDD = javaSparkContext.textFile("hdfs://host1:9000/user/hanxinnil/highwayInfo/201709221400");
//        JavaRDD<String> fiveMinRecordRDD = javaSparkContext.textFile("hdfs://host1:9000/user/hanxinnil/highwayInfo/201709221420");
//        JavaRDD<String> fiveMinRecordRDD = javaSparkContext.textFile("hdfs://host1:9000/user/hanxinnil/highwayInfo/201709221440");
//        JavaRDD<String> fiveMinRecordRDD = javaSparkContext.textFile("hdfs://host1:9000/user/hanxinnil/highwayInfo/201709290200");


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
                        list.add(new Tuple2<>(record.getMsisdn(), tmp));
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
                } else if(isAllCgiSame(tuple._2)) { //TODO，删除isAllCgiSame()方法
                    return false;
                } else {
                    return true;
                }
            }
        }).flatMap(new FlatMapFunction<Tuple2<String, List<Record>>, Tuple2<String, List<UserReport>>>() {
            @Override
            public Iterable<Tuple2<String, List<UserReport>>> call(Tuple2<String, List<Record>> tuple2) throws Exception {
                List<UserReport> userReportList = userAnalyze(tuple2._2);       //userReportList会有多条，因为存在同一高速跨路段的情况
                List<Tuple2<String, List<UserReport>>> t = new ArrayList<>();   //tuple2 -> key:sectionId:directon, value:userReport

                for (UserReport ur : userReportList) {
                    List<UserReport> listTemp = new ArrayList<>();
                    listTemp.add(ur);
                    Tuple2<String, List<UserReport>> tuplt2Temp = new Tuple2<String, List<UserReport>>(String.format("%s:%s", ur.getSectionId(), ur.getDirection()), listTemp);
                    t.add(tuplt2Temp);
                }
                return t;
            }
        }).mapToPair(new PairFunction<Tuple2<String, List<UserReport>>, String, List<UserReport>>() {
            @Override
            public Tuple2<String, List<UserReport>> call(Tuple2<String, List<UserReport>> tuple2) throws Exception {
                Tuple2<String, List<UserReport>> t = new Tuple2<String, List<UserReport>>(tuple2._1, tuple2._2);
                return t;
            }
        }).reduceByKey(new Function2<List<UserReport>, List<UserReport>, List<UserReport>>() {
            @Override
            public List<UserReport> call(List<UserReport> list1, List<UserReport> list2) throws Exception {
                list1.addAll(list2);
                return list1;
            }
        }).map(new Function<Tuple2<String,List<UserReport>>, SectionReport>() {
            public SectionReport call(Tuple2<String, List<UserReport>> tuple2) throws Exception {  //key:sectionId:directon, value:userReport
                return sectionAnalyze(tuple2._2);
            }
        }).foreach(new VoidFunction<SectionReport>() {
            public void call(SectionReport sectionReport) throws Exception {
                System.out.println(sectionReport);
            }
        });

    }


    private void show4Record(Tuple2<String, List<Record>> tuple2) {
        for (Record record : tuple2._2) {
            System.out.println(record);
        }
        System.out.println("---------------------");
    }

    private void showUserRecord(List<Record> userRecordList) {
        for (Record record : userRecordList) {
            System.out.println(record);
        }
    }


    /**
     * 根据用户的快照记录，判断用户属于哪条高速。
     *
     * 用户快照记录的cgi可能会属于多条高速的路段
     * 如果用户有4条记录，第一条记录的cgi查询得到属于高速1和高速2
     * 第二、三、四条记录属于高速2，
     * 属于高速2的数量多，则用户属于高速2
     *
     * 如果出现跨高速，则返回-2
     * 判断跨高速条件：
     * 存在一个高速id出现次数等于该用户快照记录的条数，则该用户没有跨高速
     * 否则用户为跨高速
     */
    private int belongToWhichHighway(List<Record> userRecordList) {
        Map<String, Integer> highwayIdNumberMap = new HashMap<>();  //key->"highwayId:sectionId", value->次数
        for (Record record : userRecordList) {
            List<Cell> listTemp = cgiCellMap.get(record.getCgi());
            for (Cell cell : listTemp) {
                String highwayId = String.valueOf(cell.getHighwayId());
                String sectionId = String.valueOf(cell.getSectionId());
                String keyTemp = String.format("%s:%s",highwayId,sectionId);
                if (null == highwayIdNumberMap.get(keyTemp)) {
                    highwayIdNumberMap.put(keyTemp, 1);
                } else {
                    int num = highwayIdNumberMap.get(keyTemp) + 1;
                    highwayIdNumberMap.put(keyTemp, num);
                }
            }
        }

        int key = -1;       //highwayId
        int value = -1;     //highwayId 出现的次数
        Iterator<Map.Entry<String, Integer>> it = highwayIdNumberMap.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<String, Integer> entry = it.next();
            if (entry.getValue() >= value) {
                key = Integer.valueOf(entry.getKey().split(":")[0]);
                value = entry.getValue();
            }
        }

        //判断是否跨高速
        if (value == userRecordList.size()) {
            return key;
        } else {
            return -2;
        }
    }


    /**
     * 当cgi反向查询有多条section信息时，用cgi和highwayId获取section信息（前提该用户没有跨高速）
     *
     */
    private Cell getSectionInfo(String cgi, int highwayId) {
        List<Cell> listTemp = cgiCellMap.get(cgi);
        if (null == listTemp) {
            System.out.println(String.format("cgiCellMap hasn't cgi: %s", cgi));
            return null;
        } else {
            for (Cell cell : listTemp) {
                if (highwayId == cell.getHighwayId()) {
                    return cell;
                }
            }
        }
        System.out.println(String.format("用cgi从cgiCellMap查出的sectionInfo，没有包含该highwayId的记录：highwayId: %s, cgi:%s", highwayId, cgi));
        return null;
    }



    /**
     * 计算单个用户运行信息:
     * 传入单个用户的所有快照记录（最多4条），
     */
    private List<UserReport> userAnalyze(List<Record> userRecordList) {
        int highwayId = 0;
        if (userRecordList != null && userRecordList.size() > 1) {
            if(null == cgiCellMap) {
                HighwayDao dao = new HighwayDao();
                cgiCellMap = dao.getAllCell();
            }
            if(null == cellIdDistanceMap) {
                HighwayDao dao = new HighwayDao();
                cellIdDistanceMap = dao.getCellDistance();
            }

            //把单个用户的快照记录（最多4条）按lasttime排序
            Collections.sort(userRecordList, new Comparator<Record>() {
                public int compare(Record o1, Record o2) {
                    long startTime1 = o1.getLasttime();
                    long startTime2 = o2.getLasttime();
                    if(startTime1 > startTime2) {
                        return 1;
                    } else if(startTime1 < startTime2) {
                        return -1;
                    }
                    return 0;
                }
            });

            //判断用户是否跨高速
            int belongToHighwayId = belongToWhichHighway(userRecordList);
            if(belongToHighwayId < 0) {     //belongToHighwayId可能是-1（表示没有该高速id），-2（表示跨高速，跨高速无法计算运行距离）
//                System.out.println("用户跨高速");
                return new ArrayList<UserReport>(); //返回空的list
            }

            //sectionId
            Set<Integer> sectionSet = new HashSet<>();
            for (Record record : userRecordList) {
                Cell cellTemp = getSectionInfo(record.getCgi(), belongToHighwayId);
                int s = cellTemp.getSectionId();
                sectionSet.add(s);
            }

            //highwayId
            highwayId = belongToHighwayId;

            //运行方向
            String direction = "firstCell=lastCell";
            Cell firstCell = getSectionInfo(userRecordList.get(0).getCgi(), highwayId);
            Cell lastCell = getSectionInfo(userRecordList.get(userRecordList.size()-1).getCgi(), highwayId);
            if(firstCell.getCellId() >= lastCell.getCellId()) {     //TODO,为什么会有firstCell=lastCell
                direction = firstCell.getPostiveDirection();
            }
            if(firstCell.getCellId() < lastCell.getCellId()) {
                direction = firstCell.getNegativeDirection();
            }

            //运行时间
            long beginTime = userRecordList.get(0).getLasttime();
            long endTime = userRecordList.get(userRecordList.size()-1).getLasttime();
            double runTime = endTime - beginTime;

            //运行距离
            double distance = 0D;
            Set<Integer> cellIdSet = new HashSet<Integer>();
            for (Record record : userRecordList) {
                cellIdSet.add(getSectionInfo(record.getCgi(), highwayId).getCellId());
            }
            int minCellId = Collections.min(cellIdSet);
            int maxCellId = Collections.max(cellIdSet);
            if (maxCellId - minCellId < 152) {
                for (int i = minCellId; i < maxCellId; i++) {
                    distance += cellIdDistanceMap.get(i);
                }
            } else {
                System.out.println("error: maxCellId - minCellId > 152");
            }


            //检查各种运行信息是否合理
            boolean isPossible = true;
            if(runTime <= 0D) {
                isPossible = false;
                showUserRecord(userRecordList);
                System.out.println(String.format("运行时间不合理，runTime=%s h", runTime));
                System.out.println("---------------");
            }
            if(distance <= 0D) {
                isPossible = false;
                System.out.println(String.format("运行距离不合理，distance=%s h", distance));
            }

            if(isPossible) {
                List<UserReport> list = new ArrayList<>();
                for (Integer sectionIdTemp : sectionSet) {
                    list.add(new UserReport(highwayId, sectionIdTemp, direction, distance, runTime));
                }

//                System.out.println(String.format("用户号码: %s, 运行距离: %.2fm, 运行时间: %.2fmin, 运行速度: %.2fkm/h",userRecordList.get(0).getMsisdn(), distance,runTime/1000D/60D,(distance/1000D)/(runTime/1000D/60D/60D)));
                return list;
            } else {
                return new ArrayList<UserReport>(); //返回空的list
            }


        } else {
            System.out.println("UserReport is null");
            return new ArrayList<UserReport>(); //返回空的list
        }
    }


    private SectionReport sectionAnalyze(List<UserReport> userReportList) {
        int highwayId = 0;
        int sectonId = 0;
        if (userReportList != null && userReportList.size() > 0) {
            if(null == cgiCellMap) {
                HighwayDao dao = new HighwayDao();
                cgiCellMap = dao.getAllCell();
            }
            if(null == cellIdDistanceMap) {
                HighwayDao dao = new HighwayDao();
                cellIdDistanceMap = dao.getCellDistance();
            }

            UserReport userReport = userReportList.get(0);

            //高速id
            highwayId = userReport.getHighwayId();

            //路段id
            sectonId = userReport.getSectionId();

            //运行方向
            String direction = userReport.getDirection();

            //路段人数
            int guestCounter = userReportList.size();

            //路段平均速度
            double sumSpeed = 0D;
            int sumGuestCounter = 0;
            for (UserReport report : userReportList) {
                double userSpeed = (report.getDistance()/1000D)/(report.getTime()/1000D/60D/60D);
                if(userSpeed >= 0D && userSpeed <= 120D) {
                    sumSpeed += userSpeed;
                    sumGuestCounter++;
                } else if (userSpeed <= 180D) {
                    sumSpeed += 120D;
                    sumGuestCounter++;
                }
            }
            double speed = sumSpeed/sumGuestCounter;

            return new SectionReport("notSet", highwayId, sectonId, direction, guestCounter, speed);
        } else {
            return new SectionReport("null", highwayId, sectonId, "null", 0, 0D);
        }
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

        int beginSectionId = cgiCellMap.get(beginCgi).get(0).getSectionId();
        int endSectionId = cgiCellMap.get(endCgi).get(0).getSectionId();

        if(beginSectionId == endSectionId) {
            return true;
        } else {
            return false;
        }
    }

}
