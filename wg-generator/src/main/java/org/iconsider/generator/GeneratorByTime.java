package org.iconsider.generator;

import org.iconsider.model.Report;
import org.iconsider.utils.SpringContextInstance;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.support.rowset.SqlRowSet;
import org.springframework.stereotype.Component;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Random;

/**
 * Created by liuzhenxing on 2017-9-21.
 * 产生从beginTime到endTime内，每20分钟的数据
 */
@Component
public class GeneratorByTime {
    public static String env = "unknown";


    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            System.out.println("plz input env and [beginTime,endTime), env format:test or product; time format:2017-09-19 00:00:00");
            return;
        }

        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        GeneratorByTime generator = SpringContextInstance.getBean("generatorByTime", GeneratorByTime.class);

        if("test".equals(args[0]) || "product".equals(args[0])) {
            GeneratorEvery20Mins.env = args[0];
        } else {
            System.out.println("plz input args(env=test or product)");
            return;
        }



//        String beginTime = "2017-09-19 00:00:00";
//        String endTime = "2017-09-20 00:00:00";
        String beginTime = args[0];
        String endTime = args[1];

        long beginTimestamp = sdf.parse(beginTime).getTime();
        long endTimestamp = sdf.parse(endTime).getTime();

        while(true) {
            if (beginTimestamp - endTimestamp == 0) {
                break;
            }
            generator.generate(sdf.format(new Date(beginTimestamp)));
            beginTimestamp = beginTimestamp + 20*60*1000L;

        }

        System.exit(0);
    }


    public void generate(String dateTime) {
        System.out.println(dateTime);
        List<Report> list = this.queryForSection();

        List<Report> resultList = new ArrayList<Report>();
        for (Report report : list) {
            Report r0 = new Report();
            r0.setTime(dateTime);
            r0.setHighwayName(report.getHighwayName());
            r0.setSectionName(report.getSectionName());
            r0.setDirection(report.getPositiveDirection());
            r0.setGuest(new Random().nextInt(300) + 100);
            r0.setSpeed(new Random().nextInt(40) + 70 + (new Random().nextDouble()));
            Report r1 = new Report();
            r1.setTime(dateTime);
            r1.setHighwayName(report.getHighwayName());
            r1.setSectionName(report.getSectionName());
            r1.setDirection(report.getNegative_direction());
            r1.setGuest(new Random().nextInt(300) + 100);
            r1.setSpeed(new Random().nextInt(40) + 70 + (new Random().nextDouble()));

//            System.out.println(r0);
//            System.out.println(r1);

            resultList.add(r0);
            resultList.add(r1);
        }

//        for (Report report : resultList) {
//            System.out.println(report);
//        }
//        System.out.println("路段数量：" + resultList.size());

        this.insertTable(resultList);
    }

    public List<Report> queryForSection() {
        String sql = "SELECT highway_name,section_name,positive_direction,negative_direction FROM d_hx_highway_cell GROUP BY section_name,highway_name,positive_direction,negative_direction";
        JdbcTemplate template = SpringContextInstance.getBean("oracleJdbcTemplate", JdbcTemplate.class);
        SqlRowSet sqlRowSet = template.queryForRowSet(sql);

        List<Report> list = new ArrayList<Report>();
        while(sqlRowSet.next()) {
            Report report = new Report();
            report.setHighwayName(sqlRowSet.getString("highway_name"));
            report.setSectionName(sqlRowSet.getString("section_name"));
            report.setPositiveDirection(sqlRowSet.getString("positive_direction"));
            report.setNegative_direction(sqlRowSet.getString("negative_direction"));
            list.add(report);
        }
        return list;
    }

    public void insertTable(List<Report> list) {
        String sql = "";

        if("unknown".equals(GeneratorEvery20Mins.env)) {
            System.out.println("env is unknown, plz input args");
            return;
        }

        if("test".equals(GeneratorEvery20Mins.env)) {
            //测试环境
//          sql = "INSERT INTO f_hx_highway_statistic_20m (start_time, highway_name, section_name, direction, guest_count, speed) VALUES (to_timestamp(?,'YYYY-MM-DD HH24:MI:SS'),?,?,?,?,?)";
            sql = "INSERT INTO temp_hx_highway_result2 (start_time, highway_name, section_name, direction, guest_count, speed) VALUES (?,?,?,?,?,?)";
        }
        if("product".equals(GeneratorEvery20Mins.env)) {
            //生产环境
            sql = "INSERT INTO f_hx_highway_statistic_20m (start_time, highway_name, section_name, direction, guest_count, speed) VALUES (?,?,?,?,?,?)";
        }

        JdbcTemplate template = SpringContextInstance.getBean("oracleJdbcTemplate", JdbcTemplate.class);
        for (Report r : list) {
            template.update(sql, r.getTime(), r.getHighwayName(), r.getSectionName(), r.getDirection(), r.getGuest(), r.getSpeed());
        }
    }
}
