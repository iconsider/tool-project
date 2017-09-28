package org.iconsider.generator;

import org.iconsider.model.Report;
import org.iconsider.utils.DateUtils;
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
 */
@Component
public class GeneratorEvery20Mins {
    public static String env = "unknown";

    public static void main(String[] args) {
        if(args.length != 1) {
            System.out.println("plz input args(env=test or product)");
            return;
        } else {
            if("test".equals(args[0]) || "product".equals(args[0])) {
                GeneratorEvery20Mins.env = args[0];
            } else {
                System.out.println("plz input args(env=test or product)");
                return;
            }
        }

        GeneratorEvery20Mins generatorEvery20Mins = SpringContextInstance.getBean("generatorEvery20Mins", GeneratorEvery20Mins.class);
        while(true) {
            try {
                if(DateUtils.isMod20Min()) {
                    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:00");
                    generatorEvery20Mins.generate(sdf.format(new Date()));
                }
                Thread.currentThread().sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

        }
    }


    public void generate(String dateTime) {
        System.out.println(dateTime);
        List<Report> list = this.queryForSection();
//        System.out.println(list.size());

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

            resultList.add(r0);
            resultList.add(r1);
        }

        this.insertTable(resultList);
    }

    public List<Report> queryForSection() {
        String sql = "SELECT highway_name,section_name,positive_direction,negative_direction FROM d_hx_highway_cell GROUP BY section_name,highway_name,positive_direction,negative_direction";
        //测试环境
//        JdbcTemplate template = SpringContextInstance.getBean("pgJdbcTemplate", JdbcTemplate.class);
        //生产环境
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
        //本地环境
//        String sql = "INSERT INTO f_hx_highway_statistic_20m (start_time, highway_name, section_name, direction, guest_count, speed) VALUES (to_timestamp(?,'YYYY-MM-DD HH24:MI:SS'),?,?,?,?,?)";
//        JdbcTemplate template = SpringContextInstance.getBean("pgJdbcTemplate", JdbcTemplate.class);


        if("unknown".equals(GeneratorEvery20Mins.env)) {
            System.out.println("env is unknown, plz input args");
            return;
        }

        String sql = "";
        if("product".equals(GeneratorEvery20Mins.env)) {
            //生产环境表
            sql = "INSERT INTO f_hx_highway_statistic_20m (start_time, highway_name, section_name, direction, guest_count, speed) VALUES (?,?,?,?,?,?)";
        } else if("test".equals(GeneratorEvery20Mins.env)) {
            //测试环境表
            sql = "INSERT INTO temp_hx_highway_result2 (start_time, highway_name, section_name, direction, guest_count, speed) VALUES (?,?,?,?,?,?)";
        }

        JdbcTemplate template = SpringContextInstance.getBean("oracleJdbcTemplate", JdbcTemplate.class);

        for (Report r : list) {
            template.update(sql, r.getTime(), r.getHighwayName(), r.getSectionName(), r.getDirection(), r.getGuest(), r.getSpeed());
        }
    }
}
