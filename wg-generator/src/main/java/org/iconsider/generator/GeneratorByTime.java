package org.iconsider.generator;

import org.iconsider.model.Report;
import org.iconsider.utils.SpringContextInstance;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.support.rowset.SqlRowSet;
import org.springframework.stereotype.Component;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

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
        String beginTime = args[1];
        String endTime = args[2];

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
            r0.setGuest(generateGuest(dateTime));
            r0.setSpeed(generateSpeed(dateTime));
            Report r1 = new Report();
            r1.setTime(dateTime);
            r1.setHighwayName(report.getHighwayName());
            r1.setSectionName(report.getSectionName());
            r1.setDirection(report.getNegative_direction());
            r1.setGuest(generateGuest(dateTime));
            r1.setSpeed(generateSpeed(dateTime));

            resultList.add(r0);
            resultList.add(r1);
        }
        this.insertTable(resultList);
    }



    public int generateGuest(String dateTime) {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        int guest = 100;
        try {

            Calendar calendar = Calendar.getInstance();
            calendar.setTime(sdf.parse(dateTime));
            int hour = calendar.get(Calendar.HOUR_OF_DAY);

            switch (hour) {
                case 0:
                    guest = new Random().nextInt(150) + 300;
                    break;
                case 1:
                    guest = new Random().nextInt(150) + 250;
                    break;
                case 2:
                    guest = new Random().nextInt(100) + 250;
                    break;
                case 3:
                    guest = new Random().nextInt(100) + 200;
                    break;
                case 4:
                    guest = new Random().nextInt(100) + 100;
                    break;
                case 5:
                    guest = new Random().nextInt(100) + 50;
                    break;
                case 6:
                    guest = new Random().nextInt(100) + 100;
                    break;
                case 7:
                    guest = new Random().nextInt(100) + 100;
                    break;
                case 8:
                    guest = new Random().nextInt(150) + 150;
                    break;
                case 9:
                    guest = new Random().nextInt(150) + 200;
                    break;
                case 10:
                    guest = new Random().nextInt(150) + 300;
                    break;
                case 11:
                    guest = new Random().nextInt(150) + 400;
                    break;
                case 12:
                    guest = new Random().nextInt(150) + 500;
                    break;
                case 13:
                    guest = new Random().nextInt(150) + 600;
                    break;
                case 14:
                    guest = new Random().nextInt(150) + 500;
                    break;
                case 15:
                    guest = new Random().nextInt(150) + 500;
                    break;
                case 16:
                    guest = new Random().nextInt(150) + 500;
                    break;
                case 17:
                    guest = new Random().nextInt(150) + 500;
                    break;
                case 18:
                    guest = new Random().nextInt(150) + 500;
                    break;
                case 19:
                    guest = new Random().nextInt(150) + 500;
                    break;
                case 20:
                    guest = new Random().nextInt(150) + 500;
                    break;
                case 21:
                    guest = new Random().nextInt(150) + 400;
                    break;
                case 22:
                    guest = new Random().nextInt(150) + 350;
                    break;
                case 23:
                    guest = new Random().nextInt(150) + 300;
                    break;
                default:
                    guest = new Random().nextInt(150) + 300;
                    break;
            }
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return guest;
    }
    public double generateSpeed(String dateTime) {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        double speed = 100;
        try {

            Calendar calendar = Calendar.getInstance();
            calendar.setTime(sdf.parse(dateTime));
            int hour = calendar.get(Calendar.HOUR_OF_DAY);

            switch (hour) {
                case 0:
                    speed = new Random().nextInt(30) + 30 + (new Random().nextDouble());
                    break;
                case 1:
                    speed = new Random().nextInt(20) + 30 + (new Random().nextDouble());
                    break;
                case 2:
                    speed = new Random().nextInt(20) + 30 + (new Random().nextDouble());
                    break;
                case 3:
                    speed = new Random().nextInt(30) + 30 + (new Random().nextDouble());
                    break;
                case 4:
                    speed = new Random().nextInt(30) + 40 + (new Random().nextDouble());
                    break;
                case 5:
                    speed = new Random().nextInt(40) + 50 + (new Random().nextDouble());
                    break;
                case 6:
                    speed = new Random().nextInt(40) + 50 + (new Random().nextDouble());
                    break;
                case 7:
                    speed = new Random().nextInt(30) + 50 + (new Random().nextDouble());
                    break;
                case 8:
                    speed = new Random().nextInt(20) + 45 + (new Random().nextDouble());
                    break;
                case 9:
                    speed = new Random().nextInt(20) + 40 + (new Random().nextDouble());
                    break;
                case 10:
                    speed = new Random().nextInt(20) + 30 + (new Random().nextDouble());
                    break;
                case 11:
                    speed = new Random().nextInt(20) + 30 + (new Random().nextDouble());
                    break;
                case 12:
                    speed = new Random().nextInt(20) + 20 + (new Random().nextDouble());
                    break;
                case 13:
                    speed = new Random().nextInt(20) + 20 + (new Random().nextDouble());
                    break;
                case 14:
                    speed = new Random().nextInt(10) + 20 + (new Random().nextDouble());
                    break;
                case 15:
                    speed = new Random().nextInt(10) + 20 + (new Random().nextDouble());
                    break;
                case 16:
                    speed = new Random().nextInt(10) + 20 + (new Random().nextDouble());
                    break;
                case 17:
                    speed = new Random().nextInt(10) + 20 + (new Random().nextDouble());
                    break;
                case 18:
                    speed = new Random().nextInt(10) + 30 + (new Random().nextDouble());
                    break;
                case 19:
                    speed = new Random().nextInt(10) + 30 + (new Random().nextDouble());
                    break;
                case 20:
                    speed = new Random().nextInt(10) + 30 + (new Random().nextDouble());
                    break;
                case 21:
                    speed = new Random().nextInt(10) + 30 + (new Random().nextDouble());
                    break;
                case 22:
                    speed = new Random().nextInt(10) + 30 + (new Random().nextDouble());
                    break;
                case 23:
                    speed = new Random().nextInt(20) + 30 + (new Random().nextDouble());
                    break;
                default:
                    speed = new Random().nextInt(20) + 30 + (new Random().nextDouble());
                    break;
            }
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return speed;
    }

    public List<Report> queryForSection() {
        String sql = "SELECT highway_name,section_name,positive_direction,negative_direction FROM d_hx_highway_cell GROUP BY section_name,highway_name,positive_direction,negative_direction";
//        JdbcTemplate template = SpringContextInstance.getBean("oracleJdbcTemplate", JdbcTemplate.class);
        JdbcTemplate template = SpringContextInstance.getBean("pgJdbcTemplate", JdbcTemplate.class);
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

    public void insertTable(final List<Report> list) {
        String sql = "";

        if("unknown".equals(GeneratorEvery20Mins.env)) {
            System.out.println("env is unknown, plz input args");
            return;
        }

        if("test".equals(GeneratorEvery20Mins.env)) {
          //本地pg数据库
          sql = "INSERT INTO f_hx_highway_statistic_20m (start_time, highway_name, section_name, direction, guest_count, speed) VALUES (to_timestamp(?,'YYYY-MM-DD HH24:MI:SS'),?,?,?,?,?)";
          //测试表
//            sql = "INSERT INTO temp_hx_highway_result2 (start_time, highway_name, section_name, direction, guest_count, speed) VALUES (?,?,?,?,?,?)";
        }
        if("product".equals(GeneratorEvery20Mins.env)) {
            //生产环境
            sql = "INSERT INTO f_hx_highway_statistic_20m (start_time, highway_name, section_name, direction, guest_count, speed) VALUES (?,?,?,?,?,?)";
        }

//        JdbcTemplate template = SpringContextInstance.getBean("oracleJdbcTemplate", JdbcTemplate.class);
        JdbcTemplate template = SpringContextInstance.getBean("pgJdbcTemplate", JdbcTemplate.class);

        template.batchUpdate(sql, new BatchPreparedStatementSetter() {
            public void setValues(PreparedStatement ps, int i) throws SQLException {
                Report report = list.get(i);
                ps.setString(1, report.getTime());
                ps.setString(2, report.getHighwayName());
                ps.setString(3, report.getSectionName());
                ps.setString(4, report.getDirection());
                ps.setInt(5, report.getGuest());
                ps.setDouble(6, report.getSpeed());
            }
            public int getBatchSize() {
                return list.size();
            }
        });
    }
}
