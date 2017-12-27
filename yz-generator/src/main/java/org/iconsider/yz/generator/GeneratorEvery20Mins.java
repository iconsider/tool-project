package org.iconsider.yz.generator;

import org.iconsider.yz.model.Report;
import org.iconsider.yz.utils.DateUtils;
import org.iconsider.yz.utils.SpringContextInstance;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.support.rowset.SqlRowSet;
import org.springframework.stereotype.Component;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Created by liuzhenxing on 2017-12-27.
 */
@Component
public class GeneratorEvery20Mins {
    public static void main(String[] args) {
        System.out.println("start generate data app");

        GeneratorEvery20Mins generator = SpringContextInstance.getBean("generatorEvery20Mins", GeneratorEvery20Mins.class);
        while (true) {
            if (DateUtils.isMod20Min()) {
              generator.generate();
            }
            try {
                Thread.currentThread().sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
                System.out.println("睡眠异常");
            }
        }
//        generator.generate();
    }

    public void generate() {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        List<Report> sectioninfoList = this.getSectionInfo();
        List<Report> reportList = this.generateData(sectioninfoList);
        int num = this.saveReports(reportList);
        System.out.println(String.format("updated %s records", num));
        System.out.println(String.format("current time: %s", sdf.format(new Date())));
        System.out.println("---------------");
    }


    /**
     * 获取高速路段信息
     * 包括：highway_id,section_id,positive_direction,negative_direction
     */
    public List<Report> getSectionInfo() {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:00");
        JdbcTemplate jdbcTemplate = SpringContextInstance.getBean("jdbcTemplate", JdbcTemplate.class);
        String sql = "SELECT highway_id,highway_name,section_id,section_name,positive_direction,negative_direction FROM d_hx_highway_cell GROUP BY highway_id,highway_name,section_id,section_name,positive_direction,negative_direction";
        SqlRowSet rowSet = jdbcTemplate.queryForRowSet(sql);
        List<Report> list = new ArrayList<>();
        while (rowSet.next()) {
            Report report = new Report();
            report.setStart_time(Timestamp.valueOf(sdf.format(new Date())));
            report.setHighway_id(rowSet.getInt("highway_id"));
            report.setHighway_name(rowSet.getString("highway_name"));
            report.setSection_id(rowSet.getInt("section_id"));
            report.setSection_name(rowSet.getString("section_name"));
            report.setPostitive_direction(rowSet.getString("positive_direction"));
            report.setNegative_direction(rowSet.getString("negative_direction"));
            list.add(report);
        }
        System.out.println(String.format("共获取到%s个路段信息", list.size()));
        return list;
    }

    /**
     * 生成路段速度和人数
     */
    public List<Report> generateData(List<Report> reports) {
        List<Report> list = new ArrayList<>();
        Calendar calendar = Calendar.getInstance();
        int hour = calendar.get(Calendar.HOUR_OF_DAY);

        for (Report r : reports) {
            int r_postitive_guest = this.generateGuest(hour);
            int r_negative_guest = this.generateGuest(hour);
            double r_postitive_speed = this.generateSpeed(hour);
            double r_negative_speed = this.generateSpeed(hour);
            Report r_postitive = new Report(r.getStart_time(),r.getHighway_id(),r.getHighway_name(),r.getSection_id(),r.getSection_name(),r_postitive_guest,r_postitive_speed,r.getPostitive_direction());
            Report r_negative = new Report(r.getStart_time(), r.getHighway_id(), r.getHighway_name(), r.getSection_id(), r.getSection_name(), r_negative_guest, r_negative_speed, r.getNegative_direction());
            list.add(r_postitive);
            list.add(r_negative);
        }
        System.out.println(String.format("共生成%s个路段信息", list.size()));
        return list;
    }

    /**
     * 保存数据到数据库
     */
    public int saveReports(final List<Report> list) {
        JdbcTemplate jdbcTemplate = SpringContextInstance.getBean("jdbcTemplate", JdbcTemplate.class);
        String sql = "INSERT INTO f_hx_highway_statistic_20m (start_time, highway_name, section_name, direction, guest_count, speed) VALUES (to_timestamp(?,'YYYY-MM-DD HH24:MI:SS'),?,?,?,?,?)";
        int[] result = jdbcTemplate.batchUpdate(sql, new BatchPreparedStatementSetter() {
            @Override
            public void setValues(PreparedStatement preparedStatement, int i) throws SQLException {
                Report report = list.get(i);
                preparedStatement.setTimestamp(1, report.getStart_time());
                preparedStatement.setString(2, report.getHighway_name());
                preparedStatement.setString(3, report.getSection_name());
                preparedStatement.setString(4, report.getDirection());
                preparedStatement.setInt(5, report.getGuest_count());
                preparedStatement.setDouble(6, report.getSpeed());
            }
            @Override
            public int getBatchSize() {
                return list.size();
            }
        });
        return result.length;
    }

    /**
     * 生成人数
     */
    public int generateGuest(int hour) {
        int guest = 0;
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
        return guest;
    }

    /**
     * 生成路段速度
     */
    public double generateSpeed(int hour) {
        double speed = 0D;
        switch (hour) {
            case 0:
                speed = new Random().nextInt(30) + 80 + (new Random().nextDouble());
                break;
            case 1:
                speed = new Random().nextInt(20) + 90 + (new Random().nextDouble());
                break;
            case 2:
                speed = new Random().nextInt(10) + 100 + (new Random().nextDouble());
                break;
            case 3:
                speed = new Random().nextInt(15) + 100 + (new Random().nextDouble());
                break;
            case 4:
                speed = new Random().nextInt(10) + 105 + (new Random().nextDouble());
                break;
            case 5:
                speed = new Random().nextInt(10) + 105 + (new Random().nextDouble());
                break;
            case 6:
                speed = new Random().nextInt(10) + 105 + (new Random().nextDouble());
                break;
            case 7:
                speed = new Random().nextInt(15) + 100 + (new Random().nextDouble());
                break;
            case 8:
                speed = new Random().nextInt(20) + 90 + (new Random().nextDouble());
                break;
            case 9:
                speed = new Random().nextInt(20) + 90 + (new Random().nextDouble());
                break;
            case 10:
                speed = new Random().nextInt(30) + 80 + (new Random().nextDouble());
                break;
            case 11:
                speed = new Random().nextInt(30) + 70 + (new Random().nextDouble());
                break;
            case 12:
                speed = new Random().nextInt(40) + 60 + (new Random().nextDouble());
                break;
            case 13:
                speed = new Random().nextInt(50) + 50 + (new Random().nextDouble());
                break;
            case 14:
                speed = new Random().nextInt(50) + 50 + (new Random().nextDouble());
                break;
            case 15:
                speed = new Random().nextInt(50) + 50 + (new Random().nextDouble());
                break;
            case 16:
                speed = new Random().nextInt(50) + 50 + (new Random().nextDouble());
                break;
            case 17:
                speed = new Random().nextInt(40) + 60 + (new Random().nextDouble());
                break;
            case 18:
                speed = new Random().nextInt(40) + 60 + (new Random().nextDouble());
                break;
            case 19:
                speed = new Random().nextInt(40) + 60 + (new Random().nextDouble());
                break;
            case 20:
                speed = new Random().nextInt(30) + 70 + (new Random().nextDouble());
                break;
            case 21:
                speed = new Random().nextInt(30) + 70 + (new Random().nextDouble());
                break;
            case 22:
                speed = new Random().nextInt(30) + 70 + (new Random().nextDouble());
                break;
            case 23:
                speed = new Random().nextInt(20) + 80 + (new Random().nextDouble());
                break;
            default:
                speed = new Random().nextInt(30) + 80 + (new Random().nextDouble());
                break;
        }
        return speed;
    }

}
