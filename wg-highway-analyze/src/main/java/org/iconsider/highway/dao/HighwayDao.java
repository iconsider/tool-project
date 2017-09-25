package org.iconsider.highway.dao;

import org.iconsider.highway.model.Cell;
import org.iconsider.highway.model.Record;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.support.rowset.SqlRowSet;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

/**
 * Created by liuzhenxing on 2017-9-25.
 */
public class HighwayDao {
    public Map<String, Cell> getAllCell() {
        ClassPathXmlApplicationContext context = new ClassPathXmlApplicationContext("applicationContext.xml");
        JdbcTemplate jdbcTemple = context.getBean("postgresJdbcTemplate", JdbcTemplate.class);
        Map<String, Cell> map = new HashMap<String, Cell>();

        String sql = "select cell_id,cell_name,highway_id,highway_name,section_id,section_name,positive_direction,negative_direction,distance,cgi from d_hx_highway_cell";
        SqlRowSet rs = jdbcTemple.queryForRowSet(sql);
        while(rs.next()) {
            int cellId = rs.getInt("cell_id");
            String cellName = rs.getString("cell_name");
            int highwayId = rs.getInt("highway_id");
            String highwayName = rs.getString("highway_name");
            int sectionId = rs.getInt("section_id");
            String sectionName = rs.getString("section_name");
            String postiveDirection = rs.getString("positive_direction");
            String negative_direction = rs.getString("negative_direction");
            double distance = rs.getDouble("distance");
            String cgi = rs.getString("cgi");

            map.put(cgi, new Cell(cellId,cellName,highwayId,highwayName,sectionId,sectionName,postiveDirection,negative_direction,distance,cgi));

        }
        return map;
    }

    public int getSectionIdByCgi(String cgi) {
        ClassPathXmlApplicationContext context = new ClassPathXmlApplicationContext("applicationContext.xml");
        JdbcTemplate jdbcTemple = context.getBean("postgresJdbcTemplate", JdbcTemplate.class);

        List<Integer> list = jdbcTemple.query(String.format("select section_id from d_hx_highway_cell where cgi='%s' limit 1", cgi), new RowMapper<Integer>() {
            public Integer mapRow(ResultSet resultSet, int i) throws SQLException {
                return resultSet.getInt("section_id");
            }
        });

        if(list.size() == 1) {
            return list.get(0);
        } else {
            return -1;
        }
    }


    public static void main(String[] args) {
        HighwayDao dao = new HighwayDao();
//        Map<String, Cell> map = dao.getAllCell();
//        System.out.println(map.get("10411-1326"));


        System.out.println(dao.getSectionIdByCgi("657635-11"));
    }
}
