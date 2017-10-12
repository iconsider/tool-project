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
    public Map<String, List<Cell>> getAllCell() {
        ClassPathXmlApplicationContext context = new ClassPathXmlApplicationContext("applicationContext.xml");
        JdbcTemplate jdbcTemple = context.getBean("postgresJdbcTemplate", JdbcTemplate.class);
        Map<String, List<Cell>> map = new HashMap<>();

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

            if (map.get(cgi) == null) {
                List<Cell> listTemp = new ArrayList<>();
                Cell cellTemp = new Cell(cellId,cellName,highwayId,highwayName,sectionId,sectionName,postiveDirection,negative_direction,distance,cgi);
                listTemp.add(cellTemp);
                map.put(cgi, listTemp);
            } else {
                boolean isExist = false; //判断是否存在一个highwayId、sectionId和新加highwayId、sectionId一样的Cell
                List<Cell> listTemp = map.get(cgi);
                for (Cell cell : listTemp) {
                    if (cell.getHighwayId() == highwayId && cell.getSectionId() == sectionId) {
                        isExist = true;
                        break;
                    }
                }
                if (!isExist) {     //容器不存在highwayId、sectionId和准备新加cell一样的highwayId、sectionId，才把该cell添加
                    listTemp.add(new Cell(cellId, cellName, highwayId, highwayName, sectionId, sectionName, postiveDirection, negative_direction, distance, cgi));
                }
            }
        }
        return map;
    }

    public Map<Integer, Double> getCellDistance() {
        ClassPathXmlApplicationContext context = new ClassPathXmlApplicationContext("applicationContext.xml");
        JdbcTemplate jdbcTemple = context.getBean("postgresJdbcTemplate", JdbcTemplate.class);
        Map<Integer,Double> map = new HashMap<Integer,Double>();

        String sql = "select cell_id,distance from d_hx_highway_cell";
        SqlRowSet rs = jdbcTemple.queryForRowSet(sql);
        while(rs.next()) {
            int cellId = rs.getInt("cell_id");
            double distance = rs.getDouble("distance");

            map.put(cellId, distance);

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

        if(list != null && list.size() == 1) {
            return list.get(0);
        } else {
            return -1;
        }
    }

    public double getDistanceByCellId(int minCellId, int maxCellId) {
        ClassPathXmlApplicationContext context = new ClassPathXmlApplicationContext("applicationContext.xml");
        JdbcTemplate jdbcTemple = context.getBean("postgresJdbcTemplate", JdbcTemplate.class);

        List<Double> list = jdbcTemple.query(String.format("select sum(distance) as \"distance\" from d_hx_highway_cell where cell_id>=%s and cell_id<=%s;", minCellId, maxCellId), new RowMapper<Double>() {
            public Double mapRow(ResultSet resultSet, int i) throws SQLException {
                return resultSet.getDouble("distance");
            }
        });

        if(list != null && list.size() == 1) {
            return list.get(0);
        } else {
            return -1;
        }
    }


    public static void main(String[] args) {
        HighwayDao dao = new HighwayDao();
        Map<String, List<Cell>> map = dao.getAllCell();

        Iterator<Map.Entry<String, List<Cell>>> it = map.entrySet().iterator();
        int count = 0;
        while (it.hasNext()) {
            Map.Entry<String, List<Cell>> entry = it.next();
            if (entry.getValue().size() >= 2) {
                System.out.println(String.format("key:%s, value:%s", entry.getKey(), entry.getValue().size()));
                count++;
            }
        }

        System.out.println(String.format("more than 2: %s", count));


    }
}
