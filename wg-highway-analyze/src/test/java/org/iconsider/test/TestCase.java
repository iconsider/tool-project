package org.iconsider.test;

import org.iconsider.highway.model.Record;
import org.junit.Test;

import java.util.*;

/**
 * Created by liuzhenxing on 2017-9-25.
 */
public class TestCase {

    @Test
    public void test1() {
        Map<String, String> map = new HashMap<String, String>();
        map.put("a", "1");
        map.put("b", "2");
        map.put("a", "3");

        System.out.println(map.get("a"));
    }

    @Test
    public void test2() {
        List<Record> list = new ArrayList<Record>();

        Record r1 = new Record();
        r1.setLasttime(1506060000000L);
        Record r2 = new Record();
        r2.setLasttime(1506060900000L);
        Record r3 = new Record();
        r3.setLasttime(1506060600000L);
        Record r4 = new Record();
        r4.setLasttime(1506060300000L);

        list.add(r4);
        list.add(r2);
        list.add(r1);
        list.add(r3);

        Collections.sort(list, new Comparator<Record>() {
            public int compare(Record o1, Record o2) {
                if(o1.getLasttime() - o2.getLasttime() > 0) {
                    return 1;
                } else if(o1.getLasttime() - o2.getLasttime() < 0) {
                    return -1;
                }
                return 0;
            }
        });

        for (Record record : list) {
            System.out.println(record);
        }
    }

    @Test
    public void test3() {
        List<String> list = new ArrayList<String>();
        list.add("a");
        list.add("b");
        list.add("c");


        Set<String> tmpCgiSet = new HashSet<String>();
        for (String cgi : list) {
            tmpCgiSet.add(cgi);
        }

        if(tmpCgiSet.size() > 1) {
            System.out.println(false);
        } else {
            System.out.println(true);
        }
    }
}
