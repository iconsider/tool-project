package org.iconsider.redis;

import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.data.redis.core.SetOperations;
import org.springframework.data.redis.core.StringRedisTemplate;

import java.util.Set;

/**
 * Created by liuzhenxing on 2017-12-15.
 * 把highway路段存在redis的多个key(hr:*)的所有数据存档到一个key(highway-resident)
 */
public class RedisOperation {
    public static void main(String[] args) {
        ApplicationContext applicationContext = new ClassPathXmlApplicationContext("applicationContext.xml");
        StringRedisTemplate stringRedisTemplate = applicationContext.getBean("stringRedisTemplate", StringRedisTemplate.class);

        Set<String> set = stringRedisTemplate.keys("hr:*");

        System.out.println(String.format("hr:*的个数：%s", set.size()));


        //总常驻用户数量
        long num = 0;

        SetOperations<String, String> so = stringRedisTemplate.opsForSet();
        //key
        for (String key : set) {
            Set<String> residentUserSet = so.members(key);
            num += residentUserSet.size();
            for (String userNumber : residentUserSet) {
                so.add("highway-resident", userNumber);
            }
        }

        System.out.println(String.format("各个路段的用户总数：%s", num));
        System.out.println(String.format("新创建的单个set用户总数：%s", so.size("highway-resident")));

    }
}
