package org.iconsider.driver;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.spark.SparkConf;
import org.iconsider.job.SparkJob;

import java.io.IOException;

/**
 * Created by liuzhenxing on 2017-10-24.
 */
public class SparkDriver {
    public static void main(String[] args) {
        if (args.length < 1) {
            System.out.println("plz input args");
            return;
        }
        System.out.println(String.format("my args: %s", args[0]));
        SparkConf conf = new SparkConf();
        SparkDriver.spark_kerberos();
        SparkJob job = new SparkJob();
        job.setPath(args[0]);
        job.execute(conf);
    }

    //kerberos验证
    public static void spark_kerberos() {
        try {
            Configuration conf = new Configuration();
            System.setProperty("java.security.krb5.conf", "/etc/krb5.conf");
            UserGroupInformation.setConfiguration(conf);
            UserGroupInformation.loginUserFromKeytab("hanxinnil", "/home/hanxinnil/hanxinnil.keytab");
            System.out.println("kerberos in highway spark, 认证成功(seccess)");
        } catch (IOException e) {
            e.printStackTrace();
            System.out.println("kerberos in highway spark, 认证失败(fail)");
        }
    }
}
