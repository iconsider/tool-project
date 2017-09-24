package org.iconsider.highway.model;

import java.io.Serializable;
import java.text.ParseException;
import java.text.SimpleDateFormat;

/**
 * Created by liuzhenxing on 2017-9-24.
 * 对5分钟快照的每一行记录进行封装成Record对象
 * 通过带参数构造函数创建对象，如果出现异常，
 * 字符串成员变量取值为unkonwn，long类型成员变量为0L。
 *
 */
public class Record implements Serializable {
    private static final long serialVersionUID = -8705530133947691340L;
    private String msisdn;
    private long lasttime;
    private String cgi;
    private long startTimeStamp;

    public String getMsisdn() {
        return msisdn;
    }

    public void setMsisdn(String msisdn) {
        this.msisdn = msisdn;
    }

    public long getLasttime() {
        return lasttime;
    }

    public void setLasttime(long lasttime) {
        this.lasttime = lasttime;
    }

    public String getCgi() {
        return cgi;
    }

    public void setCgi(String cgi) {
        this.cgi = cgi;
    }

    public long getStartTimeStamp() {
        return startTimeStamp;
    }

    public void setStartTimeStamp(long startTimeStamp) {
        this.startTimeStamp = startTimeStamp;
    }

    public Record(String record) {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        String[] recordInfo = record.split(",");

        if(recordInfo == null || recordInfo.length != 12) {
            this.setNull();
        } else {
            try {
                this.msisdn = recordInfo[1];
                this.lasttime = sdf.parse(recordInfo[9]).getTime();
                this.cgi = recordInfo[10];
                this.startTimeStamp = Long.parseLong(recordInfo[11]);
            } catch (ParseException e) {
                setNull();
                e.printStackTrace();
            }
        }

    }

    private void setNull() {
        this.msisdn = "unknown";
        this.lasttime = 0L;
        this.cgi = "unknown";
        this.startTimeStamp = 0L;
    }

    @Override
    public String toString() {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        return "Record{" +
                "msisdn='" + msisdn + '\'' +
                ", lasttime=" + sdf.format(lasttime) +
                ", cgi='" + cgi + '\'' +
                ", startTimeStamp=" + sdf.format(startTimeStamp) +
                '}';
    }

    public static void main(String[] args) {
        Record record1 = new Record("2017-09-22 14:15:00,88888888888,3,4G,888888,888888,000000000000000,0000000000000000,20170922,2017-09-22 14:10:04,830811-129,1506060900000");
        Record record2 = new Record("");
        System.out.println(record1);
        System.out.println(record2);
    }
}
