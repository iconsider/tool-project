package org.iconsider.highway.model;

import java.io.Serializable;

/**
 * Created by liuzhenxing on 2017-9-25.
 */
public class UserReport implements Serializable {
    private static final long serialVersionUID = 2790936357289775224L;
    private String sectionId;
    private String direction;
    private double distance;
    private double time;  //hour

    public String getSectionId() {
        return sectionId;
    }

    public void setSectionId(String sectionId) {
        this.sectionId = sectionId;
    }

    public String getDirection() {
        return direction;
    }

    public void setDirection(String direction) {
        this.direction = direction;
    }

    public double getDistance() {
        return distance;
    }

    public void setDistance(double distance) {
        this.distance = distance;
    }

    public double getTime() {
        return time;
    }

    public void setTime(double time) {
        this.time = time;
    }

    public UserReport(String sectionId, String direction, double distance, double time) {
        this.sectionId = sectionId;
        this.direction = direction;
        this.distance = distance;
        this.time = time;
    }

    @Override
    public String toString() {
        return "UserReport{" +
                "sectionId='" + sectionId + '\'' +
                ", direction='" + direction + '\'' +
                ", distance=" + distance +
                ", time=" + time +
                '}';
    }
}
