package org.iconsider.highway.model;

import java.io.Serializable;

/**
 * Created by liuzhenxing on 2017-9-25.
 */
public class UserReport implements Serializable {
    private static final long serialVersionUID = 2790936357289775224L;
    private int highwayId;
    private int sectionId;
    private String direction;
    private double distance;
    private double time;
    private double speed;


    public int getHighwayId() {
        return highwayId;
    }

    public void setHighwayId(int highwayId) {
        this.highwayId = highwayId;
    }

    public int getSectionId() {
        return sectionId;
    }

    public void setSectionId(int sectionId) {
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

    public double getSpeed() {
        return speed;
    }

    public void setSpeed(double speed) {
        this.speed = speed;
    }

    public UserReport(int highwayId, int sectionId, String direction, double distance, double time) {
        this.highwayId = highwayId;
        this.sectionId = sectionId;
        this.direction = direction;
        this.distance = distance;
        this.time = time;
    }

    @Override
    public String toString() {
        return "UserReport{" +
                "highwayName='" + highwayId + '\'' +
                ", sectionName='" + sectionId + '\'' +
                ", direction='" + direction + '\'' +
                ", distance=" + String.format("%.2f",distance) +
                ", time=" + String.format("%.2f",time) +
                ", speed=" + speed +
                '}';
    }
}
