package org.iconsider.model;

/**
 * Created by liuzhenxing on 2017-8-31.
 */
public class Report {
    String time;
    String highwayName;
    String sectionName;
    String direction;
    int guest;
    double speed;
    String positiveDirection;
    String negative_direction;

    public String getTime() {
        return time;
    }

    public void setTime(String time) {
        this.time = time;
    }

    public String getHighwayName() {
        return highwayName;
    }

    public void setHighwayName(String highwayName) {
        this.highwayName = highwayName;
    }

    public String getSectionName() {
        return sectionName;
    }

    public void setSectionName(String sectionName) {
        this.sectionName = sectionName;
    }

    public String getDirection() {
        return direction;
    }

    public void setDirection(String direction) {
        this.direction = direction;
    }

    public int getGuest() {
        return guest;
    }

    public void setGuest(int guest) {
        this.guest = guest;
    }

    public double getSpeed() {
        return speed;
    }

    public void setSpeed(double speed) {
        this.speed = speed;
    }

    public String getPositiveDirection() {
        return positiveDirection;
    }

    public void setPositiveDirection(String positiveDirection) {
        this.positiveDirection = positiveDirection;
    }

    public String getNegative_direction() {
        return negative_direction;
    }

    public void setNegative_direction(String negative_direction) {
        this.negative_direction = negative_direction;
    }

    @Override
    public String toString() {
        return "Report{" +
                "time='" + time + '\'' +
                ", highwayName='" + highwayName + '\'' +
                ", sectionName='" + sectionName + '\'' +
                ", direction='" + direction + '\'' +
                ", guest=" + guest +
                ", speed=" + speed +
                ", positiveDirection='" + positiveDirection + '\'' +
                ", negative_direction='" + negative_direction + '\'' +
                '}';
    }
}
