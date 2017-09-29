package org.iconsider.highway.model;

/**
 * Created by liuzhenxing on 2017-9-26.
 */
public class SectionReport {
    private String startTime = "unknown";
    private int highwayId = 0;
    private int sectionId = 0;
    private String direction = "unknown";
    private int guestCounter = 0;
    private double speed = 0D;


    public String getStartTime() {
        return startTime;
    }

    public void setStartTime(String startTime) {
        this.startTime = startTime;
    }

    public String getDirection() {
        return direction;
    }

    public void setDirection(String direction) {
        this.direction = direction;
    }

    public int getGuestCounter() {
        return guestCounter;
    }

    public void setGuestCounter(int guestCounter) {
        this.guestCounter = guestCounter;
    }

    public double getSpeed() {
        return speed;
    }

    public void setSpeed(double speed) {
        this.speed = speed;
    }

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


    public SectionReport(String startTime, int highwayId, int sectionId, String direction, int guestCounter, double speed) {
        this.startTime = startTime;
        this.highwayId = highwayId;
        this.sectionId = sectionId;
        this.direction = direction;
        this.guestCounter = guestCounter;
        this.speed = speed;
    }

    @Override
    public String toString() {
        return "SectionReport{" +
                "startTime='" + startTime + '\'' +
                ", direction='" + direction + '\'' +
                ", guestCounter=" + guestCounter +
                ", speed=" + speed +
                ", highwayId=" + highwayId +
                ", sectionId=" + sectionId +
                '}';
    }
}
