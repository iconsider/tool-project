package org.iconsider.yz.model;

import java.io.Serializable;
import java.sql.Timestamp;

/**
 * Created by liuzhenxing on 2017-12-27.
 */
public class Report implements Serializable {
    private static final long serialVersionUID = -6256973492398563625L;
    private Timestamp start_time;
    private int highway_id;
    private String highway_name;
    private int section_id;
    private String section_name;
    private String postitive_direction;
    private String negative_direction;
    private int guest_count;
    private double speed;
    private String direction;

    /**
     * constructor
     */
    public Report() {
    }

    public Report(Timestamp start_time, int highway_id, String highway_name, int section_id, String section_name, int guest_count, double speed, String direction) {
        this.start_time = start_time;
        this.highway_id = highway_id;
        this.highway_name = highway_name;
        this.section_id = section_id;
        this.section_name = section_name;
        this.guest_count = guest_count;
        this.speed = speed;
        this.direction = direction;
    }

    /**
     * getter and setter
     */
    public Timestamp getStart_time() {
        return start_time;
    }

    public void setStart_time(Timestamp start_time) {
        this.start_time = start_time;
    }

    public int getHighway_id() {
        return highway_id;
    }

    public void setHighway_id(int highway_id) {
        this.highway_id = highway_id;
    }

    public String getHighway_name() {
        return highway_name;
    }

    public void setHighway_name(String highway_name) {
        this.highway_name = highway_name;
    }

    public int getSection_id() {
        return section_id;
    }

    public void setSection_id(int section_id) {
        this.section_id = section_id;
    }

    public String getSection_name() {
        return section_name;
    }

    public void setSection_name(String section_name) {
        this.section_name = section_name;
    }

    public String getPostitive_direction() {
        return postitive_direction;
    }

    public void setPostitive_direction(String postitive_direction) {
        this.postitive_direction = postitive_direction;
    }

    public String getNegative_direction() {
        return negative_direction;
    }

    public void setNegative_direction(String negative_direction) {
        this.negative_direction = negative_direction;
    }

    public int getGuest_count() {
        return guest_count;
    }

    public void setGuest_count(int guest_count) {
        this.guest_count = guest_count;
    }

    public double getSpeed() {
        return speed;
    }

    public void setSpeed(double speed) {
        this.speed = speed;
    }

    public String getDirection() {
        return direction;
    }

    public void setDirection(String direction) {
        this.direction = direction;
    }

    @Override
    public String toString() {
        return "Report{" +
                "start_time=" + start_time +
                ", highway_id=" + highway_id +
                ", highway_name='" + highway_name + '\'' +
                ", section_id=" + section_id +
                ", section_name='" + section_name + '\'' +
                ", postitive_direction='" + postitive_direction + '\'' +
                ", negative_direction='" + negative_direction + '\'' +
                ", guest_count=" + guest_count +
                ", speed=" + speed +
                ", direction='" + direction + '\'' +
                '}';
    }
}
