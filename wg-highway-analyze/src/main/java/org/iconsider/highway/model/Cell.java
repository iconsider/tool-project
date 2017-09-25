package org.iconsider.highway.model;

import java.io.Serializable;

/**
 * Created by liuzhenxing on 2017-9-25.
 *
 */
public class Cell implements Serializable {
    private static final long serialVersionUID = -848866047528892390L;
    private int cellId;
    private String cellName;
    private int highwayId;
    private String highwayName;
    private int sectionId;
    private String sectionName;
    private String postiveDirection;
    private String negativeDirection;
    private double distance;
    private String cgi;

    public static long getSerialVersionUID() {
        return serialVersionUID;
    }

    public int getCellId() {
        return cellId;
    }

    public void setCellId(int cellId) {
        this.cellId = cellId;
    }

    public String getCellName() {
        return cellName;
    }

    public void setCellName(String cellName) {
        this.cellName = cellName;
    }

    public int getHighwayId() {
        return highwayId;
    }

    public void setHighwayId(int highwayId) {
        this.highwayId = highwayId;
    }

    public String getHighwayName() {
        return highwayName;
    }

    public void setHighwayName(String highwayName) {
        this.highwayName = highwayName;
    }

    public int getSectionId() {
        return sectionId;
    }

    public void setSectionId(int sectionId) {
        this.sectionId = sectionId;
    }

    public String getSectionName() {
        return sectionName;
    }

    public void setSectionName(String sectionName) {
        this.sectionName = sectionName;
    }

    public String getPostiveDirection() {
        return postiveDirection;
    }

    public void setPostiveDirection(String postiveDirection) {
        this.postiveDirection = postiveDirection;
    }

    public String getNegativeDirection() {
        return negativeDirection;
    }

    public void setNegativeDirection(String negativeDirection) {
        this.negativeDirection = negativeDirection;
    }

    public double getDistance() {
        return distance;
    }

    public void setDistance(double distance) {
        this.distance = distance;
    }

    public String getCgi() {
        return cgi;
    }

    public void setCgi(String cgi) {
        this.cgi = cgi;
    }

    public Cell(int cellId, String cellName, int highwayId, String highwayName, int sectionId, String sectionName, String postiveDirection, String negativeDirection, double distance, String cgi) {
        this.cellId = cellId;
        this.cellName = cellName;
        this.highwayId = highwayId;
        this.highwayName = highwayName;
        this.sectionId = sectionId;
        this.sectionName = sectionName;
        this.postiveDirection = postiveDirection;
        this.negativeDirection = negativeDirection;
        this.distance = distance;
        this.cgi = cgi;
    }

    @Override
    public String toString() {
        return "Cell{" +
                "cellId=" + cellId +
                ", cellName='" + cellName + '\'' +
                ", highwayId=" + highwayId +
                ", highwayName='" + highwayName + '\'' +
                ", sectionId=" + sectionId +
                ", sectionName='" + sectionName + '\'' +
                ", postiveDirection='" + postiveDirection + '\'' +
                ", negativeDirection='" + negativeDirection + '\'' +
                ", distance=" + distance +
                ", cgi='" + cgi + '\'' +
                '}';
    }
}
