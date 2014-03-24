package com.opendatagroup.hyperspectral.geo;

import java.lang.StringBuilder;

public class NinePoints {
    public double bottomLeftLng, bottomLeftLat;
    public double bottomMiddleLng, bottomMiddleLat;
    public double bottomRightLng, bottomRightLat;

    public double middleLeftLng, middleLeftLat;
    public double middleMiddleLng, middleMiddleLat;
    public double middleRightLng, middleRightLat;

    public double topLeftLng, topLeftLat;
    public double topMiddleLng, topMiddleLat;
    public double topRightLng, topRightLat;

    public String toString() {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("{");

        stringBuilder.append(String.format("\"bottom-left\": {\"lng\": %.16g, \"lat\": %.16g}, ", bottomLeftLng, bottomLeftLat));
        stringBuilder.append(String.format("\"bottom-middle\": {\"lng\": %.16g, \"lat\": %.16g}, ", bottomMiddleLng, bottomMiddleLat));
        stringBuilder.append(String.format("\"bottom-right\": {\"lng\": %.16g, \"lat\": %.16g}, ", bottomRightLng, bottomRightLat));

        stringBuilder.append(String.format("\"middle-left\": {\"lng\": %.16g, \"lat\": %.16g}, ", middleLeftLng, middleLeftLat));
        stringBuilder.append(String.format("\"middle-middle\": {\"lng\": %.16g, \"lat\": %.16g}, ", middleMiddleLng, middleMiddleLat));
        stringBuilder.append(String.format("\"middle-right\": {\"lng\": %.16g, \"lat\": %.16g}, ", middleRightLng, middleRightLat));

        stringBuilder.append(String.format("\"top-left\": {\"lng\": %.16g, \"lat\": %.16g}, ", topLeftLng, topLeftLat));
        stringBuilder.append(String.format("\"top-middle\": {\"lng\": %.16g, \"lat\": %.16g}, ", topMiddleLng, topMiddleLat));
        stringBuilder.append(String.format("\"top-right\": {\"lng\": %.16g, \"lat\": %.16g}", topRightLng, topRightLat));

        stringBuilder.append("}");
        return stringBuilder.toString();
    }
}
