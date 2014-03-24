package com.opendatagroup.hyperspectral.geo;

import com.opendatagroup.hyperspectral.geo.NinePoints;

public class GeoTransformations {
    final double A = 6378.1370;
    final double B = 6356.7523142;
    final double ECCENTRICITY2 = (A*A - B*B)/A/A;

    int width = 0;
    int height = 0;
    double dlngdx = 0.0;
    double dlatdy = 0.0;
    double dlatdx = 0.0;
    double dlngdy = 0.0;
    double d2latdy2 = 0.0;
    double d2lngdx2 = 0.0;
    double d2lngdy2 = 0.0;
    double d2latdx2 = 0.0;
    double d2latdxdy = 0.0;
    double d2lngdxdy = 0.0;
    double CENTER_LON = 0.0;
    double CENTER_LAT = 0.0;
    double LON_TO_KM = 0.0;
    double LAT_TO_KM = 0.0;

    public GeoTransformations(NinePoints n, int _width, int _height) {
        width = _width;
        height = _height;

        dlngdx = (n.middleRightLng - n.middleLeftLng) / width;
        dlatdy = (n.topMiddleLat - n.bottomMiddleLat) / height;
        dlatdx = (n.middleRightLat - n.middleLeftLat) / width;
        dlngdy = (n.topMiddleLng - n.bottomMiddleLng) / height;
        d2latdy2 = 2.0 * (n.topMiddleLat - 2.0*n.middleMiddleLat + n.bottomMiddleLat) / Math.pow(height, 2);
        d2lngdx2 = 2.0 * (n.middleRightLng - 2.0*n.middleMiddleLng + n.middleLeftLng) / Math.pow(width, 2);
        d2lngdy2 = 2.0 * (n.topMiddleLng - 2.0*n.middleMiddleLng + n.bottomMiddleLng) / Math.pow(height, 2);
        d2latdx2 = 2.0 * (n.middleRightLat - 2.0*n.middleMiddleLat + n.middleLeftLat) / Math.pow(width, 2);
        d2latdxdy = (n.topRightLat - n.bottomRightLat - n.topLeftLat + n.bottomLeftLat) / (height * width);
        d2lngdxdy = (n.topRightLng - n.bottomRightLng - n.topLeftLng + n.bottomLeftLng) / (height * width);

        CENTER_LON = n.middleMiddleLng;
        CENTER_LAT = n.middleMiddleLat;
        LON_TO_KM = (Math.PI*A) * Math.cos(CENTER_LAT*Math.PI/180.0) / 180.0 / Math.sqrt(1.0 - ECCENTRICITY2*Math.pow(Math.sin(CENTER_LAT*Math.PI/180.0), 2));
        LAT_TO_KM = (Math.PI*A) * (1.0 - ECCENTRICITY2) / 180.0 / Math.pow(1.0 - ECCENTRICITY2*Math.pow(Math.sin(CENTER_LAT*Math.PI/180.0), 2), 1.5);
    }

    static public class LngLat {
        public double lng = 0.0;
        public double lat = 0.0;
        public LngLat() {}
        public LngLat(double _lng, double _lat) {
            lng = _lng;
            lat = _lat;
        }
    }

    static public class Meters {
        public double metersEast = 0.0;
        public double metersNorth = 0.0;
        public Meters(double _metersEast, double _metersNorth) {
            metersEast = _metersEast;
            metersNorth = _metersNorth;
        }
    }

    public LngLat getLngLat(int indexX, int indexY) {
        double x = (indexX - width/2.0);
        double y = (indexY - height/2.0);
        return new LngLat(CENTER_LON + dlngdx*x + dlngdy*y + d2lngdx2*x*x + d2lngdy2*y*y + d2lngdxdy*x*y,
                          CENTER_LAT + dlatdy*y + dlatdx*x + d2latdy2*y*y + d2latdx2*x*x + d2latdxdy*x*y);
    }

    public Meters getMeters(LngLat lnglat) {
        return new Meters((lnglat.lng - CENTER_LON) * LON_TO_KM * 1000.0, (lnglat.lat - CENTER_LAT) * LAT_TO_KM * 1000.0);
    }

    public Meters getMeters(int indexX, int indexY) {
        return getMeters(getLngLat(indexX, indexY));
    }
}
