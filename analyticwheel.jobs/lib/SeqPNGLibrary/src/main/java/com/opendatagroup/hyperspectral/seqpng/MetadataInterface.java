package com.opendatagroup.hyperspectral.seqpng;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.Double;
import java.lang.IllegalArgumentException;
import java.lang.Long;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ObjectNode;

import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.LocalDateTime;
import org.joda.time.DateTimeZone;

import com.opendatagroup.hyperspectral.seqpng.ImageBandArray;
import com.opendatagroup.hyperspectral.geo.NinePoints;
import com.opendatagroup.hyperspectral.geo.GeoTransformations;

public class MetadataInterface {
    ObjectMapper objectMapper = new ObjectMapper();
    ObjectNode rootNode = null;
    DateTimeFormatter dateTimeFormatter = DateTimeFormat.forPattern("Y D H:m:s");

    public MetadataInterface(String metadata) {
        try {
            rootNode = (ObjectNode)objectMapper.readTree(metadata);
        }
        catch (IOException exception) {
            throw new RuntimeException("IOException: " + exception.toString());
        }
    }

    public String toString() {
        try {
            ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
            objectMapper.writeValue(byteArrayOutputStream, rootNode);
            return new String(byteArrayOutputStream.toByteArray());
        }
        catch (IOException exception) {
            throw new RuntimeException("IOException: " + exception.toString());
        }
    }

    public JsonNode path(String p) {
        return rootNode.path(p);
    }

    public String getOriginalDirName() {
        return rootNode.path("originalDirName").getTextValue();
    }

    public int getWidth() {
        return rootNode.path("width").getIntValue();
    }

    public int getHeight() {
        return rootNode.path("height").getIntValue();
    }

    public boolean getBase64encoded() {
        return rootNode.path("base64encoded").getBooleanValue();
    }

    public String getProjection() {
        return rootNode.path("Projection").getTextValue();
    }

    public NinePoints getRegistration() {
        ObjectNode reg = (ObjectNode)rootNode.path("registration");
        NinePoints result = new NinePoints();

        result.bottomLeftLng = reg.path("bottom-left").path("lng").getDoubleValue();
        result.bottomLeftLat = reg.path("bottom-left").path("lat").getDoubleValue();

        result.bottomMiddleLng = reg.path("bottom-middle").path("lng").getDoubleValue();
        result.bottomMiddleLat = reg.path("bottom-middle").path("lat").getDoubleValue();

        result.bottomRightLng = reg.path("bottom-right").path("lng").getDoubleValue();
        result.bottomRightLat = reg.path("bottom-right").path("lat").getDoubleValue();

        result.middleLeftLng = reg.path("middle-left").path("lng").getDoubleValue();
        result.middleLeftLat = reg.path("middle-left").path("lat").getDoubleValue();

        result.middleMiddleLng = reg.path("middle-middle").path("lng").getDoubleValue();
        result.middleMiddleLat = reg.path("middle-middle").path("lat").getDoubleValue();

        result.middleRightLng = reg.path("middle-right").path("lng").getDoubleValue();
        result.middleRightLat = reg.path("middle-right").path("lat").getDoubleValue();

        result.topLeftLng = reg.path("top-left").path("lng").getDoubleValue();
        result.topLeftLat = reg.path("top-left").path("lat").getDoubleValue();

        result.topMiddleLng = reg.path("top-middle").path("lng").getDoubleValue();
        result.topMiddleLat = reg.path("top-middle").path("lat").getDoubleValue();

        result.topRightLng = reg.path("top-right").path("lng").getDoubleValue();
        result.topRightLat = reg.path("top-right").path("lat").getDoubleValue();

        return result;
    }

    public List<String> getBandNames() {
        List<String> result = new ArrayList<String>();
        for (JsonNode bandName : rootNode.path("bandNames")) {
            result.add(bandName.getTextValue());
        }
        return result;
    }

    public ImageBandArray.Corners getCornersByIndex() {
        ImageBandArray.Corners result = new ImageBandArray.Corners();

        result.leftX = rootNode.path("cornersByIndex").get(0).get(0).getIntValue();
        result.leftY = rootNode.path("cornersByIndex").get(0).get(1).getIntValue();

        result.topX = rootNode.path("cornersByIndex").get(1).get(0).getIntValue();
        result.topY = rootNode.path("cornersByIndex").get(1).get(1).getIntValue();

        result.rightX = rootNode.path("cornersByIndex").get(2).get(0).getIntValue();
        result.rightY = rootNode.path("cornersByIndex").get(2).get(1).getIntValue();

        result.bottomX = rootNode.path("cornersByIndex").get(3).get(0).getIntValue();
        result.bottomY = rootNode.path("cornersByIndex").get(3).get(1).getIntValue();

        return result;
    }

    public static class LngLatCorners {
        public GeoTransformations.LngLat left = new GeoTransformations.LngLat();
        public GeoTransformations.LngLat top = new GeoTransformations.LngLat();
        public GeoTransformations.LngLat right = new GeoTransformations.LngLat();
        public GeoTransformations.LngLat bottom = new GeoTransformations.LngLat();

        public String toString() {
            return String.format("[[%s, %s], [%s, %s], [%s, %s], [%s, %s]]", left.lng, left.lat, top.lng, top.lat, right.lng, right.lat, bottom.lng, bottom.lat);
        }
    }

    public LngLatCorners getCornersByLngLat() {
        ImageBandArray.Corners byIndex = getCornersByIndex();
        NinePoints ninePoints = getRegistration();
        GeoTransformations geoTransformations = new GeoTransformations(ninePoints, getWidth(), getHeight());

        LngLatCorners result = new LngLatCorners();
        result.left = geoTransformations.getLngLat(byIndex.leftX, byIndex.leftY);
        result.top = geoTransformations.getLngLat(byIndex.topX, byIndex.topY);
        result.right = geoTransformations.getLngLat(byIndex.rightX, byIndex.rightY);
        result.bottom = geoTransformations.getLngLat(byIndex.bottomX, byIndex.bottomY);
        return result;
    }

    public Map<String, Double> getRadianceScaling() {
        Map<String, Double> result = new HashMap<String, Double>();
        JsonNode p = rootNode.path("L1T");
        if (p != null)
            p = p.path("L1_METADATA_FILE");
        if (p != null)
            p = p.path("RADIANCE_SCALING");
        if (p != null) {
            Iterator<String> fieldNames = p.getFieldNames();
            while (fieldNames.hasNext()) {
                String fieldName = fieldNames.next();
                result.put(fieldName, Double.valueOf(p.path(fieldName).getDoubleValue()));
            }
        }
        return result;
    }

    public Double getSunAzimuth() {
        JsonNode p = rootNode.path("L1T");
        if (p != null)
            p = p.path("L1_METADATA_FILE");
        if (p != null)
            p = p.path("PRODUCT_PARAMETERS");
        if (p != null)
            p = p.path("SUN_AZIMUTH");
        if (p != null)
            return Double.valueOf(p.getDoubleValue());
        return null;
    }

    public Double getSensorLookAngle() {
        JsonNode p = rootNode.path("L1T");
        if (p != null)
            p = p.path("L1_METADATA_FILE");
        if (p != null)
            p = p.path("PRODUCT_PARAMETERS");
        if (p != null)
            p = p.path("SENSOR_LOOK_ANGLE");
        if (p != null)
            return Double.valueOf(p.getDoubleValue());
        return null;
    }

    public Double getSunElevation() {
        JsonNode p = rootNode.path("L1T");
        if (p != null)
            p = p.path("L1_METADATA_FILE");
        if (p != null)
            p = p.path("PRODUCT_PARAMETERS");
        if (p != null)
            p = p.path("SUN_ELEVATION");
        if (p != null)
            return Double.valueOf(p.getDoubleValue());
        return null;
    }

    public String getStartTime() {
        JsonNode p = rootNode.path("L1T");
        if (p != null)
            p = p.path("L1_METADATA_FILE");
        if (p != null)
            p = p.path("PRODUCT_METADATA");
        if (p != null)
            p = p.path("START_TIME");
        if (p != null)
            return p.getTextValue();
        else
            return "";
    }

    public Long getAcquisitionStart() {
        try {
            return Long.valueOf(dateTimeFormatter.parseLocalDateTime(getStartTime()).toDateTime(DateTimeZone.UTC).toInstant().getMillis());
        }
        catch (IllegalArgumentException exception) {
            return null;
        }
    }

    public Long getAcquisitionEnd() {
        JsonNode p = rootNode.path("L1T");
        if (p != null)
            p = p.path("L1_METADATA_FILE");
        if (p != null)
            p = p.path("PRODUCT_METADATA");
        if (p != null)
            p = p.path("END_TIME");
        if (p != null) {
            try {
                return Long.valueOf(dateTimeFormatter.parseLocalDateTime(p.getTextValue()).toDateTime(DateTimeZone.UTC).toInstant().getMillis());
            }
            catch (IllegalArgumentException exception) {
                return null;
            }
        }
        return null;
    }

    public double getMaxAverageRadiance() {
        return rootNode.path("maxAverageRadiance").getDoubleValue();
    }
}
