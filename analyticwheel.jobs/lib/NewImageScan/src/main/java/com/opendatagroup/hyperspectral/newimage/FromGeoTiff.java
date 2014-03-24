package com.opendatagroup.hyperspectral.newimage;

import java.awt.image.Raster;
import java.awt.image.RenderedImage;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.lang.StringBuilder;
import java.util.Scanner;

import org.geotools.coverage.grid.GridCoordinates2D;
import org.geotools.coverage.grid.GridCoverage2D;
import org.geotools.coverage.grid.GridGeometry2D;
import org.geotools.data.DataSourceException;
import org.geotools.gce.geotiff.GeoTiffFormat;
import org.geotools.gce.geotiff.GeoTiffReader;
import org.geotools.referencing.CRS;
import org.opengis.geometry.DirectPosition;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.operation.MathTransform;
import org.opengis.referencing.operation.TransformException;

import com.opendatagroup.hyperspectral.seqpng.ImageBandArray;
import com.opendatagroup.hyperspectral.geo.NinePoints;

public class FromGeoTiff {
    static public ImageBandArray readBandFile(String fileName) throws DataSourceException, IOException {
        GeoTiffReader reader = null;

        try {
            File file = new File(fileName);
            reader = (GeoTiffReader) ((new GeoTiffFormat()).getReader(file));
            GridCoverage2D coverage = reader.read(null);
            RenderedImage image = coverage.getRenderedImage();
            Raster raster = image.getData();

            int x1 = raster.getMinX();
            int y1 = raster.getMinY();
            int width = raster.getWidth();
            int height = raster.getHeight();

            int pixels[] = new int[width*height];
            raster.getPixels(x1, y1, width, height, pixels);

            return new ImageBandArray(width, height, pixels);
        }
        finally {
            if (reader != null)
                reader.dispose();
        }
    }

    static public ImageBandArray calculateMask(String[] fileNames) throws DataSourceException, IOException {
        int width = -1;
        int height = -1;
        int mask[] = null;

        for (String fileName : fileNames) {
            GeoTiffReader reader = null;
            try {
                File file = new File(fileName);
                reader = (GeoTiffReader) ((new GeoTiffFormat()).getReader(file));
                GridCoverage2D coverage = reader.read(null);
                RenderedImage image = coverage.getRenderedImage();
                Raster raster = image.getData();

                int x1 = raster.getMinX();
                int y1 = raster.getMinY();

                if (mask == null) {
                    width = raster.getWidth();
                    height = raster.getHeight();
                    mask = new int[width * height];
                    raster.getPixels(x1, y1, width, height, mask);

                    for (int i = 0;  i < width*height;  i++) {
                        if (mask[i] > 0) {
                            mask[i] = 1;
                        }
                    }
                }
                else {
                    if (width != raster.getWidth()  ||  height != raster.getHeight()) {
                        throw new IOException("mismatch in TIF files width or height");
                    }

                    int pixels[] = new int[width*height];
                    raster.getPixels(x1, y1, width, height, pixels);

                    for (int i = 0;  i < width*height;  i++) {
                        if (pixels[i] > 0) {
                            mask[i] = 1;
                        }
                    }
                }
            }
            finally {
                if (reader != null)
                    reader.dispose();
            }
        }

        if (mask == null)
            throw new IOException("no image files found");

        return new ImageBandArray(width, height, mask);
    }

    static public NinePoints getNinePoints(String fileName) throws IOException, TransformException, FactoryException {
        GeoTiffReader reader = null;
        
        try {
            File file = new File(fileName);
            reader = (GeoTiffReader) ((new GeoTiffFormat()).getReader(file));
            GridCoverage2D coverage = reader.read(null);
            RenderedImage image = coverage.getRenderedImage();

            int width = image.getWidth();
            int height = image.getHeight();

            CoordinateReferenceSystem tiffSystem = coverage.getCoordinateReferenceSystem2D();
            CoordinateReferenceSystem pureLngLat = CRS.decode("EPSG:4326");
            MathTransform transform = CRS.findMathTransform(tiffSystem, pureLngLat);
            GridGeometry2D geom = coverage.getGridGeometry();

            NinePoints ninePoints = new NinePoints();

            DirectPosition bottomLeft = transform.transform(geom.gridToWorld(new GridCoordinates2D(0, 0)), null);
            ninePoints.bottomLeftLng = bottomLeft.getOrdinate(0);
            ninePoints.bottomLeftLat = bottomLeft.getOrdinate(1);

            DirectPosition bottomMiddle = transform.transform(geom.gridToWorld(new GridCoordinates2D(width/2, 0)), null);
            ninePoints.bottomMiddleLng = bottomMiddle.getOrdinate(0);
            ninePoints.bottomMiddleLat = bottomMiddle.getOrdinate(1);

            DirectPosition bottomRight = transform.transform(geom.gridToWorld(new GridCoordinates2D(width - 1, 0)), null);
            ninePoints.bottomRightLng = bottomRight.getOrdinate(0);
            ninePoints.bottomRightLat = bottomRight.getOrdinate(1);

            DirectPosition middleLeft = transform.transform(geom.gridToWorld(new GridCoordinates2D(0, height/2)), null);
            ninePoints.middleLeftLng = middleLeft.getOrdinate(0);
            ninePoints.middleLeftLat = middleLeft.getOrdinate(1);

            DirectPosition middleMiddle = transform.transform(geom.gridToWorld(new GridCoordinates2D(width/2, height/2)), null);
            ninePoints.middleMiddleLng = middleMiddle.getOrdinate(0);
            ninePoints.middleMiddleLat = middleMiddle.getOrdinate(1);

            DirectPosition middleRight = transform.transform(geom.gridToWorld(new GridCoordinates2D(width - 1, height/2)), null);
            ninePoints.middleRightLng = middleRight.getOrdinate(0);
            ninePoints.middleRightLat = middleRight.getOrdinate(1);

            DirectPosition topLeft = transform.transform(geom.gridToWorld(new GridCoordinates2D(0, height - 1)), null);
            ninePoints.topLeftLng = topLeft.getOrdinate(0);
            ninePoints.topLeftLat = topLeft.getOrdinate(1);

            DirectPosition topMiddle = transform.transform(geom.gridToWorld(new GridCoordinates2D(width/2, height - 1)), null);
            ninePoints.topMiddleLng = topMiddle.getOrdinate(0);
            ninePoints.topMiddleLat = topMiddle.getOrdinate(1);

            DirectPosition topRight = transform.transform(geom.gridToWorld(new GridCoordinates2D(width - 1, height - 1)), null);
            ninePoints.topRightLng = topRight.getOrdinate(0);
            ninePoints.topRightLat = topRight.getOrdinate(1);

            return ninePoints;
        }
        finally {
            if (reader != null)
                reader.dispose();
        }
    }

    static public String getGeoMetadata(String fileName, String originalDirName, String hdfsFileName, String[] bandNames, String l1t, String cornersByIndex, double maxAverageRadiance, String wavelengthFile, String multiplierFile) throws IOException, TransformException, FactoryException {
        GeoTiffReader reader = null;

        try {
            File file = new File(fileName);
            reader = (GeoTiffReader) ((new GeoTiffFormat()).getReader(file));
            GridCoverage2D coverage = reader.read(null);
            RenderedImage image = coverage.getRenderedImage();

            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.append("{");

            stringBuilder.append("\"Projection\": \"" + coverage.getCoordinateReferenceSystem2D().toWKT().replace("\n", "\\n").replace("\"", "\\\"") + "\", ");
            stringBuilder.append(String.format("\"width\": %d, ", image.getWidth()));
            stringBuilder.append(String.format("\"height\": %d, ", image.getHeight()));
            stringBuilder.append("\"registration\": " + getNinePoints(fileName).toString() + ", ");
            stringBuilder.append("\"originalDirName\": \"" + originalDirName + "\", ");
            stringBuilder.append("\"hdfsFileName\": \"" + hdfsFileName + "\", ");
            stringBuilder.append("\"bandNames\": [");
            boolean first = true;
            for (String bandName : bandNames) {
                if (first)
                    first = false;
                else
                    stringBuilder.append(", ");

                stringBuilder.append("\"" + bandName + "\"");
            }
            stringBuilder.append("], ");
            stringBuilder.append("\"base64encoded\": false");

            if (l1t != null)
                stringBuilder.append(", \"L1T\": " + l1t);

            if (cornersByIndex != null)
                stringBuilder.append(", \"cornersByIndex\": " + cornersByIndex);

            stringBuilder.append(String.format(", \"maxAverageRadiance\": %s", maxAverageRadiance));

            if (wavelengthFile != null)
                stringBuilder.append(String.format(", \"bandWavelength\": %s", new Scanner(FromGeoTiff.class.getResourceAsStream(wavelengthFile)).useDelimiter("\\A").next()));

            if (multiplierFile != null)
                stringBuilder.append(String.format(", \"bandMultiplier\": %s", new Scanner(FromGeoTiff.class.getResourceAsStream(multiplierFile)).useDelimiter("\\A").next()));

            stringBuilder.append("}");
            return stringBuilder.toString();
        }
        finally {
            if (reader != null)
                reader.dispose();
        }
    }
}
