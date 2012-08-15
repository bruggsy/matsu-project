package org.occ.matsu;

import org.occ.matsu.ByteOrder;
import org.occ.matsu.ZeroSuppressed;
import org.occ.matsu.GeoPictureWithMetadata;

import org.apache.avro.Schema;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.ValidatingDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificData;

import it.sauronsoftware.base64.Base64InputStream;

import java.io.IOException;
import java.io.InputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.lang.Double;
import java.util.List;
import java.util.ArrayList;
import java.util.Collections;

import java.awt.Color;
import java.awt.image.BufferedImage;
import java.lang.Math;

import javax.script.ScriptEngineManager;
import javax.script.ScriptEngine;
import javax.script.ScriptException;

import javax.imageio.ImageIO;

class InvalidGeoPictureException extends Exception { }

class GeoPictureSerializer extends Object {
    public static final Schema schema = new Schema.Parser().parse(
        "{\"type\": \"record\", \"name\": \"GeoPictureWithMetadata\", \"fields\":\n" +
	"    [{\"name\": \"metadata\", \"type\": {\"type\": \"map\", \"values\": \"string\"}},\n" +
	"     {\"name\": \"bands\", \"type\": {\"type\": \"array\", \"items\": \"string\"}},\n" +
	"     {\"name\": \"height\", \"type\": \"int\"},\n" +
	"     {\"name\": \"width\", \"type\": \"int\"},\n" +
	"     {\"name\": \"depth\", \"type\": \"int\"},\n" +
	"     {\"name\": \"dtype\", \"type\": \"int\"},\n" +
	"     {\"name\": \"itemsize\", \"type\": \"int\"},\n" +
	"     {\"name\": \"nbytes\", \"type\": \"long\"},\n" +
	"     {\"name\": \"fortran\", \"type\": \"boolean\"},\n" +
	"     {\"name\": \"byteorder\", \"type\": {\"type\": \"enum\", \"name\": \"ByteOrder\", \"symbols\": [\"LittleEndian\", \"BigEndian\", \"NativeEndian\", \"IgnoreEndian\"]}},\n" +
	"     {\"name\": \"data\", \"type\":\n" +
	"        {\"type\": \"array\", \"items\":\n" +
	"            {\"type\": \"record\", \"name\": \"ZeroSuppressed\", \"fields\":\n" +
	"                [{\"name\": \"index\", \"type\": \"long\"}, {\"name\": \"strip\", \"type\": \"bytes\"}]}}}\n" +
	"     ]}");

    String[] bands;
    int height;
    int width;
    int depth;
    double[][][] data;
    boolean valid = false;

    public GeoPictureSerializer() {}

    public String[] bandNames() {
	return bands;
    }

    public void loadSerialized(String serialized) throws IOException {
	loadSerialized(new ByteArrayInputStream(serialized.getBytes()));
    }

    public void loadSerialized(InputStream serialized) throws IOException {
	InputStream inputStream = new Base64InputStream(serialized);

	DecoderFactory decoderFactory = new DecoderFactory();
	ValidatingDecoder d = decoderFactory.validatingDecoder(schema, decoderFactory.binaryDecoder(inputStream, null));

	DatumReader<GeoPictureWithMetadata> reader = new SpecificDatumReader<GeoPictureWithMetadata>(GeoPictureWithMetadata.class);
	GeoPictureWithMetadata p = reader.read(null, d);

	bands = new String[p.getBands().size()];
	int b = 0;
	for (java.lang.CharSequence band : p.getBands()) {
	    bands[b] = band.toString();
	    b++;
	}

	height = p.getHeight();
	width = p.getWidth();
	depth = p.getDepth();

	data = new double[height][width][depth];
	for (int i = 0;  i < height;  i++) {
	    for (int j = 0;  j < width;  j++) {
		for (int k = 0;  k < depth;  k++) {
		    data[i][j][k] = 0.;
		}
	    }
	}

	for (ZeroSuppressed zs : p.getData()) {
	    long index = zs.getIndex();

	    ByteBuffer strip = zs.getStrip();
	    if (! p.getByteorder().equals(ByteOrder.BigEndian)) {
		strip.order(java.nio.ByteOrder.LITTLE_ENDIAN);
	    }

	    while (strip.hasRemaining()) {
		int i, j, k;
		if (p.getFortran()) {
		    i = (int)((index / height / width) % depth);
		    j = (int)((index / height) % width);
		    k = (int)(index % height);
		}
		else {
		    i = (int)((index / depth / width) % height);
		    j = (int)((index / depth) % width);
		    k = (int)(index % depth);
		}
		index++;

		data[i][j][k] = strip.getDouble();
	    }
	}

	valid = true;
    }

    public byte[] image(String red, String green, String blue) throws IOException, InvalidGeoPictureException, ScriptException {
	if (!valid) throw new InvalidGeoPictureException();
	return subImage(0, 0, width, height, red, green, blue);
    }

    public byte[] subImage(int x1, int y1, int x2, int y2, String red, String green, String blue) throws IOException, InvalidGeoPictureException, ScriptException {
	if (!valid) throw new InvalidGeoPictureException();

	int redSimple = -1;
	int greenSimple = -1;
	int blueSimple = -1;
	for (int k = 0;  k < depth;  k++) {
	    if (bands[k].equals(red)) { redSimple = k; }
	    if (bands[k].equals(green)) { greenSimple = k; }
	    if (bands[k].equals(blue)) { blueSimple = k; }
	}
	
	ScriptEngineManager scriptEngineManager = new ScriptEngineManager();
	ScriptEngine scriptEngine = scriptEngineManager.getEngineByName("JavaScript");

	double[][] reds = new double[x2 - x1][y2 - y1];
	double[][] greens = new double[x2 - x1][y2 - y1];
	double[][] blues = new double[x2 - x1][y2 - y1];
	boolean[][] alphas = new boolean[x2 - x1][y2 - y1];
	
	List<Double> redrad = new ArrayList<Double>();
	List<Double> greenrad = new ArrayList<Double>();
	List<Double> bluerad = new ArrayList<Double>();

	for (int i = 0;  i < x2 - x1;  i++) {
	    for (int j = 0;  j < y2 - y1;  j++) {
		alphas[i][j] = true;

		if (redSimple == -1  ||  greenSimple == -1  ||  blueSimple == -1) {
		    for (int k = 0;  k < depth;  k++) {
			scriptEngine.put(bands[k], data[j + y1][i + x1][k]);
			if (data[j + y1][i + x1][k] == 0.) { alphas[i][j] = false; }
		    }
		}

		if (redSimple == -1) {
		    reds[i][j] = (Double)scriptEngine.eval(red);
		}
		else {
		    reds[i][j] = data[j + y1][i + x1][redSimple];
		    if (reds[i][j] == 0.) { alphas[i][j] = false; }
		}

		if (greenSimple == -1) {
		    greens[i][j] = (Double)scriptEngine.eval(green);
		}
		else {
		    greens[i][j] = data[j + y1][i + x1][greenSimple];
		    if (greens[i][j] == 0.) { alphas[i][j] = false; }
		}

		if (blueSimple == -1) {
		    blues[i][j] = (Double)scriptEngine.eval(blue);
		}
		else {
		    blues[i][j] = data[j + y1][i + x1][blueSimple];
		    if (blues[i][j] == 0.) { alphas[i][j] = false; }
		}

		if (alphas[i][j]) {
		    redrad.add(reds[i][j]);
		    greenrad.add(greens[i][j]);
		    bluerad.add(blues[i][j]);
		}
	    }
	}

	Collections.sort(redrad);
	Collections.sort(greenrad);
	Collections.sort(bluerad);
	
	int redIndex5 = Math.max((int)Math.ceil(redrad.size() * 0.05), 0);
	int redIndex95 = Math.min((int)Math.floor(redrad.size() * 0.95), redrad.size() - 1);

	int greenIndex5 = Math.max((int)Math.ceil(greenrad.size() * 0.05), 0);
	int greenIndex95 = Math.min((int)Math.floor(greenrad.size() * 0.95), greenrad.size() - 1);

	int blueIndex5 = Math.max((int)Math.ceil(bluerad.size() * 0.05), 0);
	int blueIndex95 = Math.min((int)Math.floor(bluerad.size() * 0.95), bluerad.size() - 1);

	double minvalue = Math.min(redrad.get(redIndex5), Math.min(greenrad.get(greenIndex5), bluerad.get(blueIndex5)));
	double maxvalue = Math.max(redrad.get(redIndex95), Math.max(greenrad.get(greenIndex95), bluerad.get(blueIndex95)));

	BufferedImage bufferedImage = new BufferedImage(x2 - x1, y2 - y1, BufferedImage.TYPE_4BYTE_ABGR);
	for (int i = 0;  i < x2 - x1;  i++) {
	    for (int j = 0;  j < y2 - y1;  j++) {
		int r = Math.min(Math.max((int)Math.floor((reds[i][j] - minvalue) / (maxvalue - minvalue) * 256), 0), 255);
		int g = Math.min(Math.max((int)Math.floor((greens[i][j] - minvalue) / (maxvalue - minvalue) * 256), 0), 255);
		int b = Math.min(Math.max((int)Math.floor((blues[i][j] - minvalue) / (maxvalue - minvalue) * 256), 0), 255);

		int abgr = new Color(b, g, r).getRGB();
		if (!alphas[i][j]) {
		    abgr &= 0x00ffffff;
		}
		bufferedImage.setRGB(i, j, abgr);
	    }
	}

	ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
	ImageIO.write(bufferedImage, "PNG", byteArrayOutputStream);

	System.out.println("hey");

	FileOutputStream tmp = new FileOutputStream("test.png");
	byteArrayOutputStream.writeTo(tmp);

	return null; // byteArrayOutputStream.toByteArray();
    }

}
