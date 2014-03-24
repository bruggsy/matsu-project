package com.opendatagroup.hyperspectral.newimage;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.Double;
import java.lang.NumberFormatException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.codehaus.jackson.io.JsonStringEncoder;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ObjectNode;

import com.opendatagroup.hyperspectral.newimage.FromGeoTiff;
import com.opendatagroup.hyperspectral.newimage.TypedBytesToSequenceFile;
import com.opendatagroup.hyperspectral.seqpng.BinaryHadoop;
import com.opendatagroup.hyperspectral.seqpng.ImageBandArray;

public class LocalDirToHdfsFile {
    public static String doGeoTiff(String localDir, String hdfsFileUri, Pattern metadataFilePattern, String wavelengthFile, String multiplierFile) {
        if (!(new File(localDir).exists()))
            return "Could not find file named " + localDir;

        String allMetadata = null;

        try {
            File[] allFilesInDir = new File(localDir).listFiles();
            String l1tMetadataFile = null;

            Pattern pattern = Pattern.compile(".*_(B[0-9]+)_.*");
            List<String> localFileNames = new ArrayList<String>();
            List<String> bandNames = new ArrayList<String>();
            for (File file : allFilesInDir) {
                if (file.isFile()  &&  file.getName().endsWith(".TIF")) {
                    Matcher matcher = pattern.matcher(file.getName());                    
                    if (matcher.find()) {
                        localFileNames.add(file.getAbsolutePath());
                        bandNames.add(matcher.group(1));
                    }
                }
                if (file.isFile()  &&  metadataFilePattern.matcher(file.getName()).find()) {
                    l1tMetadataFile = file.getAbsolutePath();
                }
            }

            String[] localFileNamesArray = new String[localFileNames.size()];
            localFileNames.toArray(localFileNamesArray);
            String[] bandNamesArray = new String[bandNames.size()];
            bandNames.toArray(bandNamesArray);

            String l1t = null;
            if (l1tMetadataFile != null) {
                try {
                    l1t = l1tMetadata(new BufferedReader(new FileReader(l1tMetadataFile)));
                }
                catch (IOException exception) {
                    throw new RuntimeException("IOException: " + exception.toString());
                }
            }

            ImageBandArray mask = FromGeoTiff.calculateMask(localFileNamesArray);
            String cornersByIndex = mask.findCorners(0.0).toString();


            double maxAverageRadiance = 0.0;
            for (String localFileName : localFileNames) {
                ImageBandArray band = FromGeoTiff.readBandFile(localFileName);
                double radiance = band.averageRadiance(mask, 0.0);
                if (radiance > maxAverageRadiance)
                    maxAverageRadiance = radiance;
            }

            allMetadata = FromGeoTiff.getGeoMetadata(localFileNamesArray[0], localDir, hdfsFileUri, bandNamesArray, l1t, cornersByIndex, maxAverageRadiance, wavelengthFile, multiplierFile);

            TypedBytesToSequenceFile out = new TypedBytesToSequenceFile(hdfsFileUri);
            out.write("metadata".getBytes(), allMetadata.getBytes(), BinaryHadoop.TYPEDBYTES_JSON);
            out.write("mask".getBytes(), mask.serialize(16), BinaryHadoop.TYPEDBYTES_PNG);

            for (int i = 0;  i < localFileNames.size();  i++) {
                out.write(bandNamesArray[i].getBytes(), FromGeoTiff.readBandFile(localFileNamesArray[i]).serialize(16), BinaryHadoop.TYPEDBYTES_PNG);
            }
            out.close();
        }
        catch (Exception exception) {
            StringWriter stringWriter = new StringWriter();
            PrintWriter printWriter = new PrintWriter(stringWriter);
            exception.printStackTrace(printWriter);
            return stringWriter.toString();
        }

        return "SUCCESS " + allMetadata;
    }

    private static ObjectNode addItem(BufferedReader bufferedReader, ObjectMapper objectMapper) throws IOException {
        ObjectNode node = (ObjectNode)objectMapper.readTree("{}");
        String line = bufferedReader.readLine();
        while (line != null) {
            String[] pair = line.trim().split(" = ");
            if (pair.length == 2) {
                if (pair[0].equals("GROUP")) {
                    node.put(pair[1], addItem(bufferedReader, objectMapper));
                }
                else if (pair[0].equals("END_GROUP")) {
                    return node;
                }
                else if (!pair[0].equals("END")) {
                    JsonNode obj = objectMapper.readTree(pair[1]);
                    if (obj.isNumber()) {
                        try {
                            Double.parseDouble(pair[1]);
                        }
                        catch (NumberFormatException exception) {
                            obj = objectMapper.readTree("\"" + new String(JsonStringEncoder.getInstance().quoteAsString(pair[1])) + "\"");
                        }
                    }
                    node.put(pair[0], obj);
                }
            }
            line = bufferedReader.readLine();
        }
        return node;
    }

    private static String l1tMetadata(BufferedReader bufferedReader) throws IOException {
        ObjectMapper objectMapper = new ObjectMapper();
        ObjectNode rootNode = addItem(bufferedReader, objectMapper);
        bufferedReader.close();

        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        objectMapper.writeValue(byteArrayOutputStream, rootNode);
        return new String(byteArrayOutputStream.toByteArray());
    }
}
