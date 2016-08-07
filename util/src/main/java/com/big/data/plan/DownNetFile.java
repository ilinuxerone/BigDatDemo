package com.big.data.plan;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.net.HttpURLConnection;
import java.net.URL;

public class DownNetFile {
    public static void main(String[] args) {
        String sURL = "http://www1.ncdc.noaa.gov/pub/data/normals/1981-2010/products/hourly/hly-temp-normal.txt";
        File dir = new File(System.getProperty("user.dir") + File.separator + "data");
        File file = new File(dir, "hly-temp-normal.txt");
        if (!dir.exists()) {
            dir.mkdir();
        }

        file.deleteOnExit();
        try {
            file.createNewFile();
        } catch (IOException e) {
            System.out.println(e.getMessage());
        }

        downloadNet(sURL, file, 0);
    }

    public static void downloadNet(String sURL, File file, int nStartPos) {
        int nRead = 0;
        HttpURLConnection httpConnection = null;
        try {
            URL url = new URL(sURL);
            httpConnection = (HttpURLConnection) url.openConnection();
            long nEndPos = getFileSize(sURL);
            RandomAccessFile oSavedFile = new RandomAccessFile(file, "rw");
            httpConnection.setRequestProperty("User-Agent", "Internet Explorer");
            String sProperty = "bytes=" + nStartPos + "-";

            httpConnection.setRequestProperty("RANGE", sProperty);
            System.out.println(sProperty);
            InputStream input = httpConnection.getInputStream();
            byte[] b = new byte[1024];

            while ((nRead = input.read(b, 0, 1024)) > 0 && nStartPos < nEndPos) {
                oSavedFile.write(b, 0, nRead);
                nStartPos += nRead;
            }

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            httpConnection.disconnect();
        }
    }

    public static long getFileSize(String sURL) {
        int nFileLength = -1;
        try {
            URL url = new URL(sURL);
            HttpURLConnection httpConnection = (HttpURLConnection) url.openConnection();
            httpConnection.setRequestProperty("User-Agent", "Internet Explorer");

            int responseCode = httpConnection.getResponseCode();
            if (responseCode >= 400) {
                System.err.println("Error Code : " + responseCode);
                return -2; // -2 represent access is error
            }
            String sHeader;
            for (int i = 1;; i++) {
                sHeader = httpConnection.getHeaderFieldKey(i);
                if (sHeader != null) {
                    if (sHeader.equals("Content-Length")) {
                        nFileLength = Integer.parseInt(httpConnection.getHeaderField(sHeader));
                        break;
                    }
                } else
                    break;
            }
        } catch (IOException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.out.println(nFileLength);
        return nFileLength;
    }

}
