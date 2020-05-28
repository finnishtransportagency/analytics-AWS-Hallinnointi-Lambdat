package com.vayla.lambda.velho.collector.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.util.zip.DataFormatException;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;
import java.util.zip.Inflater;

public class GzipString {
	
	public static byte[] uncompress(String s) {
		if(s == null || s.isEmpty()) throw new IllegalArgumentException("Input string can't be null");
		
		
		byte[] result = new byte[]{};
        try (ByteArrayInputStream bis = new ByteArrayInputStream(s.getBytes());
             ByteArrayOutputStream bos = new ByteArrayOutputStream();
             GZIPInputStream gzipIS = new GZIPInputStream(bis)) {
            byte[] buffer = new byte[1024];
            int len;
            while ((len = gzipIS.read(buffer)) != -1) {
                bos.write(buffer, 0, len);
            }
            result = bos.toByteArray();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return result;
		
	}
	
	public static byte[] compressString(String s) {
		if(s == null || s.isEmpty()) throw new IllegalArgumentException("Input string can't be null");
		
		try (ByteArrayOutputStream byteOut = new ByteArrayOutputStream()) {
			try (GZIPOutputStream gzipOut = new GZIPOutputStream(byteOut)) {
				gzipOut.write(s.getBytes(StandardCharsets.UTF_8));
			}
			
			return byteOut.toByteArray();
			
		} catch (IOException e) {
			e.printStackTrace();
			throw new RuntimeException("Failed to gzip string");
		}
		
	}
	
	public static byte[] compress(byte[] bArray) {
		if(bArray == null || bArray.length < 1) throw new IllegalArgumentException("Input array can't be null");
		try (ByteArrayOutputStream byteOut = new ByteArrayOutputStream()) {
			try (GZIPOutputStream gzipOut = new GZIPOutputStream(byteOut)) {
				gzipOut.write(bArray);
			}
			
			return byteOut.toByteArray();
			
		} catch (IOException e) {
			e.printStackTrace();
			throw new RuntimeException("Failed to gzip byte array");
		}
	}

}
