package com.vayla.lambda.velho.dataloader;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.zip.GZIPOutputStream;

public class GzipString {
	
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
