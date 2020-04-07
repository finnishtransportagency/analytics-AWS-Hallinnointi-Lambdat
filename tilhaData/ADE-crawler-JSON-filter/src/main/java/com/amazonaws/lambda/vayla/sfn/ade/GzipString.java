package com.amazonaws.lambda.vayla.sfn.ade;

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

}
