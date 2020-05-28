package com.vayla.lambda.velho.metadata.converter.ade;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import com.amazonaws.util.StringUtils;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.vayla.lambda.velho.metadata.converter.ade.AdeManifest.AdeEntry;


public class AdeManifestHelper {
	static final Gson gson;
	
	static {
		GsonBuilder gsonBuilder = new GsonBuilder();
        gsonBuilder.serializeNulls();
        gson = gsonBuilder.create();
	}
	
	public static String createManifest(List<String> urls, List<String> columns) {
		AdeManifest adeManifest = new AdeManifest();
		
		List<AdeEntry> entries = new ArrayList<AdeEntry>();
		for(String url : urls) {
			AdeEntry adeEntry = adeManifest.new AdeEntry();
			adeEntry.setMandatory(true);
			adeEntry.setUrl(url);
			entries.add(adeEntry);
		}
		adeManifest.setEntries(entries);
		
		List<String> formattedColumns = new ArrayList<String>();
		Stream<String> stream1 = columns.stream();
        stream1.forEach(s -> formattedColumns.add(formatColumn(s)));
		String[] columnsArray = new String[formattedColumns.size()];
		formattedColumns.toArray(columnsArray);
		adeManifest.setColumns(columnsArray);
		
		return gson.toJson(adeManifest);
	}
	
	// add more string format as needed by ADE
	// for starters we change - to _
	static String formatColumn(String s) {
		if(StringUtils.isNullOrEmpty(s)) return s;
		return s.replace('-', '_');
	}

}
