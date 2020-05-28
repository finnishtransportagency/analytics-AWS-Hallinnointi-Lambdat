package com.vayla.lambda.velho.metadata.converter.ade;

import java.util.List;

public class NimikeCsvHelper {
	static final String QUOTE = "\"";
	static final String HEADERS = "\"NIMI\",\"KOODI\",\"OTSIKKO\",\"TR_MAPPAUKSET\"";
	public static enum HEADER {
			NIMI,
			KOODI,
			OTSIKKO,
			TR_MAPPAUKSET
	}
	
	public static String getHeadersForCsv() {
		return HEADERS;
	}
	
	public static String getNimikeAsCsv(Nimike n) {
		StringBuilder sb = new StringBuilder();
		sb.append(QUOTE+n.getNimi()+QUOTE).append(",");
		sb.append(QUOTE+n.getKoodi()+QUOTE).append(",");
		sb.append(QUOTE+n.getOtsikko()+QUOTE).append(",");
		sb.append(QUOTE);
		List<TrMappaus> mappaukset = n.getTrMappaukset();
		if(mappaukset!=null) {
			for(int i=0; i< mappaukset.size(); i++) {
				sb.append(mappaukset.get(i).toString());
				if(i<mappaukset.size()-1) sb.append(",");
			}
		}
		
		sb.append(QUOTE);
		
		return sb.toString();
	}

}
