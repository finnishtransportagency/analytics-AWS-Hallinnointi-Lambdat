package com.vayla.lambda.velho.metadata.converter.ade;

import java.util.List;

public class AdeManifest {
	
	List<AdeEntry> entries;
	String columns[];
	
	class AdeEntry {
		boolean mandatory;
		String url;
		public boolean isMandatory() {
			return mandatory;
		}
		public void setMandatory(boolean mandatory) {
			this.mandatory = mandatory;
		}
		public String getUrl() {
			return url;
		}
		public void setUrl(String url) {
			this.url = url;
		}
		
		
	}

	public List<AdeEntry> getEntries() {
		return entries;
	}

	public void setEntries(List<AdeEntry> entries) {
		this.entries = entries;
	}

	public String[] getColumns() {
		return columns;
	}

	public void setColumns(String[] columns) {
		this.columns = columns;
	}
	
	
}
