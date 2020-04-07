package com.amazonaws.lambda.vayla.sfn.ade;

import java.util.List;

public class Tablefilter {
	String tablename;
	List<Columnfilter> filters;
	
	public String getTablename() {
		return tablename;
	}

	public void setTablename(String tablename) {
		this.tablename = tablename;
	}

	public List<Columnfilter> getFilters() {
		return filters;
	}

	public void setFilters(List<Columnfilter> filters) {
		this.filters = filters;
	}
}
