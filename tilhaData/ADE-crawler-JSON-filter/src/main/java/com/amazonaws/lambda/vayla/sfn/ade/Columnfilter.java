package com.amazonaws.lambda.vayla.sfn.ade;

import java.util.List;

public class Columnfilter {
	String column;
	List<String> values;
	
	public String getColumn() {
		return column;
	}
	public void setColumn(String column) {
		this.column = column;
	}
	public List<String> getValues() {
		return values;
	}
	public void setValues(List<String> values) {
		this.values = values;
	}

}
