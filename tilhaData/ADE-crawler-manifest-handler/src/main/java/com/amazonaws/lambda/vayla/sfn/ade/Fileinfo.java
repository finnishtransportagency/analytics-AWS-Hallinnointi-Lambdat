package com.amazonaws.lambda.vayla.sfn.ade;

import java.util.List;

public class Fileinfo {
	String manifestfile;
	List<String> datafiles;
	
	public String getManifestfile() {
		return manifestfile;
	}
	public void setManifestfile(String manifestfile) {
		this.manifestfile = manifestfile;
	}
	public List<String> getDatafiles() {
		return datafiles;
	}
	public void setDatafiles(List<String> datafiles) {
		this.datafiles = datafiles;
	}
	
}
