package com.vayla.lambda.velho.metadata.converter.ade;

public class TrMappaus {
	String koodiryhma;
	String koodi;
	public String getKoodiryhma() {
		return koodiryhma;
	}
	public void setKoodiryhma(String koodiryhma) {
		this.koodiryhma = koodiryhma;
	}
	public String getKoodi() {
		return koodi;
	}
	public void setKoodi(String koodi) {
		this.koodi = koodi;
	}
	
	public String toString() {
		
		return "koodiryhma:"+ koodiryhma + " " +"koodi:" + koodi;
	}
}