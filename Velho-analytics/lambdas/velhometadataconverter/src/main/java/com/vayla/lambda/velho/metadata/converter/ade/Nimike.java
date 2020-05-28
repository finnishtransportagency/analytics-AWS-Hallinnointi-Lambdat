package com.vayla.lambda.velho.metadata.converter.ade;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;

public class Nimike {
	String nimi;
	String koodi;
	String otsikko;
	@JsonProperty("tr-mappaukset")
	List<TrMappaus> trMappaukset;

	public String getNimi() {
		return nimi;
	}

	public void setNimi(String nimi) {
		this.nimi = nimi;
	}

	public String getOtsikko() {
		return otsikko;
	}

	public void setOtsikko(String otsikko) {
		this.otsikko = otsikko;
	}

	public List<TrMappaus> getTrMappaukset() {
		return trMappaukset;
	}

	public void setTrMappaukset(List<TrMappaus> trMappaykset) {
		this.trMappaukset = trMappaykset;
	}

	public String getKoodi() {
		return koodi;
	}

	public void setKoodi(String koodi) {
		this.koodi = koodi;
	}
	

}
