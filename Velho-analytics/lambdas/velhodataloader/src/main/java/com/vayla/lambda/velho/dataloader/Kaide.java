package com.vayla.lambda.velho.dataloader;

import com.google.gson.annotations.SerializedName;

public class Kaide {
	String kohdeluokka = "";
	String luotu = "";
	String paattyen = "";
	String luoja = "";
	String muokkaaja = "";
	String oid = "";
	String alkaen = "";
	String muokattu = "";
	@SerializedName("objekti-oid")
	String objektiOid = "";
	String schemaversio = "";
	String poistettu = "";
	
	Sijaintitarkenne sijaintitarkenne = new Sijaintitarkenne();
	Ominaisuudet ominaisuudet = new Ominaisuudet();
	Sijainti alkusijainti = new Sijainti();
	Sijainti loppusijainti = new Sijainti();
	
	class Sijaintitarkenne {
		String puoli = "";
		String ajorata = "";
	}
	
	class Sijainti {
		String osa = "";
		String tie = "";
		String etaisyys = "";
		String ajorata = "";
		String enkoodattu = "";
	}
	
	class Ominaisuudet {
		@SerializedName("tr-pylvasvika")
		String trPylvasvika = "";
		@SerializedName("tr-kaidemaal")
		String trKaidemaal = "";
		@SerializedName("tr-kaidevinos")
		String trKaidevinos = "";
		@SerializedName("tr-kaidematal")
		String trKaidematal = "";
		@SerializedName("tr-kaideruost")
		String trKaideruost = "";
		@SerializedName("tr-muuvaurio")
		String trMuuvaurio = "";
		@SerializedName("tr-kiinvaur")
		String trKiinvaur = "";
		@SerializedName("tr-kaidekolh")
		String trKaidekolh = "";
		@SerializedName("kaiteenpaa-nollaamatta")
		String kaiteenpaaNollaamatta = "";
		@SerializedName("kaidepylvaan-tyyppi")
		String kaidepylvaanTyyppi = "";
		String materiaali = "";
		String tyyppi = "";
		String lisatehtava = "";
	}

}
