#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <string.h>
#include <libgen.h>

typedef struct {
	int aika;
	double pit;
	int nop;
} ed_havainto;

unsigned char *find_eol(unsigned char *map,char eol, int start, int end);
int decode_line(FILE *outfile,int piste, unsigned char *inmap,
                 int start,int end,ed_havainto kaista_ed_hav[],
		 int jonoalkuaika[], int jonolkm[], int tpaiva);
int kaista(int kaista_suunta_ryhma);
int sury(int kaista_suunta_ryhma);

int main(int argc, char *argv[])
{
	const char hpk[]="hpk";
	int infile,end,cur,start,linecount,linelimit,startset;
	unsigned char *inmap,*eol;
	int piste=0,i,ok,retval,vuosi,paiva;
	struct stat infilestat;
	FILE *outfile;
	char tmpfile[L_tmpnam];
	char realfile[30];
	char *hpkfilename;
	/*Jonolaskennan muuttujat*/
	/*Kaistoja max 24 suuntaansa 1-24 suunta 1 ja 25-49 suunta 2*/
	/*nolla jätetään käyttämättä*/
	ed_havainto kaista_ed_hav[49];
        int jonolkm[49];
        int jonoalkuaika[49];

	if (argc != 2) {
		printf("Ohjelman käyttö hpkpurku infile\n");
		exit(-1);
	}

	hpkfilename=basename(argv[1]);
	//printf("infile=%s\n",hpkfilename);
	infile=open(argv[1],O_RDONLY); 

	if (infile==-1) {
		printf("Tiedoston %s avaus lukua varten ei onnistu\n",argv[1]);
		exit(-2);
	}
	
	/* Etsitäänpä koko mmap:ia varten */
	if (fstat(infile,&infilestat) < 0) {
		printf("Tiedoston koon luenta ei onnistu\n");
		exit(-3);
	}

	/* Etsitään piste ja päivä tiedoston nimestä */
	/* tarkistetaan ensin että alku on hpk */	
	if (strncasecmp((const char*)hpkfilename,(const char*)&hpk,3)==0) {
		ok=1;
		piste=paiva=0;
		/* Varmistetaan että merkit ovat vain numeroita */
		for (i=3;i<=6;i++) {
			if (!isdigit(hpkfilename[i])) ok=0;
		}
		if (ok) {
			sscanf((const char*)hpkfilename+3,"%4d",&piste);
		}
		if (piste==0) {
			printf("Pistettä ei löydy tiedoston nimestä\n");
			exit(-2);
		}
		for (i=8;i<=10;i++) {
			if (!isdigit(hpkfilename[i])) ok=0;
		}
		if (ok) {
			sscanf((const char*)hpkfilename+8,"%4d",&paiva);
		}
		if (paiva==0) {
			printf("Päivää ei löydy tiedoston nimestä\n");
			exit(-2);
		}
	}
	else {
		printf("Tiedoston nimi %s ei ole oikeaa formaattia\n",hpkfilename);
		exit(-2);
	}

	/* Tehdään tilapäinen nimi. Nimetään tiedosto uudelleen kun on saatu */
	/* piste ja päivä tiedostosta 			  		     */
	tmpnam(tmpfile);
	outfile=fopen(tmpfile,"w");
	if (outfile==NULL) {
		printf("Tiedoston %s avaus kirjoitusta varten ei onnistu\n",
			argv[2]);
		exit(-2);
	}

	/* Muodostetaan tiedostosta mmap ts. koko tiedosto on yksi iso */
	/* char taulukko					       */
	
	if (infilestat.st_size==0) {
		printf("Tiedosto %s on 0 mittainen. Ei muunnosta\n",argv[1]);
		exit(-4);
	}
	inmap = (unsigned char *)mmap (0,infilestat.st_size,
					PROT_READ,MAP_PRIVATE,infile,0);
	if (inmap == MAP_FAILED) {
		printf("mmap virhe tiedosto=%s\n",argv[1]);
		exit(-4);
	}

	/* Tiedoston kokonaisformaatista tiedetään se, että data on 	*/
	/* sulkujen välissä. Ensimmäiset 3-4 riviä sisältävät kontrolli */
	/* tms. dataa ja ohitetaan					*/
	/* Tämän jälkeen tunnit ovat omilla riveillään			*/
	/* Siitä formaatista tarkemmin decode_line():ssa		*/
	/* Tiedosto voi olla sekaisin, joten otetaan ensimmäinen ( ja   */
	/* viimeinen ) ja toivotaan parasta				*/

	/* Alkusulun hakeminen (voi olla eka merkkitiedostossa eli 0)*/
	cur=0;
	start=startset=0;
        while (!startset && cur<=infilestat.st_size) {
                if (inmap[cur]=='(') {
				startset=1;
				start=cur;
		}
		cur++;
        }

	/*Loppusulun hakeminen*/
	end=0;
        cur=infilestat.st_size;
        while (!end && cur>0) {
                if (inmap[cur--]==')') end=cur;
        }

	
	//printf("start=%d end=%d\n",start,end);
	cur=start;
	vuosi=-1; /*alustetaan niin että huomataan onko saatu */
	linecount=0;
	linelimit=2;

	/*Alustetaan jonolaskennan taulukot */
	for (i=0; i<49; i++) {
               	kaista_ed_hav[i].aika=0;
               	kaista_ed_hav[i].pit=0.0;
               	kaista_ed_hav[i].nop=0;
               	jonoalkuaika[i]=0;
               	jonolkm[i]=1;
	}
 
	/* Kelataan rivit tiedostosta */
	while (cur<=end) {
		eol=find_eol(inmap,'\r', cur, end);
		/* tarkistetaan että header on takanapäin ja lähetään    */
		/* purkamaan riviä. Välitetään rivin sijainti taulukossa */
		if (linecount>linelimit && eol) {
			retval=decode_line(outfile,piste,inmap,cur,eol-inmap,
			       kaista_ed_hav,jonoalkuaika,jonolkm,paiva);
			/* otetaan vuosi ekalta riviltä ja toivotaan parasta*/
			if (vuosi==-1) vuosi=retval;
		}
		if (eol) {
			/* Ohitetaan rivinvaihto 			   */
			/* Jos rivinvaihtoa seuraa LF niin ohitetaan sekin */
			/* Osasta tiedostoista LF puuttuu		   */
			if (((unsigned char*)(eol+1))[0]==10) {
				cur=eol-inmap+2;
			}
			else {
				cur=eol-inmap+1; 
			}
			/* voi olla 4:s header rivi */
			//printf("%d:%.20s\n",linecount,inmap+cur);
			if (linecount==linelimit && strlen((char*)inmap+cur)<60)
			{	
				linelimit++;
			}
			linecount++;
		}
		else break;
	}

	fclose(outfile);
	vuosi=retval;
	sprintf(realfile,"lamraw_%d_%d_%d.csv",piste,vuosi,paiva);
	rename(tmpfile,realfile);
	munmap(inmap,infilestat.st_size);
	close(infile);
	return 0;
	
}

int decode_line(FILE *outfile, int piste, unsigned char *inmap,
			int start, int end,ed_havainto kaista_ed_hav[],
			int jonoalkuaika[],int jonolkm[],int tpaiva) 
{
	/* Parametrit: 						*/
	/* outfile	tiedosto johon kirjoitetaan		*/
	/* piste	Piste ensimmäiseen sarakkeeseen		*/
	/* inmap	char taulukko tiedostosta		*/
	/* start	rivin alun sijainti			*/
	/* end		rivin lopun sijainti			*/
	/* return	palautetaan vuosi 			*/
	/*							*/
	/* Rivi näyttäisi olevan seuraavassa formaatissa:  	*/
	/* Ensimmäinen tavu on vuosiluku kahdella numerolla 	*/
	/* Tavut 5-6 muodostavat päivän				*/
	/* Näitä seuraavat havainnot 7 tavun pätkissä 		*/
	/* rivinvaihtoon saakka 				*/
	int paiva, tunti, /*ed_tunti=0,*/ min, sek, ssek, kasr, ka, su, lu;
	float pit,vali_tmp;
	//double pit,vali_tmp;
	
	int nop, vuosi, faulty, palaute=1,i,relend;
	int palvuosi=0;
	unsigned char* line=inmap+start;
	int kaista_aika_nyt;
	int vali;
	int jonoalku;
	int rsury;
	int arr_place;

	vuosi=line[0]-48;
	if (vuosi>=0 && vuosi<100) palvuosi=vuosi;
	paiva=200*(line[4]-48)+line[5]-48;
	//printf("%.40s\n",line);
	i=6;
	relend=end-start;
	//printf("i=%d start=%d end=%d relend=%d\n",i,start,end,relend);
	while (i<relend && i+6<relend) {
		tunti = line[i] - 48;
		//if (ed_tunti==0) ed_tunti=tunti;
		min = line[i+1] - 48;
		sek = line[i+2] - 48;
		ssek = line[i+3] - 48;
		pit = (line[i+4] - 48)/5.0;
		kasr = line[i+5] - 48;
		nop = line[i+6] - 48;
		ka = kaista( kasr);
		rsury = sury(kasr);
		su = rsury/8 + 1;
		lu= rsury - (rsury/8)*8;
		arr_place=ka+(25*(su-1));
		if (arr_place<0 || arr_place>48) {
			printf("virhe kaista+suunta kombinatiossa pist=%d, paiva=%d aika=%d:%d:%d:%d\n",piste,paiva,tunti,min,sek,ssek);
			arr_place=0;
		}
		if (ka>0 && ka<25 && su>=1 && su<=2) {
			/* lasketaan aika sadasosina */
			kaista_aika_nyt=(tunti*360000)+(min*6000)+(sek*100)+ssek;	
		}
		if (kaista_ed_hav[arr_place].aika!=0 && 
		    kaista_ed_hav[arr_place].pit!=0.0 &&
		    kaista_ed_hav[arr_place].nop!=0) {
			vali_tmp=(kaista_aika_nyt-kaista_ed_hav[arr_place].aika-kaista_ed_hav[arr_place].pit/kaista_ed_hav[arr_place].nop*360);
			if (vali_tmp<=0) vali=-1;
			else vali=vali_tmp;
		}
		else {
			vali=-2;
			jonoalku=0;
		}
		if (vali>-2 && vali<200) {
			jonolkm[arr_place]++;
			switch (jonolkm[arr_place]) {
				case  2: 
					jonoalkuaika[arr_place]=kaista_ed_hav[arr_place].aika;
				case 3: 
				case 4: break;
				default:
					 jonoalku=jonoalkuaika[arr_place];
			}
		}
		else {
			jonolkm[arr_place]=1;
			jonoalku=0;
		}
		faulty=0;
		if (min < 0 || kasr <= 0 || nop < 2 || min > 59 ||
			sek < 0 || sek > 59 || ssek < 0 || ssek > 99 ||
			nop >= 199 || su < 1 || su > 2 ||
			lu < 1 || lu > 7 || ka <= 0 ||
			tunti < 0 || tunti > 23 || paiva < 1 || paiva > 366 ||
			//tunti > ed_tunti+1 || tunti < ed_tunti-1 ||
			vuosi <0 || vuosi > 99 || pit<=1.0)
		{
			/* Mikäli ulkona em arvoista, niin merkitään 	*/
			/* virheelliseksi				*/
				//fprintf(stderr,"faulty:%d;%d;%d;%d;%d;%d;%d;%.2f;%d;%d;%d;%d;%d;%d;%d\n",
				//	piste,vuosi,paiva,tunti,min,sek,ssek,pit,ka,
				//	su,lu,nop,faulty,kasr,ed_tunti);
				faulty=1;
		}
		else {
			kaista_ed_hav[arr_place].pit=pit;
			kaista_ed_hav[arr_place].aika=kaista_aika_nyt;
			kaista_ed_hav[arr_place].nop=nop;
		}
		/* Otetaan mukaan vain ne päivät jotka täsmäävät tiedoston
 		 * nimen päivään */
		if (paiva==tpaiva) {
			fprintf(outfile,"%d;%d;%d;%d;%d;%d;%d;%.2f;%d;%d;%d;%d;%d;%d;%d;%d\n",
			piste,vuosi,paiva,tunti,min,sek,ssek,pit,ka,
			su,lu,nop,faulty,kaista_aika_nyt,vali,jonoalku);
		}
		//ed_tunti=tunti;
		i+=7;
	}
	return (palvuosi);
}

int kaista(int kaista_suunta_ryhma)
/****------------------------------------------------------------------------
 *     INPUT:
 *      - kaista_suunta_ryhma = kaista-suuntaryhma ( = 17 ... 135 )
 *     OUTPUT:
 *     - kaista() = kaista
 *     TOIMINTA:
 *     - muuttaa kaista-suuntaryhman kaistaksi.
 *
 * -------------------------------------------------------------------------***/
{
    int kasu = kaista_suunta_ryhma;
    //while( (kasu % 16) != 0)
    //      --kasu;
    return( kasu / 16);
}

int sury(int kaista_suunta_ryhma)
/****------------------------------------------------------------------------
    INPUT:
      - kaista_suunta_ryhma = kaista-suuntaryhma ( = 17 ... 135 )
    OUTPUT:
      - sury() = suuntaryhma
    TOIMINTA:
      - muuttaa kaista-suuntaryhman suuntaryhmaksi.

-------------------------------------------------------------------------***/
{
    /*int sr = 0;
    int kasu = kaista_suunta_ryhma;
    while( (kasu % 16) != 0){
	  --kasu;
	  ++sr;
    }
    return ( sr);*/
    return (kaista_suunta_ryhma%16);
}

unsigned char* find_eol(unsigned char *map, char eol,int start,int end)
{
	int i=start;
	while (i<=end) {
		if (map[i++]==eol) return map+i;
	}
	return (unsigned char *)map+i;
}
