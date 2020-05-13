#!/bin/sh 

#AWS asetukset synkronointivarten
profiili=default
s3data=s3://vayla-lam-hpk-in/data/hpk/sonjain # mista haetaan purettava data, ennen tuupattu purkajan palvelimelle suoraan
s3bucketti=s3://lam-data-dummy/
#s3bucketti=s3://vayla-lamdata/ #mihin S3 buckettiin  ja rakenteeseen synkronointitehdään   esim s3://<s3-bucketti>/<kansio>/<alikansio>

#ymparistomuuttuja jsqsh:lle
export JSQSH_JAVA_OPTS="$JSQSH_JAVA_OPTS -Djline.terminal=jline.UnsupportedTerminal"

base=/data/hpk
hpkpurku=$base/bin/hpkpurku
ybase=$base/172.18.32.36/LAM
vuodet='2019'		#jos tässä on kaksi arvoa niin täytä serraavakin
vuosi_vanha=''
vuosi_uusi='2019'
jako_paiva='350' 	#paivanumero jota suuremmat ovat vanhaa vuott
		        #toimii vain jos vuosi_vanha on asetettu 
alueet='01 02 03 04 08 09 10 12 14'
sonja_in=/data/hpk/sonjain

echo `date +'%d.%m.%Y %H:%M:%S'` LAMdaily ajo alkaa 
cd $base

echo AWS testi
echo `aws s3 ls`

#echo Levytila
#echo `df -hv`

# Haetaan purettava data s3:sta
# TODO: paatettava tehdaanko tiedostojen paivamaarasuodatus ja tai sailytys aws paassa,
# vai taalla clientin paassa esim.:
# aws s3api list-objects --bucket lam-data-dummy --query 'Contents[?LastModified>=`2020-04-20`][].{Key: Key}'
# ja sitten parsinta jq:lla (https://stedolan.github.io/jq/tutorial/) esim. jq .[].Key
for vuosi in $vuodet;
do
	mkdir $sonja_in/$vuosi;cd $sonja_in/$vuosi
	aws s3 cp $s3data/$vuosi/ . --recursive --include "hpk*" #vain hpk tiedostot, ei muuta tauhkaa tai lopputuotteita
	
	#echo luodaan testipiste
	#mkdir -p $sonja_in/2019/01/;touch $sonja_in/2019/01/hpk0001.001

	#siirretaan kaikki varmasti juurihakemistoon
	find $sonja_in/$vuosi -name 'hpk*' | xargs -I {} mv {} $sonja_in
done

echo Siirretyt tiedostot
echo `ls -alt $sonja_in`

#2.10.2017 : Poistettu Mulo -pisteiden erikoiskäsittely
#/data/hpk/bin/loadMuloStations.sh

#Tyokansioiden alustus volatiilissa kontissa/levyjarjestelmassa
for alue in $alueet;
do
	mkdir -p $ybase/$vuosi_uusi/$alue
	if [ ! -z $vuosi_vanha ];
		then
			mkdir -p $ybase/$vuosi_vanha/$alue
		fi
	fi
done

# Alla oleva pisteet.sql kysely 
# select * from piste_ely
# go -m csv
jsqsh livbigsql < /data/hpk/bin/pisteet.sql  > /tmp/pisteet.txt
#haetaan mulpisteet sonjan hakemistoon ennen käsitettelyä
#käsitellään pisteet
for hpkfile in $sonja_in/hpk*;
do 
	filename=$(basename "$hpkfile")
	piste="${filename:3:4}"
#     alue=`grep -F $piste /tmp/pisteet.txt | cut -d',' -f 2`
	alue=01
	paiva=${filename#*.}
	kohde=$vuosi_uusi
	if [ ! -z $vuosi_vanha ];
        then
                if [ "$paiva" \> "$jako_paiva" ];
                then
                        kohde=$vuosi_vanha
                fi
        fi
		echo Siirretaan kohdekansioon $ybase/$kohde/$alue tiedosto $hpkfile
        mv $hpkfile $ybase/$kohde/$alue
done

#käsitellään hpk-tiedostot csv:ksi

cd $base
echo `date +'%d.%m.%Y %H:%M:%S'` HPK tiedostojen käsittely alkaa 
for vuosi in $vuodet;
do
	for alue in $alueet;
	do
		echo `date +'%d.%m.%Y %H:%M:%S'` Hakemiston $ybase/$vuosi/$alue HPK tiedostojen käsittely alkaa 
		cd $ybase/$vuosi/$alue
		#/etsitään kaikki alle vrk:n ikäiset koska ne on just noudettu
		find . -name hpk\* -mtime -10 -printf '%f\0' | xargs -L1 -0 -P8 $hpkpurku #muista muuttaa takaisin 1 paivaksi, testin vuoksi 10pv
																				#tosin jos suodatus tehty jo aws s3:ssa tai aiempana dataa ladattessa -> tama turha

	done
done

# Yksisuuntainen  CSV synkronointi S3:n määritellyllä profiililla joka määrittää mihin s3 buckettiin data siirtyy. 
# Lisäksi määritelty että ämpärin omistajalla on täydet oikeudet tiedostoihin vaikkei se periaatteessa ole välttämätön tässä tapauksessa
echo Synkronoidaan datat AWS:n S3:n kanssa 
echo `find $ybase -name '*.csv'`
aws s3 sync $ybase $s3bucketti --exclude "*" --include "*.csv" #--only-show-errors &  #--profile $profiili --acl bucket-owner-full-control 


#echo `date +'%d.%m.%Y %H:%M:%S'` Siirretään datat julkiselle puolelle
#cd $ybase
#for vuosi in $vuodet;
#do
#	sed -e s/vuosi/$vuosi/g /data/hpk/bin/aineistotsync.lftp > /data/hpk/bin/aineistotsynccur.lftp 
#	lftp -f /data/hpk/bin/aineistotsynccur.lftp
#	#rsync -r --exclude-from ./lamexclude.txt $vuosi/ lam-rawdata@livialk01n1.vally.local:/data2/users/lam-rawdata/upload/$vuosi
#done

#echo `date +'%d.%m.%Y %H:%M:%S'` Siirretään hpk livapp03:lle
#for vuosi in $vuodet;
#do
#	cd $ybase/$vuosi
#	sed -e s/vuosi/$vuosi/g /data/hpk/bin/toLivApp03.lftp > /data/hpk/bin/toLivAppcur.lftp
#	lftp -f /data/hpk/bin/toLivAppcur.lftp  
#done

echo `date +'%d.%m.%Y %H:%M:%S'` LAMdaily ajo päättyi
	
