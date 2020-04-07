#!/bin/sh
# Package lambdas
echo ... Creating fmi-saaennusteet.zip ...
cd lambda;cd fmisaaennusteet;rm fmi-saaennusteet.zip;zip -r fmi-saaennusteet.zip .;cd ..; cd ..
pwd

echo ... Creating fmi-manifestit.zip ...
cd lambda; cd fmimanifestit;rm fmi-manifestit.zip;zip -r fmi-manifestit.zip .;cd ..; cd ..
pwd

echo ... Creating fmi-havaintodata.zip ...
cd lambda; cd fmihavaintodata;rm fmi-havaintodata.zip;zip -r fmi-havaintodata.zip .; cd ..; cd ..
pwd

echo ... Creating fmi-havaintoasemat.zip ...
cd lambda; cd fmihavaintoasemat;rm fmi-havaintoasemat.zip;zip -r fmi-havaintoasemat.zip .;cd ..; cd ..
pwd

echo ... Creating fmi-fraktiilit ...
cd lambda; cd fmifraktiilit;rm fmi-fraktiilit.zip;zip -r fmi-fraktiilit.zip .;cd ..; cd ..
pwd

echo Done!
