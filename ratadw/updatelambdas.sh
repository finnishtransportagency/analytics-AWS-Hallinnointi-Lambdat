#!/bin/sh
# Package lambdas
echo ... Creating ratadw-causecodes.zip ...
cd lambda;cd causecodes;rm ratadw-causecodes.zip;zip -r ratadw-causecodes.zip .;cd ..; cd ..
pwd

echo ... Creating ratadw-detailedcodes.zip ...
cd lambda; cd detailedcodes;rm ratadw-detailedcodes.zip;zip -r ratadw-detailedcodes.zip .;cd ..; cd ..
pwd

echo ... Creating ratadw-thirdcategorycodes.zip ...
cd lambda; cd thirdcategorycodes;rm ratadw-thirdcategorycodes.zip;zip -r ratadw-thirdcategorycodes.zip .; cd ..; cd ..
pwd

echo ... Creating ratadw-combinedcodes.zip ...
cd lambda; cd combinedcodes;rm ratadw-combinedcodes.zip;zip -r ratadw-combinedcodes.zip .;cd ..; cd ..
pwd

echo ... Creating ratadw-xlscodereader ...
cd lambda; cd xlscodereader;rm ratadw-xlscodereader.zip;zip -r ratadw-xlscodereader.zip .;cd ..; cd ..
pwd

echo ... Creating ratadw-csv2ade ...
cd lambda; cd csv2ade;rm ratadw-csv2ade.zip;zip -r ratadw-csv2ade.zip .;cd ..; cd ..
pwd

echo Done!