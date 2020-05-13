FROM centos:7

USER root
RUN yum install epel-release curl lftp unzip gcc -y
# Jos tehdaan s3 tiedostojen ja kyselyjen rajaus clientin paassa niin asenna jq
# lukemaan json vastauksia
# RUN yum install jq -y

RUN curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip"
RUN unzip awscliv2.zip
RUN ./aws/install

RUN mkdir -p /data/hpk/bin; mkdir -p /data/hpk/sonjain; mkdir -p /data/hpk/172.18.32.36/LAM
RUN chmod -R 777 /data/hpk

COPY . .

RUN gcc hpkpurku.c -o hpkpurku;chmod 777 hpkpurku;mv hpkpurku /data/hpk/bin/

#EXPOSE 8080

#CMD ["./LAMdaily.sh"]
# Paikallista docker kehitysta varten pitaa kontin elossa... MUISTA POISTAA!
CMD tail -f /dev/null