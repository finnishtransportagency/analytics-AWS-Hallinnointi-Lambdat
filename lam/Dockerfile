FROM centos:7

USER root
RUN yum install epel-release curl lftp unzip gcc lftp nc openssh-server openssh-clients -y
# Jos tehdaan s3 tiedostojen ja kyselyjen rajaus clientin paassa niin asenna jq
# lukemaan json vastauksia
RUN yum install jq -y

# AWS CLI
RUN curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip"
RUN unzip awscliv2.zip
RUN ./aws/install

# Snowflake CLI
RUN curl -o snowflake-snowsql-1.2.5-1.x86_64.rpm https://sfc-repo.snowflakecomputing.com/snowsql/bootstrap/1.2/linux_x86_64/snowflake-snowsql-1.2.5-1.x86_64.rpm
RUN rpm -i snowflake-snowsql-1.2.5-1.x86_64.rpm

# Daily skriptin tarvitsemat kansiot
RUN mkdir -p /data/hpk/bin; mkdir -p /data/hpk/sonjain; mkdir -p /data/hpk/172.18.32.36/LAM
RUN chmod -R 777 /data/hpk

COPY . .

# HPK csv-purkajan C kaannos
RUN gcc hpkpurku.c -o hpkpurku;chmod 777 hpkpurku;mv hpkpurku /data/hpk/bin/
# Avoin data siirretaan lftp clientin avulla, siirtoa ohjataan ao. tiedostolla
RUN cp aineistotsync.lftp /data/hpk/bin/

# Kontin suoritus
#CMD ["./LAMdaily.sh"]
# Paikallista docker kehitysta varten pitaa kontin elossa... MUISTA POISTAA!
CMD tail -f /dev/null