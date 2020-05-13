# LAM-siirtojen hpk-tiedostojen purkaja
Palvelintoteutus Docker imagena AWS Fargate ymparistossa ajettavaksi. 

# Docker imagen luonti ja AWS rekisteriin tallennus
Suorita seuraavat komennot imagen luomiseksi ja AWS:n rekisteriin kopioimiseksi.

```
aws ecr get-login-password --region eu-west-1 | docker login --username AWS --password-stdin 772136757925.dkr.ecr.eu-west-1.amazonaws.com
docker build -t lam-daily-repo .
docker tag lam-daily-repo:latest 772136757925.dkr.ecr.eu-west-1.amazonaws.com/lam-daily-repo:latest
docker push 772136757925.dkr.ecr.eu-west-1.amazonaws.com/lam-daily-repo:latest
```