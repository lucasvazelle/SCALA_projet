# Scala Template
Routine qui converti à l'aide de scala et  spark des données en fichier parquet avec un retraitement intermédiaire.

#En local
Bash lancer
$ /usr/lib/jvm/java-11-openjdk-amd64/bin/java -jar target/scala_spark_mosef_vazelle-1.1-jar-with-dependencies.jar local[1] src/main/resources/rappelconso0.csv /tmp/output/ csv

Options : 
local[1] → Exécute Spark en mode local avec 1 cœur.

/chemin/vers/fichier.csv → Ton fichier d'entrée (remplace avec un vrai chemin).

/chemin/vers/output → Dossier où écrire la sortie.

csv → Format du fichier (peut être csv ou parquet).

# Avec Docker


$ docker run --rm \
  -e MASTER_URL='local[1]' \
  -e SRC_PATH='/data/rappelconso0.csv' \
  -e DST_PATH='/data/output/' \
  -e FORMAT='csv' \
  -v /chemin/vers/ton/dossier/src/main/resources:/data \
  -v /chemin/vers/ton/dossier/output:/data/output \
  scala-spark-image

docker run --rm   -e MASTER_URL='local[1]'   -e SRC_PATH='/data/rappelconso0.csv'   -e 
DST_PATH='/tmp/output/'   -e FORMAT='csv'   -v /mnt/c/Users/lucas/OneDrive/Documents/M2\ MSOEF/Linux/SCALA_projet/src/main/resources:/data   -v  /mnt/c/Users/lucas/OneDrive/Documents/M2\ MSOEF/Linux/SCALA_projet/default:/data/output   scala-spark-image













M
