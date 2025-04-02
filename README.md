# Scala Template
Routine qui converti à l'aide de scala et  spark des données en fichier parquet avec un retraitement intermédiaire.
Projet développé par fr.mosef.scala.template


**En local**

1. Installer java 11, Scala	2.13.13,  Apache Spark	3.4.2 (avec support Scala 2.13), Apache Spark	3.4.2 (avec support Scala 2.13), 
Maven	3.6+

$ sudo apt install openjdk-11-jdk
$ ..

2. Lancer
$ /usr/lib/jvm/java-11-openjdk-amd64/bin/java -jar target/scala_spark_mosef_vazelle-1.1-jar-with-dependencies.jar local[1] src/main/resources/rappelconso0.csv /tmp/output/ csv

Options : 

local[1] → Exécute Spark en mode local avec 1 cœur.

/chemin/vers/fichier.csv → Ton fichier d'entrée (remplace avec un vrai chemin).

/chemin/vers/output → Dossier où écrire la sortie.

csv → Format du fichier (peut être csv ou parquet).

**Avec Docker**
1. Installer docker
$ sudo apt docker

2. Build l'image OU pull l'image
   
$ docker build -t scala-spark-image .

ou 

$ docker pull lucasvazelle/scala-spark-image

3. Lancer
    
$docker run --rm \
  -e MASTER_URL='local[1]' \
  -e SRC_PATH='/data/your_input_file.csv' \
  -e DST_PATH='/data/output' \
  -e FORMAT='csv' \
  -v /absolute/path/to/your/resources:/data \
  scala-spark-image












