#Projet GeoPro
Projet de géolocalisation à destination des commerciaux permettant de géolocaliser
les professionnels affiliés à Pages Jaunes (cliens et prospects). Les informations
de chaque établissement affiché sont liées aux activités et communes.


## Section d'initialisation des données
Ce projet contient les scripts permettant d'initialiser les données pour l'outil
GeoPro dans Elasticsearch.
Un script shell est également présent : convert_utf8.sh ; il permet de convertir 
les fichiers sources (souvent en iso-8859-1) en UTF8, format pris en charge par 
Swallow.


##Fonctionnement
Pour lancer l'import de données dans Elasticsearch :
- configurer le traitement : éditer init-conf.json
	- configurer la partie elasticsearch du fichier :
		- url : indiquer l'url du serveur Elasticsearch avec son port
		- host : indiquer le nome de la machine hébergeant Elasticsearch
		- port : indiquer le port d'écoute Elasticsearch
		- number_of_replicas : vu la volumétrie traitée par GeoPro, éviter les réplicats. Cela ralentira légèrement la recherche mais économisera l'espace disque de la machine
		- number_of_shards : spécifier le nombre de resources allouées aux opérations de recherches Elasticsearch. Préférer un nombre équivalent aux processeurs physiques disponibles.
		- bulk_size : spécifie la taille des blocs envoyés au serveur (1000 par défaut)
		- index : important, spécifie le nom de l'index qui contiendra toutes les données de GeoPro
	- configurer la partie log :
		- level_values : ne pas modifier
		- level : la sensibilité de tracage des opérations : DEBUG/CRITICAL/ERROR/WARNING/INFO
		- dir : le répertoire ou seront stockés les logs
		- filename : le nom des fichiers de logs générés
		- max_filesize : la taille en octet maximal d'un fichier de log
		- max_files : le nombre maximum de fichiers de logs générés
- Une fois la configuration effectuée, éditer le script shell de traitement d'import : init.sh
	- paramétrer le chemin du dossier contenant les données sources (fichiers CSV) DATA_PATH
	- paramétrer le nom de l'environnement virtuel python afin que celui utilisé soit en python 3.4 (si problème cf. note)
	- spécifier le type de traitement : init pour un traitement d'initialiation des données (première fois) ou update pour une mise à jour sur un socle déja existant
	- commenter ou décommenter les lignes à traiter en fonction du type de données à importer
- Lancer le traitement depuis un terminal en se positionnant dans le dossier contenant le fichier init.sh et en exécutant ./init.sh


## Note
Si aucun environnement virtual python (PEW) n'est paramétré, ou que python 3.4 est directement utilisé par la machine d'import, replacer "pew in $PEW_ENV python" par "python3.4" à chaque ligne de traitement.


## Compatibilité
Elasticsearch 1.7


##Requirements
Python 3.4
Swallow
DocOpt
Elasticsearch API Python