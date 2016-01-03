#!/bin/bash
#
#	Script d'initialisation de l'environnement
# 	Lance les scripts Python dans l'environnement virtual adapté
#	Charge les données dans Elasticsearch avec les données dans 
#	le dossier init/
#

DATA_PATH=/home/tfalcher/devs/GeoPro/geopro-init/data #EDIT THIS
PEW_ENV=p3.4_new
PARAM=init # init ou update

# Import initial des données :
# Chargement des données issues des fichiers dans l'index ES
pew in $PEW_ENV python init_data.py --type_doc=referentiel_activites --source_file=$DATA_PATH/referentiel_activites.csv --$PARAM
# pew in $PEW_ENV python init_data.py --type_doc=activites_connexes --source_file=$DATA_PATH/activites_connexes.csv --$PARAM
# pew in $PEW_ENV python init_data.py --type_doc=communes --source_file=$DATA_PATH/communes.geojson --$PARAM
# pew in $PEW_ENV python init_data.py --type_doc=referentiel_communes --source_file=$DATA_PATH/referentiel_communes.csv --$PARAM
# pew in $PEW_ENV python init_data.py --type_doc=etablissements --source_file=$DATA_PATH/etablissements_part2.csv --update
# pew in $PEW_ENV python init_data.py --type_doc=activites --source_file=$DATA_PATH/activites.csv --$PARAM
# pew in $PEW_ENV python init_data.py --type_doc=etablissements --source_file=$DATA_PATH/etablissements_part1.csv --$PARAM

# pew in $PEW_ENV python init_data.py --type_doc=activites --source_file=$DATA_PATH/activites_test.csv --$PARAM

# pew in $PEW_ENV python init_data.py --type_doc=communes --source_file=$DATA_PATH/communes_zu.geojson --$PARAM






# python init_data.py --type_doc=activites --source_file=/home/tfalcher/devs/GeoPro/geopro-init/data/activites_part1.csv --update
# python init_data.py --type_doc=activites --source_file=/home/tfalcher/devs/GeoPro/geopro-init/data/activites_part2.csv --update
# python init_data.py --type_doc=activites --source_file=/home/tfalcher/devs/GeoPro/geopro-init/data/activites_part3.csv --update
# python init_data.py --type_doc=activites --source_file=/home/tfalcher/devs/GeoPro/geopro-init/data/activites_part4.csv --update