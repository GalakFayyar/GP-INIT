#!/usr/bin/env python
"""
    Intègres les données à destination de l'application GeoPro dans Elasticsearch

    Usage:
        init_data.py --type_doc=<doc_type> --source_file=<file_path> [--init] [--update] [--debug] 

    Example:
        python init_data.py --type_doc=referentiel_activites --source_file=./data/csv/referentiel_activites.csv

    Options:
        --help                      Affiche l'aide
        --type_doc=<doc_type>       Type de document à traiter
        --source_file=<file_path>   Fichier contenant les données à importer ou à mettre à jour
        --init                      Initialise les données pour ce type de documents
        --update                    Met à jour les données pour ce type de documents
"""
from elasticsearch import Elasticsearch, TransportError
from logger import logger, configure
from docopt import docopt
import json, time

from swallow.inout.ESio import ESio
from swallow.inout.CSVio import CSVio
from swallow.inout.JsonFileio import JsonFileio
from swallow.Swallow import Swallow

def file_to_elasticsearch(p_docin, p_type, p_es_conn, p_es_index, p_arguments):
    doc = {}

    if p_type == "activites":
        # Traitement des valeurs mensuelles
        tab_nb_recherches_totales_mensuelles = []
        for i in range(19, 30):
            tab_nb_recherches_totales_mensuelles.append(p_docin[i])

        tab_nb_recherches_pures_mensuelles = []
        for i in range(31, 42):
            tab_nb_recherches_pures_mensuelles.append(p_docin[i])

        tab_nb_recherches_alpha_mensuelles = []
        for i in range(43, 54):
            tab_nb_recherches_alpha_mensuelles.append(p_docin[i])

        code_commune = p_docin[1].zfill(5)
        code_activite = p_docin[0].zfill(6)

        doc = {
            'code_activite': code_activite,
            'commune': {
                'code_commune': code_commune,
                'libelle': p_docin[2],
                'centroid_y': 0,
                'centroid_x': 0,
                'type': 'null',
                'geometry': {
                    'type': 'Polygon',
                    'coordinates': [[[0,0], [0,0.0001], [0.0001,0], [0,0]]]
                }
            },
            'pot_audience': p_docin[3],
            'paniers': {
                'panier1': int(float(p_docin[4])) if p_docin[4] else -1,
                'panier2': int(float(p_docin[5])) if p_docin[5] else -1,
                'panier3': int(float(p_docin[6])) if p_docin[6] else -1,
                'panier4': int(float(p_docin[7])) if p_docin[7] else -1,
                'panier5': int(float(p_docin[8])) if p_docin[8] else -1,
                'panier_total': int(float(p_docin[9])) if p_docin[9] else -1
            },
            'nb_recherches': {
                'total': {
                    'somme': int(float(p_docin[10])) if p_docin[10] else 0,
                    'mensuel': tab_nb_recherches_totales_mensuelles
                },
                'alpha': {
                    'somme': int(float(p_docin[11])) if p_docin[11] else 0,
                    'mensuel': tab_nb_recherches_alpha_mensuelles
                },
                'pures': {
                    'somme': int(float(p_docin[12])) if p_docin[12] else 0,
                    'mensuel': tab_nb_recherches_pures_mensuelles
                }
            },
            'nb_clics': {
                'total': int(float(p_docin[13])) if p_docin[13] else 0,
                'alpha': int(float(p_docin[14])) if p_docin[14] else 0,
                'pures': int(float(p_docin[15])) if p_docin[15] else 0
            },
            'nb_contacts': {
                'total': int(float(p_docin[16])) if p_docin[16] else 0,
                'alpha': int(float(p_docin[17])) if p_docin[17] else 0,
                'pures': int(float(p_docin[18])) if p_docin[18] else 0
            }
        }

        if code_commune:
            try:
                es_commune = p_es_conn.get(id=code_commune, doc_type='communes', index=p_es_index)
            except TransportError as e:
                logger.info("Commune non référencée : %s", code_commune)
            else:
                # Selon le potentiel d'audience
                if p_docin[3] and len(p_docin[3]) > 0:
                    doc['commune']['centroid_y'] = es_commune['_source']['properties']['centroide_y']
                    doc['commune']['centroid_x'] = es_commune['_source']['properties']['centroide_x']
                    doc['commune']['type'] = es_commune['_source']['type']
                    doc['commune']['geometry'] = es_commune['_source']['geometry']
                else:
                    # Création d'un triangle minimal autour du centroide (contrainte elasticsearch)
                    doc['commune']['centroid_y'] = es_commune['_source']['properties']['centroide_y']
                    doc['commune']['centroid_x'] = es_commune['_source']['properties']['centroide_x']
                    #doc['commune']['type'] = None
                    doc['commune']['geometry'] = { 
                        "type": "Polygon", 
                        "coordinates": [ 
                            [ 
                                [ es_commune['_source']['properties']['centroide_x'] - 0.00001, es_commune['_source']['properties']['centroide_y'] ], 
                                [ es_commune['_source']['properties']['centroide_x'] + 0.00001, es_commune['_source']['properties']['centroide_y'] ], 
                                [ es_commune['_source']['properties']['centroide_x'], es_commune['_source']['properties']['centroide_y'] - 0.00001 ], 
                                [ es_commune['_source']['properties']['centroide_x'] - 0.00001, es_commune['_source']['properties']['centroide_y'] ] 
                            ] 
                        ] 
                    }

        if not p_arguments['--init'] or p_arguments['--update']:
            # TODO : Optimiser : éviter recherche _id par la création d'un id prédictible : 
            #                    _id = concat(code_commune, code_activite)

            # Récupération de l'id de l'activité
            body_query = {
                "filter": {
                    "and": {
                            "filters": [
                                {
                                    "query" : {
                                        "match" : {
                                            "commune.code_commune" : code_commune
                                        }
                                    }
                                },
                                {
                                    "query" : {
                                        "match" : {
                                            "code_activite" : code_activite
                                        }
                                    }
                                }
                            ]
                        }   
                    }
                }
            try:
                es_activite = p_es_conn.search(index=p_es_index, doc_type=p_type, body=body_query, from_=0, size=1)
            except TransportError as e:
                logger.debug("Activité %s pour la localité %s non trouvée : Création", code_activite, code_commune)
                document = {
                    "_type": p_type,
                    "_source": doc
                }
                return [document]
            else:
                if len(es_activite['hits']['hits']) > 0:
                    # Script : si commune est une feature (type) et contient un potentiel audience
                    script_update = """
                        if (ctx._source['pot_audience'] != doc['pot_audience']) {
                            ctx._source['commune'] = doc['commune']
                        }
                        ctx._source['code_activite'] = doc['code_activite']
                        ctx._source['pot_audience'] = doc['pot_audience']
                        ctx._source['paniers'] = doc['paniers']
                        ctx._source['nb_recherches'] = doc['nb_recherches']
                        ctx._source['nb_clics'] = doc['nb_clics']
                        ctx._source['nb_contacts'] = doc['nb_contacts']
                    """

                    document = {
                        '_op_type': 'update',
                        '_type': p_type,
                        '_id': es_activite['hits']['hits'][0]['_id'],
                        'script': script_update,
                        'params': {
                            "doc": doc
                        },
                        'upsert': doc,
                        '_retry_on_conflict': 100
                    }
                    return [document]
                else:
                    logger.debug("Donnée activité %s trouvée mais vide pour la commune %s.", code_activite, code_commune)

    elif p_type == "activites_connexes":
        doc = {
            'rubrique_src': {
                'code_rubrique': p_docin[1],
                'libelle': p_docin[0]
            },
            'rubrique_connexe': {
                'code_rubrique': p_docin[3],
                'libelle': p_docin[2]
            }
        }

    elif p_type == "referentiel_activites":
        doc = {
            'code_rubrique': p_docin[0].zfill(6),
            'activite': p_docin[1],
            'zu': True if p_docin[2] == "1" else False
        }

    elif p_type == "referentiel_communes":
        doc = {
            'code_commune': p_docin[0].zfill(5),
            'libelle': p_docin[1],
            'centroid_x': float(p_docin[2].replace(",", ".")) if p_docin[2] else 0,
            'centroid_y': float(p_docin[3].replace(",", ".")) if p_docin[3] else 0
        }

    elif p_type == "etablissements":
        # Traitement des départements
        tab_parutions_departements = ""
        for i in range(15, 74):
            if p_docin[i]:
                tab_parutions_departements = tab_parutions_departements + p_docin[i] + ","

        tab_parution_dep_returned = []
        if tab_parutions_departements:
            tab_parution_dep = tab_parutions_departements.split(",")

            for parution_dep in tab_parution_dep:
                if parution_dep:
                    tab_info_parution = parution_dep.split("|")
                    if len(tab_info_parution) > 0:
                        tab_parution_dep_returned.append({
                            'code_activite': tab_info_parution[0],
                            'code_departement': tab_info_parution[1],
                            'polpo': True if tab_info_parution[2] == "1" else False,
                            'cvip': True if tab_info_parution[3] == "1" else False,
                            'cvi': True if tab_info_parution[4] == "1" else False,
                            'acces': True if tab_info_parution[5] == "1" else False,
                            'lvs': True if tab_info_parution[6] == "1" else False,
                            'ebp': True if tab_info_parution[7] == "1" else False,
                            'lien_trans': True if tab_info_parution[8] == "1" else False
                        })

        # Traitement des départements
        tab_parution_communes = ""
        for i in range(75, len(p_docin)):
            if p_docin[i]:
                tab_parution_communes = tab_parution_communes + p_docin[i] + ","

        tab_parution_com_returned = []
        if tab_parution_communes:
            tab_parution_com = tab_parution_communes.split(",")
            
            for parution_com in tab_parution_com:
                if parution_com:
                    tab_info_parution = parution_com.split("|")
                    if len(tab_info_parution) > 0:
                        tab_parution_com_returned.append({
                            'code_activite': tab_info_parution[0],
                            'code_commune': tab_info_parution[1],
                            'polpo': True if tab_info_parution[2] == "1" else False,
                            'cvip': True if tab_info_parution[3] == "1" else False,
                            'cvi': True if tab_info_parution[4] == "1" else False,
                            'acces': True if tab_info_parution[5] == "1" else False,
                            'lvs': True if tab_info_parution[6] == "1" else False,
                            'ebp': True if tab_info_parution[7] == "1" else False,
                            'lien_trans': True if tab_info_parution[8] == "1" else False
                        })

        # Liste des rubriques
        tab_rubriques_returned = []
        tab_rubriques = p_docin[7].split('|')
        for rubrique in tab_rubriques:
            if rubrique:
                tab_rubriques_returned.append(rubrique)

        id_etablissement = p_docin[1].zfill(6)

        doc = {
            'typo': p_docin[0],
            'no_etab': id_etablissement,
            'coordinates': {
                'lat': float(p_docin[3]) if p_docin[3] else 0,
                'lon': float(p_docin[2]) if p_docin[2] else 0
            },
            'libelle': p_docin[5],
            'appetence_prospect': p_docin[6],
            'rubrique_principale': {
                'code_rubrique': p_docin[13],
                'libelle': p_docin[14]
            },
            'rubriques': tab_rubriques_returned,
            'montants': {
                'ts': p_docin[8],
                'site': p_docin[9],
                'display': p_docin[10],
                'search': p_docin[11],
                'print': p_docin[12]
            },
            'parution_dep': tab_parution_dep_returned,
            'parution_com': tab_parution_com_returned
        }

        document = {
            '_op_type': 'update',
            '_type': p_type,
            '_id': id_etablissement,
            'script': "ctx._source = doc",
            'params': {
                "doc": doc
            },
            'upsert': doc,
            '_retry_on_conflict': 100
        }

        return [document]

    elif p_type == "communes":
        tab_communes =  []
        for commune in p_docin['features']:
            tab_communes.append({
                "_id":commune['properties']['code'],
                "_type": p_type,
                "_source": commune
            })

        return tab_communes

    document = {
        "_type": p_type,
        "_source": doc
    }

    return [document]

def run_import(type_doc = None, source_file = None):
    conf = json.load(open('./init-conf.json'))

    # Command line args
    arguments = docopt(__doc__, version=conf['version'])

    configure(conf['log']['level_values'][conf['log']['level']],
              conf['log']['dir'], 
              conf['log']['filename'],
              conf['log']['max_filesize'], 
              conf['log']['max_files'])

    #
    #   Création du mapping
    # 

    es_mappings = json.load(open('data/es.mappings.json'))

    # Connexion ES métier
    try:
        param = [{'host': conf['connectors']['elasticsearch']['host'],
                  'port': conf['connectors']['elasticsearch']['port']}]
        es = Elasticsearch(param)
        logger.info('Connected to ES Server: %s', json.dumps(param))
    except Exception as e:
        logger.error('Connection failed to ES Server : %s', json.dumps(param))
        logger.error(e)

    # Création de l'index ES metier cible, s'il n'existe pas déjà
    index = conf['connectors']['elasticsearch']['index']
    if not es.indices.exists(index):
        logger.debug("L'index %s n'existe pas : on le crée", index)
        body_create_settings = {
            "settings" : {
                "index" : {
                    "number_of_shards" : conf['connectors']['elasticsearch']['number_of_shards'],
                    "number_of_replicas" : conf['connectors']['elasticsearch']['number_of_replicas']
                },
                "analysis" : {
                    "analyzer": {
                        "lower_keyword": {
                            "type": "custom",
                            "tokenizer": "keyword",
                            "filter": "lowercase"
                        }
                    }
                }
            }
        }
        es.indices.create(index, body=body_create_settings)
        # On doit attendre 5 secondes afin de s'assurer que l'index est créé avant de poursuivre
        time.sleep(2)

        # Création des type mapping ES
        for type_es, properties in es_mappings['geopro'].items():
            logger.debug("Création du mapping pour le type de doc %s", type_es)
            es.indices.put_mapping(index=index, doc_type=type_es, body=properties)

        time.sleep(2)

    #
    #   Import des données initiales
    #

    # Objet swallow pour la transformation de données
    swal = Swallow()

    # Tentative de récupération des paramètres en argument
    type_doc = arguments['--type_doc'] if not type_doc else type_doc
    source_file = arguments['--source_file'] if not source_file else ('./upload/' + source_file)

    if arguments['--update']:
        if type_doc in ['referentiel_activites', 'referentiel_communes', 'communes', 'activites_connexes']:
            # Suppression des docs
            logger.debug("Suppression des documents de type %s", type_doc)
            p_es_conn.delete(index=p_es_index, doc_type=type_doc)
            time.sleep(3)

    # On lit dans un fichier
    if type_doc == "communes":
        reader = JsonFileio()
        swal.set_reader(reader, p_file=source_file)
    else:
        reader = CSVio()
        swal.set_reader(reader, p_file=source_file, p_delimiter=';')

    # On écrit dans ElasticSearch
    writer = ESio(conf['connectors']['elasticsearch']['host'],
                  conf['connectors']['elasticsearch']['port'],
                  conf['connectors']['elasticsearch']['bulk_size'])
    swal.set_writer(writer, p_index=conf['connectors']['elasticsearch']['index'], p_timeout=30)

    # On transforme la donnée avec la fonction
    swal.set_process(file_to_elasticsearch, p_type=type_doc, p_es_conn=es, p_es_index=conf['connectors']['elasticsearch']['index'], p_arguments=arguments)

    if arguments['--init']:
        logger.debug("Opération d'initialisation")
    elif arguments['--update']:
        logger.debug("Opération de mise à jour")
    else:
        logger.error("Type d'opération non défini")

    logger.debug("Indexation sur %s du type de document %s", conf['connectors']['elasticsearch']['index'], type_doc)
    
    swal.run(1)

    logger.debug("Opération terminée pour le type de document %s ", type_doc)

if __name__ == '__main__':
    run_import()