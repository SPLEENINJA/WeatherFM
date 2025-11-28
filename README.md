# WeatherFM

Un petit projet Python pour ingérer, enrichir et analyser des données météo et métadonnées musicales.
Ce projet WeatherFM permet :

  - de requêter 3 API : OPENWEATHER, LASTFM et SOUNDCHART
  - de gérer les ingestions via batch avec sauvegarde des fichiers RAW
  - de piloter la pipeline ETL via un orchestrateur lancé au démarrage du projet en monitorant chaque étape
  - de garder une backup des bases de données
  - d'analyser la qualité de la donnée, ainsi que d'étudier à première vue les résultats, via notebook

## Description courte
- Ingestion de données brutes dans `data/raw/`.
- Pipelines ETL dans `src/etl/` pour transformer et enrichir les données.
- Outils d'analyse et visualisation dans `src/` et `notebooks/`.

## Prérequis
- Python 3.9+ (utiliser un environnement virtuel recommandé)
- Docker & Docker Compose (optionnel pour exécution conteneurisée)

## Installation locale
1. Créez un environnement virtuel et activez-le.
2. Installez les dépendances :

```powershell
python -m pip install -r requirements.txt
```

## Commandes utiles
- Lancer le script d'analyse du flux de données :

```powershell
python analyze_data_flow.py
```

- Lancer l'application principale (depuis la racine) :

```powershell
python -m src.main
```

- Démarrer avec Docker Compose :

```powershell
docker-compose up --build
```

- Script d'aide (bash) :

```powershell
./start.sh
```

## Tests
Les tests sont fournis au niveau du dépôt (fichiers `test_*.py`). Pour lancer les tests :

```powershell
pytest -q
```

## Structure importante
- `src/` : code applicatif principal
  - `src/etl/` : orchestrateur et pipeline ETL
  - `src/ingestion/` : ingestors (batch et raw)
  - `src/utils/` : helpers et logger
- `data/` : fichiers d'entrée/sortie
  - `data/raw/` : JSON bruts ingestés
  - `data/ingestion_reports/` : rapports de lot
  - `data/failed_ingestions/` : ingest échouées
- `notebooks/` : analyses exploratoires

## Points d'intégration / configuration
- Les clés API et configurations sensibles sont fournies via variables d'environnement. Voir `test_api_keys.py` pour exemples d'utilisation et tests rapides.
- Les collectors/enrichers (ex. `src/lastfm_weather_collector.py`) appellent des APIs externes ; vérifiez les quotas et clés avant d'exécuter des jobs en production.

## Flux de données (très bref)
1. Ingestion (scripts dans `src/ingestion/`) dépose des JSON dans `data/raw/`.
2. ETL (`src/etl/etl_orchestrator.py`, `src/etl/etl_pipeline.py`) transforme et enrichit les données.
3. Analyses et visualisations lisent les sorties transformées (ou utilisent les notebooks).

## Besoin d'aide / contributions
Ouvrez une issue ou proposez une PR. Si vous voulez des instructions plus détaillées (exemples d'env vars, recette de déploiement), dites-moi ce que vous préférez voir.
