#!/bin/bash
set -e

# Installer les dépendances si requirements.txt existe
if [ -e "/opt/airflow/requirements.txt" ]; then
  python -m pip install --upgrade pip
  pip install --user -r /opt/airflow/requirements.txt
fi

# Initialiser la base de données et créer un utilisateur administrateur si nécessaire
if [ ! -f "/opt/airflow/airflow.db" ]; then
  airflow db init && \
  airflow users create \
    --username admin \
    --firstname admin \
    --lastname admin \
    --role Admin \
    --email admin@example.com \
    --password admin
fi

# Mettre à jour la base de données d'Airflow
airflow db upgrade

# Démarrer le serveur web Airflow
exec airflow webserver
