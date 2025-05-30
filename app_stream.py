import streamlit as st
import pandas as pd
import os
import glob
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np

# Chemin du dossier où sont sauvegardées les prédictions
output_path = "/tmp/shared_data/predictions_outputlast4"

# Titre de l'application Streamlit sans icône
st.markdown("""
    <h1 style='text-align: center; color: #4B0082;'> 
        Visualisation des Prédictions de Vente
    </h1>
""", unsafe_allow_html=True)

# Fonction pour charger les données à partir des fichiers CSV
@st.cache_data(ttl=2)  # Met à jour automatiquement les données toutes les 2 secondes
def load_predictions():
    all_files = sorted(
        glob.glob(os.path.join(output_path, "*.csv")), 
        key=os.path.getmtime
    )

    if not all_files:
        return pd.DataFrame()

    # Charger tous les fichiers CSV et concaténer les données
    df_list = [pd.read_csv(file, header=None, names=["Invoice ID", "Total", "Prediction", "Prediction Time"]) for file in all_files]
    data = pd.concat(df_list, ignore_index=True)

    # Garder uniquement les 50 dernières lignes
    return data.tail(50)

# Charger les prédictions
predictions_data = load_predictions()

# Vérifier si des données sont disponibles
if predictions_data.empty:
    st.warning("Aucune prédiction disponible pour l'instant. Veuillez patienter pendant que les données sont générées.")
else:
    # Affichage du tableau avec style
    st.subheader("Tableau des Prédictions", anchor="tableau_predictions")
    st.dataframe(predictions_data.style.set_table_styles(
        [{'selector': 'thead th', 'props': [('background-color', '#1a4aba'), ('color', 'white'), ('font-size', '14px'), ('text-align', 'center')]},  # Entête en bleu ciel
         {'selector': 'tbody td', 'props': [('background-color', '#F5F5F5'), ('text-align', 'center')]},
         {'selector': 'tbody tr:nth-child(odd)', 'props': [('background-color', '#E6E6FA')]},
         {'selector': 'table', 'props': [('width', '100%'), ('border-collapse', 'collapse'), ('margin', '0 auto')]},  # Tableau de largeur 100% et centré
         {'selector': 'td', 'props': [('border', '1px solid #ccc'), ('padding', '10px')]},
         {'selector': 'th', 'props': [('border', '1px solid #ccc'), ('padding', '10px')]}]))

    # Graphique de comparaison des valeurs réelles et prédites
    st.subheader("Comparaison des Valeurs Réelles et Prédites")
    fig, ax = plt.subplots(figsize=(10, 6))
    ax.plot(predictions_data["Invoice ID"], predictions_data["Total"], label="Valeur Réelle", color="#1f77b4", marker='o')
    ax.plot(predictions_data["Invoice ID"], predictions_data["Prediction"], label="Valeur Prédite", color="#ff7f0e", marker='x')
    ax.set_xlabel("Invoice ID", fontsize=12)
    ax.set_ylabel("Montant", fontsize=12)
    ax.set_title("Comparaison des Valeurs Réelles et Prédites", fontsize=14)
    ax.legend()
    st.pyplot(fig)

    # Graphique des erreurs de prédiction
    st.subheader("Erreur de Prédiction")
    errors = predictions_data["Prediction"] - predictions_data["Total"]
    fig, ax = plt.subplots(figsize=(10, 6))
    ax.bar(predictions_data["Invoice ID"], errors, color="#2ca02c")
    ax.set_xlabel("Invoice ID", fontsize=12)
    ax.set_ylabel("Erreur de Prédiction", fontsize=12)
    ax.set_title("Erreur de Prédiction par Invoice ID", fontsize=14)
    st.pyplot(fig)

    # Calcul de l'erreur cumulée
    cumulative_error = errors.cumsum()

    # Graphique de l'erreur cumulée
    st.subheader("Erreur Cumulée")
    fig, ax = plt.subplots(figsize=(10, 6))
    ax.plot(predictions_data["Invoice ID"], cumulative_error, label="Erreur Cumulée", color="#d62728", marker='o')
    ax.set_xlabel("Invoice ID", fontsize=12)
    ax.set_ylabel("Erreur Cumulée", fontsize=12)
    ax.set_title("Erreur Cumulée des Prédictions", fontsize=14)
    ax.legend()
    st.pyplot(fig)

    # Statistiques sur les prédictions
    st.subheader("Statistiques sur les Prédictions")
    st.write(predictions_data.describe())

    # Graphique du temps de prédiction
    st.subheader("Temps de Prédiction")
    fig, ax = plt.subplots(figsize=(10, 6))
    ax.plot(predictions_data["Invoice ID"], predictions_data["Prediction Time"], label="Temps de Prédiction (sec)", color="#9467bd", marker='o')
    ax.set_xlabel("Invoice ID", fontsize=12)
    ax.set_ylabel("Temps de Prédiction (sec)", fontsize=12)
    ax.set_title("Temps de Prédiction pour chaque Commande", fontsize=14)
    ax.legend()
    st.pyplot(fig)

# Rafraîchissement manuel avec bouton
if st.button("Rafraîchir les données"):
    st.cache_data.clear()
    st.experimental_rerun()