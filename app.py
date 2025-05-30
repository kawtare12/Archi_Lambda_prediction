import streamlit as st
import pandas as pd
import os
import glob
import matplotlib.pyplot as plt

# Chemins des dossiers où sont sauvegardées les prédictions
stream_output_path = "/tmp/shared_data/predictions_outputlast4"
batch_output_path = "/tmp/shared_data/predictions_output"

# Titre de l'application Streamlit sans icône
st.markdown("""
    <h1 style='text-align: center; color: #4B0082;'> 
        Comparaison des Prédictions de Vente (Stream Layer vs Batch Layer)
    </h1>
""", unsafe_allow_html=True)

# Fonction pour charger les données à partir des fichiers CSV
@st.cache_data(ttl=2)  # Met à jour automatiquement les données toutes les 2 secondes
def load_predictions(output_path):
    all_files = sorted(
        glob.glob(os.path.join(output_path, "*.csv")), 
        key=os.path.getmtime
    )

    if not all_files:
        return pd.DataFrame()

    # Charger tous les fichiers CSV et concaténer les données
    df_list = [pd.read_csv(file, header=None, names=["Invoice ID", "Total", "Prediction", "Prediction Time (s)"]) for file in all_files]
    data = pd.concat(df_list, ignore_index=True)

    # Garder uniquement les 50 dernières lignes
    return data.tail(50)

# Charger les prédictions pour les deux couches
stream_data = load_predictions(stream_output_path)
batch_data = load_predictions(batch_output_path)

# Vérifier si des données sont disponibles
if stream_data.empty or batch_data.empty:
    st.warning("Aucune prédiction disponible pour l'une des couches. Veuillez vérifier les données.")
else:
    # Affichage des tableaux
    st.subheader("Tableau des Prédictions - Stream Layer")
    st.dataframe(stream_data)

    st.subheader("Tableau des Prédictions - Batch Layer")
    st.dataframe(batch_data)

    # Fusionner les données pour comparaison
    merged_data = pd.merge(
        stream_data, batch_data, on="Invoice ID", suffixes=("_stream", "_batch")
    )

    # Graphique de comparaison des prédictions
    st.subheader("Comparaison des Valeurs Prédites (Stream vs Batch)")
    fig, ax = plt.subplots(figsize=(10, 6))
    ax.plot(merged_data["Invoice ID"], merged_data["Prediction_stream"], label="Stream Layer", color="#1f77b4", marker='o')
    ax.plot(merged_data["Invoice ID"], merged_data["Prediction_batch"], label="Batch Layer", color="#ff7f0e", marker='x')
    ax.set_xlabel("Invoice ID", fontsize=12)
    ax.set_ylabel("Valeur Prédite", fontsize=12)
    ax.set_title("Comparaison des Prédictions (Stream vs Batch)", fontsize=14)
    ax.legend()
    st.pyplot(fig)

    # Graphique des erreurs (différence entre les prédictions)
    st.subheader("Différence entre les Prédictions (Stream - Batch)")
    merged_data["Prediction_Difference"] = merged_data["Prediction_stream"] - merged_data["Prediction_batch"]
    fig, ax = plt.subplots(figsize=(10, 6))
    ax.bar(merged_data["Invoice ID"], merged_data["Prediction_Difference"], color="#2ca02c")
    ax.set_xlabel("Invoice ID", fontsize=12)
    ax.set_ylabel("Différence", fontsize=12)
    ax.set_title("Différence des Prédictions entre Stream et Batch", fontsize=14)
    st.pyplot(fig)

    # Statistiques sur la différence
    st.subheader("Statistiques sur la Différence des Prédictions")
    st.write(merged_data["Prediction_Difference"].describe())

# Rafraîchissement manuel avec bouton
if st.button("Rafraîchir les données"):
    st.cache_data.clear()
    st.experimental_rerun()