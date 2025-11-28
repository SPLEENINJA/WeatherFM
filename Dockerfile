# Dockerfile
FROM python:3.9-slim

# Définir le répertoire de travail
WORKDIR /app

# Installer les dépendances système
RUN apt-get update && apt-get install -y \
    curl \
    sqlite3 \
    && rm -rf /var/lib/apt/lists/*

# Copier les requirements d'abord pour mieux utiliser le cache Docker
COPY requirements.txt .

# Installer les dépendances Python
RUN pip install --no-cache-dir -r requirements.txt

# Copier le code source
COPY . .

# Créer les répertoires nécessaires
RUN mkdir -p /app/data /app/logs /app/notebooks

# Exposer les ports (Jupyter, API)
EXPOSE 8888 8000

# Définir la variable pour lancement automatique ETL
ENV AUTO_RUN_ETL=true

# Commande par défaut : lancement du main.py
CMD ["python", "src/main.py"]
