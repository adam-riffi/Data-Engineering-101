set -e

GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

echo ""
echo "🎌 AniData Lab — Initialisation des volumes Docker"
echo "=================================================="
echo ""

# --- Vérifier Docker ---
if ! docker info > /dev/null 2>&1; then
    echo -e "${RED}❌ Docker n'est pas démarré. Lancez Docker Desktop d'abord.${NC}"
    exit 1
fi

# --- Noms des volumes (Docker Compose préfixe avec le nom du dossier) ---
PROJECT=$(basename "$(pwd)")
VOLUME_DATA="${PROJECT}_airflow_data"
VOLUME_PIPELINE="${PROJECT}_logstash_pipeline"

echo "Projet détecté    : $PROJECT"
echo "Volume data       : $VOLUME_DATA"
echo "Volume pipeline   : $VOLUME_PIPELINE"
echo ""

# --- Créer les volumes s'ils n'existent pas ---
docker volume create "$VOLUME_DATA" > /dev/null
docker volume create "$VOLUME_PIPELINE" > /dev/null
echo -e "${GREEN}✅ Volumes créés (ou déjà existants)${NC}"

# --- Copier data/ → volume airflow_data ---
echo ""
echo "📦 Copie de data/ dans le volume $VOLUME_DATA..."

# Compter les fichiers CSV disponibles
CSV_COUNT=$(ls data/*.csv 2>/dev/null | wc -l)
if [ "$CSV_COUNT" -eq 0 ]; then
    echo -e "${YELLOW}⚠️  Aucun CSV trouvé dans data/ — le volume sera vide.${NC}"
    echo "   Téléchargez les données Kaggle et relancez ce script."
else
    echo "   $CSV_COUNT fichier(s) CSV trouvé(s) dans data/"
fi

docker run --rm \
    -v "$(pwd)/data":/source:ro \
    -v "$VOLUME_DATA":/dest \
    alpine sh -c "cp -r /source/. /dest/ && echo 'Copie data OK'"

echo -e "${GREEN}✅ data/ copié dans $VOLUME_DATA${NC}"

# --- Copier elk/logstash/pipeline/ → volume logstash_pipeline ---
echo ""
echo "⚙️  Copie du pipeline Logstash dans $VOLUME_PIPELINE..."

docker run --rm \
    -v "$(pwd)/elk/logstash/pipeline":/source:ro \
    -v "$VOLUME_PIPELINE":/dest \
    alpine sh -c "cp -r /source/. /dest/ && echo 'Copie pipeline OK'"

echo -e "${GREEN}✅ Pipeline Logstash copié dans $VOLUME_PIPELINE${NC}"

# --- Vérification ---
echo ""
echo "🔍 Vérification des volumes..."

DATA_FILES=$(docker run --rm -v "$VOLUME_DATA":/v alpine ls /v 2>/dev/null | wc -l)
PIPELINE_FILES=$(docker run --rm -v "$VOLUME_PIPELINE":/v alpine ls /v 2>/dev/null | wc -l)

echo "   $VOLUME_DATA    : $DATA_FILES fichier(s)"
echo "   $VOLUME_PIPELINE : $PIPELINE_FILES fichier(s)"

echo ""
echo "=================================================="
echo -e "${GREEN}✅ Volumes initialisés — vous pouvez lancer :${NC}"
echo ""
echo "   docker compose up -d"
echo ""
echo "⚠️  Si vous ajoutez de nouveaux fichiers dans data/ après"
echo "   le premier run, relancez ce script pour les synchroniser."
echo "=================================================="