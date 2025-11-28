#!/bin/bash

# =============================================
# Music Weather Analyzer - Script de Démarrage
# =============================================

set -e  # Arrêter en cas d'erreur

echo ""
echo "🎵 🌦️  MUSIC WEATHER ANALYZER"
echo "================================"
echo ""

# Couleurs pour le output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Fonctions colorées
print_info() { echo -e "${BLUE}ℹ️  $1${NC}"; }
print_success() { echo -e "${GREEN}✅ $1${NC}"; }
print_warning() { echo -e "${YELLOW}⚠️  $1${NC}"; }
print_error() { echo -e "${RED}❌ $1${NC}"; }

# Vérification des prérequis
check_requirements() {
    print_info "Vérification des prérequis..."
    
    # Vérifier Docker
    if ! command -v docker &> /dev/null; then
        print_error "Docker n'est pas installé"
        exit 1
    fi
    
    # Vérifier Docker Compose
    if ! command -v docker-compose &> /dev/null && ! docker compose version &> /dev/null; then
        print_error "Docker Compose n'est pas installé"
        exit 1
    fi
    
    print_success "Docker et Docker Compose sont installés"
}

# Vérification du fichier .env
check_env() {
    if [ ! -f .env ]; then
        print_warning "Fichier .env manquant"
        cp .env.example .env
        print_info "Fichier .env créé à partir de .env.example"
        print_warning "⚠️  Veuillez éditer le fichier .env avec vos clés API avant de continuer"
        exit 1
    fi
    
    # Vérifier que les clés API sont configurées
    if grep -q "votre_cle_" .env; then
        print_warning "Des clés API ne sont pas configurées dans .env"
        print_info "Ouvrez le fichier .env et remplacez:"
        echo "   LASTFM_API_KEY=votre_cle_lastfm_ici"
        echo "   OPENWEATHER_API_KEY=votre_cle_openweather_ici"
        echo ""
        print_info "Obtenez vos clés:"
        echo "   🌐 Last.fm: https://www.last.fm/api/account/create"
        echo "   🌦️ OpenWeather: https://openweathermap.org/api"
        exit 1
    fi
    
    print_success "Fichier .env configuré"
}

# Démarrage des services
start_services() {
    print_info "Démarrage des services Docker..."
    
    # Construction des images si nécessaire
    # docker-compose build --pull
    
    # Démarrage des services
    docker-compose up -d
    
    # Attendre que les services soient prêts
    print_info "Attente du démarrage des services..."
    sleep 10
}

# Affichage des informations
show_info() {
    print_success "🎉 Services démarrés avec succès!"
    echo ""
    echo "📊 SERVICES DISPONIBLES:"
    echo "   🌐 Grafana Dashboard:  http://localhost:3000"
    echo "       👤 admin / $(grep GRAFANA_PASSWORD .env | cut -d '=' -f2)"
    echo ""
    echo "   📓 Jupyter Notebook:   http://localhost:8888"
    # echo "   🔌 API REST:           http://localhost:8000"
    echo ""
    echo "   📝 Collection données: docker-compose logs -f music-weather-collector"
    echo ""
    
    print_info "🔍 Vérification du statut des services..."
    docker-compose ps
}

# Arrêt des services
stop_services() {
    print_info "Arrêt des services..."
    docker-compose down
    print_success "Services arrêtés"
}

# Statut des services
status_services() {
    print_info "Statut des services:"
    docker-compose ps
}

# Logs des services
show_logs() {
    print_info "Affichage des logs (Ctrl+C pour quitter):"
    docker-compose logs -f "$1"
}

# Menu d'aide
show_help() {
    echo "Usage: $0 [COMMANDE]"
    echo ""
    echo "Commandes:"
    echo "   start     Démarre tous les services (défaut)"
    echo "   stop      Arrête tous les services"
    echo "   restart   Redémarre les services"
    echo "   status    Affiche le statut des services"
    echo "   logs [service] Affiche les logs d'un service"
    echo "   test      Test rapide du système"
    echo "   help      Affiche cette aide"
    echo ""
    echo "Exemples:"
    echo "   $0 start          # Démarre tous les services"
    echo "   $0 logs collector # Affiche les logs du collecteur"
    echo "   $0 test           # Test rapide"
}

# Test rapide
run_test() {
    print_info "Lancement du test rapide..."
    docker-compose run --rm music-weather-collector python src/main.py --test
}

# Gestion des commandes
case "${1:-start}" in
    start)
        check_requirements
        check_env
        start_services
        show_info
        ;;
    stop)
        stop_services
        ;;
    restart)
        stop_services
        sleep 2
        check_requirements
        check_env
        start_services
        show_info
        ;;
    status)
        status_services
        ;;
    logs)
        show_logs "${2:-}"
        ;;
    test)
        check_requirements
        check_env
        run_test
        ;;
    help|--help|-h)
        show_help
        ;;
    *)
        print_error "Commande inconnue: $1"
        echo ""
        show_help
        exit 1
        ;;
esac

echo ""