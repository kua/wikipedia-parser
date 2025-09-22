#!/bin/bash
set -euo pipefail

CONFIG_DIR="${MW_CONFIG_PATH:-/mw-config}"
HTML_CONFIG="/var/www/html/LocalSettings.php"
VOLUME_CONFIG="${CONFIG_DIR}/LocalSettings.php"
DB_DIR="${MW_DB_DIRECTORY:-/var/www/data}"

mkdir -p "${CONFIG_DIR}" "${DB_DIR}"

if [ ! -f "${VOLUME_CONFIG}" ]; then
    echo "Installing MediaWiki for the first time..."
    php maintenance/install.php \
        --confpath "${CONFIG_DIR}" \
        --dbname "${MW_DB_NAME:-mediawiki}" \
        --dbpath "${DB_DIR}" \
        --dbtype "${MW_DB_TYPE:-sqlite}" \
        --installdbpass "${MW_ADMIN_PASS:-adminpass}" \
        --installdbuser "${MW_ADMIN_USER:-admin}" \
        --pass "${MW_ADMIN_PASS:-adminpass}" \
        --scriptpath "" \
        --server "${MW_SITE_SERVER:-http://localhost:8080}" \
        "${MW_SITE_NAME:-LocalWiki}" \
        "${MW_ADMIN_USER:-admin}"
    echo "MediaWiki installation completed."
fi

cp "${VOLUME_CONFIG}" "${HTML_CONFIG}"
chown www-data:www-data "${HTML_CONFIG}"
