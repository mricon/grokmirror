#!/bin/bash
# This is a command to install in gitolite's local-code.
# Don't forget to enable it via .gitolite.rc
#
# Change this to where grok-manifest is writing manifest.js
MANIFILE="/var/www/html/grokmirror/manifest.js.gz"

if [[ -z "$GL_USER" ]]; then
    echo "ERROR: GL_USER is unset. Run me via ssh, please."
    exit 1
fi

# Make sure we only accept credential replication from the mirrors
for MIRROR in $(GL_USER='' gitolite mirror list slaves gitolite-admin); do
    if [[ $GL_USER == "server-${MIRROR}" ]]; then
        AOK="yes"
        break
    fi
done

if [[ -z "$AOK" ]]; then
    echo "You are not allowed to do this"
    exit 1
fi

if [[ ! -s $MANIFILE ]]; then
    echo "Manifest file not found"
    exit 1
fi

R_LASTMOD=$1
if [[ -z "$R_LASTMOD" ]]; then
    R_LASTMOD=0
fi

L_LASTMOD=$(stat --printf='%Y' $MANIFILE)
if [[ $L_LASTMOD -le $R_LASTMOD ]]; then
    exit 127
fi

if [[ $MANIFILE == *.gz ]]; then
    zcat $MANIFILE
else
    cat $MANIFILE
fi

exit 0
