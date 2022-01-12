#*/10 * * * * root ((which service > /dev/null 2>&1 && (service proton-server condstart ||:)) || /etc/init.d/proton-server condstart) > /dev/null 2>&1
