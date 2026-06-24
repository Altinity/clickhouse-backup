#!/bin/bash

echo ">>> Applying ARM64 Fixes..."

# 1. DISABLE PROBLEMATIC MODULES (Mod Shaper causes segfaults on M1/OrbStack)
# Comment out LoadModule lines in the modules config
sed -i 's/^LoadModule mod_shaper.c/#LoadModule mod_shaper.c/' /etc/proftpd/modules.conf
sed -i 's/^LoadModule mod_exec.c/#LoadModule mod_exec.c/' /etc/proftpd/modules.conf
sed -i 's/^LoadModule mod_sftp_pam.c/#LoadModule mod_sftp_pam.c/' /etc/proftpd/modules.conf
sed -i 's/^LoadModule mod_wrap.c/#LoadModule mod_wrap.c/' /etc/proftpd/modules.conf

# 2. CREATE USER (Bypass system useradd/shadow/PAM)
if [ -n "$FTP_USER_NAME" -a -n "$FTP_USER_PASS" ]; then
    echo ">>> Creating user $FTP_USER_NAME in virtual file..."

    # Create home directory
    mkdir -p /home/ftpusers/$FTP_USER_NAME
    chown 1000:1000 /home/ftpusers/$FTP_USER_NAME

    # Generate password file (using DES crypt, UID 1000, GID 1000)
    perl -e "print '$FTP_USER_NAME:' . crypt('$FTP_USER_PASS', 'aa') . ':1000:1000:FTP User:/home/ftpusers/$FTP_USER_NAME:/bin/sh\n'" > /etc/proftpd/ftpd.passwd

    # Fix permissions (ProFTPD is paranoid about this)
    chown root:root /etc/proftpd/ftpd.passwd
    chmod 440 /etc/proftpd/ftpd.passwd
fi

# 3. GENERATE CONFIG
CONF_FILE="/etc/proftpd/conf.d/custom.conf"
echo ">>> Generating config..."

cat <<EOF > $CONF_FILE
# --- M1/OrbStack Fixes ---
AuthPAM off
AuthOrder mod_auth_file.c
AuthUserFile /etc/proftpd/ftpd.passwd
RequireValidShell off
WtmpLog off
UseLastlog off
IdentLookups off
# -------------------------

DefaultRoot ~
HiddenStores on
MaxInstances ${FTP_MAX_CONNECTIONS:-20}
EOF

# Handle passive ports
if [ -n "$FTP_PASSIVE_PORTS" ]; then
    # Replace colon with space if necessary
    PORTS=$(echo $FTP_PASSIVE_PORTS | sed "s/:/ /g")
    echo "PassivePorts $PORTS" >> $CONF_FILE
fi

# Handle MasqueradeAddress
if [[ "${FTP_MASQUERADEADDRESS,,}" == "yes" ]]; then
    # Simple way to get container IP
    MY_IP=$(hostname -i)
    echo "MasqueradeAddress $MY_IP" >> $CONF_FILE
elif [[ -n "$FTP_MASQUERADEADDRESS" && "${FTP_MASQUERADEADDRESS,,}" != "no" ]]; then
    echo "MasqueradeAddress $FTP_MASQUERADEADDRESS" >> $CONF_FILE
fi

# 4. START
echo ">>> Starting ProFTPD..."
# Run in foreground (-n)
exec /usr/sbin/proftpd -n -c /etc/proftpd/proftpd.conf