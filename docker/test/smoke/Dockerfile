# PROTON_VERSION set to specified version, PROTON_REPO set to specified repo
# docker build . -f Dockerfile --build-arg FROM_TAG=$PROTON_VERSION --build-arg FROM_REPO=$PROTON_REPO -t timeplus/proton-smoke-test:$PROTON_VERSION

ARG FROM_TAG=latest
ARG FROM_REPO=timeplus/proton
FROM $FROM_REPO:$FROM_TAG

# Switch to root to install packages and setup environment.
USER 0:0

ARG USERNAME=default

RUN apt-get update \
 && env DEBIAN_FRONTEND=noninteractive \
    apt-get install --yes --no-install-recommends \
    sudo \
    openssh-server \
    vim \
    jq \
 && apt-get clean \
 && rm -rf /var/lib/apt/lists/*

RUN useradd -ms /bin/bash "$USERNAME" \
 && usermod -aG sudo "$USERNAME"

RUN sed -i'' 's|#PermitRootLogin prohibit-password|PermitRootLogin prohibit-password|g' /etc/ssh/sshd_config \
 && sed -i'' 's|#PubkeyAuthentication yes|PubkeyAuthentication yes|g' /etc/ssh/sshd_config \
 && sed -i'' 's|#PasswordAuthentication yes|PasswordAuthentication yes|g' /etc/ssh/sshd_config \
 # use authorized_keys2 to avoid conflict with authorized_keys on ec2 instance
 && echo 'AuthorizedKeysFile .ssh/authorized_keys2 /home/$USERNAME/.ssh/authorized_keys2' >> /etc/ssh/sshd_config \
 && sed -i '4a\sudo service ssh start' /entrypoint.sh \
 && echo 'timeplus ALL=(ALL) NOPASSWD: ALL' > /etc/sudoers.d/timeplus

EXPOSE 22

# Switch back to timeplus user
USER 101:101

# Aggressive cleanup for tests to catch more issues
COPY database_atomic.xml /etc/proton-server/config.d/
COPY database_catalog_drop_table_concurrency.xml /etc/proton-server/config.d/

ENTRYPOINT ["/entrypoint.sh"]
