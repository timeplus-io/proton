#!/bin/bash

DEFAULT_SSH_USER=$(whoami)
DEFAULT_SSH_DIR=$HOME/.ssh
DEFAULT_TEST_DIR="."

SSH_USER=${SSH_USER:-$DEFAULT_SSH_USER}
SSH_DIR=${SSH_DIR:-$DEFAULT_SSH_DIR}
TEST_DIR=${TEST_DIR:-$DEFAULT_TEST_DIR}

SSH_SMOKE_DIR=$SSH_DIR/smoke  # support ssh between probe and proton, root permission
SSH_BACKUP_DIR=$SSH_DIR/temp  # store host ssh files if exist to avoid conflict
SSH_FILES=("id_rsa" "id_rsa.pub" "authorized_keys2")
SCRIPT_NAME=$(basename "$0")

function echo_main() {
    echo "[$SCRIPT_NAME] $1"
}

function backup_home_ssh() {
    echo_main "Backup Home SSH key files (SSH User: $SSH_USER, SSH Dir: $SSH_DIR)"
    rm -rf $SSH_BACKUP_DIR
    mkdir -p $SSH_BACKUP_DIR

    for file in "${SSH_FILES[@]}"; do
        src="$SSH_DIR/$file"
        if [ -e "$src" ]; then
            mv "$src" "$SSH_BACKUP_DIR"
            echo "backup: move $src to $SSH_BACKUP_DIR"
        else
            echo "file $src not exist, skip"
        fi
    done

    sudo chown -R $SSH_USER:$SSH_USER $SSH_BACKUP_DIR
}

function revert_home_ssh() {
    if [ -d $SSH_BACKUP_DIR ] && [ "$(ls -A $SSH_BACKUP_DIR)" ]; then
        echo_main "Revert Home SSH key files (SSH User: $SSH_USER, SSH Dir: $SSH_DIR)"
        mv $SSH_BACKUP_DIR/* $SSH_DIR
        rm -rf $SSH_BACKUP_DIR
    fi 
}

function replace_ssh_config() {
    files=(
        "p3k1.yaml"
        "p3m1.yaml"
        "p1k1.yaml"
        "p1m1.yaml"
    )
    for file in "${files[@]}"; do
        echo_main "Replace SSH_USER in cluster config (tests/cluster/deployment/smoke/$file)"
        sed -i "s|SSH_USER|${SSH_USER}|g" ${TEST_DIR}/deployment/smoke/$file
    done
}

function revert_ssh_config() {
    files=(
        "p3k1.yaml"
        "p3m1.yaml"
        "p1k1.yaml"
        "p1m1.yaml"
    )
    for file in "${files[@]}"; do
        echo_main "Revert SSH_USER in cluster config (tests/cluster/deployment/smoke/$file)"
        sed -i "s|${SSH_USER}|SSH_USER|g" ${TEST_DIR}/deployment/smoke/$file
    done
}

function add_user_docker_group() {
    echo_main "Append user ($SSH_USER) to docker group"
    sudo usermod -aG docker $SSH_USER
    echo $(getent group docker)
}

function generate_ssh() {
    echo_main "Generate SSH key files (SSH User: $SSH_USER, SSH Dir: $SSH_DIR)"

    # backup home ssh keys
    if [ "$CI" == "true" ]; then
        backup_home_ssh
    fi
    replace_ssh_config
    
    # generate home ssh keys
    if [[ -f "$SSH_DIR/id_rsa.pub" && -f "$SSH_DIR/id_rsa" ]]; then
        echo_main "SSH keys already exist, skip new generation"
    else
        echo_main "SSH keys not found, generating new keys ($SSH_DIR/id_rsa)"
        ssh-keygen -t rsa -b 4096 -f $SSH_DIR/id_rsa -N "" -q
    fi
    cat $SSH_DIR/id_rsa.pub > $SSH_DIR/authorized_keys2
    chmod 600 $SSH_DIR/id_rsa
    chmod 600 $SSH_DIR/authorized_keys2
    for file in "${SSH_FILES[@]}"; do
        sudo chown -R $SSH_USER:$SSH_USER $SSH_DIR/$file
    done

    # generate smoke ssh keys
    sudo rm -rf $SSH_SMOKE_DIR
    mkdir -p -m 0700 $SSH_SMOKE_DIR
    for file in "${SSH_FILES[@]}"; do
        cp $SSH_DIR/$file $SSH_SMOKE_DIR
    done
    sudo chown -R root:root $SSH_SMOKE_DIR
    add_user_docker_group
}

function clean_ssh() {
    echo_main "Clean SSH key files (SSH User: $SSH_USER, SSH Dir: $SSH_DIR)"
    sudo rm -rf $SSH_SMOKE_DIR
    if [ "$CI" == "true" ]; then
        revert_home_ssh
    fi
    revert_ssh_config
}

if [[ $# -lt 1 ]]; then
  echo_main "Usage: $0 <init|clean>"
  exit 1
fi

ACTION=$1

case "$ACTION" in
    init)
        generate_ssh
        ;;
    clean)
        clean_ssh
        ;;
    *)
    echo_main "Invalid parameter: $ACTION"
    echo_main "Usage: $0 <init|clean>"
    exit 1
    ;;
esac
