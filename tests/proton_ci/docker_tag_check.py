import sys
import logging
import requests

from delete_docker_tags import fetch_token
from env_helper import (
    PROTON_VERSION,
    PROTON_REPO,
    DOCKERHUB_USERNAME,
    DOCKERHUB_TOKEN,
)


def get_image_tag(repository, tag, token):
    """Get image tag from Docker Hub."""
    url = f"https://hub.docker.com/v2/repositories/{repository}/tags/{tag}"
    headers = {"Authorization": f"Bearer {token}"}
    try:
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            tag_name = response.json()['name']
            return tag_name
        else:
            return None
    except requests.RequestException as e:
        logging.error(f"Request failed: {e}")
        return None


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    if PROTON_VERSION is None or PROTON_REPO is None:
        logging.error("PROTON_VERSION or PROTON_REPO is None, could not find proton image")
        sys.exit(1)
        
    token = fetch_token(DOCKERHUB_USERNAME, DOCKERHUB_TOKEN)
    if not token:
        logging.error("Authentication failed, token not retrieved.")
        sys.exit(1)

    if get_image_tag(PROTON_REPO, PROTON_VERSION, token):
        logging.info(f"Image '{PROTON_REPO}':'{PROTON_VERSION}' already exist.")
        sys.exit(1)

    logging.info(f"Image '{PROTON_REPO}':'{PROTON_VERSION}' not exist.")
