import requests
import os
import logging
from datetime import datetime, timedelta, timezone

# Setup basic logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

DOCKERHUB_USERNAME = "timeplus"
DOCKERHUB_PASSWORD = os.getenv("DOCKERHUB_TOKEN")
TAG_TO_DELETE = os.getenv("TAG_TO_DELETE")
REPO_TO_DELETE = os.getenv("REPO_TO_DELETE")
DOCKER_REPOSITORY = ["timeplus/proton_testing", "timeplus/proton"]
PREFIX = ["testing", "manual"]
DEV_PREFIX = ["develop-"]
AGE_LIMIT_HOURS = 72
AGE_LIMIT_DAYS = 90

# NOT using PAT to delete a tag from Docker Hub: See https://github.com/docker/hub-feedback/issues/2398
def fetch_token(username, password):
    """Fetch the JWT token from Docker Hub using the provided username and password."""
    url = "https://hub.docker.com/v2/users/login/"
    headers = {"Content-Type": "application/json"}
    payload = {"username": username, "password": password}
    
    try:
        response = requests.post(url, json=payload, headers=headers)
        response.raise_for_status()
        return response.json().get('token')
    except requests.RequestException as e:
        logging.error(f"Failed to retrieve token: {e}")
        return None

def delete_image_tag(repository, tag, token):
    """Delete a specific image tag from Docker Hub."""
    url = f"https://hub.docker.com/v2/repositories/{repository}/tags/{tag}/"
    headers = {"Authorization": f"JWT {token}"}
    try:
        response = requests.delete(url, headers=headers)
        if response.status_code == 204:
            logging.info(f"Deleted tag '{tag}' from '{repository}' successfully")
        elif response.status_code == 404:
            logging.info(f"Image '{repository}':'{tag}' doesn't exist.")
        else:
            logging.error(f"Failed to delete tag '{tag}', status code: {response.status_code}, response: {response.text}")
    except requests.RequestException as e:
        logging.error(f"Request failed: {e}")

from dateutil import parser

def delete_based_on_condition(repository, token, prefix, age_limit_hours=0, age_limit_days=0):
    """Delete tags based on a prefix and age condition."""
    page_size = 100
    next_url = f"https://hub.docker.com/v2/repositories/{repository}/tags/?page_size={page_size}"
    headers = {"Authorization": f"JWT {token}"}

    try:
        while next_url:
            response = requests.get(next_url, headers=headers)
            if response.status_code == 404:
                logging.error("Received a 404 error, no more pages to fetch or the page does not exist.")
                break
            response.raise_for_status()

            data = response.json()
            tags_data = data.get("results", [])
            next_url = data.get('next')  # Continue with next page if available

            cutoff_time = datetime.now(timezone.utc) - timedelta(days=age_limit_days, hours=age_limit_hours)
            for tag_data in tags_data:
                tag_name = tag_data["name"]
                tag_created_at = parser.parse(tag_data["last_updated"])
                if any(tag_name.startswith(pre) for pre in prefix) and tag_created_at < cutoff_time:
                    delete_image_tag(repository, tag_name, token)
    except requests.RequestException as e:
        logging.error(f"Error fetching tags: {e}")

def main():
    """Main execution function."""
    token = fetch_token(DOCKERHUB_USERNAME, DOCKERHUB_PASSWORD)
    if not token:
        logging.error("Authentication failed, token not retrieved.")
        return

    if TAG_TO_DELETE and REPO_TO_DELETE:
        logging.info(f"Attempting to delete tag '{TAG_TO_DELETE}'")
        delete_image_tag(REPO_TO_DELETE, TAG_TO_DELETE, token)
    else:
        logging.info("No specific tag provided, checking for tags based on conditions")
        [delete_based_on_condition(repo, token, PREFIX, age_limit_hours=AGE_LIMIT_HOURS) for repo in DOCKER_REPOSITORY]
        [delete_based_on_condition(repo, token, DEV_PREFIX, age_limit_days=AGE_LIMIT_DAYS) for repo in DOCKER_REPOSITORY]

if __name__ == "__main__":
    main()
