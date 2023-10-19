echo "try to delete image with tag 'testing-$ARCH-$GITHUB_SHA'"

curl -L \
-X POST \
-H "Accept: application/vnd.github+json" \
-H "Authorization: Bearer $GH_PERSONAL_ACCESS_TOKEN" \
-H "X-GitHub-Api-Version: 2022-11-28" \
https://api.github.com/repos/timeplus-io/proton/actions/workflows/manual_trigger_delete_image.yml/dispatches \
-d "{\"ref\":\"develop\",\"inputs\":{\"tag\":\"testing-$ARCH-$GITHUB_SHA\"}}"
