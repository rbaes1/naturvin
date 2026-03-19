#!/bin/bash
# Run this once to create and push the GitHub repo
# Usage: bash setup.sh your-github-username

USERNAME=${1:-"your-username"}
REPO="naturvin"

echo "Setting up $USERNAME/$REPO..."

cd "$(dirname "$0")"

git init
git add .
git commit -m "Initial commit"
git branch -M main
git remote add origin "https://github.com/$USERNAME/$REPO.git"

echo ""
echo "Now:"
echo "  1. Create the repo at https://github.com/new (name: $REPO, public)"
echo "  2. Add secrets at https://github.com/$USERNAME/$REPO/settings/secrets/actions:"
echo "       ANTHROPIC_API_KEY"
echo "       SYSTEMBOLAGET_API_KEY"
echo "  3. Run: git push -u origin main"
echo "  4. Enable Pages: Settings → Pages → main branch → /docs folder"
echo "  5. Trigger first run: Actions → Update natural wine list → Run workflow"
