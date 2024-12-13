#!/bin/bash

# Check if a commit SHA is provided
if [ -z "$1" ]; then
    echo "Please provide a commit SHA"
    echo "Usage: $0 <commit-sha>"
    exit 1
fi

# Check if OPENAI_API_KEY is set
if [ -z "$OPENAI_API_KEY" ]; then
    echo "OPENAI_API_KEY environment variable is not set"
    exit 1
fi

COMMIT_SHA=$1

# Get the commit diff
DIFF=$(git show --pretty=format:"%s" --patch $COMMIT_SHA)

# Prepare the prompt - escape it properly for JSON
PROMPT=$(cat <<EOF
Given the following git diff, please generate a concise changelog entry that describes the changes made. Focus on the business impact and user-facing changes rather than technical details. Format the response as a single line, starting with a present-tense verb.

Git diff:
$DIFF
EOF
)

# Make the API call to OpenAI - properly escape the JSON payload
RESPONSE=$(curl -s https://api.openai.com/v1/chat/completions \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $OPENAI_API_KEY" \
  -d "$(jq -n \
    --arg prompt "$PROMPT" \
    '{
      model: "gpt-3.5-turbo",
      messages: [{role: "user", content: $prompt}],
      temperature: 0.7,
      max_tokens: 100
    }')")

# Extract the changelog entry from the response
CHANGELOG_ENTRY=$(echo $RESPONSE | jq -r '.choices[0].message.content' | tr -d '\n')

echo "Changelog entry:"
echo "- $CHANGELOG_ENTRY"