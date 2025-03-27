#!/bin/bash

USER="$PAM_USER"
API_URL="http://127.0.0.1:5001/api/check_access/$USER"

# Users to restrict
RESTRICTED_USERS=("alice" "bob" "carol")

# Only enforce for restricted users
if printf '%s\n' "${RESTRICTED_USERS[@]}" | grep -qx "$USER"; then
  RESPONSE=$(curl -s "$API_URL" | grep -o '"access":true')
  if [[ "$RESPONSE" == '"access":true' ]]; then
    exit 0
  else
    logger "SSH access denied for $USER – no active reservation"
    exit 1
  fi
else
  exit 0  # Non-restricted user, allow
fi
