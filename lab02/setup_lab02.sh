#!/usr/bin/env bash
# Link ~/bda/lab02 (the path the manual uses) to ~/Big_Data/lab02 (the repo).
# Safe to run more than once. If ~/bda/lab02 already exists as a real folder,
# its files are copied into the repo WITHOUT overwriting anything, and the old
# folder is kept as ~/bda/lab02.before_link (nothing is deleted).
set -e
REPO=~/Big_Data/lab02
mkdir -p "$REPO/src" "$REPO/runs" "$REPO/out" "$REPO/figures" ~/bda

if [ -L ~/bda/lab02 ]; then
  echo "already a link: $(readlink -f ~/bda/lab02)"
elif [ -d ~/bda/lab02 ]; then
  cp -rn ~/bda/lab02/. "$REPO/"
  mv ~/bda/lab02 ~/bda/lab02.before_link
  ln -s "$REPO" ~/bda/lab02
  echo "copied old files (no overwrite), kept ~/bda/lab02.before_link, linked ~/bda/lab02"
else
  ln -s "$REPO" ~/bda/lab02
  echo "linked ~/bda/lab02 -> $REPO"
fi
ls -la ~/bda/lab02/ ~/bda/lab02/src/
