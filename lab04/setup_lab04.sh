#!/usr/bin/env bash
# Link ~/bda/lab04 (the path the manual uses) to ~/Big_Data/lab04 (the repo).
# Replaces manual section 2.4's "cp /mnt/c/lab-files/lab04/*.py": the scripts are already in src/
# with /user/thaianh/ changed to /user/tdat1/.
# Safe to run more than once. If ~/bda/lab04 already exists as a real folder, its files are copied
# into the repo WITHOUT overwriting anything, and the old folder is kept as ~/bda/lab04.before_link.
set -e
REPO=~/Big_Data/lab04
mkdir -p "$REPO/src" "$REPO/runs" "$REPO/out" "$REPO/figures" ~/bda
if [ -L ~/bda/lab04 ]; then
  echo "already a link: $(readlink -f ~/bda/lab04)"
elif [ -d ~/bda/lab04 ]; then
  cp -rn ~/bda/lab04/. "$REPO/"
  mv ~/bda/lab04 ~/bda/lab04.before_link
  ln -s "$REPO" ~/bda/lab04
  echo "copied old files (no overwrite), kept ~/bda/lab04.before_link, linked ~/bda/lab04"
else
  ln -s "$REPO" ~/bda/lab04
  echo "linked ~/bda/lab04 -> $REPO"
fi
ls -la ~/bda/lab04/ ~/bda/lab04/src/
