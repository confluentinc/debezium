#!/bin/bash
export CHERRY_PICK_BASE_BRANCH=worktree-run_manual
export CHERRY_PICK_BASE_REMOTE=origin
export CHERRY_PICK_UPSTREAM_URL=https://github.com/confluentinc/debezium
export CHERRY_PICK_UPSTREAM_REMOTE=origin
export CHERRY_PICK_CONFIG_REF=HEAD
export CHERRY_PICK_GH_REPO=confluentinc/debezium
export CHERRY_PICK_DRY_RUN=true
# Upstream tag the target branch is based on (for skip detection)
export TARGET_UPSTREAM_TAG=v3.3.2.Final
