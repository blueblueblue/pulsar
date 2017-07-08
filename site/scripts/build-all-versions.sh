#!/bin/bash

BUNDLER_VERSION=1.15.1

for version in $(cat VERSIONS); do
  bundle _${BUNDLER_VERSION}_ exec jekyll build \
    --config _config.yml,docs/$version/_config.version.yml
done
