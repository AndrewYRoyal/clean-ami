#!/bin/sh

echo "Filter? (enter none if none)"
read filter

Rscript clean.R --utility=electric --filter=$(filter)
Rscript clean.R --utility=gas --filter=$(filter)

echo "Complete"
read complete
