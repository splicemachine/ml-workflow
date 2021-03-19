#!/usr/bin/env bash

echo "Setting up store data"
sqlshell.sh -f feature_store_data.sql
echo "Creating feature sets"
python create_feature_set.py
echo "Running backfill"
sqlshell.sh -f run_backfill.sql
echo "Populating feature sets"
sqlshell.sh -f populate_feature_set.sql
echo "Creating training views"
python create_training_view.py
echo "Creating training sets"
sqlshell.sh -f create_training_set.sql
