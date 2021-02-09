## PyTest directory for Feature Store

To run with docker:

```
docker-compose up -d --build feature_store 
docker-compose exec feature_store su -c "pytest /opt/feature_store -o log_cli=true -p no:cacheprovider" -- postgres
```

To test a particular file, run
```
docker-compose up -d --build feature_store 
docker-compose exec feature_store su -c "pytest /opt/feature_store/tests/<test_file_name>.py -o log_cli=true -p no:cacheprovider" -- postgres
```
