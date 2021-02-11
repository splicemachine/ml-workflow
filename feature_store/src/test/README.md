## PyTest directory for Feature Store

To run with docker:

```
docker-compose up -d feature_store 
docker-compose exec feature_store su -c "pytest /opt/feature_store -o log_cli=true -p no:cacheprovider" -- postgres
```

To test a particular file, run
```
docker-compose up -d feature_store 
docker-compose exec feature_store su -c "pytest /opt/feature_store/test/tests/<test_file_name>.py -o log_cli=true -p no:cacheprovider" -- postgres
```

To run from a fresh build, modify the first line to
```
docker-compose up -d --build feature_store 
```
