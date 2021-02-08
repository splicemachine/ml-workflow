## PyTest directory for Feature Store

To run:

```
docker-compose up -d --build feature_store 
docker-compose exec feature_store su -c "pytest /opt/feature_store -o log_cli=true" -- postgres
```
