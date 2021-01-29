# Build

* Edit ../docker-compose.yml to the incremented image tag
```bash
../docker-compose build
```

# Publish

```bash
docker push <IMAGE> 
```

# Local

```bash
../docker-compose up
```
**Note:** Edit the Makefile to provide:
- `AWS_ACCESS_KEY_ID`
- `AWS_SECRET_ACCESS_KEY`
- `JDBC_URL`
- `FRAMEWORK_NAME`
- `S3_BUCKET_NAME`
