# Running

```bash
docker \
    run \
    --privileged \
    -e S3_BUCKET_NAME='<YOUR S3 BUCKET' \
    -e JDBC_URL='<YOUR JDBC URL>' \
    -e USER='splice' \
    -e PASSWORD='admin' \
    -e AWS_ACCESS_KEY_ID='' \
    -e AWS_SECRET_ACCESS_KEY='' \
    -e DASH_PORT='2375' \
    -p 2375:2375 \
    --name bobby \
    bobby
```
