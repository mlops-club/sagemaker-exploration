# OpenLineage

I followed the marquez tutorial [here](https://openlineage.io/docs/guides/airflow-quickstart/#get-marquez).

To start up a marquez server, do

```bash
cd ./marquez
./docker/up.sh --db-port 12345 --api-port 9000
```

Then go to http://localhost:3000 to see the UI

![](../openlineage-playground/docs/marquez-ui.png)