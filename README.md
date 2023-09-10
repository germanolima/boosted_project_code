# boosted_project_code
Pipeline to ingest data from CC News, store it into a Delta Lake and make it accessible via webquery. Coded in 3 hours.

This project needs Java to be installed in the linux environment to work.

## running the api (Linux):
After installing requirements , enter folder  api and run command:

```console
uvicorn main:app --reload
```

When the server starts, you can webquery the delta tables in localhost:8000 passing arguments date and table_name in the url.

Exaple: localhost:8000/query?table_name=CC-NEWS&date=2022-01-02