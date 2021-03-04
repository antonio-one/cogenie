# Cogenie

A simple consumer generator

Generate consumers with:
```bash
python -m cogenie.entrypoints.generate
```


Execute a consumer for fun locally (you may have to set `--runner="DirectRunner"`).

```bash
python -m consumers.your_consumer \
  --project=your-project-id \
  --service-account-email=email@your-project-id.iam.gserviceaccount.com \
  --subscription=projects/your-project-id/subscriptions/your_subscription \
  --dataset=your_dataset_name \
  --table=your_table_name
```

[Setting other Cloud Dataflow pipeline options](https://cloud.google.com/dataflow/docs/guides/specifying-exec-params#setting-other-cloud-dataflow-pipeline-options)
