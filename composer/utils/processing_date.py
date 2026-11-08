def get_processing_date(**context):
    dag_run = context.get("dag_run")

    if dag_run and dag_run.conf:
        processing_date = dag_run.conf.get("processing_date")
        if processing_date:
            return processing_date

    return context["ds"]
