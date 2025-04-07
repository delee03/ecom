def _handle_failed_dagrun(context):
    print("LOGGIN EROR")
    print(f"Dag run failed with the task '{context['task_instance'].task_id}' for the data interval between {context['data_interval_start']} or {context['prev_ds']} and {context['next_ds']}")

def _handle_check_size(context):
    print("LOGGIN EROR")
    print(f"There is no cocktail to process for the data interval between {context['prev_ds']} and {context['next_ds']} ")