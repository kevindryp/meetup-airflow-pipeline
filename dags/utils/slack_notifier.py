from airflow.providers.slack.hooks.slack_webhook import SlackWebhookHook

def send_slack_notification(context, status: str, conn_id: str = 'slack_conn'):

    task_id = context.get('task_instance').task_id
    dag_id = context.get('task_instance').dag_id
    execution_date = context.get('data_interval_end').strftime('%Y-%m-%d %H:%M:%S')
    log_url = context.get('task_instance').log_url
    exception = context.get('exception')

    if status == 'SUCCESS_TASK':
        color = "#47a636"
        title = f"⚙️ Tarea Completada: `{task_id}`"
        message = (
            f"*DAG:* `{dag_id}`\n"
            f"*Tarea:* `{task_id}`\n"
            f"*Fecha/Hora:* {execution_date}"
        )

    elif status == 'SUCCESS_DAG':
        color = "#003723"
        title = f"🎉 PIPELINE COMPLETO Y EXITOSO: `{dag_id}`"
        message = (
            f"*DAG:* `{dag_id}`\n"
            f"*Estado:* El Dag ha finalizado con exito.\n"
            f"*Fecha/Hora ventana:* {execution_date}"
        )

    else:  # FAILURE
        color = '#ff0000'  # Rojo
        title = f"🚨 FALLO EN TAREA: `{task_id}`"
        message = (
            f"*DAG:* `{dag_id}`\n"
            f"*Tarea con error:* `{task_id}`\n"
            f"*Fecha/Hora:* {execution_date}\n"
            f"*Error:* `{exception}`\n"
            f"*Logs:* <{log_url}|Ver Logs en Airflow>"
        )

    slack_msg = {
        'attachments': [
            {
                'color': color,
                'title': title,
                'text': message,
                'ts': context.get('ts')
            }
        ]
    }

    slack_hook = SlackWebhookHook(slack_webhook_conn_id=conn_id)
    slack_hook.send(attachments=slack_msg['attachments'])


# Callbacks exported
def on_failure_callback(context):
    send_slack_notification(context, status='FAILURE')

def on_success_task_callback(context):
    send_slack_notification(context, status='SUCCESS_TASK')

def on_success_dag_callback(context):
    send_slack_notification(context, status='SUCCESS_DAG')