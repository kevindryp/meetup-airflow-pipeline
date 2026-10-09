from airflow.providers.slack.hooks.slack_webhook import SlackWebhookHook
from datetime import datetime, timezone

def format_duration(seconds: float) -> str:
    mins, secs = divmod(int(seconds), 60)
    hours, mins = divmod(mins, 60)
    if hours > 0:
        return f"{hours}h {mins}m {secs}s"
    elif mins > 0:
        return f"{mins}m {secs}s"
    return f"{secs}s"

def send_slack_notification(context, status: str, conn_id: str = 'slack_conn'):

    task_id = context.get('task_instance').task_id
    dag_id = context.get('task_instance').dag_id
    execution_date = context.get('data_interval_end').strftime('%Y-%m-%d %H:%M:%S')
    log_url = context.get('task_instance').log_url
    exception = context.get('exception')

    dag_run = context.get('dag_run')
    duration_str = "N/A"
    if dag_run and dag_run.start_date:
        now = datetime.now(timezone.utc)
        total_seconds = (now - dag_run.start_date).total_seconds()
        duration_str = format_duration(total_seconds)

    if status == 'START':
        color = '#3AA3E3'  # Azul
        title = f"🚀 INICIANDO PIPELINE: `{dag_id}`"
        message = (
            f"*DAG:* `{dag_id}`\n"
            f"*Primera tarea:* `{task_id}`\n"
            f"*Fecha/Hora ejecución:* {execution_date}"
        )

    elif status == 'SUCCESS_TASK':
        color = '#36a64f'  # Verde
        title = f"⚙️ Tarea Completada: `{task_id}`"
        message = (
            f"*DAG:* `{dag_id}`\n"
            f"*Tarea:* `{task_id}`\n"
            f"*Fecha/Hora:* {execution_date}"
        )

    elif status == 'SUCCESS_DAG':
        color = '#2eb886'  # Verde brillante
        title = f"🎉 PIPELINE COMPLETO Y EXITOSO: `{dag_id}`"
        message = (
            f"*DAG:* `{dag_id}`\n"
            f"*Duración Total:* `{duration_str}` ⏱️\n"
            f"*Estado:* Carga Medallion finalizada y exportada a S3.\n"
            f"*Fecha/Hora:* {execution_date}"
        )

    else:  # FAILURE
        color = '#ff0000'  # Rojo
        title = f"🚨 FALLO EN TAREA: `{task_id}`"
        message = (
            f"*DAG:* `{dag_id}`\n"
            f"*Tarea con error:* `{task_id}`\n"
            f"*Duración transcurrida:* `{duration_str}`\n"
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


def on_start_task_callback(context):
    send_slack_notification(context, status='START')
    
def on_failure_callback(context):
    send_slack_notification(context, status='FAILURE')

def on_success_task_callback(context):
    send_slack_notification(context, status='SUCCESS_TASK')

def on_success_dag_callback(context):
    send_slack_notification(context, status='SUCCESS_DAG')