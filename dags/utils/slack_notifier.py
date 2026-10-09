from datetime import datetime, timezone
from airflow.providers.slack.hooks.slack_webhook import SlackWebhookHook
from airflow.utils import timezone as airflow_timezone

def format_duration(seconds: float) -> str:
    mins, secs = divmod(int(seconds), 60)
    hours, mins = divmod(mins, 60)
    if hours > 0:
        return f"{hours}h {mins}m {secs}s"
    elif mins > 0:
        return f"{mins}m {secs}s"
    return f"{secs}s"

def send_slack_notification(context, status: str, conn_id: str = 'slack_conn'):
    task_instance = context.get('task_instance')
    task_id = task_instance.task_id if task_instance else 'N/A'
    dag_id = task_instance.dag_id if task_instance else context.get('dag').dag_id
    
    data_interval = context.get('data_interval_end')
    execution_date = data_interval.strftime('%Y-%m-%d %H:%M:%S') if data_interval else 'N/A'
    
    log_url = task_instance.log_url if task_instance else ''
    exception = context.get('exception')

    dag_run = context.get('dag_run')
    duration_str = "N/A"
    if dag_run and dag_run.start_date:
        now = airflow_timezone.utcnow()
        start = dag_run.start_date
        if start.tzinfo is None:
            start = start.replace(tzinfo=timezone.utc)
        total_seconds = (now - start).total_seconds()
        duration_str = format_duration(total_seconds)

    if status == 'START':
        color = '#3AA3E3'
        title = f"🚀 INICIANDO PIPELINE: `{dag_id}`"
        message = (
            f"*DAG:* `{dag_id}`\n"
            f"*Primera tarea:* `{task_id}`\n"
            f"*Fecha/Hora ejecución:* {execution_date}"
        )

    elif status == 'SUCCESS_TASK':
        color = '#36a64f' 
        title = f"⚙️ Tarea Completada: `{task_id}`"
        message = (
            f"*DAG:* `{dag_id}`\n"
            f"*Tarea:* `{task_id}`\n"
            f"*Fecha/Hora:* {execution_date}"
        )

    elif status == 'SUCCESS_DAG':
        color = '#2eb886'
        title = f"🎉 PIPELINE COMPLETO Y EXITOSO: `{dag_id}`"
        message = (
            f"*DAG:* `{dag_id}`\n"
            f"*Duración Total:* `{duration_str}` ⏱️\n"
            f"*Estado:* Carga Medallion finalizada y exportada a S3.\n"
            f"*Fecha/Hora:* {execution_date}"
        )

    else:
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
                'text': message
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