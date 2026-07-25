from airflow.utils.email import send_email

def notify_email_on_failure(context):
    ti = context.get('task_instance')
    subject = f"Airflow Alert: {ti.dag_id}.{ti.task_id} FAILED"
    
    body = f"""
    Task Failed.<br>
    <b>DAG:</b> {ti.dag_id}<br>
    <b>Task:</b> {ti.task_id}<br>
    <b>Execution Date:</b> {context.get('ds')}<br>
    <b>Log URL:</b> <a href="{ti.log_url}">Click here to view logs</a><br>
    <br>
    <b>Error:</b><br>
    <pre>{str(context.get('exception'))}</pre>
    """
    
    send_email(
        to=["your-team@yourcompany.com"],
        subject=subject,
        html_content=body
    )