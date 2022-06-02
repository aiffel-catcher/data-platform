from airflow.exceptions import AirflowException
from airflow.exceptions import AirflowSkipException


def task_to_fail(error):
    raise AirflowException("Error message >>>> ", error)


def task_to_skip():
    raise AirflowSkipException
