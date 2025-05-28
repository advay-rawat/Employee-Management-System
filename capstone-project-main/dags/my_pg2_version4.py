from airflow import DAG
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.operators.python import BranchPythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta, datetime
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule
# Default DAG arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Daily DAG for 07:00 UTC jobs
dag_daily = DAG(
    'employee_daily_glue_pipeline',
    default_args=default_args,
    description='Daily employee Glue jobs - 07:00 UTC',
    schedule_interval='0 7 * * *',
    start_date=days_ago(1),
    catchup=False
)


# Kafka Streaming Glue job (manual trigger or event based)
# dag_kafka = DAG(
#     'employee_kafka_stream_glue_pipeline',
#     default_args=default_args,
#     description='Kafka streaming strike management with Glue',
#     schedule_interval=None,
#     start_date=days_ago(1),
#     catchup=False
# )

#python check operator functions
def monthly_checker():
    today = datetime.utcnow()
    current_date = today.day 
    if current_date == 1:
        return 'processing_for_availed_leaves'
    else:
        return 'skip_monthly'
    

def yearly_checker():
    today =datetime.utcnow()
    current_date = today.day
    current_month = today.month

    if current_date ==1 and current_month ==1:
        return 'run_yearly_group'
    else:
        return 'skip_yearly'
    

# Daily Glue jobs
#-------On incoming data
t1_employeeData = GlueJobOperator(
    task_id = "processing_employee_data",
    job_name="poc-bootcamp-grp2-employee-data",
    wait_for_completion=True,
    aws_conn_id='aws_default',
    dag = dag_daily
)
t2_employeeTimeframeData = GlueJobOperator(
    task_id='processing_employee_timeframe_data',
    job_name = 'poc-bootcamp-grp2-employee-timeframe',
    wait_for_completion=True,
    aws_conn_id = 'aws_default',
    trigger_rule= TriggerRule.ALL_DONE ,
    dag = dag_daily
 
)

t3_employeeLeavesData = GlueJobOperator(
    task_id = 'processing_employee_leaves_data',
    job_name = 'poc-bootcamp-glue-job-gp2-employee_leave_data',
    wait_for_completion=True,
    aws_conn_id = 'aws_default',
    trigger_rule = TriggerRule.ONE_SUCCESS,
    dag = dag_daily
)

#---on timeframe data old and new
t4_employeeReportDeptWiseDaily = GlueJobOperator(
    task_id = 'processing_dept_wise',
    job_name = 'poc-bootcamp-grp2-deptWiseReport',
    wait_for_completion=True,
    aws_conn_id = 'aws_default',
    trigger_rule = TriggerRule.ALL_DONE,
    dag = dag_daily
)
#----on leaves data new and old
t5_employeeReportProspectiveLeavesDaily = GlueJobOperator(
    task_id = 'processing_for_8perOfFutureLeaves',
    job_name = 'poc-bootcamp-grp2-employee-leave-report',
    wait_for_completion=True,
    aws_conn_id = 'aws_default',
    trigger_rule =TriggerRule.ALL_DONE,
    dag = dag_daily
)

t9_timeframeToStrikes = GlueJobOperator(
    task_id ="loading_data_from_timeframe_to_Strikes",
    job_name='poc-bootcamp-grp2-timeframeToStrikes',
    wait_for_completion= True,
    aws_conn_id="aws_default",
    trigger_rule = TriggerRule.ALL_DONE,
    dag = dag_daily
)

#monthly job
#----on leaves data old
t6_employeeAvailedLeaves = GlueJobOperator(
    task_id = "processing_for_availed_leaves",
    job_name = "poc-bootcamp-glue-job-gp2-employee_reporting_80%",
    wait_for_completion=True,
    aws_conn_id = "aws_default",
    retries=3,
    retry_delay=timedelta(minutes=10),
    retry_exponential_backoff=True,
    max_retry_delay=timedelta(minutes=30),
    trigger_rule = TriggerRule.ONE_SUCCESS,
    dag = dag_daily
)

#yearly jobs
#-----on incoming leaves data
t7_employeeLeaveQuotaData = GlueJobOperator(
    task_id = 'processing_leave_quota_data',
    job_name = 'poc-bootcamp-glue-job-gp2-employee_leave_quota',
    wait_for_completion=True,
    aws_conn_id = 'aws_default',
    trigger_rule = TriggerRule.ALL_SUCCESS,
    dag = dag_daily
)

#----on incoming calendar data
t8_employeeLeaveCalendarData = GlueJobOperator(
    task_id = 'processing_the_calendar_data',
    job_name = 'poc-bootcamp-glue-job-gp2-employee_leave_calendar-',
    wait_for_completion= True,
    aws_conn_id = 'aws_default',
    trigger_rule = TriggerRule.ALL_SUCCESS,
    dag = dag_daily
)

#checker tasks
monthly_Check = BranchPythonOperator(
    task_id = 'Monthly_check',
    python_callable=monthly_checker,
    trigger_rule = TriggerRule.ALL_DONE,
    dag = dag_daily
)

yearly_Check = BranchPythonOperator(
    task_id = 'Yearly_check',
    python_callable = yearly_checker,
    dag =dag_daily
)

#empty operator for skipping the monthly and yearly tasks
skip_monthly = EmptyOperator(
    task_id='skip_monthly',
    trigger_rule = TriggerRule.ONE_SUCCESS,
    dag=dag_daily
)

skip_yearly = EmptyOperator(
    task_id='skip_yearly',
    dag=dag_daily
)
# Dummy group task to wrap multiple yearly jobs
run_yearly_group = EmptyOperator(
    task_id='run_yearly_group',
    trigger_rule= TriggerRule.ALL_SUCCESS,
    dag=dag_daily
)

yearly_data_check = EmptyOperator(
    task_id="yearly_data_process_check",
    trigger_rule= TriggerRule.ALL_SUCCESS,
    dag = dag_daily
)

# Basic ETL ordering:
[t1_employeeData >> t2_employeeTimeframeData ]

# Reporting based on cleaned data
t2_employeeTimeframeData >> [t4_employeeReportDeptWiseDaily,t9_timeframeToStrikes]
t3_employeeLeavesData >> t5_employeeReportProspectiveLeavesDaily

# Monthly reporting depends on historical leave data
t3_employeeLeavesData >> monthly_Check
monthly_Check >> t6_employeeAvailedLeaves
monthly_Check >> skip_monthly

# Yearly jobs depend on leave and calendar data ingestion
yearly_Check >> run_yearly_group
yearly_Check >>skip_yearly

run_yearly_group>>[ t7_employeeLeaveQuotaData,t8_employeeLeaveCalendarData] >>yearly_data_check>>t3_employeeLeavesData
skip_yearly >> t3_employeeLeavesData
