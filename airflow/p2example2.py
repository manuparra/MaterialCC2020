from datetime import timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(2),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
    # 'wait_for_downstream': False,
    # 'dag': dag,
    # 'sla': timedelta(hours=2),
    # 'execution_timeout': timedelta(seconds=300),
    # 'on_failure_callback': some_function,
    # 'on_success_callback': some_other_function,
    # 'on_retry_callback': another_function,
    # 'sla_miss_callback': yet_another_function,
    # 'trigger_rule': 'all_success'
}

#Inicialización del grafo DAG de tareas para el flujo de trabajo
dag = DAG(
    'practica2_ejemplo_Mapas',
    default_args=default_args,
    description='Varias tareas mas elaboradas,
    schedule_interval=timedelta(days=1),
)

# Operadores o tareas
# t1, t2 t3 y t4 son ejemplos de tareas que son operadores bash, es decir comandos
DescargaDatos1 = BashOperator(
    task_id='descarga1',
    bash_command='curl -o /tmp/parte1.csv https://data.cityofnewyork.us/Transportation/2017-Yellow-Taxi-Trip-Data/biws-g3hs',
    dag=dag,
)


DescargaDatos2 = BashOperator(
    task_id='descarga2',
    bash_command='curl -o /tmp/parte2.csv https://data.cityofnewyork.us/Transportation/2017-Yellow-Taxi-Trip-Data/biws-g1hs',
    dag=dag,
)

MoverDataStore = BashOperator(
    task_id='mover_aDataStore',
    bash_command='cat /tmp/parte1.csv <(tail +2 /tmp/parte2.csv) > /tmp/DataStore/output.csv',
    dag=dag,
)

def generaMapa():
NY_COORDINATES = (40, -73)
gdata = pd.read_csv('green_tripdata.csv')
MAX_RECORDS = 1048576
map_nyctaxi = folium.Map(location=NY_COORDINATES, zoom_start=9)
marker_cluster = folium.MarkerCluster().add_to(map_nyctaxi)
for each in gdata[0:MAX_RECORDS].iterrows():
   folium.Marker(
 location = [each[1]['Pickup_latitude'],each[1]['Pickup_longitude']],      
         popup='picked here').add_to(marker_cluster)
map_nyctaxi

CreaVisualizacion = PythonOperator(
    task_id='extrae_mapa',
    provide_context=True,
    python_callable=generaMapa,
    dag=dag,
)


#Dependencias - Construcción del grafo DAG
[DescargaDatos1, DescargarDatos2] >> MoverDataStore >> CrearVisualizacion
