from airflow.hooks.base import BaseHook
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models.baseoperator import BaseOperator


class MySqlToPostgreOperator(BaseOperator):
    
    
    def __init__(self,
                 sql_create_postgres=None,
                 sql_select_mysql=None,
                 target_table=None,
                 database_origem=None,
                 identifier=None,
                 mysql_conn_id='mysql_trial_sga2', 
                 postgres_conn_id='postgresql_trial_sga2',
                 *args,
                 **kwargs):
        
        super().__init__(*args, **kwargs)
        self.sql_select_mysql = sql_select_mysql
        self.sql_create_postgres = sql_create_postgres
        self.target_table = target_table
        self.database_origem=database_origem
        self.identifier = identifier
        self.mysql_conn_id = mysql_conn_id
        self.postgres_conn_id = postgres_conn_id

    def execute(self, context):
        
        start_date=context['data_interval_start'].strftime('%Y-%m-%d %H:%M:%S')
        end_date=context['data_interval_end'].strftime('%Y-%m-%d %H:%M:%S')
        
        self.sql_select_mysql = self.sql_select_mysql.format(start_date=start_date, end_date=end_date)
        print("sql !!!!!!!!!!!!!!!", self.sql_select_mysql)
        
        source = MySqlHook(self.mysql_conn_id, schema=self.database_origem)
        target = PostgresHook(self.postgres_conn_id)
        
        conn = source.get_conn()
        conn2 = target.get_conn()
        
        cursor = conn.cursor()
        cursor2 = conn2.cursor()
                
        
        cursor2.execute(self.sql_create_postgres)
        conn2.commit()
        cursor.execute(self.sql_select_mysql)
        
        target_fields = [x[0] for x in cursor.description]                        
        rows = cursor.fetchall()

        target.insert_rows(self.target_table,
                           rows,
                           target_fields=target_fields,
                           replace_index=self.identifier,
                           replace=True)


        cursor.close()
        #cursor2.close()
        conn.close()
        #conn2.close()
        
        
