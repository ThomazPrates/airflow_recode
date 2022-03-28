from datetime import datetime, timedelta
import airflow
from airflow import DAG 
from custom import MySqlToPostgreOperator


dag = DAG(
    dag_id="Ambiente_BR",
    start_date=datetime.today() - timedelta(days=1),
    schedule_interval="0 */4 * * *",
    #schedule_interval="* * * * *",
    max_active_tasks=100
)

Usuario = MySqlToPostgreOperator(    
	task_id= f"sga2_Usuario",
	database_origem='sga2' ,
	sql_create_postgres="CREATE TABLE IF NOT EXISTS ambiente_br.Usuario (Id CHAR(36) PRIMARY KEY, Email VARCHAR(100),IdMoodle INTEGER, CreateAt TIMESTAMP, NomeCompleto VARCHAR(100), Cpf VARCHAR(20), DataNascimento TIMESTAMP, Perfil INTEGER, CedId CHAR(36), EnderecoId CHAR(36));", 
	sql_select_mysql="select Id,CreateAt,NomeCompleto,Email,Cpf,DataNascimento,Perfil,CedId,IdMoodle,EnderecoId from Usuario;",         
	target_table='ambiente_br.Usuario', 
	identifier='Id',
	dag=dag,)

Ced = MySqlToPostgreOperator(    
	    task_id= f"sga2_Ced",
	    database_origem='sga2' ,
	    sql_create_postgres="CREATE TABLE IF NOT EXISTS ambiente_br.Ced (Id CHAR(36) PRIMARY KEY, CreateAt TIMESTAMP, Cnpj VARCHAR(30), NomeInstituicao VARCHAR(100)) ;", 
	    sql_select_mysql="select Id,CreateAt,Cnpj,NomeInstituicao from Ced;",         
	    target_table='ambiente_br.Ced', 
	    identifier='Id',
	    dag=dag,
	    
	)

Endereco = MySqlToPostgreOperator(    
	    task_id= f"sga2_Endereco",
	    database_origem='sga2' ,
	    sql_create_postgres="CREATE TABLE IF NOT EXISTS ambiente_br.Endereco (Id CHAR(36) PRIMARY KEY, Estado VARCHAR(100), Cidade VARCHAR(100)) ;", 
	    sql_select_mysql="select Id,Estado,Cidade from Endereco;",         
	    target_table='ambiente_br.Endereco', 
	    identifier='Id',
	    dag=dag,
	    
	)

QuestionarioUsuario = MySqlToPostgreOperator(    
	    task_id= f"sga2_QuestionarioUsuario",
	    database_origem='sga2' ,
	    sql_create_postgres="CREATE TABLE IF NOT EXISTS ambiente_br.QuestionarioUsuario (Id CHAR(36) PRIMARY KEY,Resposta VARCHAR(200),QuestionarioId CHAR(36),UsuarioId CHAR(36)) ;", 
	    sql_select_mysql="select Id,Resposta,QuestionarioId,UsuarioId from QuestionarioUsuario;",         
	    target_table='ambiente_br.QuestionarioUsuario', 
	    identifier='Id',
	    dag=dag,
	    
	)

Questionario = MySqlToPostgreOperator(    
	    task_id= f"sga2_Questionario",
	    database_origem='sga2' ,
	    sql_create_postgres="CREATE TABLE IF NOT EXISTS ambiente_br.Questionario (Id CHAR(36) PRIMARY KEY,ValorQuestionario VARCHAR(200),Perfil INTEGER) ;", 
	    sql_select_mysql="select Id,ValorQuestionario,Perfil from Questionario;",         
	    target_table='ambiente_br.Questionario', 
	    identifier='Id',
	    dag=dag,
	    
	)

Resposta = MySqlToPostgreOperator(    
	    task_id= f"sga2_Resposta",
	    database_origem='sga2' ,
	    sql_create_postgres="CREATE TABLE IF NOT EXISTS ambiente_br.Resposta (Id CHAR(36) PRIMARY KEY,ValorResposta VARCHAR(100),QuestionarioId CHAR(36)) ;", 
	    sql_select_mysql="select Id,ValorResposta,QuestionarioId from Resposta;",         
	    target_table='ambiente_br.Resposta', 
	    identifier='Id',
	    dag=dag,
	    
	)
	

Mdl_Enrol = MySqlToPostgreOperator(    
	    task_id= f"rec_lms-new_mdl_enrol",
	    database_origem='rec_lms-new' ,
	    sql_create_postgres="CREATE TABLE IF NOT EXISTS ambiente_br.mdl_enrol (id BIGINT PRIMARY KEY,courseid BIGINT) ;", 
	    sql_select_mysql="select id,courseid from mdl_enrol;",         
	    target_table='ambiente_br.mdl_enrol', 
	    identifier='id',
	    dag=dag,
	    
	)

Mdl_User_Enrolments = MySqlToPostgreOperator(    
	    task_id= f"rec_lms-new_mdl_user_enrolments",
	    database_origem='rec_lms-new' ,
	    sql_create_postgres="CREATE TABLE IF NOT EXISTS ambiente_br.mdl_user_enrolments (id BIGINT PRIMARY KEY,userid BIGINT,enrolid BIGINT, timecreated BIGINT) ;", 
	    sql_select_mysql="select id,userid,enrolid,timecreated from mdl_user_enrolments;",         
	    target_table='ambiente_br.mdl_user_enrolments', 
	    identifier='id',
	    dag=dag,
	    
	)

Mdl_Course_Modules = MySqlToPostgreOperator(    
	    task_id= f"rec_lms-new_mdl_course_modules",
	    database_origem='rec_lms-new' ,
	    sql_create_postgres="CREATE TABLE IF NOT EXISTS ambiente_br.mdl_course_modules (id BIGINT PRIMARY KEY,module BIGINT, course BIGINT,section BIGINT) ;", 
	    sql_select_mysql="select id,module,course,section from mdl_course_modules;",         
	    target_table='ambiente_br.mdl_course_modules', 
	    identifier='id',
	    dag=dag,
	    
	)

Mdl_Course_Modules_Completion = MySqlToPostgreOperator(    
	    task_id= f"rec_lms-new_mdl_course_modules_completion",
	    database_origem='rec_lms-new' ,
	    sql_create_postgres="CREATE TABLE IF NOT EXISTS ambiente_br.mdl_course_modules_completion (id BIGINT PRIMARY KEY,coursemoduleid BIGINT,userid BIGINT, completionstate SMALLINT, timemodified BIGINT) ;", 
	    sql_select_mysql="select id,coursemoduleid,userid,completionstate,timemodified from mdl_course_modules_completion;",         
	    target_table='ambiente_br.mdl_course_modules_completion', 
	    identifier='id',
	    dag=dag,
	    
	)

Mdl_Course_Categories = MySqlToPostgreOperator(    
	    task_id= f"rec_lms-new_mdl_course_categories",
	    database_origem='rec_lms-new' ,
	    sql_create_postgres="CREATE TABLE IF NOT EXISTS ambiente_br.mdl_course_categories (id BIGINT PRIMARY KEY,name VARCHAR(255)) ;", 
	    sql_select_mysql="select id,name from mdl_course_categories;",         
	    target_table='ambiente_br.mdl_course_categories', 
	    identifier='id',
	    dag=dag,
	    
	)

Mdl_Course = MySqlToPostgreOperator(    
	    task_id= f"rec_lms-new_mdl_course",
	    database_origem='rec_lms-new' ,
	    sql_create_postgres="CREATE TABLE IF NOT EXISTS ambiente_br.mdl_course (id BIGINT PRIMARY KEY,category BIGINT,fullname VARCHAR(254),shortname VARCHAR(255),timecreated BIGINT) ;", 
	    sql_select_mysql="select id,category,fullname,shortname,timecreated from mdl_course;",         
	    target_table='ambiente_br.mdl_course', 
	    identifier='id',
	    dag=dag,
	    
	)

Mdl_Course_Sections = MySqlToPostgreOperator(    
	    task_id= f"rec_lms-new_mdl_sections",
	    database_origem='rec_lms-new' ,
	    sql_create_postgres="CREATE TABLE IF NOT EXISTS ambiente_br.mdl_course_sections (id BIGINT PRIMARY KEY,course BIGINT, name VARCHAR(255)) ;", 
	    sql_select_mysql="select id,course,name from mdl_course_sections;",         
	    target_table='ambiente_br.mdl_course_sections', 
	    identifier='id',
	    dag=dag,
	    
	)
Usuario >> QuestionarioUsuario >> [Questionario, Endereco, Ced, Mdl_Enrol,Resposta, Mdl_User_Enrolments, Mdl_Course_Modules, Mdl_Course_Modules_Completion, Mdl_Course_Categories,Mdl_Course, Mdl_Course_Sections]
