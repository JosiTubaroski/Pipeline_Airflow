
from airflow import DAG
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.providers.microsoft.mssql.operators.mssql import MsSqlOperator
from airflow.operators.python import PythonOperator
from datetime import datetime
from airflow.utils.dates import days_ago
import pandas as pd

# Define os parâmetros da DAG
default_args = {
    'owner': 'Josi',
    'start_date': days_ago(1),  # ajuste a data de início
    'retries': 1,
}

# Função para ler dados do SQL Server

def get_filename_from_db():
    mssql_hook = MsSqlHook(mssql_conn_id='sqlserver')
    sql = "select (select  rtrim(convert(varchar(255),vl_campo)) as nm_arquivo from dbo.tsv_server_status where cd_campo = 27)+ (select convert(varchar, dt_campo, 112) from tsv_server_status where cd_campo = 4)"
    result = mssql_hook.get_first(sql)
    return result[0]

# Função para leitura do arquivo CSV usando o nome do arquivo
def read_csv_file(ti):
    file_name = ti.xcom_pull(task_ids='Ler_Nome_Data_Arq_Clientes')
     # Lógica para leitura do CSV usando pandas
    df = pd.read_csv(f'dags/data/{file_name}.csv', 
                     sep=';', 
                     encoding='latin1',
                     header=None # Define que não há cabeçalho no CSV
                     ) 
    
    # Adiciona nomes das colunas, substitua pelos nomes que desejar
    df.columns = ['cd_cliente_legado', 'nm_cliente', 'cd_cpf_cnpj', 'cd_passaporte', 'tp_pessoa',
                'id_pep', 'dt_cadastro']  # Exemplo de nomes de colunas
   
    # Processamento do DataFrame
    print(df.head())  # Exemplo: imprimindo as primeiras linhas do DataFrame

    # Gera as instruções SQL de insert

    sql_inserts = []
    for _, row in df.iterrows():
       sql = f"""
       INSERT INTO ttp_cliente (cd_cliente_legado, cd_cpf_cnpj, cd_passaporte, tp_pessoa, nm_cliente, id_pep, dt_cadastro) 
       VALUES ('{row['cd_cliente_legado']}', '{row['cd_cpf_cnpj']}', '{row['cd_passaporte']}', '{row['tp_pessoa']}', '{row['nm_cliente']}', '{row['id_pep']}','{row['dt_cadastro']}');
       """

       sql_inserts.append(sql)

    # Retorna as instruções SQL como uma única string concatenada
    return "\n".join(sql_inserts)

# Função para ler dados do SQL Server para Detalhes Clientes

def get_filename_from_db_2():
    mssql_hook = MsSqlHook(mssql_conn_id='sqlserver')
    sql = "select (select  rtrim(convert(varchar(255),vl_campo)) as nm_arquivo from dbo.tsv_server_status where cd_campo = 41)+ (select convert(varchar, dt_campo, 112) from tsv_server_status where cd_campo = 4)"
    result = mssql_hook.get_first(sql)
    return result[0]

# Função para leitura do arquivo CSV usando o nome do arquivo
def read_csv_file_2(ti):
    file_name = ti.xcom_pull(task_ids='Ler_Nome_Data_Arq_Detalhes_Cli')
     # Lógica para leitura do CSV usando pandas
    df = pd.read_csv(f'dags/data/{file_name}.csv', 
                     sep=';', 
                     encoding='latin1',
                     header=None # Define que não há cabeçalho no CSV
                     ) 
    
    # Adiciona nomes das colunas, substitua pelos nomes que desejar
    df.columns = ['cd_cliente_legado', 'dc_endereco', 'dc_numero', 'dc_bairro', 'dc_complemento', 
                'dc_cidade', 'dc_estado', 'dc_cep', 'dc_pais', 'dc_fone', 'dc_email', 'vl_renda', 
                'vl_patrimonio', 'vl_cap_finan_anual', 'dc_ramo_atividade', 'cd_radar', 
                'dt_inicio_relac', 'dt_nascimento','dt_constituicao']  # Exemplo de nomes de colunas
    
    # Alterar o tipo de 'coluna1' para string
    df['dc_cidade'] = df['dc_cidade'].astype(str)

    print(df.dtypes)
   
    # Processamento do DataFrame
    print(df.head())  # Exemplo: imprimindo as primeiras linhas do DataFrame

# Gera as instruções SQL de insert

    sql_inserts = []
    for _, row in df.iterrows():
       sql = f"""
       INSERT INTO ttp_cliente_detalhe_generico (cd_cliente_legado, dc_endereco, dc_numero, dc_complemento, dc_bairro,
                dc_cidade, dc_estado, dc_cep, dc_fone, dc_email, dt_inicio_relac, 
                vl_cap_finan_anual, vl_renda, vl_patrimonio, cd_radar, dt_constituicao,
                 dc_pais, dc_ramo_atividade,  dt_nascimento) 
       VALUES ('{row['cd_cliente_legado']}', 
               '{row['dc_endereco'].replace("'", ",")}', 
               '{row['dc_numero']}', 
               '{row['dc_complemento']}', 
               '{row['dc_bairro']}', 
               '{row['dc_cidade'].replace("'", ",")}', 
               '{row['dc_estado']}', 
               '{row['dc_cep']}', 
               '{row['dc_fone']}', 
               '{row['dc_email']}', 
               '{row['dt_inicio_relac']}', 
               '{row['vl_cap_finan_anual']}', 
               '{row['vl_renda']}', 
               '{row['vl_patrimonio']}', 
               '{row['cd_radar']}', 
               '{row['dt_constituicao']}', 
               '{row['dc_pais']}', 
               '{row['dc_ramo_atividade']}', 
               '{row['dt_nascimento']}'
               );
       """

       sql_inserts.append(sql)

    # Retorna as instruções SQL como uma única string concatenada
    return "\n".join(sql_inserts)



# Cria a DAG
with DAG(
    dag_id='LOAD_CLIENTE',
    default_args=default_args,
    schedule_interval='0 15 * * *',  # Executa todos os dias às 15:00,  # Execução de Hora em Hora '@hourly'
    catchup=False,  # Evita execuções retroativas
    tags=['Cambio', 'Clientes', 'Sircoi'],  # Adicionando tags à DAG
 ) as dag:
    
    # Task 1: Armazenar o Nome e a Data que serão utilidados na carga de Clientes
    get_filename_task = PythonOperator(
    task_id='Ler_Nome_Data_Arq_Clientes',
    python_callable=get_filename_from_db,
    )

    # Task 2: Limpar dados da tabela temporaria de Clientes
    delete_table = MsSqlOperator(
        task_id='Limpa_Tabela_Temporaria',
        mssql_conn_id='sqlserver',  # Nome da conexão configurada no Airflow
        sql="truncate table ttp_cliente;",  # Substitua pelo comando para deletar a tabela
    )

    # Task 3: Ler dados do arquivo .csv
    read_csv_task = PythonOperator(
    task_id='Ler_Arquivo_CSV_Clientes',
    python_callable=read_csv_file,
    )
 
    # Task 4: Inserir os dados na tabela temporaria de Clientes
    insert_into_sql = MsSqlOperator(
        task_id='Inserir_Tabela_Temporaria_Clientes',
        mssql_conn_id='sqlserver',
        sql="{{ task_instance.xcom_pull(task_ids='Ler_Arquivo_CSV_Clientes') }}",
        autocommit=True,
    )

    # Task 5: Armazenar o Nome e a Data que serão utilidados na carga de Detalhes Clientes
    get_filename_task_2 = PythonOperator(
    task_id='Ler_Nome_Data_Arq_Detalhes_Cli',
    python_callable=get_filename_from_db_2,
    )

    # Task 6: Limpar dados da tabela temporaria de Detalhes Clientes
    delete_table_Clien = MsSqlOperator(
        task_id='Limpar_Tabela_Temporaria_Detal_Cliente',
        mssql_conn_id='sqlserver',  # Nome da conexão configurada no Airflow
        sql="truncate table ttp_cliente_detalhe_generico;",  # Substitua pelo comando para deletar a tabela
    )

    # Task 7: Ler Arquivo CSV de Detalhes Clientes
    read_csv_task_2 = PythonOperator(
    task_id='Ler_Arquivo_CSV_Detal_Cliente',
    python_callable=read_csv_file_2,
    )

    # Task 8: Inserir os dados na tabela temporaria de Clientes
    insert_into_sql_2 = MsSqlOperator(
        task_id='Inserir_Tabela_Temporaria_Detal_Cliente',
        mssql_conn_id='sqlserver',
        sql="{{ task_instance.xcom_pull(task_ids='Ler_Arquivo_CSV_Detal_Cliente') }}",
        autocommit=True,
    )

    # Task 9: Executar Procedure
    executar_procedure = MsSqlOperator(
        task_id='executar_procedure',
        mssql_conn_id='sqlserver',
        sql="""
        DECLARE @dt_referencia DATETIME;

        -- Armazenar o resultado da consulta na variável
        SELECT @dt_referencia = dt_campo 
        FROM tsv_server_status 
        WHERE cd_campo = 4;

        -- Executar a stored procedure com o valor da variável
        EXEC dbo.spcl_carga_clientes @dt_referencia = @dt_referencia;
        """
    )


   

# Define a ordem de execução das tasks (neste caso, é apenas uma)
get_filename_task >> delete_table >> read_csv_task >> insert_into_sql >> get_filename_task_2 >>  delete_table_Clien >> read_csv_task_2 >> insert_into_sql_2 >>  executar_procedure