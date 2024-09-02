# A Jornada dos Dados

### Cenário: ETL de Clientes
Imagine que você faz parte de uma força-tarefa de inteligência financeira, responsável por proteger o sistema contra atividades suspeitas de lavagem de dinheiro. Todos os dias, um grande volume de dados de clientes é gerado e precisa ser processado, transformado, e analisado cuidadosamente para garantir a segurança do sistema financeiro. Para isso, você criou uma pipeline de dados para a Carga de Clientes, onde são capturadas todas as informações da Base de Origem e transportada para uma base onde a análise de dados será posteriormente aplicada.

 Nesse cenários especifico, vamos realizar a leitura de um arquivo .csv que é disponibilizado em um diretorio no servidor diariamente. 

### 1. Coleta de Dados: 
Armazenar o Nome e a Data do arquivo que sera lido para a carga de Clientes.

### 2. Coleta de Dados: 
Nesse etapa é realizada a limpeza da tabela temporaria que receberá todos os dados do arquivo de Clientes.
Essa tabela está preparada apenas para receber os dados do Modo que estiverem na origem.

### 3. Transferencia dos dados: 
Nessa etapa é realizada a leitura dos dados de acordo com o nome definido na etapa 1.

### 4.  Transferencia dos dados: 
Os dados são inseridos na tabela temporaria do banco de dados SQL Server.

### 5. Coleta de Dados: 
Armazenar o Nome e a Data do arquivo que sera lido para a carga de Detalhes do Clientes (Informações como Endereço, Valor Renda, Valor Patrimonio) entre outros.

### 6. Coleta de Dados: 
Nesse etapa é realizada a limpeza da tabela temporaria que receberá todos os dados do arquivo de Detalhes Clientes.
Essa tabela está preparada apenas para receber os dados do Modo que estiverem na origem.

### 7. Transferencia dos dados: 
Nessa etapa é realizada a leitura dos dados de acordo com o nome definido na etapa 5.

### 8.  Transferencia dos dados: 
Os dados são inseridos na tabela temporaria de Detalhes Clientes do banco de dados SQL Server.

### 9. Limpeza e Tratamento dos dados: 
Nessa etapa é executada uma procedure que realiza o tratamento dos dados, como formatação de datas, limpeza de duplicidades entre outros.
Após o tratameto os dados serão inseridos ou atualizadas nas tabelas definitivas de Clientes e Detalhes Clientes.

### Abaixo estão a dag, os arquivos e a procedure que efetua as operações mencionadas acima.

<div> 
<p><a href="https://github.com/JosiTubaroski/Pipeline_Airflow/blob/main/Anexos/ETL_CLIENTES.py">01. Dag ETL CLIENTES </a></p>
</div> 

