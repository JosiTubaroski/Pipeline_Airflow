
/****** Object:  StoredProcedure [dbo].[spcl_carga_clientes]    Script Date: 02/09/2024 17:08:48 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

ALTER procedure [dbo].[spcl_carga_clientes]  
 @dt_referencia smalldatetime  
as  
begin try  
  
 --Atualiza todas as datas '00/00/0000' para um formato do sql  
  
 update dbo.ttp_cliente_detalhe_generico set dt_constituicao = '01/01/1900' where dt_constituicao = '00/00/0000'  
 update dbo.ttp_cliente_detalhe_generico set dt_nascimento = '01/01/1900' where dt_nascimento = '00/00/0000'  
 update dbo.ttp_cliente_detalhe_generico set dt_inicio_relac = '01/01/1900' where dt_inicio_relac = '00/00/0000 00:00:00'  
  
 --Converter as colunas de documentos para numeric  
  
 update dbo.ttp_cliente set cd_cpf_cnpj = cast(cd_cpf_cnpj as numeric)  
 update dbo.ttp_cliente set cd_cliente_legado = cast(cd_cliente_legado as numeric)  
 update dbo.ttp_cliente_detalhe_generico set cd_cliente_legado = cast(cd_cliente_legado as numeric)  

 ---Limpeza de emails que entram em looping de retentagem

  update dbo.tsv_envio_email
  set id_problema = 0, id_enviado = 1, dt_agendamento = null, dt_envio = getdate()
  where id_problema = 1

  update dbo.tsv_envio_email_horarios
  set id_envio_ativo = 0
  where cd_dia_semana in (1,2,3,4,5,6)

  delete dbo.tsv_envio_email
  where dt_criacao <= '2024-01-01'

---Iniciando a carga de Clientes
  
 declare @cd_carga int   
  
 select @cd_carga = isnull(max(cd_carga), 0) + 1 from dbo.tgr_cargas    
  
 insert into dbo.tgr_cargas(cd_carga, cd_produto, dt_referencia, nm_arquivo, nm_package, dt_inicio, dt_termino, id_termino)  
 values(@cd_carga, 1, @dt_referencia, 'SIRCOI_CLIENTE' , 'spcl_carga_clientes', getdate(), getdate(), 0);  
  
 delete dbo.ttp_cliente  
 where nm_cliente is null;  
   
--- Primeiro Bloco

 with ttp_cliente_ins as (  
 select ori.cd_cliente_legado,  
     cast(ori.cd_cliente_legado as numeric) as cd_cpf_cnpj, -- Passar para selecionar como numeric  
     ori.tp_pessoa,  
     max(ori.nm_cliente) as nm_cliente,  
     isnull(ori.id_pep, 0) as id_pep  
 from   dbo.ttp_cliente ori with(nolock)  
 left join dbo.tcl_cliente des with(nolock) on LTRIM(SUBSTRING(ori.cd_cliente_legado, PATINDEX('%[^0]%', ori.cd_cliente_legado + ' '), LEN(ori.cd_cliente_legado))) =   
   LTRIM(SUBSTRING(des.cd_cliente_legado, PATINDEX('%[^0]%', des.cd_cliente_legado + ' '), LEN(des.cd_cliente_legado)))   
 where   des.cd_cliente is null  
 group by ori.cd_cliente_legado,  
       ori.tp_pessoa,  
     isnull(ori.id_pep, 0)  
 )  
 --insere novos clientes  
 insert into dbo.tcl_cliente  
 (  
  cd_cliente_legado,  
  nm_cliente,   
  tp_pessoa,   
  cd_cpf_cnpj,   
  id_pep,   
  cd_carga,   
  dt_criacao,  
  dt_timestamp  
 )   
 select distinct   
   ori.cd_cliente_legado,  
   ori.nm_cliente,  
   ori.tp_pessoa,  
   ori.cd_cpf_cnpj,  
   ori.id_pep,  
   @cd_carga,  
   getdate() as dt_criacao,  
   getdate() as dt_timestamp   
 from ttp_cliente_ins ori with(nolock);  
  
 with ttp_cliente_upd as (  
 select ori.cd_cliente_legado,  
     cast(ori.cd_cliente_legado as numeric) as cd_cpf_cnpj,  
     ori.tp_pessoa,  
     ori.nm_cliente,  
     isnull(ori.id_pep, 0) as id_pep  
 from   dbo.ttp_cliente ori with(nolock)  
 join   dbo.tcl_cliente des with(nolock) on LTRIM(SUBSTRING(ori.cd_cliente_legado, PATINDEX('%[^0]%', ori.cd_cliente_legado + ' '), LEN(ori.cd_cliente_legado))) =   
   LTRIM(SUBSTRING(des.cd_cliente_legado, PATINDEX('%[^0]%', des.cd_cliente_legado + ' '), LEN(des.cd_cliente_legado))))  
  
 --Atualiza clientes  
 update des   
 set  des.nm_cliente  = ori.nm_cliente,  
   des.tp_pessoa  = ori.tp_pessoa,  
   des.id_pep   = ori.id_pep,  
   des.dt_timestamp = getdate(),  
   des.dt_alteracao = getdate(),  
   des.cd_carga  = @cd_carga  
 from dbo.tcl_cliente des  
 join dbo.ttp_cliente ori on LTRIM(SUBSTRING(ori.cd_cliente_legado, PATINDEX('%[^0]%', ori.cd_cliente_legado + ' '), LEN(ori.cd_cliente_legado))) =   
   LTRIM(SUBSTRING(des.cd_cliente_legado, PATINDEX('%[^0]%', des.cd_cliente_legado + ' '), LEN(des.cd_cliente_legado)))  
 where  isnull(des.nm_cliente,'') <> isnull(ori.nm_cliente,'')  
 or    isnull(des.id_pep,0)   <> isnull(ori.id_pep,0)  
 or    isnull(des.tp_pessoa,'')  <> isnull(ori.tp_pessoa,'');  
  
 --Trata os clientes que vem duplicado na tabela de detalhe  
 --exec spcl_trata_clientes_duplicados @cd_carga;  
  
 --with ttp_cliente_detalhe_ins  
 --as (  
 --select distinct  
 --    cli.cd_cliente,  
 --    ori.cd_cliente_legado,  
 --      ori.dc_endereco,  
 --    ori.dc_numero,  
 --    ori.dc_complemento,  
 --    ori.dc_bairro,  
 --    ori.dc_cidade,  
 --    ori.dc_estado,  
 --    ori.dc_cep,  
 --    ori.dc_pais,  
 --    ori.dc_fone,  
 --    ori.dc_email,  
 --    ori.dc_ramo_atividade,  
 --    cast(substring(ori.dt_inicio_relac,7,4) + '-' + substring(ori.dt_inicio_relac,4,2) + '-' + substring(ori.dt_inicio_relac,1,2) as date) as dt_inicio_relac,  
 --    replace(replace(ori.vl_cap_finan_anual,'.',''),',','.') as vl_cap_finan_anual,  
 --    replace(replace(ori.vl_renda,'.',''),',','.') as vl_renda,  
 --    replace(replace(ori.vl_patrimonio,'.',''),',','.') as vl_patrimonio,  
 --    ori.cd_radar,  
 --    cast(substring(ori.dt_constituicao,7,4) + '-' + substring(ori.dt_constituicao,4,2) + '-' + substring(ori.dt_constituicao,1,2) as date) as dt_constituicao,  
 --    cast(substring(ori.dt_nascimento,7,4) + '-' + substring(ori.dt_nascimento,4,2) + '-' + substring(ori.dt_nascimento,1,2) as date) as dt_nascimento  
 --from   dbo.ttp_cliente_detalhe_generico ori with(nolock)  
 --join dbo.tcl_cliente cli with(nolock) on LTRIM(SUBSTRING(ori.cd_cliente_legado, PATINDEX('%[^0]%', ori.cd_cliente_legado + ' '), LEN(ori.cd_cliente_legado))) =   
 --  LTRIM(SUBSTRING(cli.cd_cliente_legado, PATINDEX('%[^0]%', cli.cd_cliente_legado + ' '), LEN(cli.cd_cliente_legado)))    
 --left join dbo.tcl_cliente_detalhe_generico des with(nolock) on LTRIM(SUBSTRING(ori.cd_cliente_legado, PATINDEX('%[^0]%', ori.cd_cliente_legado + ' '), LEN(ori.cd_cliente_legado))) =   
 --  LTRIM(SUBSTRING(des.cd_cliente_legado, PATINDEX('%[^0]%', des.cd_cliente_legado + ' '), LEN(des.cd_cliente_legado)))  
 --where   des.cd_cliente_legado is null)  
  
   
 -- insere detalhes cliente  
 --insert into dbo.tcl_cliente_detalhe_generico (  
 -- cd_cliente,  
 -- cd_cliente_legado,  
 -- dc_endereco,  
 -- dc_numero,  
 -- dc_complemento,  
 -- dc_bairro,  
 -- dc_cidade,  
 -- dc_estado,  
 -- dc_pais,  
 -- dc_cep,  
 -- dc_fone,  
 -- dc_email,  
 -- dc_ramo_atividade,  
 -- dt_inicio_relac,  
 -- vl_cap_finan_anual,  
 -- vl_renda,  
 -- vl_patrimonio,  
 -- cd_radar,  
 -- dt_constituicao,  
 -- dt_nascimento,  
 -- dt_criacao,  
 -- cd_carga  
 --)  
 --select distinct   
 --    ori.cd_cliente,  
 --    cast(ori.cd_cliente_legado as numeric),  
 --       ori.dc_endereco,  
 --    ori.dc_numero,  
 --    ori.dc_complemento,  
 --    ori.dc_bairro,  
 --    ori.dc_cidade,  
 --    ori.dc_estado,  
 --    ori.dc_pais,  
 --    ori.dc_cep,  
 --    ori.dc_fone,  
 --    ori.dc_email,  
 --    ori.dc_ramo_atividade,  
 --    ori.dt_inicio_relac,  
 --    ori.vl_cap_finan_anual,  
 --    ori.vl_renda,  
 --    ori.vl_patrimonio,  
 --    ori.cd_radar,  
 --    ori.dt_constituicao,  
 --    ori.dt_nascimento,  
 --    getdate() as dt_criacao,  
 --    @cd_carga  
 --from   ttp_cliente_detalhe_ins ori with(nolock);  
  
 --with ttp_cliente_detalhe_upd  
 --as (  
 --select distinct  
 --    cli.cd_cliente,  
 --    ori.cd_cliente_legado,  
 --      ori.dc_endereco,  
 --    ori.dc_numero,  
 --    ori.dc_complemento,  
 --    ori.dc_bairro,  
 --    ori.dc_cidade,  
 --    ori.dc_estado,  
 --    ori.dc_cep,  
 --    ori.dc_pais,  
 --    ori.dc_fone,  
 --    ori.dc_email,  
 --    ori.dc_ramo_atividade,  
 --    cast(substring(ori.dt_inicio_relac,7,4) + '-' + substring(ori.dt_inicio_relac,4,2) + '-' + substring(ori.dt_inicio_relac,1,2) as date) as dt_inicio_relac,  
 --  replace(replace(ori.vl_cap_finan_anual,'.',''),',','.') as vl_cap_finan_anual,  
 --  replace(replace(ori.vl_renda,'.',''),',','.') as vl_renda,  
 --  replace(replace(ori.vl_patrimonio,'.',''),',','.') as vl_patrimonio,  
 --    ori.cd_radar,  
 --    cast(substring(ori.dt_constituicao,7,4) + '-' + substring(ori.dt_constituicao,4,2) + '-' + substring(ori.dt_constituicao,1,2) as date) as dt_constituicao,  
 --    cast(substring(ori.dt_nascimento,7,4) + '-' + substring(ori.dt_nascimento,4,2) + '-' + substring(ori.dt_nascimento,1,2) as date) as dt_nascimento  
 --from   dbo.ttp_cliente_detalhe_generico ori with(nolock)  
 --join dbo.tcl_cliente cli with(nolock) on LTRIM(SUBSTRING(ori.cd_cliente_legado, PATINDEX('%[^0]%', ori.cd_cliente_legado + ' '), LEN(ori.cd_cliente_legado))) =   
 --  LTRIM(SUBSTRING(cli.cd_cliente_legado, PATINDEX('%[^0]%', cli.cd_cliente_legado + ' '), LEN(cli.cd_cliente_legado)))    
 --join dbo.tcl_cliente_detalhe_generico des with(nolock) on LTRIM(SUBSTRING(ori.cd_cliente_legado, PATINDEX('%[^0]%', ori.cd_cliente_legado + ' '), LEN(ori.cd_cliente_legado))) =   
 --  LTRIM(SUBSTRING(des.cd_cliente_legado, PATINDEX('%[^0]%', des.cd_cliente_legado + ' '), LEN(des.cd_cliente_legado))))  
  
 -- --LTRIM(SUBSTRING(ori.cd_cliente_legado, PATINDEX('%[^0]%', ori.cd_cliente_legado + ' '), LEN(ori.cd_cliente_legado))) =   
 --  --LTRIM(SUBSTRING(des.cd_cliente_legado, PATINDEX('%[^0]%', des.cd_cliente_legado + ' '), LEN(des.cd_cliente_legado)))  
   
 ----Atualiza clientes Detalhe  
 --update des   
 --set      
 --     des.dc_endereco         = ori.dc_endereco,  
 --     des.dc_numero           = ori.dc_numero,  
 --     des.dc_complemento      = ori.dc_complemento,  
 --     des.dc_bairro           = ori.dc_bairro,  
 --     des.dc_cidade           = ori.dc_cidade,  
 --     des.dc_estado           = ori.dc_estado,  
 --     des.dc_pais             = ori.dc_pais,  
 --     des.dc_cep              = ori.dc_cep,       
 --     des.dc_fone             = ori.dc_fone,  
 --     des.dc_email            = ori.dc_email,  
 --     des.dc_ramo_atividade   = ori.dc_ramo_atividade,  
 --     des.dt_inicio_relac     = ori.dt_inicio_relac,  
 --     des.vl_cap_finan_anual  = ori.vl_cap_finan_anual,  
 --     des.vl_renda            = ori.vl_renda,  
 --     des.vl_patrimonio       = ori.vl_patrimonio,  
 --     des.cd_radar            = ori.cd_radar,  
 --     des.dt_constituicao     = ori.dt_constituicao,  
 --     des.dt_nascimento       = ori.dt_nascimento,  
 --     des.cd_carga   = @cd_carga,  
 --     des.dt_alteracao  = getdate()  
 --from dbo.tcl_cliente_detalhe_generico des   
 --join ttp_cliente_detalhe_upd ori on LTRIM(SUBSTRING(ori.cd_cliente_legado, PATINDEX('%[^0]%', ori.cd_cliente_legado + ' '), LEN(ori.cd_cliente_legado))) =   
 --  LTRIM(SUBSTRING(des.cd_cliente_legado, PATINDEX('%[^0]%', des.cd_cliente_legado + ' '), LEN(des.cd_cliente_legado)))  
 --where     
 --(     
 --        isnull(des.dc_endereco,'')         != isnull(ori.dc_endereco,'')   
 --or      isnull(des.dc_numero,'')           != isnull(ori.dc_numero,'')   
 --or      isnull(des.dc_complemento,'')      != isnull(ori.dc_complemento,'')   
 --or      isnull(des.dc_bairro,'')           != isnull(ori.dc_bairro,'')   
 --or      isnull(des.dc_cidade,'')           != isnull(ori.dc_cidade,'')   
 --or      isnull(des.dc_estado,'')           != isnull(ori.dc_estado,'')   
 --or      isnull(des.dc_pais,'')             != isnull(ori.dc_pais,'')   
 --or      isnull(des.dc_cep,'')              != isnull(ori.dc_cep,'')   
 --or      isnull(des.dc_fone,'')             != isnull(ori.dc_fone,'')   
 --or      isnull(des.dc_email,'')            != isnull(ori.dc_email,'')   
 --or      isnull(des.dc_ramo_atividade,'')   != isnull(ori.dc_ramo_atividade,'')   
 --or      isnull(des.dt_inicio_relac,'')     != isnull(ori.dt_inicio_relac,'')   
 --or      isnull(des.vl_cap_finan_anual,'')  != isnull(ori.vl_cap_finan_anual,'')   
 --or      isnull(des.vl_renda,'')            != isnull(ori.vl_renda,'')   
 --or      isnull(des.vl_patrimonio,'')       != isnull(ori.vl_patrimonio,'')   
 --or      isnull(des.cd_radar,'')            != isnull(ori.cd_radar,'')   
 --or      isnull(des.dt_constituicao,'')     != isnull(ori.dt_constituicao,'')   
 --or      isnull(des.dt_nascimento,'')       != isnull(ori.dt_nascimento,''))  

   
 --Fim carga  
 update  dbo.tgr_cargas  
 set  dt_termino = getdate(),  
   id_termino = 1  
 where cd_carga = @cd_carga    
  
   
end try  
begin catch  
  
    exec dbo.spgr_tratar_erro   
  
end catch  