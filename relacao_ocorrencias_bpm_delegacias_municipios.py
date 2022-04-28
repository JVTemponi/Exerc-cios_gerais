# imports necessários:
import pandas as pd
from pathlib import PosixPath 
from datetime import datetime
import sqlite3 as sql

#Endereço dos arquivos CSV:
local_arquivos = PosixPath('/content/FontesDeDados')
#Endereço bases de daados ODS e DW:
endereco = PosixPath('/content/BasesDeDados')

"""# Coleta dos Dados

## Coleta dos dados: IBGE
"""

#Coletar dados dos municípios do Rio de Janeiro (site IBGE)
url = 'https://www.ibge.gov.br/explica/codigos-dos-municipios.php#RJ'

#Transformar os dados em um Data Frame
ibge_dados_rj = pd.DataFrame(pd.read_html(url, match='Municípios do Rio de Janeiro')[0])

#Ajustar a nomenclatura das colunas do Data Frame:
ibge_dados_rj = ibge_dados_rj.rename(columns = {'Municípios do Rio de Janeiro': 'nomeMunic', 'Códigos': 'codMunic'})

#Adicionar coluna de tempo ao Data Frame:
ibge_dados_rj['dtCarga'] = datetime.today().strftime('%d/%m/%Y %H:%M')

#Ajustar index do Data Frame para idMunic
ibge_dados_rj.index.name = 'sk_idMunic'
#Ajustar numeração do índice, para começar em 1
ibge_dados_rj.index = ibge_dados_rj.index + 1

"""## Coleta dos dados: Delegacias

### Cadastro das Delegacias de Polícia RJ
"""

#Criar Data Frame com os dados das DPs
tb_log_dp = pd.DataFrame(pd.read_csv(local_arquivos / "DP.csv"))

#Ajustar a nomenclatura das colunas do Data Frame:
tb_log_dp = tb_log_dp.rename(columns = {'COD_DP': 'idDP', 'NM_DP':'nomeDP', 'Endereço':'enderDP'})

#Adicionar coluna de Data e Hora da carga no Data Frame 
tb_log_dp['dtCarga'] = datetime.today().strftime('%d/%m/%Y %H:%M')

"""### Cadastro dos Responsáveis pelas Delegacias de Polícia RJ

"""

#Criar Data Frame com os dados dos responsáveis pelas DPs
tb_log_resp_dp = pd.DataFrame(pd.read_csv(local_arquivos / "ResponsavelDP.csv"))
                                     
#Ajustar a nomenclatura das colunas do Data Frame:
tb_log_resp_dp = tb_log_resp_dp.rename(columns = {'COD_DP': 'idDP', 'Responsavel':'nomeResp'})

#Adicionar coluna de Data e Hora da carga no Data Frame 
tb_log_resp_dp['dtCarga'] = datetime.today().strftime('%d/%m/%Y %H:%M')

"""## Coleta dos dados: Batalhões Polícia Militar

### Cadastro dos registros dos batalhões
"""

#Criar Data Frame com os dados dos BPM
tb_log_bpm = pd.DataFrame(pd.read_csv(local_arquivos / "BPM.csv"))
tb_log_bpm['dtCarga'] = datetime.today().strftime('%d/%m/%Y %H:%M')

#Ajustar a nomenclatura das colunas do Data Frame:
tb_log_bpm = tb_log_bpm.rename(columns = {'COD_BPM': 'idBPM', 'NM_BPM':'nomeBPM','Endereco':'enderBPM'})

"""### Cadastro das áreas dos BPM"""

#Criar Data Frame com os dados das áreas dos BPM
tb_log_area_bpm = pd.DataFrame(pd.read_csv(local_arquivos / "areaBPMv1.csv"))
tb_log_area_bpm['dtCarga'] = datetime.today().strftime('%d/%m/%Y %H:%M')

#Ajustar a nomenclatura das colunas do Data Frame:
tb_log_area_bpm = tb_log_area_bpm.rename(columns = {'aisp': 'idBPM', 'area_aisp_km2':'areaBPM'})

"""## Coleta dos dados: Ocorrências

### Cadastro dos registros das ocorrências
"""

#Criar Data Frame com os dados das áreas dos BPM
tb_log_ocorrencias = pd.DataFrame(pd.read_csv(local_arquivos / "OcorrenciaV2.csv"))
tb_log_ocorrencias['dtCarga'] = datetime.today().strftime('%d/%m/%Y %H:%M')

#Ajustar a nomenclatura das colunas do Data Frame:
tb_log_ocorrencias = tb_log_ocorrencias.rename(columns = {'COD_DP': 'idDP', 'COD_BPM':'idBPM', 'Regiao':'regiao', 'COD_Munic_IBGE':'codMunic','Ocorrencia':'ocorrencia','Soma de Qtde':'somaQtd'})

tb_log_ocorrencias

"""# Configuração Banco de Dados: ODS e DW - SQLite"""

#Local onde serão salvas as bases de dados
bd_ods = endereco / "baseODS.db"
bd_dw = endereco / "baseDW.db"

#Validar a existência das bases
if endereco.exists():
    if (bd_ods.exists() and bd_dw.exists()):
        print('Bancos de dados já existem!')
    else:
        bd_ods.touch()
        bd_dw.touch()
        print('Bancos de dados criados!')
else:
    print('Endereço não existe, favor verificar!')

"""### Criação das tabelas - ODS"""

#Conectar no ODS
conexao_ods = sql.connect(bd_ods)

#Definir variável para manipulação
cursor_ods = conexao_ods.cursor()

#Criar tabelase e índices, caso não existam:
####---------------tbLogMunic---------------####
cursor_ods.execute(

    '''
        CREATE TABLE IF NOT EXISTS "tbLogMunic" (
          "sk_idMunic" INTEGER,
          "nomeMunic" TEXT,
          "codMunic" INTEGER,
          "dtCarga" TEXT
        )
     '''
)
cursor_ods.execute('CREATE INDEX IF NOT EXISTS idx_tbLogMunic_sk_idMunic ON tbLogMunic(sk_idMunic)')
####----------------------------------------####
####---------------tbLogDP------------------####
cursor_ods.execute(

    '''
        CREATE TABLE IF NOT EXISTS "tbLogDP" (
          "idDP" INTEGER,
          "nomeDP" VARCHAR(200),
          "enderDP" VARCHAR(200),
          "dtCarga" DATETIME
        )
        
    '''
)
cursor_ods.execute('CREATE INDEX IF NOT EXISTS idx_tbLogDP_idDP ON tbLogDP(idDP)')
####----------------------------------------####
####---------------tbLogRespDP--------------####
cursor_ods.execute(

    '''
        CREATE TABLE IF NOT EXISTS "tbLogRespDP" (
          "idDP" INTEGER,
          "nomeResp" VARCHAR(200),
          "dtCarga" DATETIME
        )
     '''
)
cursor_ods.execute('CREATE INDEX IF NOT EXISTS idx_tbLogRespDP_idDP ON tbLogRespDP(idDP)')
####----------------------------------------####
####---------------tbLogBPM-----------------####
cursor_ods.execute(

    '''
        CREATE TABLE IF NOT EXISTS "tbLogBPM" (
          "idBPM" INTEGER,
          "nomeBPM" VARCHAR(50),
          "enderBPM" VARCHAR(200),
          "dtCarga" DATETIME
        )
        
    '''
)
cursor_ods.execute('CREATE INDEX IF NOT EXISTS idx_tbLogBPM_idBPM ON tbLogBPM(idBPM)')
####----------------------------------------####
####--------------tbLogAreaBPM----------------####
cursor_ods.execute(

    '''
        CREATE TABLE IF NOT EXISTS "tbLogAreaBPM" (
          "idBPM" INTEGER,
          "areaBPM" REAL(5,2),
          "dtCarga" DATETIME
        )
        
    '''
)
cursor_ods.execute('CREATE INDEX IF NOT EXISTS idx_tbLogAreaBPM_idBPM ON tbLogAreaBPM(idBPM)')
####----------------------------------------------####
####--------------tbLogOcorrencias----------------####
cursor_ods.execute(

    '''
        CREATE TABLE IF NOT EXISTS "tbLogOcorrencias" (
          "idDP" INTEGER,
          "idBPM" INTEGER,
          "ano" INTEGER,
          "mes" INTEGER,
          "mes_ano" VARCHAR(10),
          "regiao" INTEGER,
          "codMunic" INTEGER,
          "ocorrencia" VARCHAR(30),
          "somaQtd" INTEGER,
          "dtCarga" DATETIME
        )
        
    '''
)
cursor_ods.execute('CREATE INDEX IF NOT EXISTS idx_tbLogOcorrencias ON tbLogOcorrencias(idDP,idBPM,codMunic)')
####----------------------------------------####

#Confirmar insert e encerrando a conexão
conexao_ods.commit()
conexao_ods.close()

"""### Criação das tabelas - DW"""

#Conectar no DW
conexao_dw = sql.connect(bd_dw)

#Definir variável para manipulação
cursor_dw = conexao_dw.cursor()

#Criar tabelase índices, caso não existam:
####---------------dMunicipio---------------####
cursor_dw.execute(

    '''
        CREATE TABLE IF NOT EXISTS "dMunicipio" (
          "sk_idMunic" INTEGER PRIMARY KEY AUTOINCREMENT,
          "codMunic" INTEGER,
          "nomeMunic" TEXT
        )

     '''
)
cursor_dw.execute('CREATE INDEX IF NOT EXISTS idx_dMunicipio_sk_idMunic ON dMunicipio(sk_idMunic)')
####---------------------------------------####
####------------------dDP------------------####
cursor_dw.execute(

    '''
        CREATE TABLE IF NOT EXISTS "dDP" (
          "sk_idDP" INTEGER PRIMARY KEY AUTOINCREMENT,
          "codDP" INTEGER,
          "nomeDP" VARCHAR(200),
          "enderDP" VARCHAR(200),
          "nomeResp" VARCHAR(200)
        )

     '''
)
cursor_dw.execute('CREATE INDEX IF NOT EXISTS idx_dDP_sk_idDP ON dDP(sk_idDP)')
####----------------------------------------####
####------------------dBPM------------------####
cursor_dw.execute(

    '''
        CREATE TABLE IF NOT EXISTS "dBPM" (
          "sk_idBPM" INTEGER PRIMARY KEY AUTOINCREMENT ,
          "codBPM" INTEGER,
          "nomeBPM" VARCHAR(50),
          "enderBPM" VARCHAR(200),
          "areaBPM" REAL(5,2)
        )

     '''
)
cursor_dw.execute('CREATE INDEX IF NOT EXISTS idx_dBPM_sk_idBPM ON dBPM(sk_idBPM)')
####--------------------------------------------####
####------------------dPeriodo------------------####
cursor_dw.execute(

    '''
         CREATE TABLE IF NOT EXISTS "dPeriodo" (
          "sk_idPeriodo" INTEGER PRIMARY KEY AUTOINCREMENT ,
          "data" DATETIME,
          "mes" INTEGER,
          "ano" INTEGER,
          "trimestre" VARCHAR(2),
          "semestre" VARCHAR(2)
        )

     '''
)
cursor_dw.execute('CREATE INDEX IF NOT EXISTS idx_dPeriodo_sk_idPeriodo ON dPeriodo(sk_idPeriodo)')
cursor_dw.execute('CREATE INDEX IF NOT EXISTS idx_dPeriodo ON dPeriodo(mes,ano)')
####------------------------------------------------####
####------------------FOcorrencias------------------####
cursor_dw.execute(

    '''
        CREATE TABLE IF NOT EXISTS "fOcorrencias" (
          "idDP" INTEGER REFERENCES dDP(sk_idDP) ON UPDATE NO ACTION ON DELETE NO ACTION,
          "idBPM" INTEGER REFERENCES dBPM(sk_idBPM) ON UPDATE NO ACTION ON DELETE NO ACTION,
          "idPeriodo" INTEGER REFERENCES dPeriodo(sk_idPeriodo) ON UPDATE NO ACTION ON DELETE NO ACTION,
          "regiao" INTEGER,
          "idMunic" INTEGER REFERENCES dMunicipio(sk_idMunic) ON UPDATE NO ACTION ON DELETE NO ACTION,
          "ocorrencia" VARCHAR(30),
          "somaQtd" INTEGER
        )
        
    '''
)

cursor_dw.execute('CREATE INDEX IF NOT EXISTS idx_fOcorrencias ON fOcorrencias(idDP,idBPM,idPeriodo,idMunic)')
####-----------------------------------------------####

#Confirmar create e encerrar a conexão
conexao_dw.commit()
conexao_dw.close()

"""# Manipulação dos Dados - ODS

## Inserções

### Inserir os registros ODS: Cadastro Municípios
"""

#Conectar no ODS
conexao_ods = sql.connect(bd_ods)

#Definir variável para manipulação
cursor_ods = conexao_ods.cursor()

#Adicionar novos registros à tabela tbLogMunic
ibge_dados_rj.to_sql('tbLogMunic', conexao_ods, if_exists='append')

#Confirmar insert e encerrando a conexão
conexao_ods.commit()
conexao_ods.close()

print("Carga ODS Cadastro de Municípios finalizada!",len(ibge_dados_rj),"Registros inseridos!")

"""### Inserir os registros ODS: Cadastro DPs"""

#Conectar no ODS
conexao_ods = sql.connect(bd_ods)

#Definir variável para manipulação
cursor_ods = conexao_ods.cursor()

#Inserir dados
cursor_ods.executemany('''INSERT INTO tbLogDP(idDP,nomeDP,enderDP,dtCarga) VALUES(?,?,?,?)''',tb_log_dp.values.tolist())

#Confirmar insert e encerrando a conexão
conexao_ods.commit()
conexao_ods.close()

#Mensagem de conslusão
print("Carga ODS Cadastro de DPs finalizada!",len(tb_log_dp),"Registros inseridos!")

"""### Inserir os registros ODS: Cadastro Responsáveis DPs"""

#Conectar no ODS
conexao_ods = sql.connect(bd_ods)

#Definir variável para manipulação
cursor_ods = conexao_ods.cursor()

#Inserir dados
cursor_ods.executemany('''INSERT INTO tbLogRespDP(idDP,nomeResp,dtCarga) VALUES(?,?,?)''',tb_log_resp_dp.values.tolist())

#Confirmar insert e encerrando a conexão
conexao_ods.commit()
conexao_ods.close()

#Mensagem de conslusão
print("Carga ODS Responsáveis de DPs finalizada!",len(tb_log_resp_dp),"Registros inseridos!")

"""### Inserir os registros ODS: Cadastro BPM"""

#Conectar no ODS
conexao_ods = sql.connect(bd_ods)

#Definir variável para manipulação
cursor_ods = conexao_ods.cursor()

#Inserir dados
cursor_ods.executemany('''INSERT INTO tbLogBPM(idBPM,nomeBPM,enderBPM,dtCarga) VALUES(?,?,?,?)''',tb_log_bpm.values.tolist())

#Confirmar insert e encerrando a conexão
conexao_ods.commit()
conexao_ods.close()

#Mensagem de conslusão
print("Carga ODS Cadastro dos BPM finalizados!",len(tb_log_bpm),"Registros inseridos!")

"""### Inserir os registros ODS: Cadastro áreas BPM"""

#Conectar no ODS
conexao_ods = sql.connect(bd_ods)

#Definir variável para manipulação
cursor_ods = conexao_ods.cursor()

#Inserir dados
cursor_ods.executemany('''INSERT INTO tbLogAreaBPM(idBPM,areaBPM,dtCarga) VALUES(?,?,?)''',tb_log_area_bpm.values.tolist())

#Confirmar insert e encerrando a conexão
conexao_ods.commit()
conexao_ods.close()

#Mensagem de conslusão
print("Carga ODS Cadastro das áreas BPM finalizada!",len(tb_log_area_bpm),"Registros inseridos!")

"""### Inserir os registros ODS: Cadastro das Ocorrências"""

#Conectar no ODS
conexao_ods = sql.connect(bd_ods)

#Definir variável para manipulação
cursor_ods = conexao_ods.cursor()

#Inserir dados
cursor_ods.executemany('''
                          INSERT INTO tbLogOcorrencias (idDP,idBPM,ano,mes,mes_ano,regiao,codMunic,ocorrencia,somaQtd,dtCarga) 
                          VALUES(?,?,?,?,?,?,?,?,?,?)
                          
                        ''',tb_log_ocorrencias.values.tolist()
                      )


#Confirmar insert e encerrando a conexão
conexao_ods.commit()
conexao_ods.close()

#Mensagem de conslusão
print("Carga ODS Cadastro das áreas BPM finalizada!",len(tb_log_ocorrencias),"Registros inseridos!")

"""## Validações

### Validar os registros inseridos ODS - Cadastro DPs
"""

#Conectar no ODS
conexao_ods = sql.connect(bd_ods)

#Select de validação:
select_ods = 'SELECT * FROM tbLogDP ORDER BY dtCarga DESC LIMIT '+ str(len(tb_log_dp))
select_ods = pd.read_sql(select_ods,conexao_ods)

#Encerrar a conexão
conexao_ods.close()

#Exibir select de validação
select_ods

"""### Validar os registros inseridos ODS - Cadastro Responsáveis DPs"""

#Conectar no ODS
conexao_ods = sql.connect(bd_ods)

#Select de validação:
select_ods = 'SELECT * FROM tbLogRespDP  ORDER BY dtCarga DESC LIMIT ' + str(len(tb_log_resp_dp))
select_ods = pd.read_sql(select_ods,conexao_ods)

#Encerrar a conexão
conexao_ods.close()

#Exibir select de validação
select_ods

"""### Validar os registros inseridos ODS - Cadastro BPM"""

#Conectar no ODS
conexao_ods = sql.connect(bd_ods)

#Select de validação:
select_ods = 'SELECT * FROM tbLogBPM  ORDER BY dtCarga DESC LIMIT ' + str(len(tb_log_bpm))
select_ods = pd.read_sql(select_ods,conexao_ods)

#Encerrar a conexão
conexao_ods.close()

#Exibir select de validação
select_ods

"""### Validar os registros inseridos ODS - Cadastro áreas BPM


"""

#Conectar no ODS
conexao_ods = sql.connect(bd_ods)

#Select de validação:
select_ods = 'SELECT * FROM tbLogBPM  ORDER BY dtCarga DESC LIMIT ' + str(len(tb_log_area_bpm))
select_ods = pd.read_sql(select_ods,conexao_ods)

#Encerrar a conexão
conexao_ods.close()

#Exibir select de validação
select_ods

"""### Validar os registros inseridos ODS - Cadastro de Ocorrências

"""

#Conectar no ODS
conexao_ods = sql.connect(bd_ods)

#Select de validação:
select_ods = 'SELECT * FROM tbLogOcorrencias ORDER BY dtCarga DESC LIMIT ' + str(len(tb_log_ocorrencias))
select_ods = pd.read_sql(select_ods,conexao_ods)

#Encerrar a conexão
conexao_ods.close()

#Exibir select de validação
select_ods

"""# Manipulação dos Dados - DW

### Inserir os registros: Cadastro de Municípios
"""

#Conectar no DW
conexao_dw = sql.connect(bd_dw)

#Definir variável para manipulação
cursor_dw = conexao_dw.cursor()

#Criar Data Frame para popular o DW
ibge_dados_rj_dw = ibge_dados_rj[['codMunic','nomeMunic']]

#Criar e/ou popular a dimensão dMunicipio com o DF ibge_dados_rj_dw
ibge_dados_rj_dw.to_sql('dMunicipio', conexao_dw, if_exists='replace')

#Confirmar insert e encerrando a conexão
conexao_dw.commit()
conexao_dw.close()

#Mensagem de conslusão
print("Carga DW Municípios finalizada!",len(ibge_dados_rj_dw),"Registros inseridos!")

"""### Inserir os registros: DPs e responsáveis"""

#Conectar no DW
conexao_dw = sql.connect(bd_dw)

#Definir variável para manipulação
cursor_dw = conexao_dw.cursor()

#Query para dimensão dDP:

dDP ='''
SELECT 
    [Identificador do Departamento],
    [Nome do Departamento],
    [Endereço do Departamento],
    [Nome do Responsável]
FROM
    (
        SELECT 
            DP.idDP [Identificador do Departamento],
            DP.nomeDP [Nome do Departamento],
            DP.enderDP [Endereço do Departamento],
            RESP.nomeResp [Nome do Responsável],
            MAX(DP.dtCarga) [Horário da última carga dos dados]
        FROM tbLogDP DP
            INNER JOIN tbLogRespDP RESP on RESP.idDP = DP.idDP
        WHERE DP.dtCarga = (SELECT MAX(T.dtCarga) FROM tbLogDP T)
        GROUP BY
            DP.idDP,
            DP.nomeDP,
            DP.enderDP,
            RESP.nomeResp
    ) dDP
'''
#Conectar no ODS
conexao_ods = sql.connect(bd_ods)

#Preencher o Data Frame da dimensão dDP
dDP = pd.read_sql(dDP,conexao_ods)

#Confirmar a leitura e encerrando a conexão ODS
conexao_ods.commit()
conexao_ods.close()

#Limpar a dimensão para inserir os novos registros:
cursor_dw.execute(''' DELETE FROM dDP''')
cursor_dw.execute('UPDATE sqlite_sequence SET seq=0 WHERE name="dDP"')

#Inserir os registros
cursor_dw.executemany('''INSERT INTO dDP(codDP,nomeDP,enderDP,nomeResp) VALUES(?,?,?,?)''',dDP.values.tolist())

#Confirmar o delete, insert e encerrando a conexão DW
conexao_dw.commit()
conexao_dw.close()

#Mensagem de conslusão
print("Carga DW Responsáveis e DPs finalizada!",len(dDP),"Registros inseridos!")

"""### Inserir os registros: BPM e áreas"""

#Conectar no DW
conexao_dw = sql.connect(bd_dw)

#Definir variável para manipulação
cursor_dw = conexao_dw.cursor()

#Query para dimensão dDP:

dBPM ='''
SELECT 
    [Identificador do BPM],
    [Nome do BPM],
    [Endereço do BPM],
    [Área do BPM]
FROM
    (
        SELECT 
            BPM.idBPM [Identificador do BPM],
            BPM.nomeBPM [Nome do BPM],
            BPM.enderBPM [Endereço do BPM],
            AREA.areaBPM [Área do BPM],
            MAX(BPM.dtCarga) [Horário da última carga dos dados]
        FROM tbLogBPM BPM
            INNER JOIN tbLogAreaBPM AREA on AREA.idBPM = BPM.idBPM
        WHERE BPM.dtCarga = (SELECT MAX(T.dtCarga) FROM tbLogBPM T)
        GROUP BY
            BPM.idBPM,
            BPM.nomeBPM,
            BPM.enderBPM,
            AREA.areaBPM
    ) dBPM
'''

#Conectar no ODS
conexao_ods = sql.connect(bd_ods)

#Preencher o Data Frame da dimensão dDP
dBPM = pd.read_sql(dBPM,conexao_ods)

#Confirmar a leitura e encerrando a conexão ODS
conexao_ods.commit()
conexao_ods.close()

#Limpar a dimensão para inserir os novos registros:
cursor_dw.execute(''' DELETE FROM dBPM''')
cursor_dw.execute('UPDATE sqlite_sequence SET seq=0 WHERE name="dBPM"')

#Inserir os registros
cursor_dw.executemany('''INSERT INTO dBPM(codBPM,nomeBPM,enderBPM,areaBPM) VALUES(?,?,?,?)''',dBPM.values.tolist())

#Confirmar o delete, insert e encerrando a conexão DW
conexao_dw.commit()
conexao_dw.close()

#Mensagem de conslusão
print("Carga DW Batalhões e Áreas finalizada!",len(dBPM),"Registros inseridos!")

"""### Inserir os registros: Períodos"""

#Conectar no DW
conexao_dw = sql.connect(bd_dw)

#Definir variável para manipulação
cursor_dw = conexao_dw.cursor()

#Query CTE para dimensão dPeriodo:
dPeriodo ='''
WITH _DATA(D) AS
(
  VALUES('2018-01-01')
  UNION ALL
  SELECT DATE(D,'+1 month')
  FROM _DATA
  WHERE D < DATE('NOW')
)
SELECT 
  STRFTIME('%d/%m/%Y',D) [data],
  CAST(STRFTIME('%m',D) AS INTEGER) [mes],
  CAST(STRFTIME('%Y',D) AS INTEGER) [ano],
  CASE	
    WHEN CAST(STRFTIME('%m',D) AS INTEGER) IN (1,2,3) THEN 'T1'
    WHEN CAST(STRFTIME('%m',D) AS INTEGER) IN (4,5,6) THEN 'T2'
    WHEN CAST(STRFTIME('%m',D) AS INTEGER) IN (7,8,9) THEN 'T3'
    ELSE 'T4'
  END AS [trimestre],
  CASE	
    WHEN CAST(STRFTIME('%m',D) AS INTEGER) IN (1,2,3,4,5,6) THEN 'S1'
    ELSE 'S2'
  END AS [semestre]
FROM _DATA

'''

#Criar Data Frame com os dados da consulta base
dPeriodo = pd.read_sql(dPeriodo,conexao_dw)

#Limpar a dimensão para inserir os novos registros:
cursor_dw.execute(''' DELETE FROM dPeriodo''')
cursor_dw.execute('UPDATE sqlite_sequence SET seq=0 WHERE name="dPeriodo"')

#Inserir os registros
cursor_dw.executemany('''INSERT INTO dPeriodo(data,mes,ano,trimestre,semestre) VALUES(?,?,?,?,?)''',dPeriodo.values.tolist())

#Confirmar o delete, insert e encerrando a conexão DW
conexao_dw.commit()
conexao_dw.close()

#Mensagem de conslusão
print("Carga DW Período finalizadas!",len(dPeriodo),"Registros inseridos!")

"""### Inserir os registros: Fato Ocorrências"""

#Conectar no DW
conexao_dw = sql.connect(bd_dw)

#Definir variável para manipulação
cursor_dw = conexao_dw.cursor()

#Query para tabela temporário Fato ocorrências:
fOcorrencias ='''
SELECT 
    [Identificador do Departamento],
    [Identificador do BPM],
    [Ano da ocorrência],
    [Mês da ocorrência],
    [Mês e ano da ocorrência],
    [Região da ocorrência],
    [Cod do Município da ocorrência],
    [Ocorrência],
    [Somatório da quantidade de Ocorrências]
FROM
    (
        SELECT 
            Ocorr.idDP [Identificador do Departamento],
            Ocorr.idBPM [Identificador do BPM],
            Ocorr.ano [Ano da ocorrência],
            Ocorr.mes [Mês da ocorrência],
            Ocorr.mes_ano [Mês e ano da ocorrência],
            Ocorr.regiao [Região da ocorrência],
            Ocorr.codMunic [Cod do Município da ocorrência],
            Ocorr.ocorrencia [Ocorrência],
            Ocorr.somaQtd [Somatório da quantidade de Ocorrências],
            MAX(Ocorr.dtCarga) [Horário da última carga dos dados]
        FROM tbLogOcorrencias Ocorr
        WHERE Ocorr.dtCarga = (SELECT MAX(T.dtCarga) FROM tbLogOcorrencias T)
        GROUP BY
            Ocorr.idDP,
            Ocorr.idBPM,
            Ocorr.ano,
            Ocorr.mes,
            Ocorr.mes_ano,
            Ocorr.regiao,
            Ocorr.codMunic,
            Ocorr.ocorrencia,
            Ocorr.somaQtd
    ) fOcorrencias

'''
#Conectar no ODS
conexao_ods = sql.connect(bd_ods)

#Preencher o Data Frame da tabela temporária fato Ocorrências
fOcorrencias = pd.read_sql(fOcorrencias,conexao_ods)

#Confirmar a leitura e encerrando a conexão ODS
conexao_ods.commit()
conexao_ods.close()

#Criar tabela temporária de ocorrências:
fOcorrencias.to_sql('tempOcorrencias',conexao_dw,if_exists="replace")

#Consulta para carga incremental tabela persistente de ocorrencias
cargafOcorrencias= '''              
        SELECT 
          DP.sk_idDP,
          BPM.sk_idBPM,
          PER.sk_idPeriodo,
          OC.[Região da ocorrência],
          MUNIC.sk_idMunic,
          OC.[Ocorrência],
          OC.[Somatório da quantidade de Ocorrências]
        FROM tempOcorrencias OC
        INNER JOIN dDP DP 
          ON DP.codDP = OC.[Identificador do Departamento]
        INNER JOIN dBPM BPM 
          ON BPM.codBPM = OC.[Identificador do BPM]
        INNER JOIN dPeriodo PER 
          ON PER.ano = OC.[Ano da ocorrência] AND PER.mes = OC.[Mês da ocorrência]
        INNER JOIN dMunicipio MUNIC 
          ON MUNIC.codMunic = OC.[Cod do Município da ocorrência]
        LEFT JOIN fOcorrencias FOC
          ON FOC.idDP = DP.sk_idDP 
          AND FOC.idBPM = BPM.sk_idBPM
          AND FOC.idPeriodo = PER.sk_idPeriodo
          AND FOC.idMunic = MUNIC.sk_idMunic
        WHERE FOC.idDP IS NULL 
          AND FOC.idBPM IS NULL 
          AND FOC.idPeriodo IS NULL 
          AND FOC.idMunic IS NULL 
 	'''         


#Criando Data Frame para a carga da fato ocorrências
cargafOcorrencias = pd.read_sql(cargafOcorrencias,conexao_dw) 

#Inserir os registros
cursor_dw.executemany('''INSERT INTO fOcorrencias(idDP,idBPM,idPeriodo,regiao,idMunic,ocorrencia,somaQtd) VALUES(?,?,?,?,?,?,?)''',cargafOcorrencias.values.tolist())

#Deletar tabela temporária
#cursor_dw.execute(''' DROP TABLE tempOcorrencias''')

#Confirmar insert e encerrando a conexão DW
conexao_dw.commit()
conexao_dw.close()

#Mensagem de conslusão
print("Carga DW Fato Ocorrências finalizada!",len(cargafOcorrencias),"Registros inseridos!")

"""### Atualizando dados: Fato Ocorrências"""

#Conectar no DW
conexao_dw = sql.connect(bd_dw)

#Definir variável para manipulação
cursor_dw = conexao_dw.cursor()

#Query para atualizar Fato ocorrências:
atualizacaoFato = ''' 
        SELECT 
          OC.[Somatório da quantidade de Ocorrências],
          DP.sk_idDP,
          BPM.sk_idBPM,
          PER.sk_idPeriodo,
          MUNIC.sk_idMunic,
          OC.[Região da ocorrência],
          OC.[Ocorrência]          
        FROM tempOcorrencias OC
        INNER JOIN dDP DP 
          ON DP.codDP = OC.[Identificador do Departamento]
        INNER JOIN dBPM BPM 
          ON BPM.codBPM = OC.[Identificador do BPM]
        INNER JOIN dPeriodo PER 
          ON PER.ano = OC.[Ano da ocorrência] AND PER.mes = OC.[Mês da ocorrência]
        INNER JOIN dMunicipio MUNIC 
          ON MUNIC.codMunic = OC.[Cod do Município da ocorrência]
        LEFT JOIN fOcorrencias FOC
          ON FOC.idDP = DP.sk_idDP 
          AND FOC.idBPM = BPM.sk_idBPM
          AND FOC.idPeriodo = PER.sk_idPeriodo
          AND FOC.idMunic = MUNIC.sk_idMunic
          AND FOC.regiao = OC.[Região da ocorrência]
          AND FOC.ocorrencia = OC.[Ocorrência]
        WHERE OC.[Somatório da quantidade de Ocorrências] <> FOC.somaQtd
'''

atualizacaoFato = pd.read_sql(atualizacaoFato,conexao_dw)

#Query para alterar valores 
updateFato = '''
  UPDATE fOcorrencias
    SET somaQtd = ?
  WHERE idDP = ?
    AND idBPM = ?
    AND idPeriodo = ?
    AND idMunic = ?
    AND regiao = ?
    AND ocorrencia = ?
'''

#Aterando valores
cursor_dw.executemany(updateFato,atualizacaoFato.values.tolist())

#Confirmar update e encerrando a conexão DW
conexao_dw.commit()
conexao_dw.close()

#Mensagem de conslusão
print("Atualização DW Fato Ocorrências finalizada!",len(atualizacaoFato),"Registros atualizados!")

"""### Validar os registros inseridos DW - Dimensão Municípios"""

#Conectar no DW
conexao_dw = sql.connect(bd_dw)

#Select de validação:
select_dw = pd.read_sql('SELECT * FROM dMunicipio',conexao_dw)

#Encerrando a conexão
conexao_dw.close()

#Exibindo select de validação
select_dw

"""### Validar os registros inseridos DW - Dimensão DPs"""

#Conectar no DW
conexao_dw = sql.connect(bd_dw)

#Select de validação:
select_dw = pd.read_sql('''SELECT 
                                codDP [Código do Departamento],
                                nomeDP [Nome do Departamento],
                                enderDP [Endereço do Departamento],
                                nomeResp [Nome do Responsável] 
                           FROM dDP'''
            ,conexao_dw)

#Encerrar a conexão
conexao_dw.close()

#Exibir select de validação
select_dw

"""### Validar os registros inseridos DW - Dimensão BPM"""

#Conectar no DW
conexao_dw = sql.connect(bd_dw)

#Select de validação:
select_dw = pd.read_sql('''SELECT 
                                codBPM [Código do BPM],
                                nomeBPM [Nome do BPM],
                                enderBPM [Endereço do BPM],
                                areaBPM [Área do BPM]
                           FROM dBPM'''
            ,conexao_dw)

#Encerrar a conexão
conexao_dw.close()

#Exibir select de validação
select_dw

"""### Validar os registros inseridos DW - Dimensão dPeriodo"""

#Conectar no DW
conexao_dw = sql.connect(bd_dw)

#Select de validação:
select_dw = pd.read_sql('''SELECT 
                                sk_idPeriodo [Identificador do Período],
                                data [Data],
                                mes [Mês],
                                ano [Ano],
                                trimestre [Trimestre (T1, T2 ou T3)],
                                semestre [(S1 ou S2)]
                           FROM dPeriodo
                         '''
            ,conexao_dw)

#Encerrar a conexão
conexao_dw.close()

#Exibir select de validação
select_dw

"""### Validar os registros inseridos DW - Fato Ocorrências"""

#Conectar no DW
conexao_dw = sql.connect(bd_dw)

#Select de validação:
select_dw = pd.read_sql('''SELECT 
                                *
                           FROM fOcorrencias
                         '''
            ,conexao_dw)

#Encerrar a conexão
conexao_dw.close()

#Exibir select de validação
select_dw
