# imports necessários:
import pandas as pd
from pathlib import PosixPath 
from datetime import datetime
import sqlite3 as sql

#Endereço dos arquivos CSV:
local_arquivos = PosixPath('/content/FontesDeDados')

#Coletar dados dos municípios do Rio de Janeiro (site IBGE)
url = 'https://www.ibge.gov.br/explica/codigos-dos-municipios.php#RJ'

#Transformar os dados em um Data Frame
ibge_dados_rj = pd.DataFrame(pd.read_html(url, match='Municípios do Rio de Janeiro')[0])

#Ajustar a nomenclatura das colunas do Data Frame:
ibge_dados_rj = ibge_dados_rj.rename(columns = {'Municípios do Rio de Janeiro': 'nomeMunic', 'Códigos': 'codMunic'})

#Adicionar coluna de tempo ao Data Frame:
ibge_dados_rj['dtCarga'] = datetime.today().strftime('%d/%m/%Y %H:%M')

#Ajustar index do Data Frame para idMunic
ibge_dados_rj.index.name = 'idMunic'
#Ajustar numeração do índice, para começar em 1
ibge_dados_rj.index = ibge_dados_rj.index + 1

#Criar Data Frame com os dados das DPs
tb_log_dp = pd.DataFrame(pd.read_csv(local_arquivos / "DP.csv"))

#Ajustar a nomenclatura das colunas do Data Frame:
tb_log_dp = tb_log_dp.rename(columns = {'COD_DP': 'idDP', 'NM_DP':'nomeDP', 'Endereço':'enderDP'})

#Adicionar coluna de Data e Hora da carga no Data Frame 
tb_log_dp['dtCarga'] = datetime.today().strftime('%d/%m/%Y %H:%M')

#Criar Data Frame com os dados dos responsáveis pelas DPs
tb_log_resp_dp = pd.DataFrame(pd.read_csv(local_arquivos / "ResponsavelDP.csv"))
                                     
#Ajustar a nomenclatura das colunas do Data Frame:
tb_log_resp_dp = tb_log_resp_dp.rename(columns = {'COD_DP': 'idDP', 'Responsavel':'nomeResp'})

#Adicionar coluna de Data e Hora da carga no Data Frame 
tb_log_resp_dp['dtCarga'] = datetime.today().strftime('%d/%m/%Y %H:%M')

#Criar Data Frame com os dados dos BPM
tb_log_bpm = pd.DataFrame(pd.read_csv(local_arquivos / "BPM.csv"))
tb_log_bpm['dtCarga'] = datetime.today().strftime('%d/%m/%Y %H:%M')

#Ajustar a nomenclatura das colunas do Data Frame:
tb_log_bpm = tb_log_bpm.rename(columns = {'COD_BPM': 'idBPM', 'NM_BPM':'nomeBPM','Endereco':'enderBPM'})

#Criar Data Frame com os dados das áreas dos BPM
tb_log_area_bpm = pd.DataFrame(pd.read_csv(local_arquivos / "areaBPMv1.csv"))
tb_log_area_bpm['dtCarga'] = datetime.today().strftime('%d/%m/%Y %H:%M')

#Ajustar a nomenclatura das colunas do Data Frame:
tb_log_area_bpm = tb_log_area_bpm.rename(columns = {'aisp': 'idBPM', 'area_aisp_km2':'areaBPM'})

##Manipular sistema de arquivos:
endereco = PosixPath('/content/BasesDeDados')

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


#Conectar no ODS
conexao_ods = sql.connect(bd_ods)

#Definir variável para manipulação
cursor_ods = conexao_ods.cursor()

#Criar tabelase e índices, caso não existam:
####---------------tbLogMunic---------------####
cursor_ods.execute(

    '''
        CREATE TABLE IF NOT EXISTS "tbLogMunic" (
          "idMunic" INTEGER,
          "nomeMunic" TEXT,
          "codMunic" INTEGER,
          "dtCarga" TEXT
        )
     '''
)
cursor_ods.execute('CREATE INDEX IF NOT EXISTS idx_tbLogMunic_idMunic ON tbLogMunic(idMunic)')
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
####----------------------------------------####

#Confirmar insert e encerrando a conexão
conexao_ods.commit()
conexao_ods.close()

#Conectar no DW
conexao_dw = sql.connect(bd_dw)

#Definir variável para manipulação
cursor_dw = conexao_dw.cursor()

#Criar tabelase índices, caso não existam:
####---------------dMunicipio---------------####
cursor_dw.execute(

    '''
        CREATE TABLE IF NOT EXISTS "dMunicipio" (
          "idMunic" INTEGER,
          "codMunic" INTEGER,
          "nomeMunic" TEXT
        )

     '''
)
cursor_dw.execute('CREATE INDEX IF NOT EXISTS idx_dMunicipio_idMunic ON dMunicipio(idMunic)')
####---------------------------------------####
####------------------dDP------------------####
cursor_dw.execute(

    '''
        CREATE TABLE IF NOT EXISTS "dDP" (
          "idDP"  INTEGER PRIMARY KEY AUTOINCREMENT ,
          "nomeDP" VARCHAR(200),
          "enderDP" VARCHAR(200),
          "nomeResp" VARCHAR(200)
        )

     '''
)
cursor_dw.execute('CREATE INDEX IF NOT EXISTS idx_dDP_idDP ON dDP(idDP)')
####---------------------------------------####
####------------------dBPM------------------####
cursor_dw.execute(

    '''
        CREATE TABLE IF NOT EXISTS "dBPM" (
          "idBPM"  INTEGER PRIMARY KEY AUTOINCREMENT ,
          "nomeBPM" VARCHAR(50),
          "enderBPM" VARCHAR(200),
          "areaBPM" REAL(5,2)
        )

     '''
)
cursor_dw.execute('CREATE INDEX IF NOT EXISTS idx_dBPM_idBPM ON dBPM(idBPM)')
####----------------------------------------####

#Confirmar create e encerrar a conexão
conexao_dw.commit()
conexao_dw.close()

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

#Conectar no ODS
conexao_ods = sql.connect(bd_ods)

#Select de validação:
select_ods = 'SELECT * FROM tbLogDP ORDER BY dtCarga DESC LIMIT '+ str(len(tb_log_dp))
select_ods = pd.read_sql(select_ods,conexao_ods)

#Encerrar a conexão
conexao_ods.close()

#Exibir select de validação
select_ods

#Conectar no ODS
conexao_ods = sql.connect(bd_ods)

#Select de validação:
select_ods = 'SELECT * FROM tbLogRespDP  ORDER BY dtCarga DESC LIMIT ' + str(len(tb_log_resp_dp))
select_ods = pd.read_sql(select_ods,conexao_ods)

#Encerrar a conexão
conexao_ods.close()

#Exibir select de validação
select_ods

#Conectar no ODS
conexao_ods = sql.connect(bd_ods)

#Select de validação:
select_ods = 'SELECT * FROM tbLogBPM  ORDER BY dtCarga DESC LIMIT ' + str(len(tb_log_bpm))
select_ods = pd.read_sql(select_ods,conexao_ods)

#Encerrar a conexão
conexao_ods.close()

#Exibir select de validação
select_ods


#Conectar no ODS
conexao_ods = sql.connect(bd_ods)

#Select de validação:
select_ods = 'SELECT * FROM tbLogBPM  ORDER BY dtCarga DESC LIMIT ' + str(len(tb_log_area_bpm))
select_ods = pd.read_sql(select_ods,conexao_ods)

#Encerrar a conexão
conexao_ods.close()

#Exibir select de validação
select_ods

#Conectar no DW
conexao_dw = sql.connect(bd_dw)

#Definir variável para manipulação
cursor_dw = conexao_dw.cursor()

#Criar tabela, caso não exista:
cursor_dw.execute(

    '''
        CREATE TABLE IF NOT EXISTS "dMunicipio" (
          "idMunic" INTEGER,
          "codMunic" INTEGER,
          "nomeMunic" TEXT
        )

     '''
)

#Criar Data Frame para popular o DW
ibge_dados_rj_dw = ibge_dados_rj[['codMunic','nomeMunic']]

#Criar e/ou popular a dimensão dMunicipio com o DF ibge_dados_rj_dw
ibge_dados_rj_dw.to_sql('dMunicipio', conexao_dw, if_exists='replace')

#Confirmar insert e encerrando a conexão
conexao_dw.commit()
conexao_dw.close()

#Mensagem de conslusão
print("Carga ODS Responsáveis de DPs finalizada!",len(ibge_dados_rj_dw),"Registros inseridos!")

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
cursor_dw.executemany('''INSERT INTO dDP(idDP,nomeDP,enderDP,nomeResp) VALUES(?,?,?,?)''',dDP.values.tolist())

#Confirmar o delete, insert e encerrando a conexão DW
conexao_dw.commit()
conexao_dw.close()

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
cursor_dw.executemany('''INSERT INTO dBPM(idBPM,nomeBPM,enderBPM,areaBPM) VALUES(?,?,?,?)''',dBPM.values.tolist())

#Confirmar o delete, insert e encerrando a conexão DW
conexao_dw.commit()
conexao_dw.close()

#Conectar no DW
conexao_dw = sql.connect(bd_dw)

#Select de validação:
select_dw = pd.read_sql('SELECT * FROM dMunicipio',conexao_dw)

#Encerrando a conexão
conexao_dw.close()

#Exibindo select de validação
select_dw

#Conectar no DW
conexao_dw = sql.connect(bd_dw)

#Select de validação:
select_dw = pd.read_sql('''SELECT 
                                idDP [Identificador do Departamento],
                                nomeDP [Nome do Departamento],
                                enderDP [Endereço do Departamento],
                                nomeResp [Nome do Responsável] 
                           FROM dDP'''
            ,conexao_dw)

#Encerrar a conexão
conexao_dw.close()

#Exibir select de validação
select_dw

#Conectar no DW
conexao_dw = sql.connect(bd_dw)

#Select de validação:
select_dw = pd.read_sql('''SELECT 
                                idBPM [Identificador do BPM],
                                nomeBPM [Nome do BPM],
                                enderBPM [Endereço do BPM],
                                areaBPM [Área do BPM]
                           FROM dBPM'''
            ,conexao_dw)

#Encerrar a conexão
conexao_dw.close()

#Exibir select de validação
select_dw