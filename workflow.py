###########################
#Este código é responsável por:
# 1) Atualiar as bases por meio de request à API das SuasVendas
# 2) Organiza e cria as planilhas de pedidos, info_clientes, classificação de clientes (e ciclo de vida)
# 3) Determina quais mensagens enviar para cada cliente
# 4) Registra num log de mensagens quais (e para quem) enviar
# 5) Separa por bloco de prioridade e números de mensagens as datas de envio
# 6) Faz a conexão com a API para enviar de fato as mensagens
###########################

# import packages
import time
from datetime import date
import pandas as pd
import numpy as np
from datetime import date

#módulos para atualizar a planilha de clientes e pedidos
from pull_pedidos import atualizar_pedidos
from pull_client_files import atualizar_clientes
from pull_vendedoras import atualizar_vendedoras, atualizar_contatos, atualizar_usuarios

#módulos para criar as planilhas de suporte de produto e o de-para produto descrição
from product_sheet_creation import create_product_spreadsheet
from de_para_produtos_descricao import criar_de_para

#importar a biblioteca para criar a classificação do ciclo de vida do cliente
from classificacao_ciclo_vida import criar_classificacao

#importar a biblioteca para criar o log de mensagens do dia de hoje. Todas as funções abaixo são necessárias também
from message_loger_v2 import registrar_mensagens

#importar a biblioteca para extrair os clientes blacklist
from pull_clientes_blacklist import get_clientes_blacklist

#importar o módulo que envia as mensagens de fato
from message_sender import programar_envio

#importar o módulo para realizar o upload dos arquivos na azure
from azure_manegement import upload_to_azure, criar_backup

# region CALL THE API AND REQUEST FOR NEW VERSION OF OUR SPREADSHEETS

#Primeiro a gente atualiza a planilha de pedidos
if pd.to_datetime('today').day == 7:
#if True:
    atualizar_pedidos() #chama a API para atualizar os pedidos
    atualizar_clientes() #chama a API para atualizar os clientes
    atualizar_vendedoras() #chama a API para atualizar as vendedoras e suas informações
    atualizar_contatos() #chama a API para atualizar os contatos e suas informações
    atualizar_usuarios() #chama a API para atualizar os usuários e suas informações

# endregion

# region CLEAN THE ORDERS SHEET

# Start the clock
start = time.time()

# read the file
path = "./DBs originais/pedidos.csv"
df = pd.read_csv(path)

# Choose the right columns and also rename them
cols = ['pedi_id', 'pedi_cont_id', 'pedi_pest_id', 'pedi_itens',
        'pedi_obs', 'pedi_data_cadastro', 'ys_cola_id', 'pedi_quem_digitou']

df = df[cols]

# Rename the columns
new_cols = ['PEDIDO_ID', 'CLIENTE_ID', 'PEDIDO_STATUS', 'PEDIDO_ITENS_JSON', 'PEDIDO_OBS', 'PEDIDO_DATA_CADASTRO',
            'VENDEDOR_ID', 'USUARIO_CADASTRANTE_PEDIDO_ID']
name_dict = {}
for col in cols:
    name_dict[col] = new_cols[cols.index(col)]

df = df.rename(name_dict, axis=1)

# FIX THE DATE IN THE DATAFRAME
# 1) first fill the nans in case we have something missing and then convert to datetime
df['PEDIDO_DATA_CADASTRO'] = df['PEDIDO_DATA_CADASTRO'].fillna(0)
# 2) Apply the datatime
df['PEDIDO_DATA_CADASTRO'] = df['PEDIDO_DATA_CADASTRO'].apply(pd.to_datetime)
# 3) Filter out date older than 2018
df = df.loc[df['PEDIDO_DATA_CADASTRO'].dt.year >= 2018]

# IGNORE EMPTY CLIENTE_ID VALUES AND EMPTY JSONs
df = df.loc[~df['CLIENTE_ID'].isna()]  # byte-wise operation
df = df.loc[~df['PEDIDO_ITENS_JSON'].isna()]  # byte-wise operation

def cast_price(x):
    # Pandas apply function to extract order cost from the json information
    jsons = eval(x['PEDIDO_ITENS_JSON'])  # convert that cell into an actual list of dictionaries
    cost = 0
    for json in jsons:
        product_cost = json['peit_preco']
        product_qty = json['peit_qtde']
        cost += product_cost * product_qty
    x['VALOR_PEDIDO'] = cost
    return x

def cast_items(x):
    # Pandas apply function to extract items ordered from the json
    jsons = eval(x['PEDIDO_ITENS_JSON'])  # convert that cell into an actual list of dictionaries
    items = []
    for json in jsons:
        items.append(json['peit_prod_id'])
    x['produto_id'] = items
    return x

# SPLIT BY DATE, SEMESETER AND TRIMESTER
# SEM1-2018, SEM2-2018, SEM1-2019, etc.
def cast_semester(x):
    # Pandas apply function to instate the rules stated above
    quarter = x['PEDIDO_DATA_CADASTRO'].quarter
    if quarter < 3:
        x['PERIODO'] = str(x['PEDIDO_DATA_CADASTRO'].year) + '-S1'
        return x
    else:
        x['PERIODO'] = str(x['PEDIDO_DATA_CADASTRO'].year) + '-S2'
        return x

def cast_trimester(x):
    # Pandas apply function to cast the trimester of the year
    quarter = x['PEDIDO_DATA_CADASTRO'].quarter
    x['TRIMESTRE'] = str(x['PEDIDO_DATA_CADASTRO'].year) + '-T' + str(quarter)
    return x

pedidos = df.apply(cast_price, axis=1)
pedidos = pedidos.apply(cast_items, axis=1)
pedidos = pedidos.apply(cast_semester, axis=1)
pedidos = pedidos.apply(cast_trimester, axis=1)

#Renomear as colunas para transformar de maiúsculos para minúsculos conforme acordado com o Tomás
pedidos.columns = pedidos.columns.str.lower()
pedidos = pedidos.rename({"pedidos_id_itens":"produto_id"}, axis=1)

pedidos.to_excel('./Editadas/PLANILHA-PEDIDOS-CAPELLI.xlsx')

#as duas funções a seguir são executadas uma vez por mês, a cada dia 1.
if True:
    print('Atualizando planilhas de produtos e suportes.')
    #Criamos a planilha suporte de pedidos. Esta planilha contém valores, produtos, códigos de produtos, qtde, etc.
    planilha_pedidos_de_para = create_product_spreadsheet('./Editadas/PLANILHA-PEDIDOS-CAPELLI.xlsx')
    planilha_pedidos_de_para.to_excel('./Editadas/TABELA-SUPORTE-PEDIDOS-V1.xlsx')

    #Agora criamos a planilha DE-PARA de produtos e suas descrições
    
    descricao_produtos = criar_de_para('./Editadas/planilha-pedidos-e-descricao-v1.csv')
    descricao_produtos.to_excel('./Editadas/TABELA-DE-PARA-PRODUTOS-DESCRICAO-V1.xlsx')

end = time.time()
print("The orders spreadsheet was cleaned in {} seconds".format(end - start))

# endregion

# region CLEAN CLIENTES

# start the time
start = time.time()

# read the file
path = "./DBs originais/clientes.csv"
df = pd.read_csv(path)

# Drop some cols which are clearly useless
df = df.drop(['cont_usa_grades',
              'cont_usa_cores',
              'cont_casas_decimais',
              'cont_tipo',
              'ys_cola_id',
              'ys_datahora',
              'excluido'], axis=1)

# Drop the last columns, which are most NaNs
other_cols = ['ys_datahora_atualizacao', 'cont_telefone2','cont_rg','cont_sexo','cont_estado_civil',
              'cont_obs','cont_fax','cont_cont_id','cont_website','cont_suframa','cont_inscricao_estadual']
df = df.drop(other_cols, axis=1)

# Fix the strings. Remove special chars, everybody upper. Remove "-".
replacer = {
    "Ã": "A",
    "Â": "A",
    "À": "A",
    "Á": "A",
    "Ä": "A",
    "É": "E",
    "È": "E",
    "Ê": "E",
    "Ë": "E",
    "Í": "I",
    "Ì": "I",
    "Î": "I",
    "Ï": "I",
    "Ó": "O",
    "Ò": "O",
    "Ô": "O",
    "Ö": "O",
    "Õ": "O",
    "Ú": "U",
    "Ù": "U",
    "Û": "U",
    "Ü": "U",
    "Ç": "C",
    "-": " ",
    ",": "",
    "'": "",
    "\.": ""
}

COLS_TEXT = ['cont_nome_fantasia', 'cont_razao_social', 'cont_endereco',
             'cont_bairro', 'cont_cidade', 'cont_uf', 'cont_cidade']

for col in COLS_TEXT:
    df[col] = df[col].fillna('')
    df[col] = df[col].apply(lambda x: x.upper())
    df[col].replace(replacer, regex=True, inplace=True)

# FIX THE NUMERIC COLS
replacer = {
    "\-": "",
    "\.": "",
    ",": "",
    "\_": "",
    "/": "",
    " ": "",
    "\(": "",
    "\)": "",
    "\+": "",
    "\*": "",
    "\?": "",
    'whats': "",
    "ou": "",
}

COLS_NUMERIC = ['cont_cep', 'cont_cnpj_cpf', 'cont_telefone','cont_codigo']

for col in COLS_NUMERIC:
    df[col] = df[col].str.split("/", expand=True)[0]
    df[col].replace(replacer, regex=True, inplace=True)
    df[col] = df[col].fillna('0')

# REPLACE CEPs WITH LESS THAN 8 CHARS
col = 'cont_cep'
df[col] = df[col].apply(lambda x: 0 if len(x) < 8 else x)
# note on CEP: it cannot be number, because if the CEP starts with a 0, by
# converting to numeric it erases that number.

# FIX TELEPHONE NUMBERS
col = 'cont_telefone'
#FIX TESTE COMENTANDO O CÓDIGO ABAIXO REMOVENDO OS ESPAÇOS EM BRANCO
#df = df.loc[df[col] != ""]
try:
    df[col] = df[col].apply(lambda x: x[1:] if int(x[0]) == 0 else x)
    df[col] = df[col].apply(lambda x: '0' if len(x) < 8 else x)
except IndexError:
    pass

"""
#SECTION 1) CREATE TO ADD THE DDD CSVs AND RUN THAT TO THE COLUMN
"""

# CREATE A COLUMN WITH THE CITY AND THE ADDRESS FOR THE DDD
ddd_1 = pd.read_csv('./DBs originais/ddd_new_version_complete.csv')
ddd_1 = ddd_1.T.iloc[1:, :1]
ddd_1 = ddd_1.rename({"Index": "Cidade", 0: "DDD"}, axis=1)
ddd_2 = pd.read_csv('./DBs originais/ddd_complete.csv')
ddd_2 = ddd_2.T.iloc[1:, :1]
ddd_2 = ddd_2.rename({"Index": "Cidade", 0: "DDD"}, axis=1)

ddds = [ddd_1, ddd_2]
ddd = pd.concat(ddds)
ddd['CIDADE-ESTADO'] = ddd.index
ddd = ddd.reset_index()
ddd = ddd.drop(['index'], axis=1)
ddd = ddd.drop_duplicates(keep='first')

# CREATE A COLUMN WITH THE CITY AND STATE TO THEN REFERENCE BY THE DDD AND FIX THE NUMBER
df['CIDADE-ESTADO'] = df['cont_cidade'] + '-' + df['cont_uf']

# APPEND BY CIDADE-ESTADO COMBINATION
df = df.merge(ddd, on='CIDADE-ESTADO', how='left')

# FILL THE NANS with ''
df['DDD'] = df['DDD'].fillna(0)

# CONVERT THE DDDs to NUMBER
df['DDD'] = df['DDD'].astype(int)

# RULES TO FIX THE PHONE NUMBER
# Brazilian rules for phone numbers are:
# first digit 2 - 5: landline
# other: cellphone
# Our rules will be:
# If number has 8 digits and first digit < 6:
# Put just DDD ahead (ASSUME landline)
# If number has 8 digits and first digit >= 6:
# Put DDD + '9' (ASSUME CELLPHONE)
# If number has 9 digits and first digit = 9:
# Put DDD (ASSUME CELLPHONE)
# If number has 9 digits and first digit < 6:
# Broken number
# If number has 10 digits and 3rd digit > 6:
# Add a '9' as the third digit (ASSUME CELLPHONE)
# Else:
# Keep it
# If number has 11 digits and third digit > 6:
# Keep it
# If number has 11 digits and third digit < 6:
# Broken
# If 12 number and first is '5' and fifth is > 6:
# Remove the '55'
# Add '9' as 5th number (ASSUME CELLPHONE)
# If 12 number and first is '5' and fifth < 6:
# Remove '55'

cols = df.columns.tolist()

def fix_phone_numbers(x):
    _phone = x['cont_telefone']  # store the phone number
    _ddd = x['DDD']  # store the ddd which might or might not be used
    if len(_phone) == 8:  # if len is equal to landline or wrong phone number
        if int(_phone[0]) < 6:  # is landline
            if _ddd != "" and _ddd != None and _ddd != 0:  # ddd for the area exists
                x['cont_telefone'] = str(_ddd) + str(_phone)  # return fixed number with ddd
                x['tag_telefone'] = '8 digits - landline'
                return x
            else:
                x['tag_telefone'] = '8 digits - broken'
                return x
        else:  # cellphone number
            if _ddd != "" and _ddd != None and _ddd != 0:  # ddd for the area exists
                x['cont_telefone'] = str(_ddd) + '9' + str(_phone)  # return fixed number with ddd
                x['tag_telefone'] = '8 digits - cellphone'
                return x
            else:
                x['tag_telefone'] = '8 digits - broken'
                return x
    elif len(_phone) == 9:
        if int(_phone[0]) < 6:  # broken number
            x['tag_telefone'] = '9 digits - broken'
            return x  # definitely broken
        else:
            if _ddd != "" and _ddd != None and _ddd != 0:  # ddd for the area exists
                x['cont_telefone'] = str(_ddd) + str(_phone)  # return fixed number with ddd
                x['tag_telefone'] = '9 digits - cellphone'
                return x
            else:
                x['tag_telefone'] = '9 digits - broken'
                return x
    elif len(_phone) == 10:
        if int(_phone[2]) >= 6:  # cellphone without the '9' digit but with DDD
            x['cont_telefone'] = _phone[:2] + '9' + _phone[2:]  # return fixed number with ddd
            x['tag_telefone'] = '10 digits - cellphone'
            return x
        else:
            x['tag_telefone'] = '10 digits - landline'
            return x
    elif len(_phone) == 11:
        if int(_phone[2]) == 9:
            x['cont_telefone'] = _phone
            x['tag_telefone'] = '11 digits - cellphone'
            return x
        else: # broken number
            x['tag_telefone'] = '11 digits - broken'
            return x
    elif len(_phone) == 12:
        if int(_phone[3]) == 9:
            x['cont_telefone'] = _phone[:3] + _phone[4:]
            x['tag_telefone'] = '11 digits - cellphone'
            return x
        else:
            x['tag_telefone'] = '12+ digits - broken'
            return x
    elif len(_phone) == 13:
        if int(_phone[0]) == 5:
            x['cont_telefone'] = _phone[2:]  # return fixed number with ddd
            x['tag_telefone'] = '12+ digits fixed'
            return x
    else:
        x['tag-telefone'] = 'broken-small-len'
        return x

df = df.apply(fix_phone_numbers, axis=1)  # apply the function
tag = ['tag_telefone'] #uma coluna de tag para indicar se o número está quebrado ou não
cols.extend(tag) #adiciona essa coluna às colunas de interesse no dataframe
df = df[cols] #de fato seleciona as colunas de interesse

df['tag_telefone'] = df['tag_telefone'].fillna('broken-fillna') #tudo que não tiver tag pré-estabelecida está quebrada
#FIX ORIGINAL CODE BELOW COMMENTED OUT
#clientes_wrong = df.loc[df['tag_telefone'].str.contains('broken')]  # separate a df with all the broken values
#clientes_correct = df.loc[~df['tag_telefone'].str.contains('broken')]  # we will only work with the correct ones

# Create a small version of the correct, containing only id and phone number for the groupby
cols = ['cont_id', 'cont_telefone'] #ids únicos e telefones
#FIX clientes_correct = clientes_correct[cols] ORIGINAL
clientes_correct = df[cols] #separa os clientes com números não-quebrados

# COUNT HOW MANY TIMES DOES THE CELLPHONE NUMBER REPEAT
group = clientes_correct.groupby(['cont_telefone'])['cont_telefone'].count().reset_index(name='contagem_telefone') #agrupa os clientes corretos por contagem de telefones
group = group.merge(clientes_correct, on='cont_telefone', how='left') #faz o append no df original
cols = ['cont_telefone', 'cont_id', 'contagem_telefone']
group = group[cols]
group = group.rename({"cont_telefone": "telefone_cliente", "cont_id": "cliente_id"}, axis=1)
group.to_excel('./Editadas/AGRUPADO-POR-CLIENTES.xlsx')

# Reread the files
agrupado = pd.read_excel('./Editadas/AGRUPADO-POR-CLIENTES.xlsx')
clientes = pd.read_excel('./Editadas/PLANILHA-PEDIDOS-CAPELLI.xlsx')

agrupado_clientes = clientes.groupby(['cliente_id'])['cliente_id'].count().reset_index(name='contagem_pedidos')

agrupado = agrupado.merge(agrupado_clientes, on='cliente_id', how='left')

agrupado.to_excel('./Editadas/PEDIDO-POR-CLIENTE-AGRUPADO-ID.xlsx')

# THIS SECTION IS TO IMPORT THE CLIENTS ID AND CREATE A NEW FILE CONTAINING THE CLIENT INFORMATION
# NAME, ADDRESS, STUFF LIKE THAT. ALSO ADD THE LATEST ORDER FOR EACH CLIENT ID AND FIRST ORDER
clientes = pd.read_excel('./Editadas/PEDIDO-POR-CLIENTE-AGRUPADO-ID.xlsx')
from cliente_appender import selecionar_clientes_acima_id_max #chamar a função para selecionar apenas as colunas de interesse
infos_clientes = selecionar_clientes_acima_id_max() #chama a função que retorna a planilha de clientes já com os números que temos de interesse
pedidos = pd.read_excel('./Editadas/PLANILHA-PEDIDOS-CAPELLI.xlsx')

# Ignore pedidos with value = 0
pedidos = pedidos.loc[pedidos['valor_pedido'] > 0]

# Drop that stupid index column which gets created when saving a new file
try: #caso não tenha a coluna, simplesmente avança na lógica
    clientes = clientes.drop(['Unnamed: 0'], axis=1)
except KeyError:
    pass

# Select the relevant information from the clientes database
cols = ['cont_id',
        'cont_nome_fantasia',
        'cont_razao_social',
        'cont_endereco',
        'cont_numero',
        'cont_bairro',
        'cont_cep',
        'cont_cidade',
        'cont_uf',
        'cont_cnpj_cpf',
        'cont_email',
        'cont_codigo']

infos_clientes = infos_clientes[cols]  #rearranja o dataframe pra retornar somente as colunas acima (sem número de telefone)
infos_clientes = infos_clientes.rename({'cont_id': 'cliente_id'}, axis=1) #renomeio para estar de acordo com os outros arquivos

merged_clientes_infos = clientes.merge(infos_clientes, on='cliente_id') #faz o merge entre os clientes e as informações complementares limpas

group_pedidos = pedidos.groupby(['cliente_id']).agg({"pedido_data_cadastro": ["max", "min"]}).reset_index()
group_pedidos.columns = group_pedidos.columns.droplevel(0)
group_pedidos = group_pedidos.rename({"": "cliente_id", "max": "pedido_mais_recente", "min": "primeiro_pedido"}, axis=1)

# Merge the groupby with the clients informations dataframe
merged_clientes_infos = merged_clientes_infos.merge(group_pedidos, on='cliente_id', how='left')

# Create flag to indicate how long since the first purchase
merged_clientes_infos['pedido_mais_recente'] = merged_clientes_infos['pedido_mais_recente'].apply(pd.to_datetime)
merged_clientes_infos["primeiro_pedido"] = merged_clientes_infos["primeiro_pedido"].apply(pd.to_datetime)
merged_clientes_infos['dias_desde_ult_compra'] = np.datetime64('today') - merged_clientes_infos['primeiro_pedido']
merged_clientes_infos['dias_desde_ult_compra'] = merged_clientes_infos['dias_desde_ult_compra'].dt.days

# Create flags to indicate many days until the client's yearly anniversary since first purchase
def cast_flag_first_buy(x):
    # PANDAS APPLY FUNCTION TO CAST THE FIRST BUY FLAG AND CALCULATE NEXT ANNIVERSARY SINCE
    try:
        today = pd.to_datetime('today')
        year_diff = today.year - x.year
        anniversary = x + pd.offsets.DateOffset(years=year_diff)
        if today >= anniversary:
            anniversary = x + pd.offsets.DateOffset(years=year_diff + 1)
            return (anniversary - today).days
        else:
            return (anniversary - today).days
    except:
        return None

#Executar a função criada
merged_clientes_infos['dias_ate_aniversario_compra'] = merged_clientes_infos['primeiro_pedido'].apply(cast_flag_first_buy)

# Slice the pedidos dataframe to keep only purchaes from up to 1-year ago.
pedidos['pedido_data_cadastro'] = pedidos['pedido_data_cadastro'].apply(pd.to_datetime)
pedidos_recentes = pedidos.loc[
    pedidos['pedido_data_cadastro'] >= (pd.to_datetime('today') - pd.offsets.DateOffset(days=365))]
# Now groupby with average order value
pedidos_agrupados = pedidos_recentes.groupby(['cliente_id']).agg(
    {"valor_pedido": "mean", "cliente_id": "count"}).rename(columns={'cliente_id':'freq_12_meses'}).reset_index()

# Now merge it back to the client information dataframe
merged_clientes_infos = merged_clientes_infos.merge(pedidos_agrupados, on='cliente_id', how='left')

#Rename the column of valor_pedido to the new one
pedidos_agrupados = pedidos_agrupados.rename({"valor_pedido":"ticket_medio_12_meses"}, axis=1)

# Spreadsheet to excel
merged_clientes_infos.to_excel('./Editadas/PLANILHA-INFORMACOES-CLIENTES.xlsx')

end = time.time()
print("The client info datasheet ran in {} seconds".format(end - start))

# endregion

# region CORRRECT PHONE NUMBERS WHICH ARE DOUBLE AT THE INFO

start = time.time()

# region SEND BIRTHDAY MESSAGES
cliente_infos = pd.read_excel('./Editadas/PLANILHA-INFORMACOES-CLIENTES.xlsx')

# Ignore the cols where there are no orders or first orders
cols = ['PEDIDO_MAIS_RECENTE', 'PRIMEIRO_PEDIDO', 'FREQ_12_MESES']
cols = [x.lower() for x in cols] #converte todas as colunas acima em minúsculas para estarem de acordo com a nova nomenclatura
cliente_infos = cliente_infos[~cliente_infos[cols].isna().all(axis=1)] #ignora as linhas completamente vazias

# Fill the empties with 'empty' as a marker
cliente_infos = cliente_infos.fillna('empty')

# turn everything upper so we can compare
cliente_infos['cont_nome_fantasia'] = cliente_infos['cont_nome_fantasia'].apply(lambda x: x.upper())

# WE MAY NEED TO DO THIS ON THE INFO DATABASE AT ONCE

# Get numbers which are duplicated (double registry) and slice them out
index_duplicados = cliente_infos.duplicated(subset='telefone_cliente', keep=False) #agarra os duplicados 
index_duplicados = index_duplicados.loc[index_duplicados == True].index.tolist()
cliente_duplicados = cliente_infos.loc[cliente_infos.index.isin(index_duplicados)]

# Select all the columns from the dataframe and then pop the elements you don't want
cols = cliente_duplicados.columns
first_cols = cols[:-6]  # Leave out the calculated columns
last_cols = cols[-2:]

# Replace with a character dictionary
replacer = {
    "À": "A",
    "Á": "A",
    "Â": "A",
    "Ã": "A",
    "É": "E",
    "È": "E",
    "Ê": "E",
    "Í": "I",
    "Ì": "I",
    "Î": "I",
    "Ó": "O",
    "Ò": "O",
    "Ô": "O",
    "Ú": "U",
    "Ù": "U",
    "Û": "U",
    "Ç": "C",
    "\-": "",
    "\.": "",
}

cliente_duplicados = cliente_duplicados.replace(replacer, regex=True)

# Now we apply the following logic.
# 1) Apply this to all numbers which are the same
# 2) If Name1 == Name2, fill the columns that are empty with each other's informations and then select the newest
# 3) If Name!=Name2, try CPF, Razão Social and then CEP
# End

# Just sort it out before
cliente_duplicados = cliente_duplicados.sort_values(['telefone_cliente', 'pedido_mais_recente'], ascending=False)

# Create a global variable because otherwise this won't work
counter = 1

def fill_nans_and_select_rows(x, col_list, last_col_list, cliente_input):
    # Pandas apply function to fill empty nans for duplicated rows and select the most recent row.
    # Input is the own dataframe and we fix columns inside.
    global counter
    copy_df = cliente_input.copy()
    phone_slicer = x['telefone_cliente']  # get the phone
    name_slicer = x['cont_nome_fantasia']  # get the name
    address_slicer = x['cont_endereco']  # get the address
    cpf_slicer = x['cont_cnpj_cpf']  # get the cnpj/cpf number
    cep_slicer = x['cont_cep']
    x_dupli = copy_df.loc[copy_df['telefone_cliente'] == phone_slicer]  # create a DF with only the duplicated numbers
    x_dupli = x_dupli.loc[x_dupli['telefone_cliente'] == phone_slicer]  # slice to have the elements with the same phone
    x_dupli = x_dupli.loc[
        (x_dupli['cont_nome_fantasia'] == name_slicer) | (x_dupli['cont_endereco'] == address_slicer) | (
                x_dupli['cont_cnpj_cpf'] == cpf_slicer) | (x_dupli['cont_cep'] == cep_slicer)]
    for col in col_list:  # loop through all the elements to see if they're empty
        if x[col] == 'empty':  # check if it was empty
            row_numbers = x_dupli.shape[0]  # get the number of rows so we can loop through it
            for row in range(row_numbers):  # loop through it
                _inner_x = x_dupli.iloc[row:(row + 1),
                           x_dupli.columns.get_loc(col)]  # get the column at the specific row we want it
                if _inner_x.iloc[0] == 'empty':  # check if that cell is empty as well
                    pass  # if yes, pass to next iteration
                else:
                    x[col] = _inner_x.iloc[0]  # get that result and paste it on our df
        else:  # wasn't empty, so we want to fill the lines below with it
            row_numbers = x_dupli.shape[0]  # get the number of rows so we can loop through it
            for row in range(row_numbers):  # loop through it
                _inner_x = x_dupli.iloc[row:(row + 1),
                           x_dupli.columns.get_loc(col)]  # get the column at the specific row we want it
                if _inner_x.iloc[0] == 'empty':  # check if that cell is empty as well
                    _inner_x.iloc[0] = x[col]

    # I am really terribly sorry for the next 5 lines of code,
    # but I had to repeat the df copying, otherwise it wouldn't work.
    # We'll refactore it later on.
    copy_df = cliente_input.copy()
    copy_df = copy_df.iloc[counter:, :]
    x_dupli = copy_df.loc[copy_df['telefone_cliente'] == phone_slicer]  # create a DF with only the duplicated numbers
    x_dupli = x_dupli.loc[x_dupli['telefone_cliente'] == phone_slicer]  # slice to have the elements with the same phone
    x_dupli = x_dupli.loc[
        (x_dupli['cont_nome_fantasia'] == name_slicer) | (x_dupli['cont_endereco'] == address_slicer) | (
                x_dupli['cont_cnpj_cpf'] == cpf_slicer) | (x_dupli['cont_cep'] == cep_slicer)]
    for col in last_col_list:  # loop through all the elements to see if they're empty
        if x[col] == 'empty':  # check if it was empty
            x[col] = 0  # convert that 'empty' to a numeric column
            row_numbers = x_dupli.shape[0]  # get the number of rows so we can loop through it
            for row in range(1, row_numbers):  # loop through it
                _inner_x = x_dupli.iloc[row:(row + 1),
                           x_dupli.columns.get_loc(col)]  # get the column at the specific row we want it
                if _inner_x.iloc[0] == 'empty':  # check if that cell is empty as well
                    _inner_x.iloc[0] = 0
                else:
                    x[col] += _inner_x.iloc[0]  # get that result and paste it on our df
        else:  # the value is not empty, then sum it
            row_numbers = x_dupli.shape[0]  # get the number of rows so we can loop through it
            for row in range(row_numbers):  # loop through it
                _inner_x = x_dupli.iloc[row:(row + 1),
                           x_dupli.columns.get_loc(col)]  # get the column at the specific row we want it
                if _inner_x.iloc[0] == 'empty':  # check if that cell is empty as well
                    _inner_x.iloc[0] = 0
                else:
                    x[col] += _inner_x.iloc[0]  # get that result and paste it on our df

    # Now we need to turn all columns equals. First we'll get the ones that we are sure are the same.
    # Again, I'm really sorry for the next 5 lines of code.
    copy_df = cliente_input.copy()
    x_dupli = copy_df.loc[copy_df['telefone_cliente'] == phone_slicer]  # create a DF with only the duplicated numbers
    x_dupli = x_dupli.loc[x_dupli['telefone_cliente'] == phone_slicer]  # slice to have the elements with the same phone
    x_dupli = x_dupli.loc[
        (x_dupli['cont_nome_fantasia'] == name_slicer) | (x_dupli['cont_endereco'] == address_slicer) | (
                x_dupli['cont_cnpj_cpf'] == cpf_slicer) | (x_dupli['cont_cep'] == cep_slicer)]
    for col in col_list:  # loop through all the elements to see if they're empty
        if x[col] == 'empty':  # check if it was empty
            row_numbers = x_dupli.shape[0]  # get the number of rows so we can loop through it
            for row in range(row_numbers):  # loop through it
                _inner_x = x_dupli.iloc[row:(row + 1),
                           x_dupli.columns.get_loc(col)]  # get the column at the specific row we want it
                if _inner_x.iloc[0] == 'empty':  # check if that cell is empty as well
                    pass  # if yes, pass to next iteration
                else:
                    x[col] = _inner_x.iloc[0]  # get that result and paste it on our df
        else:  # wasn't empty, so we want to fill the lines below with it
            row_numbers = x_dupli.shape[0]  # get the number of rows so we can loop through it
            for row in range(row_numbers):  # loop through it
                _inner_x = x_dupli.iloc[row:(row + 1),
                           x_dupli.columns.get_loc(col)]  # get the column at the specific row we want it
                if _inner_x.iloc[0] == 'empty':  # check if that cell is empty as well
                    _inner_x.iloc[0] = x[col]
    counter += 1
    return x

# Apply the function
cliente_dupli_editados = cliente_duplicados.apply(fill_nans_and_select_rows,
                                                  args=(first_cols, last_cols, cliente_duplicados), axis=1)

# select only the very first entry of each row
drop_clientes = cliente_dupli_editados.drop_duplicates(subset='telefone_cliente', keep='first')

# Now get a df containing only id and phone number. Also convert index to column so we can use it later
ids_e_telefones = cliente_dupli_editados.loc[:, ['Unnamed: 0', 'telefone_cliente', 'cliente_id']]

# And now merge them
cliente_final = drop_clientes.merge(ids_e_telefones, on="telefone_cliente")

# Drop the first id column which has the same values and make the id_y column the real one
cliente_final = cliente_final.drop(['cliente_id_x', 'Unnamed: 0_x'], axis=1)
cliente_final = cliente_final.rename({"cliente_id_y": "cliente_id", "Unnamed: 0_y": "Index"}, axis=1)
cliente_final = cliente_final.set_index('Index')

final_cols = ['telefone_cliente', 'cliente_id', 'contagem_telefone', 'contagem_pedidos',
              'cont_nome_fantasia', 'cont_razao_social', 'cont_endereco',
              'cont_numero', 'cont_bairro', 'cont_cep', 'cont_cidade', 'cont_uf',
              'cont_cnpj_cpf', 'cont_email', 'cont_codigo','pedido_mais_recente', 'primeiro_pedido',
              'dias_desde_ult_compra', 'dias_ate_aniversario_compra', 'valor_pedido',
              'freq_12_meses']

# Rearrange the order of the columns
cliente_final = cliente_final[final_cols]

# Replace the rows with the newly calculated rows
cliente_infos.loc[cliente_final.index, :] = cliente_final

# Drop the Unnamed column which is the index and will be recreated once the sheet is saved again
cliente_infos = cliente_infos.drop(["Unnamed: 0"], axis=1)

#Rename the columns
cliente_infos = cliente_infos.rename({"cont_nome_fantasia":"nome",
                                    "cont_razao_social":"razao_social",
                                    "cont_email":"email",
                                    "cont_cnpj_cpf":"cnpj_cpf",
                                    "cont_endereco":"endereco",
                                    "cont_numero":"numero",
                                    "cont_bairro":"bairro",
                                    "cont_cidade":"cidade",
                                    "cont_uf":"uf",
                                    "cont_cep":"cep"}, axis=1)

#Adicionado esta parte para apenas acrescentar linhas ao dataframe original, não re-rodar a base.
#primeiro lemos a planilha consolidada.
clientes_consolidados = pd.read_excel('./Editadas/INFORMACOES-CONSOLIDADAS-CLIENTES-FINAL.xlsx')
#Criamos uma coluna de chave para comparar telefone+nome dos clientes consolidados e dos novos.
clientes_consolidados['chave-nome-telefone'] = clientes_consolidados['telefone_cliente'].astype(str) + clientes_consolidados['nome']
cliente_infos['chave-nome-telefone'] = cliente_infos['telefone_cliente'].astype(str) + cliente_infos['nome']
#Pegamos a lista dos clientes já existentes na nossa base de dados
lista_clientes_consolidados = clientes_consolidados['chave-nome-telefone'].unique().tolist()
lista_clientes_novos = cliente_infos['chave-nome-telefone'].unique().tolist()
#print(lista_clientes_consolidados[:10])
#print(lista_clientes_novos[:10])
#Pegamos os clientes que não estão na base de dados
cliente_infos = cliente_infos.loc[~cliente_infos['chave-nome-telefone'].isin(lista_clientes_consolidados)]
#Criar um ID único para todos os clientes primeiro baseado no telefone.
def create_unique_id(telephone, counter_num):
    #python function to contain the logic behind unique id creation
    length = (6 - len(str(counter_num)))*'0'
    return 'ZETA'+length+str(counter_num)

telefones = cliente_infos['telefone_cliente'].unique().tolist() #pegamos os valores únicos para estes telefones e clientes e associamos um código único a eles
unique_id = {} #dicionário vazio para popularmos e depois criarmos o dataframe
#contador pega o ID único máximo já existente na base de dados atual e conta a partir dele
counter = clientes_consolidados['id_unicos'].str.replace('ZETA','') #remove a parte escrita da chave
counter = counter.apply(lambda x: int(x)) #converte em numérico o restante
counter = counter.max() #pega o número máximo
counter += 1 #adiciona mais um para reiniciar a contagem do número
for telefone in telefones:
    unique_id[telefone] = create_unique_id(telefone, counter) #implementa a funão criada acima
    counter += 1

df_id_unicos = pd.DataFrame(list(unique_id.items()),columns = ['telefone_cliente','id_unicos']) #criar o dataframe para ser possível realizar o merge

#Fazer o merge do df de info de clientes e o de id unico na chave telefone_cliente. Mas, primeiro verifica se o shape é > 1:
if df_id_unicos.shape[0] > 0:
    #Substituir empty por 0, fazer fillna por 0 e qualquer coisa não numérica por 0
    cliente_infos = pd.merge(cliente_infos, df_id_unicos, on='telefone_cliente', how='left')
else:
    pass

#É necessário adicionar o '55' a todos os telefones da lista de cliente_infos para ser possível enviar mensagens a eles
#primeiro preenchemos os vazios com 0, caso tenha algum
cliente_infos['telefone_cliente'] = cliente_infos['telefone_cliente'].fillna(0)
#realizamos a substituição de possíveis valores 'empty' para 0 também
cliente_infos['telefone_cliente'] = cliente_infos['telefone_cliente'].replace('empty',0)
#agora adicionamos o '55' como string primeiro ao número de telefone do cliente (por enquanto como string também)
cliente_infos['telefone_cliente'] = '55' + cliente_infos['telefone_cliente'].astype(str)
#E agora reconvertemos para numérico
cliente_infos['telefone_cliente'] = cliente_infos['telefone_cliente'].astype(float)

#Unimos as duas bases de dados
print(f'Shape antes {clientes_consolidados.shape}')
clientes_consolidados = pd.concat([clientes_consolidados, cliente_infos])
print(f'Shape depois {clientes_consolidados.shape}')

#Consertamos e atualizamos as colunas de compra recente e primeira compra
#uma para criar um groupby de vendas por código zeta e a outra para realizar o merge em si
from atualizador_info_clientes import atualiza_vendas, merge_vendas, atualizar_freq_e_vendas
clientes_consolidados = clientes_consolidados.copy() #uma cópia para testar
clientes_consolidados = merge_vendas(clientes_consolidados, atualiza_vendas()) #chama a função
clientes_consolidados = atualizar_freq_e_vendas(clientes_consolidados)
unnamed_cols = ['Unnamed: 0','Unnamed: 0.1','Unnamed: 0.1.1','Unnamed: 0.1.1.1','Unnamed: 0.1.1.1.1']
for cols in unnamed_cols:
    try: #check if the column is in the dataframe
        clientes_consolidados = clientes_consolidados.drop(cols, axis=1)
    except:
        pass #if not, just pass
clientes_consolidados.to_excel('./Editadas/INFORMACOES-CONSOLIDADAS-CLIENTES-FINAL.xlsx', index=False) #cria um arquivo teste para verificar se funcionou

#Criar o DE-PARA de id clientes por id unico zeta.
de_para = clientes_consolidados[['id_unicos','cliente_id']] #para realizarmos o de-para
cliente_consolidado_para_producao = clientes_consolidados.drop('cliente_id', axis=1) #ignoramos a coluna de id duplicado
cliente_consolidado_para_producao = cliente_consolidado_para_producao.drop_duplicates(subset='id_unicos', keep='first') #dropar duplicados
de_para.to_excel('./Editadas/DE-PARA-ID-ZETA-ID-SUAS-VENDAS-V1.xlsx')
cliente_consolidado_para_producao.to_excel('./Editadas/INFORMACOES-CLIENTES.xlsx')

end = time.time()
print("The duplicates were eliminated in {} seconds".format(end - start))

# endregion

# region PASS THE PHONES (PKEYS) AS REFERENCE TO THE NUMBERS AND THEN GROUP BY IT TO CORRECTLY CLASSIFY THEM

# start time
start = time.time()

# Read the orders file and the new client info file
path = "./Editadas/PLANILHA-PEDIDOS-CAPELLI.xlsx"
pedidos = pd.read_excel(path)
path = './Editadas/INFORMACOES-CONSOLIDADAS-CLIENTES-FINAL.xlsx'
cliente_infos = pd.read_excel(path)

# Get only phone number and IDs from the client infos
cols = ['id_unicos', 'cliente_id']
cliente_infos = cliente_infos[cols]

# Merge at the orders spreadhseet
pedidos = pedidos.merge(cliente_infos, on='cliente_id')

# Now get rid of the client id information
pedidos = pedidos.drop(['cliente_id'], axis=1)

# Create a new sheet just to classify them
pedidos.to_excel('./Editadas/REGISTRO-VENDAS.xlsx')

# end it
end = time.time()
print(f"Orders were rearranged in {end - start}")

# endregion

# region CLASSIFY ALL THE THINGS WE WANT TO CLASSIFY

start = time.time()

criteria_dict = {
    4000: 'DIAMANTE',
    2800: 'OURO',
    1400: 'PRATA',
    1: 'BRONZE',
    0: 'INATIVO',
}
datas_fechamento = {
    '2018-S2': '31/12/2018',
    '2019-S1': '30/06/2019',
    '2019-S2': '31/12/2019',
    '2020-S1': '30/06/2020',
    '2020-S2': '31/12/2020',
    '2021-S1': '30/06/2021',
    '2021-S2': '31/12/2021'
}

# 9300 - A - Aplicação Along Hair Professional (ALONGAMENTO)
# 9304 - M - Manutenção Along Hair Professional (MANUTENÇÃO)
# outros - D - diversos

pedidos = pd.read_excel('./Editadas/REGISTRO-VENDAS.xlsx')
pedidos["valor_pedido"] = pedidos["valor_pedido"].replace(',', '.', regex=True)
pedidos["valor_pedido"] = pedidos["valor_pedido"].apply(pd.to_numeric)

clientes_ids = list(pedidos['id_unicos'])
clientes = sorted(list(set(clientes_ids)))


def calculaPeriodo(cliente, periodo):
    pedidos_cliente = pedidos.query(f'id_unicos == "{cliente}" & periodo == "{periodo}"')

    if not pedidos_cliente.empty:
        temp = pedidos_cliente.groupby(["id_unicos", "periodo"]).agg(
            {"valor_pedido": "sum", "produto_id": "sum"}).reset_index()

        itens_str = temp['produto_id'][0]

        for a in "[] ": itens_str = itens_str.replace(a, "")
        itens = itens_str.split(',')
        itens = list(set(itens))

        amd = ''
        if '9300' in itens:
            amd = 'A'
            itens.remove('9300')
        if '9304' in itens:
            amd = amd + 'M'
            itens.remove('9304')
        if itens:
            amd = amd + 'D'

        # print(temp)
        # print(amd)
        return temp['valor_pedido'], amd
    else:
        return 0, '-'


col_names = ['id_unicos',
             'classificacao',
             'valor_compras',
             'semestre_fechamento',
             'data_fechamento',
             'AMD']

classificacao = pd.DataFrame(columns=col_names)

for cliente in clientes:

    valor_semestre = {
        '2018-S1': 0,
        '2018-S2': 0,
        '2019-S1': 0,
        '2019-S2': 0,
        '2020-S1': 0,
        '2020-S2': 0,
        '2021-S1': 0,
        '2021-S2': 0
    }

    AMD_semestre = {
        '2018-S1': '-',
        '2018-S2': '-',
        '2019-S1': '-',
        '2019-S2': '-',
        '2020-S1': '-',
        '2020-S2': '-',
        '2021-S1': '-',
        '2021-S2': '-'
    }

    semestre_inicial = ""
    trimestre_inicial = ""
    semestres = list(valor_semestre.keys())
    for semestre in semestres:
        # print(type(semestre))
        valor_semestre[semestre], AMD_semestre[semestre] = calculaPeriodo(cliente, semestre)

    i = -1
    for periodo in semestres:

        i = i + 1
        if periodo == '2018-S1':
            continue

        classificacao_semestre = ''

        valor = int(valor_semestre[semestres[i]]) + int(valor_semestre[semestres[i - 1]])

        if valor == 0 and semestre_inicial == "":
            continue

        if valor == 0 and semestre_inicial:
            classificacao_semestre = "Inativo"
        else:
            if not semestre_inicial:
                semestre_inicial = str(periodo)

            if 0 < valor < 1400:
                classificacao_semestre = 'Bronze'
            elif 1400 <= valor < 2800:
                classificacao_semestre = 'Prata'
            elif 2800 <= valor < 4000:
                classificacao_semestre = 'Ouro'
            elif valor >= 4000:
                classificacao_semestre = 'Diamante'

        line = {'id_unicos': cliente,
                'classificacao': classificacao_semestre,
                'valor_compras': valor,
                'semestre_fechamento': periodo,
                'data_fechamento': datas_fechamento[periodo],
                'AMD': AMD_semestre[periodo]
                }
        classificacao.loc[len(classificacao)] = line

#renomear a coluna de classificação
classificacao = classificacao.rename({"AMD":"AMD_semestre"}, axis=1)

#executar a função de classificação de ciclo de vida para os clientes
classificacao = criar_classificacao(classificacao)

#cria o arquivo csv para a classificação dos clientes e seu ciclo de vida
arquivo = './Editadas/CATEGORIAS-CLIENTES.csv'
classificacao.to_csv(arquivo, index=False)

end = time.time()
print(f"Clients were classified according to their purchases in {end - start} seconds")

# endregion

# region GET THE MESSAGE LOG AND APPEND NEW MESSAGES TO IT

#importar a lista de blacklists para não enviar mensagens a estas pessoas
clientes_blacklist = get_clientes_blacklist()
#exportamos os clientes blacklist caso seja interessante verificar quem são
clientes_blacklist.to_excel('./Editadas/CLIENTES-BLACKLIST.xlsx')
path = './Editadas/INFORMACOES-CONSOLIDADAS-CLIENTES-FINAL.xlsx'
cliente_infos = pd.read_excel(path)
pedidos = pd.read_excel('./Editadas/REGISTRO-VENDAS.xlsx')
#remover das informações de clientes os blacklist
#primeiro criamos uma lista contendo o número dos blacklists
blacklist = clientes_blacklist['Telefone'].unique().tolist()
#agora removemos estes números das informações de clientes
cliente_infos = cliente_infos.loc[~cliente_infos['telefone_cliente'].isin(blacklist)]
#chamar a função que registra as mensagens
log = registrar_mensagens(cliente_infos, pedidos)
#Vamos salvar o log de hoje para envio. Precisamos que o log tenha a data de hoje
today = date.today()
d1 = today.strftime("%Y-%m-%d")
#registra de fato o log a ser enviado hoje
log.to_excel('./Editadas/log-mensagem-'+d1+'.xlsx', index=False)
#endregion

# region FINAL PART, SEND THE MESSAGE
log = programar_envio(log, modo='') #por enquanto em modo test para não sair enviando mensagens equivocadamente
log.to_excel('./Editadas/log_mensagem-'+d1+'-enviado.xlsx', index=False)
# endregion

#region REALIZAR O UPLOAD NO AZURE
#aqui é necessário somente executar a função com a lista dos arquivos conforme abaixo.
print("Iniciando o processo de upload dos arquivos para a Azure.")
start = time.time()
criar_backup() #executa a função que cria o backup dos arquivos do dia de hoje
upload_to_azure([ #executa a função que realiza o upload dos arquivos recentes no azure
    "planilha-de-usuarios.xlsx",
    "planilha-de-contatos.xlsx",
    "planilha-de-vendedores.xlsx",
    "CATEGORIAS-CLIENTES.csv",
    "REGISTRO-VENDAS.xlsx",
    "INFORMACOES-CLIENTES.xlsx",
    "TABELA-DE-PARA-PRODUTOS-DESCRICAO-V1.xlsx",
    "TABELA-SUPORTE-PEDIDOS-V1.xlsx",
    "DE-PARA-ID-ZETA-ID-SUAS-VENDAS-V1.xlsx",
    "CLIENTES-BLACKLIST.xlsx",
    str("log-mensagens-"+d1+'-enviado.xlsx')
])
end = time.time()
print(f"Os arquivos foram subidos em {end-start} segundos.")
#endregion