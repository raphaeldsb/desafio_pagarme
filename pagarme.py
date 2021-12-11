#!/usr/bin/env python
# coding: utf-8

# Import das bibliotecas utilizadas 
import pandas as pd
import numpy as np
from datetime import datetime
import locale
import pygsheets
locale.setlocale(locale.LC_ALL, 'pt_BR.utf8')


# Criação das tabelas (dataframes) a partir dos arquivos csv Companies, Payables, Transactions e TransactioOperations
df_companies = pd.read_csv('csv/Companies.csv')
df_payables = pd.read_csv('csv/Payables.csv')
df_transaction_operations = pd.read_csv('csv/TransactionOperations.csv')
df_transactions = pd.read_csv('csv/Transactions.csv')


# Deixando apenas transações de cartão de débito e crédito
df_transactions = df_transactions.loc[df_transactions['payment_method'].isin(['credit_card', 'debit_card'])]


# Função para converter datas
def timezone_local(dt):
    try:
        return pd.to_datetime(dt).tz_localize('GMT').tz_convert('America/Sao_Paulo').tz_localize(None)
    except:
        return pd.to_datetime(dt).tz_convert('America/Sao_Paulo').tz_localize(None)


# Converte os campos de data, de todas as tabelas, para timezone 'America/Sao_Paulo'
df_transactions['transaction_created_at'] = df_transactions['transaction_created_at'].apply(timezone_local)
df_transactions['transaction_updated_at'] = df_transactions['transaction_updated_at'].apply(timezone_local)

df_transaction_operations['transaction_operation_created_at'] = df_transaction_operations['transaction_operation_created_at'].apply(timezone_local)

df_companies['company_created_at'] = df_companies['company_created_at'].apply(timezone_local)

df_payables['payable_created_at'] = df_payables['payable_created_at'].apply(timezone_local)


#Criação do campo Installments_range
conditions_installments_range = [
    (df_transactions['transaction_installments'].isin([2,3,4,5,6])),
    (df_transactions['transaction_installments'].isin([7,8,9,10,11,12]))    
]
choices_installments_range = ['2-6', '7-12']
df_transactions['installments_range'] = np.select(conditions_installments_range, choices_installments_range, default="a vista") 


# Lista de transaction_id que estão na Payables para identificar se a transação é psp ou gateway
list_transactions_in_payables = df_payables['transaction_id'].unique().tolist()

# Criação do campo product_name
df_transactions['product_name'] = np.where(df_transactions['transaction_id'].isin(list_transactions_in_payables), 'psp', 'gateway')


# Agrupamento da tabela payables por transaction_id e type, somando os campos amount e fee, e buscando a menor data para o campo payable_created_at
payables_group = df_payables.groupby(['transaction_id','type'], as_index=False).agg({'payable_created_at': np.min, 'amount': np.sum, 'fee': np.sum })


# Tabela Payables:
# type (varchar): Tipo da operação financeira (credit, chargeback, chargeback_refund e refund). 
# O tipo credit é usado para todas as transações de cartão que são capturadas e para todos os boletos que são conciliados;
#
# Conforme informado na descrição da tabela payables, tipo credit é usada para transações capturadas. vou considerar credit como capture
payables_group.loc[payables_group['type'] == 'credit', 'type'] = 'capture'

# Criação do campo financial_operation_type
# Por enquanto apenas as transações psp, encontradas na tabela Payables, terão esse campo preenchido.
payables_group.rename(columns={'type':'financial_operation_type'}, inplace=True)

# OBS:
# Uma dúvida que surgiu, foi sobre o campo financial_operation_type.. de onde vinha essa informação
# Pelo pouco conhecimento do negócio e considerando as descrições dos campos das tabelas, no documento do case, 
# utilizei para transações psp, a tabela Payables, campo type. Para transações gateway, tabela TransactioOperations, campo type.


# join entre transactions e a tabela agrupada payables
df_transactions_join_payables = pd.merge(
    df_transactions,
    payables_group,
    how="left",
    left_on=["transaction_id"], 
    right_on=["transaction_id"]
)


# Removendo transações no status refused
df_transactions_join_payables = df_transactions_join_payables.loc[df_transactions_join_payables['status'] != 'refused']


# Lista das transações que não possuem financial_operation_type
transaction_ids = df_transactions_join_payables.loc[df_transactions_join_payables['financial_operation_type'].isna()]['transaction_id'].unique().tolist()

# Agrupando a tabela transacionOperations, com as transações selecionadas acima
transaction_operations_group = df_transaction_operations.loc[(df_transaction_operations['transaction_id'].isin(transaction_ids)) 
                            & (df_transaction_operations['type'] != 'authorize')].groupby(
    ['transaction_id','type', 'acquirer_name'], as_index=False).agg({'transaction_operation_created_at': np.min})


# Remove transações que não foram capturados
list_transaction_ids_capture = transaction_operations_group.loc[transaction_operations_group['type'] == 'capture']['transaction_id'].unique().tolist()
transaction_operations_group = transaction_operations_group.loc[transaction_operations_group['transaction_id'].isin(list_transaction_ids_capture)]


# Join para buscar os financial_operation_type das transações que não são psp
df_joins = pd.merge(
    df_transactions_join_payables,
    transaction_operations_group,
    how="left",
    left_on=["transaction_id"], 
    right_on=["transaction_id"]
)


# Selecionando os campos após o join
# Campo transaction_amount, para transações encontradas na tabela payables (psp) utilizou o valor amount da Payables
# Transações gateway, utilizou o amount da transaction
df_joins['transaction_amount'] = np.where(df_joins['amount_y'].isna, df_joins['amount_x'], df_joins['amount_y'])

# Campo acquirer_name, para transações gateway, utilizei o acquirer_name da tabela TransactioOperations
# Transações psp utilizaram o campo acquirer_name da transaction
df_joins['acquirer_name'] = np.where(df_joins['acquirer_name_y'].isna, df_joins['acquirer_name_x'], df_joins['acquirer_name_y'])

# Campo created_at, para transações psp, foi utilizada a menor data do campo payable_created_at, da tabela Payables
# Para transações gateway, foi utilizado a menor data do campo transaction_operation_created_at, da tabela TransactioOperations
df_joins['created_at'] = np.where(df_joins['transaction_operation_created_at'].isna, df_joins['payable_created_at'], df_joins['transaction_operation_created_at'])

# Campo financial_operation_type, para psp utilizado o type, da tabela Payables
# Para gateway, utilizado o type da TransactioOperations
df_joins['financial_operation_type'] = np.where(df_joins['type'].isna, df_joins['financial_operation_type'], df_joins['type'])

# Alteração de nome fee (tabela Payables), para mdr_fee, para transações psp
df_joins.rename(columns={'fee':'mdr_fee'}, inplace=True)

# Colocando os valores nulos do campo mdr_fee como 0 (zero), para transações gateway 
df_joins['mdr_fee'] = df_joins['mdr_fee'].fillna(0)


# 4. Para operações financeiras que indiquem "entrada" de receita (capture e
# chargeback_refund), os indicadores mdr_fee e transaction_amount deverão ser positivos.
# Para operações financeiras que indiquem "saída" de receita (refund e chargeback), tais
# indicadores deverão ser negativos. Assim, se somarmos todas as entradas para uma
# mesma transação, teremos o valor que será liquidado para o cliente de fato.

# Aplicando regra acima
df_joins['transaction_amount'] = np.where(df_joins['financial_operation_type'].isin(['capture', 'chargeback_refund']), 
                                          abs(df_joins['transaction_amount']), abs(df_joins['transaction_amount']) * -1)
df_joins['mdr_fee'] = np.where(df_joins['financial_operation_type'].isin(['capture', 'chargeback_refund']), 
                                          abs(df_joins['mdr_fee']), abs(df_joins['mdr_fee']) * -1)


# 5. Considere que não há devolução da taxa de gateway em caso de estorno ou chargeback.
# Portanto, a taxa de gateway deve ser apenas contabilizada na operação financeira de
# captura (capture), permanecendo zerada para outras operações financeiras.

# Aplicando regra acima
df_joins['gateway_fee'] = np.where(df_joins['financial_operation_type'] == 'capture', df_joins['gateway_fee'], 0)


# Incluindo campos de data solicitados
df_joins['day_of_week'] = df_joins['created_at'].dt.dayofweek + 1
df_joins['month_name'] = df_joins['created_at'].dt.strftime('%B').str.capitalize()
df_joins['date'] = df_joins['created_at'].dt.date


#Deixando apenas transações com financial_operation_type in ['capture','refund','chargeback_refund','chargeback']
df_joins = df_joins.loc[df_joins['financial_operation_type'].isin(['capture','refund','chargeback_refund','chargeback'])]


# Adicionando informação de Companies
df_joins = pd.merge(
    df_joins,
    df_companies[['company_id','company_mcc','company_type']],
    how="left",
    left_on=["company_id"], 
    right_on=["company_id"]
)


#Selecionando os campos solicitados nos requisitos para a view final
view_final = df_joins[['transaction_id', 'payment_method', 'installments_range', 'financial_operation_type', 'created_at', 'date', 'month_name', 'day_of_week',
          'company_id', 'company_mcc', 'company_type', 'product_name', 'acquirer_name', 'transaction_amount', 'mdr_fee', 'gateway_fee']]


# alterando o tipo de cada campo da view, para os tipos informados nos requisitos
view_final = view_final.astype(
    {
        "transaction_id": str, 
        "payment_method": str,
        "installments_range": str,
        "financial_operation_type": str,
        "month_name": str,
        "day_of_week": int,
        "company_id": str,
        "company_mcc": str,
        "company_type": str,
        "product_name": str,
        "acquirer_name": str,
        "transaction_amount": int,
        "mdr_fee": int,
        "gateway_fee": int
    }
)

view_final['created_at']= pd.to_datetime(view_final['created_at'])
view_final['date']= pd.to_datetime(view_final['date'])


# Acessa planilha do sheets 
# OBS: Secret utilizada para acesso não estará no repositório do github, por segurança dos meus documentos no google.
gc = pygsheets.authorize()
sh = gc.open_by_key('1bZSb8-7RvOSLS80HxmgCLyH34tp7Cvo78AjgR7rQedA')
wk = sh.worksheet_by_title('view_final')


# Carrega view_final no sheets
wk.set_dataframe(view_final, f'A1')
