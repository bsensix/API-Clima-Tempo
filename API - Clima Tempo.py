# ## Código Completo

# In[21]:


import requests
import json
import pandas as pd
import snowflake.connector


# token
token = '400586ec9c13b53a9c9e5c1182e0c00c'

# ID da Cidade
ID_CITY = '6751'

# URL da API de previsão em 15 dias
url = f'https://apiadvisor.climatempo.com.br//api/v1/forecast/locale/{ID_CITY}/hours/72?token={token}'

# Faça a solicitação à API
response = requests.get(url)
data_72 = response.json()

# Transforme o JSON em um DataFrame
df = pd.json_normalize(data_72, 'data')
df['date_br'] = pd.to_datetime(df['date_br'], format='%d/%m/%Y %H:%M:%S')
df['Hora'] = df['date_br'].dt.strftime('%H:%M')
df['Data'] = df['date_br'].dt.strftime('%Y-%m-%d')
df = df.rename(columns={'humidity.humidity': 'Umidade',
                        'pressure.pressure':'Pressao',
                        'rain.precipitation':'Chuva',
                        'wind.velocity':'Velocidade_do_Vento',
                        'wind.direction':'Direcao_do_Vento',
                        'wind.directiondegrees':'Direcao_do_Vento_em_Graus',
                        'wind.gust':'Rajada_de_Vento',
                        'temperature.temperature':'Temperatura'})

columns_to_drop = ['date', 'date_br']

df = df.drop(columns=columns_to_drop)

# Conexão com o banco de dados Snowflake
conn  = snowflake.connector.connect(
        account='SVVLVJD-WCA20676',
        user='BYURI',
        password='Snowflake2023!',
        database='DADOS_API_CLIMA_TEMPO_72HORAS',
        schema='PREVISAO_TEMPO',
        warehouse='COMPUTE_WH'
)

# Excluir os dados antigos
cursor = conn.cursor()
cursor.execute("DELETE FROM previsao_tempo")
cursor.close()

# Inserir dados na tabela
for index, row in df.iterrows():
    cursor = conn.cursor()
    cursor.execute(
        f"INSERT INTO previsao_tempo (Data, Hora, Umidade, Pressao, Chuva, Velocidade_do_Vento, Direcao_Vento, Direcao_do_Vento_em_Graus, Rajada_de_Vento, Temperatura) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
        (row['Data'], row['Hora'], row['Umidade'], row['Pressao'], row['Chuva'], row['Velocidade_do_Vento'], row['Direcao_do_Vento'], row['Direcao_do_Vento_em_Graus'], row['Rajada_de_Vento'], row['Temperatura'])
    )
    cursor.close()

# Fechar a conexão com o Snowflake
conn.close()






