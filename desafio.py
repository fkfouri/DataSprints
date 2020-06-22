#!/usr/bin/env python
# coding: utf-8

# # Desafio DataSprints - Fabio Kfouri
# 
# Este é um desafio dado pela <b><i>data <span style='color: red'>sprints</span></i></b> para avaliação técnica em Engenharia de Dados.

# In[ ]:


try:
    get_ipython().system('pip install pyspark=="2.4.5" --quiet')
    get_ipython().system('pip install pandas=="1.0.4" --quiet')
    get_ipython().system('pip install seaborn=="0.9.0" --quiet')
    get_ipython().system('pip install matplotlib=="3.2.2" --quiet')
except:
    print("Running throw py file.")


# In[ ]:


import warnings
warnings.filterwarnings('ignore') #para ignorar mensagens de warnings


# In[ ]:


from pyspark import SparkContext as sc
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark import SparkFiles
import pyspark
import json
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns


# Criando uma SparkSession

# In[ ]:


spark = SparkSession        .builder        .appName("Desafio Data Sprints - Fabio Kfouri")        .getOrCreate()
spark


# ## Criação dos Dataframes
# Referenciando o endereço das fontes. Para desenvolvimento local foi incluido as fontes na pasta <b>/data/</b>.
# 
# Para uso em cluster, sera usado o endereço do Bucket AWS <b>s3://data-sprints-eng-test/</b>.

# In[ ]:


import os

dataPath = 'https://s3.amazonaws.com/data-sprints-eng-test/'
outPath = 's3://data-sprints-fk/output/'

if 'E:\\' in os.getcwd() and 'DataSprints' in os.getcwd():
    #dataPath = os.getcwd() + "/data/"
    outPath = os.getcwd() + "/output/"


print(dataPath, outPath)


# Definiçao dos Arquivos no SparkContext

# In[ ]:


spark.sparkContext.addFile(dataPath + 'data-payment_lookup-csv.csv')
spark.sparkContext.addFile(dataPath + 'data-vendor_lookup-csv.csv')
spark.sparkContext.addFile(dataPath + 'data-sample_data-nyctaxi-trips-2009-json_corrigido.json')
spark.sparkContext.addFile(dataPath + 'data-sample_data-nyctaxi-trips-2010-json_corrigido.json')
spark.sparkContext.addFile(dataPath + 'data-sample_data-nyctaxi-trips-2011-json_corrigido.json')
spark.sparkContext.addFile(dataPath + 'data-sample_data-nyctaxi-trips-2012-json_corrigido.json')


# #### Leitura e Correçao da fonte Payment

# In[ ]:


df_payment = spark.read.csv(SparkFiles.get("data-payment_lookup-csv.csv"), header = True, sep = ",")
df_payment.show(3)


# Verificado que a primeira linha precisa ser ignorada. Inclusao de index para auxiliar a correção. 
# 
# Utilização do Pandas para a leitura do CSV ignorando a linha de index 0.

# In[ ]:


temp = pd.read_csv(SparkFiles.get("data-payment_lookup-csv.csv"), skiprows=[0], sep=',', header=None)
temp.head()


# - Renomeando a Coluna pelp registro de Index 0;
# - Removendo o registro de Index 0;
# - Conversao do DataFrame Pandas para um DataFrama Pyspark.

# In[ ]:


temp.columns = temp.iloc[0]
temp.drop(0, inplace = True)
df_payment = spark.createDataFrame(temp)
df_payment.show(3)


# Criação de view payment

# In[ ]:


df_payment.createOrReplaceTempView("payment")


# #### Leitura da fonte de Vendor

# In[ ]:


df_vendor = spark.read.csv(SparkFiles.get('data-vendor_lookup-csv.csv'), header = True, sep = ",")
df_vendor.show()


# Criação da view vendor.

# In[ ]:


df_vendor.createOrReplaceTempView("vendor")


# ## Leitura das corridas de taxi no período de 2009 à 2012

# In[ ]:


df_2009 = spark.read.json(SparkFiles.get('data-sample_data-nyctaxi-trips-2009-json_corrigido.json'))
df_2009.count()


# In[ ]:


df_2010 = spark.read.json(SparkFiles.get('data-sample_data-nyctaxi-trips-2010-json_corrigido.json'))
df_2011 = spark.read.json(SparkFiles.get('data-sample_data-nyctaxi-trips-2011-json_corrigido.json'))
df_2012 = spark.read.json(SparkFiles.get('data-sample_data-nyctaxi-trips-2012-json_corrigido.json'))


# ## Preparação do DataFrame de corridas de taxi.
# Concatenando todos os dataFrames em único DataFrame e em seguinda verificando o total de linhas do DataFrame.

# In[ ]:


df = df_2012.union(df_2011).union(df_2010).union(df_2009)
print("Tamanho do DataFrame concatenado:", df.count(), 'registros')


# Identificando o Schema do DataFrame

# In[ ]:


df.printSchema()


# Visualizando o aspecto dos dados

# In[ ]:


df.show(5, truncate = False)


# Conversão de colunas [dropoff_datetime, pickup_datetime] do tipo String para tipo TimeStamp.

# In[ ]:


#DataFrame Convertido (dfc)
dfc = df.withColumn('dropoff_datetime', F.to_utc_timestamp('dropoff_datetime', "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"))        .withColumn('pickup_datetime', F.to_utc_timestamp('pickup_datetime', "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"))
dfc.printSchema()


# Visualizando o aspecto dos dados, em especial os campos dataTime

# In[ ]:


dfc.show(5, False)


# Criando uma view trip.

# In[ ]:


dfc.createOrReplaceTempView("trip")


# ## Questões do Quesito Mínimo

# ### Questão 1: Qual a distância média percorrida por viagens com no máximo 2 passageiros.
# 
# A distância média (valor arredondado) percorrida por viagens com no máximo 2 passageiros é:

# In[ ]:


df_question_1 = spark.sql("""
      SELECT ROUND(AVG(trip_distance),3) mean_trip_distance
        FROM trip t
       WHERE t.passenger_count <= 2
""")
df_question_1.show()


# Exportando para um arquivo CSV

# In[ ]:


#try:
#    os.makedirs(os.getcwd() + '/out/')
#except:
#    pass

df_question_1.write.csv(outPath + '\question_1.csv')


# ### Questão 2: Quais os 3 maiores vendors em quantidade total de dinheiro arrecadado?
# 
# O resultado em quantidade de dinheiro (valores em Milhões U$) arrecado pelos 3 maiores vendors são:

# In[ ]:


df_question_2 = spark.sql("""
    SELECT v.name, t.vendor_id, ROUND(SUM(total_amount)/1E6,3) amount 
      FROM trip t LEFT JOIN vendor v ON (t.vendor_id = v.vendor_id)
  GROUP BY t.vendor_id, v.name
  ORDER BY SUM(total_amount) DESC
     LIMIT 3
""")
df_question_2.show(truncate = False)


# Exportando para um arquivo csv

# In[ ]:


df_question_2.write.csv(outPath + '\question_2.csv')


# ### Questão 3: Um histograma da distribuição mensal, nos 4 anos, de corridas pagas em dinheiro:

# In[ ]:


df_question_3 = spark.sql("""
    WITH Dist as (--
        SELECT date_format(t.dropoff_datetime,'MMM-yyyy') month_year,
               date_format(t.dropoff_datetime,'yyyy-MM') my_idx,
               date_format(t.dropoff_datetime,'MMM') month,
               date_format(t.dropoff_datetime,'MM') m_idx, 
               p.payment_lookup, t.total_amount--, t.*
          FROM trip t JOIN payment p ON (t.payment_type = p.payment_type)
         WHERE p.payment_lookup = 'Cash' --
    )
    SELECT count(month) qty_trip, sum(total_amount) amount, month_year, my_idx, month, m_idx
      FROM Dist d
  GROUP BY month_year, my_idx, month, m_idx
  ORDER BY my_idx
""")
df_question_3.show()


# In[ ]:


dados = df_question_3.toPandas()


# In[ ]:


ax = sns.distplot(dados.qty_trip)
ax.figure.set_size_inches(20, 6)
ax.set_xlabel('Ganho Medio', fontsize=16)
ax.set_ylabel('Densidade', fontsize=16)
ax.set_title("Distribuiçao de Media de corridas pagas em Dinheiro entre 2009 e 2012", fontsize=20)
ax


# In[ ]:


#Exportando grafico
ax.figure.savefig(outPath + '\Question_3a.png')


# In[ ]:


fig, ax1 = plt.subplots()
color = 'red'
ax1.set_title("Historico mensal de corridas pagas em dinheiro entre 2009 e 2012.", fontsize=20)
ax1.set_xlabel('Meses')
ax1.figure.set_size_inches(20, 6)
plt.xticks(rotation='vertical')

# Eixo primario
ax1.set_ylabel('Quantidade em milhares de corridas', color=color)
ax1.bar(dados.month_year, dados.qty_trip/1E3, color=color)
ax1.tick_params(axis='y', labelcolor=color)

# Eixo secundario
ax2 = ax1.twinx() 
color = 'black'
ax2.set_ylabel('Ganhos em milhares de U$', color=color) 
ax2.plot(dados.month_year, dados.amount/1E3, color=color, linewidth=3)
ax2.tick_params(axis='y', labelcolor=color)

fig.tight_layout() 


# In[ ]:


#Exportando grafico
fig.savefig(outPath + '\Question_3b.png')


# ### Questão 4: Um gráfico de série temporal contando a quantidade de gorjetas de cada dia, nos
# últimos 3 meses de 2012:

# In[ ]:


df_question_4 = spark.sql("""
WITH
last_month AS (--
     SELECT date_add(add_months(to_date(date_format(MAX(dropoff_datetime),'yyyy-MM') || '-01','yyyy-MM-dd'),1),-1) last_date,
            MAX(dropoff_datetime) max_date,
            add_months(to_date(date_format(MAX(dropoff_datetime),'yyyy-MM') || '-01','yyyy-MM-dd'),-2) first_date
       FROM trip --
),
temp AS (--
     SELECT dropoff_datetime, 
            date_format(t.dropoff_datetime,'dd-MMM-yyyy') month_year,
            date_format(t.dropoff_datetime,'yyyy-MM-dd') my_idx, tip_amount
       FROM trip t, last_month lm
      WHERE dropoff_datetime between lm.first_date and lm.last_date
        AND tip_amount > 0 -- corridas que tiveram gorjetas
      )
      SELECT month_year, my_idx, COUNT(tip_amount) tips from temp
       GROUP BY month_year, my_idx
       ORDER BY my_idx   
""")
df_question_4.show(5, truncate = False)


# In[ ]:


dados = df_question_4.toPandas()
dados.head()


# In[ ]:


fig, ax1 = plt.subplots()

ax1.set_title("Serie temporal da quantidade de tips nos últimos 3 meses de 2012.", fontsize=20)
ax1.set_xlabel('3 últimos Meses')
ax1.figure.set_size_inches(20, 6)
plt.xticks(rotation='vertical')
# Eixo primario
ax1.set_ylabel('Quantidade de Gorjetas')
ax1.plot(dados.month_year, dados.tips ) 


# In[ ]:


#Exportando grafico
fig.savefig(outPath + '\Question_4.png')


# ## Questões do Quesito Bônus

# ### Questão 5: Qual o tempo médio das corridas nos dias de sábado e domingo?
# 
# O tempo médio das corridas no fim de semana é:

# In[ ]:


df_question_5 = spark.sql("""
WITH calc as (--
      SELECT dayofweek(t.dropoff_datetime) day_week_num, 
             date_format(t.dropoff_datetime, 'EEEE') day_week, 
             dropoff_datetime, pickup_datetime,
             cast(dropoff_datetime as long) - cast(pickup_datetime as long) delta,
             t.trip_distance
        FROM trip t
        WHERE dayofweek(t.dropoff_datetime) in (1,7) --
)
--SELECT c.*, round(delta/60, 2) delta_minutes, round(delta/60/60, 2) delta_hour FROM calc c
    SELECT avg(delta) delta_seconds, 
           round(avg(delta/60), 2) delta_minutes, day_week
      FROM calc
  GROUP BY day_week

""")
df_question_5.show(5, False)


# In[ ]:


#Exportando dados
df_question_5.write.csv(outPath + '\question_5.csv')


# ### Questão 6: Fazer uma visualização em mapa com latitude e longitude de pickups and dropoffs de 2010.

# In[ ]:


df_question_6 = spark.sql("""
WITH map as (--
      SELECT dropoff_latitude latitude, dropoff_longitude longitude
        FROM trip t
       WHERE date_format(t.dropoff_datetime,'yyyy') = 2010
    UNION
     SELECT pickup_latitude, pickup_longitude
        FROM trip t
       WHERE date_format(t.dropoff_datetime,'yyyy') = 2010
)
Select *
       --min(latitude) min_latitude, max(latitude) max_latitude, avg(latitude) avg_latitude
       --min(longitude) min_longitude, max(longitude) max_longitude, avg(longitude) avg_longitude
       from map m
""")
df_question_6.show(5, False)


# Verificado a existencia de <b>Outliers</b> para Latitude e Longitude.
# 
# Conversão para Pandas e realizado uma análise das Estatísticas Descritivas para identificação dos <b>Outliers</b>. 
# 
# Observa-se que os valores mínimo e máximo de latitude e longitude estão bem distantes dos Quartis. 

# In[ ]:


dados = df_question_6.toPandas()
dados.describe().round(3)


# Definindo o limite inferior e superior para Latitude.

# In[ ]:


Q1 = dados['latitude'].quantile(.25)
Q3 = dados['latitude'].quantile(.75)
IIQ = Q3 - Q1
limite_inferior_latitude = Q1 - 1.5 * IIQ
limite_superior_latitude = Q3 + 1.5 * IIQ
print(limite_inferior_latitude, limite_superior_latitude, Q1, Q3)


# Definindo o limite inferior e superior para Longitude.

# In[ ]:


Q1 = dados['longitude'].quantile(.25)
Q3 = dados['longitude'].quantile(.75)
IIQ = Q3 - Q1
limite_inferior_longitude = Q1 - 1.5 * IIQ
limite_superior_longitude = Q3 + 1.5 * IIQ
print(limite_inferior_longitude, limite_superior_longitude, Q1, Q3)


# Aplicando a limpeza do DataSet pelos limites calculados de Latitude e Longetude.

# In[ ]:


selecao = (dados['latitude'] >= limite_inferior_latitude) & (dados['latitude']<= limite_superior_latitude) &           (dados['longitude'] >= limite_inferior_longitude) & (dados['longitude']<= limite_superior_longitude) 
dados_new = dados[selecao]


# Este é um check para evidenciar que o novo DataSet é menor que o DataSet original.

# In[ ]:


print(dados.shape, dados_new.shape)


# Observa-se que no novo Dataset não há inconsistências.

# In[ ]:


dados_new.describe().round(3)


# Seguindo o tutorial https://towardsdatascience.com/easy-steps-to-plot-geographic-data-on-a-map-python-11217859a2db.

# In[ ]:


print('Latitude:', dados_new['latitude'].min(), 'e', dados_new['latitude'].max())
print('Longitude:', dados_new['longitude'].min(), 'e', dados_new['longitude'].max())


# In[ ]:


BBox = ((dados_new.longitude.min(), dados_new.longitude.max(),  
        dados_new.latitude.min(), dados_new.latitude.max()))
BBox


# Foto baseada no https://www.openstreetmap.org/#map=12/40.7530/-74.0228
# 
# 
# ![Foto Original](lib/ny_map.png) 
# 
# <center>Imagem de Mapa original</center>

# In[ ]:


ny_m = plt.imread(os.getcwd() + '/lib/ny_map.png')


# In[ ]:


ref_size = 60
fig, ax = plt.subplots(figsize = (ref_size,ref_size * 1.73))
ax.scatter(dados_new.longitude, dados_new.latitude, zorder=1, alpha= 0.2, c='b', s=10)
ax.set_title('Traçando dados espaciais em Manhattan para corridas de 2010.', fontsize=ref_size)
ax.set_xlim(BBox[0],BBox[1])
ax.set_ylim(BBox[2],BBox[3])
ax.imshow(ny_m, zorder=0, extent = BBox, aspect= 'equal')
ax.set_xlabel('Imagem baseada no Mapa original', fontsize=ref_size *.8)


# In[ ]:


fig.savefig(outPath + '\Question_6_map_ny_edited.png', dpi=72)


# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:




