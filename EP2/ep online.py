import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, mean, stddev, variance, month, year, min, max
from pyspark.sql import functions as fun
from pyspark.sql.types import *
import os
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.colors as colors
import matplotlib.dates as dt
import re
import requests

def conversor (cadeia):
	for i,c in enumerate(cadeia):
		if (c == '1'):
			if (i == 0):
				return 'Fog'
			if (i == 1):
				return 'Snow or Ice Pellets'
			if (i == 2):
				return 'Rain or Drizzle'
			if (i == 3):
				return 'Hail'
			if (i == 4):
				return 'Thunder'
			if (i == 5):
				return 'Tornado/Funnel Cloud'
	return 'No occurrence'

def get_delimitador(coluna):
	if (coluna == 'TEMP' or coluna == 'DEWP' or coluna == 'SLP' or coluna == 'STP' or coluna == 'MAX'):
		return 9998
	return 998

def ajusta_data(data):
	return (data[2] + '-' + data[1] + '-' + data[0])

def pega_dados (url):
	files = list(dict.fromkeys(re.findall('[\w\.-]+.csv',requests.get(url).text)))
	urls = [(url + '/' + f) for f in files]
	dfs = [pd.read_csv(u) for u in urls]
	df = pd.concat(dfs)
	return df;


def processa_dados (dir, data_i, data_f, coluna, tipo_agrupamento = 'A'):
	data_i = data_i.split()
	data_f = data_f.split()
	data_i_ajustado = ajusta_data(data_i)
	data_f_ajustado = ajusta_data(data_f)

	delimitador = get_delimitador(coluna)
	#print("Delimitador: " + str(delimitador))
	dados = spark.createDataFrame([], StructType([]))
	for ano in range(int(data_i[2]),(int(data_f[2]) + 1)):
		arq = spark.createDataFrame(pega_dados(dir + '/' + str(ano)).astype(str))
		d_dados = arq.select(col('DATE'),col(coluna).cast(DoubleType()).alias(coluna)).filter(fun.to_date(col('DATE'), 'yyyy-MM-dd').between(data_i_ajustado, data_f_ajustado))
		dados = dados.unionByName(d_dados.filter(col(coluna).cast(DoubleType()) <= delimitador), allowMissingColumns=True)

	if (tipo_agrupamento == 'S'):
		stats = dados.withColumn('Dia da semana', fun.date_format(col('DATE'), 'EEEE')).groupBy('Dia da semana').agg(mean(col(coluna)).alias('Média'), stddev(col(coluna)).alias('Desvio Padrão'), variance(col(coluna)).alias('Variância'))

	elif (tipo_agrupamento == 'MA'):
		stats = dados.withColumn('Mes/Ano', fun.date_format(col('DATE'), 'MM/yyyy')).groupBy('Mes/Ano').agg(mean(col(coluna)).alias('Média'), stddev(col(coluna)).alias('Desvio Padrão'), variance(col(coluna)).alias('Variância')).orderBy(fun.to_date(col('Mes/Ano'),'MM/yyyy'))

	elif (tipo_agrupamento == 'M'):
		stats = dados.withColumn('Mês', fun.date_format(col('DATE'), 'MMMM')).groupBy('Mês').agg(mean(col(coluna)).alias('Média'), stddev(col(coluna)).alias('Desvio Padrão'), variance(col(coluna)).alias('Variância'))

	elif (tipo_agrupamento == 'D'):
		stats = dados.withColumn('Dia/Mes/Ano', fun.date_format(col('DATE'), 'dd/MM/yyyy')).groupBy('Dia/Mes/Ano').agg(mean(col(coluna)).alias('Média'), stddev(col(coluna)).alias('Desvio Padrão'), variance(col(coluna)).alias('Variância')).orderBy(fun.to_date(col('Dia/Mes/Ano'),'dd/MM/yyyy'))

	else:
		stats = dados.withColumn('Ano', fun.date_format(col('DATE'), 'yyyy')).groupBy('Ano').agg(mean(col(coluna)).alias('Média'), stddev(col(coluna)).alias('Desvio Padrão'), variance(col(coluna)).alias('Variância')).orderBy('Ano')

	return(stats._jdf.showString(500, 500, False))

def calcula_b (dados, coluna_um, coluna_dois, media_um, media_dois):
	cima = 0
	baixo = 0

	for i in range(len(dados)):
		cima += float(dados[i][coluna_um])*(float(dados[i][coluna_dois]) - media_dois)
		baixo += float(dados[i][coluna_um])*(float(dados[i][coluna_um]) - media_um)

	return (cima/baixo)

def plot(desvio, dados, ymin, ymax):
	
	desvioy = [float(x[coluna_um]) for x in dados.select(col(coluna_um)).collect()]
	plt.plot(desvio(), desvio())



def predicao (dir, data_i, data_f, coluna_um, coluna_dois):
	data_i = data_i.split()
	data_f = data_f.split()
	data_i_ajustado = ajusta_data(data_i)
	data_f_ajustado = ajusta_data(data_f)

	delimitador_um = get_delimitador(coluna_um)
	delimitador_dois = get_delimitador(coluna_dois)
	dados = spark.createDataFrame([], StructType([]))

	for ano in range(int(data_i[2]), (int(data_f[2]) + 1)):
		arq = spark.createDataFrame(pega_dados(dir + '/' + str(ano)).astype(str))
		d_dados = arq.select(col('DATE'),col(coluna_um).cast(DoubleType()), col(coluna_dois).cast(DoubleType())).filter(fun.to_date(col('DATE'), 'yyyy-MM-dd').between(data_i_ajustado, data_f_ajustado))

		dados = dados.unionByName(d_dados.filter((col(coluna_um) <= delimitador_um) & (col(coluna_dois) <= delimitador_dois)), allowMissingColumns=True)

	desvio = dados.withColumn('DATE', fun.date_format(col('DATE'), 'yyyy-MM-dd')) \
							.groupBy('DATE') \
							.agg(stddev(col(coluna_um)).alias(('Desvio Padrão ' + coluna_um)), stddev(col(coluna_dois)).alias('Desvio Padrão ' + coluna_dois)) \
							.orderBy('DATE')

	aux = dados.select(mean(col(coluna_um)).alias(('Média ' + coluna_um))).collect()
	media_um = float(aux[0]['Média ' + coluna_um])

	aux = dados.select(mean(col(coluna_dois)).alias('Média ' + coluna_dois)).collect()
	media_dois = float(aux[0]['Média ' + coluna_dois])


	b = calcula_b(dados.collect(), coluna_um, coluna_dois, media_um, media_dois)
	a = media_dois - (b*media_um)

	aux = dados.select(max(col(coluna_um)).alias('Max')).collect()
	maximo = float(aux[0]['Max'])

	aux = dados.select(min(col(coluna_um)).alias('Min')).collect()
	minimo = float(aux[0]['Min'])


	#print('Função: f(x) = ' + str(a) + ' + ' + str(b) + 'x')
	#print('Máximo: ' + str(maximo) + ' Mínimo: ' + str(minimo))

	y_zero = a + (b*minimo)
	y_um = a + (b*maximo)
	yval = [y_zero, y_um]

	dadosy = [float(x[coluna_dois]) for x in dados.select(col(coluna_dois)).collect()]
	date = [str(x['DATE']) for x in dados.select(col('DATE')).collect()]
	ydata = [data_i_ajustado, data_f_ajustado]

	dadosdp = [float(x[('Desvio Padrão ' + coluna_dois)]) for x in desvio.select(col(('Desvio Padrão ' + coluna_dois))).collect()]
	datedp = [str(x['DATE']) for x in desvio.select(col('DATE')).collect()]

	date = pd.to_datetime(date)
	ydata = pd.to_datetime(ydata)
	datedp = pd.to_datetime(datedp)

	#print(dadosdp)
	#print(datedp)
	#print(len(dadosdp))
	#print(len(datedp))



	plt.scatter(date, dadosy, label = ("Dados de " + str(coluna_dois)))
	plt.scatter(datedp, dadosdp, label = ("Desvio padrão"))
	plt.plot(ydata, yval, 'red', label = "reta")
	print(yval)
	print(ydata)
	plt.xlabel('Data')
	plt.ylabel('Valor do dado')
	plt.legend()

	plt.show()

	#plot(desvio, dados_reais, y_zero, y_um)









dir = 'https://www.ncei.noaa.gov/data/global-summary-of-the-day/access/'
spark = SparkSession \
    .builder \
    .appName("gerenciador de dados") \
    .getOrCreate()
print("Digite um comando plox")
while (True):
	inpt = input("").lower()
	if (inpt == 'flavio'):
			print("manoflavio")

	if (inpt == 'get'):
		data_i = input("Digite a data de inicio (dia mes ano): \n")
		data_f = input("Digite a data de fim (dia mes ano): \n")
		coluna = input("Digite o tipo de dado que deseja (nome da coluna): \n").upper()
		tipo_agrupamento = input("Digite o tipo de agrupamento(A = ano, M = mês,MA = Mês/Ano D = Dia/Mês/Ano, S = dia da semana) \n").upper()
		print(processa_dados(dir, data_i, data_f, coluna,tipo_agrupamento))

	elif (inpt == 'pred'):
		data_i = input("Digite a data de inicio (dia mes ano): \n")
		data_f = input("Digite a data de fim (dia mes ano): \n")
		coluna_um = input("Digite o nome da coluna que deseja usar como base: \n").upper()
		coluna_dois = input("Digite o nome da coluna que deseja prever: \n").upper()
		predicao(dir, data_i, data_f, coluna_um, coluna_dois)

	elif (inpt == 'help'):
		print("-get: pega dados na coluna e no período desejado")
		print("-pred: fazer predicao dos dados de uma coluna baseado em outra")


	elif (inpt == 'exit'):
		exit()

	else:
		print("Comando invalido, para ver a lista de comandos digite \"help\" ")


"""
arq = spark.read.csv(os.getcwd() + '\\sample.csv', header=True)
type(arq)
arq.show(10)
sts = arq.select(col("FRSHTT")).collect()
strongs = []
for linha in sts:
	strongs.append(conversor(linha['FRSHTT']))
print(strongs)"""