# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

# Perform data exploration for data science

# Use the code in this notebook to perform data exploration for data science.

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Azure storage access info for open dataset diabetes
blob_account_name = "azureopendatastorage"
blob_container_name = "mlsamples"
blob_relative_path = "diabetes"
blob_sas_token = r"" # Blank since container is Anonymous access
    
# Set Spark config to access  blob storage
wasbs_path = f"wasbs://%s@%s.blob.core.windows.net/%s" % (blob_container_name, blob_account_name, blob_relative_path)
spark.conf.set("fs.azure.sas.%s.%s.blob.core.windows.net" % (blob_container_name, blob_account_name), blob_sas_token)
print("Remote blob path: " + wasbs_path)
    
# Spark read parquet, note that it won't load any data yet by now
df = spark.read.parquet(wasbs_path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# A saída mostra as linhas e colunas do conjunto de dados do diabetes. Os dados consistem em dez variáveis de linha de base, idade, sexo, índice de massa corporal, pressão arterial média e seis medidas de soro do sangue para pacientes com diabetes, bem como a resposta de interesse (uma medida quantitativa da progressão da doença um ano após a linha de base), que é rotulada como Y

# CELL ********************

# Os dados são carregados como um DataFrame do Spark. O Scikit-learn esperará que o conjunto de dados de entrada seja um dataframe do Pandas. Execute o código abaixo para converter
# seu conjunto de dados em um dataframe do Pandas:

df = df.toPandas()
df.head()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Verificar a forma dos dados
# 
# Agora que carregou os dados, você pode verificar a estrutura do conjunto de dados, como o número de linhas e colunas, os tipos de dados e os valores ausentes.
# 


# CELL ********************

# Use o ícone + Código abaixo da saída da célula para adicionar uma nova célula de código ao notebook e insira o seguinte código nela:

# Display the number of rows and columns in the dataset
print("Number of rows:", df.shape[0])
print("Number of columns:", df.shape[1])

# Display the data types of each column
print("\nData types of columns:")
print(df.dtypes)

#O conjunto de dados contém 442 linhas e 11 colunas. Isso significa que você deve ter 442 amostras e 11 recursos ou variáveis no seu conjunto de dados. A variável SEX
#  provavelmente contém dados categóricos ou cadeias de caracteres.

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Verifique se há dados ausentes


# CELL ********************

# Use o ícone + Código abaixo da saída da célula para adicionar uma nova célula de código ao notebook e insira o seguinte código nela:

missing_values = df.isnull().sum()
print("\nMissing values per column:")
print(missing_values)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #####  Gerar estatísticas descritivas para variáveis numéricas
# Agora, vamos gerar estatísticas descritivas para entender a distribuição das variáveis numéricas.
# 


# CELL ********************

# Use o ícone + Código abaixo da saída da célula para adicionar uma nova célula de código
# ao notebook e insira o seguinte código.

df.describe()

# A idade média é de aproximadamente 48,5 anos, com desvio padrão de 13,1 anos. 
# A pessoa mais jovem tem 19 anos e a mais velha tem 79 anos. A BMI média é aproximadamente 26,4,
# o que se enquadra na categoria sobrepeso de acordo 
# com os padrões da OMS. A BMI mínima é 18 e a máxima é 42,2.

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### Plotar a distribuição dos dados
# 
# Vamos verificar o recurso BMI e plotar sua distribuição para entender melhor suas características.

# CELL ********************

import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
    
# Calculate the mean, median of the BMI variable
mean = df['BMI'].mean()
median = df['BMI'].median()
   
# Histogram of the BMI variable
plt.figure(figsize=(8, 6))
plt.hist(df['BMI'], bins=20, color='skyblue', edgecolor='black')
plt.title('BMI Distribution')
plt.xlabel('BMI')
plt.ylabel('Frequency')
    
# Add lines for the mean and median
plt.axvline(mean, color='red', linestyle='dashed', linewidth=2, label='Mean')
plt.axvline(median, color='green', linestyle='dashed', linewidth=2, label='Median')
    
# Add a legend
plt.legend()
plt.show()

# A partir desse grafo, é possível observar o intervalo e a distribuição de BMI no conjunto de dados. Por exemplo, a maior parte da BMI está entre 23,2 e 29,2,
# e os dados estão distorcidos para a direit

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Realizar a análise multivariada
# 
# Vamos gerar visualizações, como gráficos de dispersão e gráficos de caixa, para descobrir padrões e relacionamentos nos dados.

# CELL ********************

import matplotlib.pyplot as plt
import seaborn as sns

# Scatter plot of BMI vs. Target variable
plt.figure(figsize=(8, 6))
sns.scatterplot(x='BMI', y='Y', data=df)
plt.title('BMI vs. Target variable')
plt.xlabel('BMI')
plt.ylabel('Target')
plt.show()

# Podemos ver que, à medida que a BMI aumenta, a variável de destino também aumenta, indicando 
# uma relação linear positiva entre essas duas variáveis.

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Adicionar outra célula de código ao notebook. Em seguida, insira o seguinte código nessa célula e execute-o.

import seaborn as sns
import matplotlib.pyplot as plt
    
fig, ax = plt.subplots(figsize=(7, 5))
    
# Replace numeric values with labels
df['SEX'] = df['SEX'].replace({1: 'Male', 2: 'Female'})
    
sns.boxplot(x='SEX', y='BP', data=df, ax=ax)
ax.set_title('Blood pressure across Gender')
plt.tight_layout()
plt.show()


# Essas observações sugerem que existem diferenças nos perfis de pressão arterial de pacientes
# do sexo masculino e feminino. Em média, as pacientes do sexo feminino têm uma pressão arterial mais alta
# do que os pacientes do sexo masculino.

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# A agregação dos dados pode torná-los mais gerenciáveis para visualização e análise. Adicionar outra célula de código ao notebook. 
# Em seguida, insira o seguinte código nessa célula e execute-o.

import matplotlib.pyplot as plt
import seaborn as sns
    
# Calculate average BP and BMI by SEX
avg_values = df.groupby('SEX')[['BP', 'BMI']].mean()
    
# Bar chart of the average BP and BMI by SEX
ax = avg_values.plot(kind='bar', figsize=(15, 6), edgecolor='black')
    
# Add title and labels
plt.title('Avg. Blood Pressure and BMI by Gender')
plt.xlabel('Gender')
plt.ylabel('Average')
    
# Display actual numbers on the bar chart
for p in ax.patches:
    ax.annotate(format(p.get_height(), '.2f'), 
                (p.get_x() + p.get_width() / 2., p.get_height()), 
                ha = 'center', va = 'center', 
                xytext = (0, 10), 
                textcoords = 'offset points')
    
plt.show()

# Esse gráfico mostra que a pressão arterial média é mais alta em pacientes do sexo feminino
# do que em pacientes do sexo masculino. Além disso, mostra que o Índice de Massa Corporal (IMC) médio
# é ligeiramente mais alto nas mulheres ao invés dos homens.

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Adicionar outra célula de código ao notebook. Em seguida, insira o seguinte código nessa célula e execute-o.

import matplotlib.pyplot as plt
import seaborn as sns
    
plt.figure(figsize=(10, 6))
sns.lineplot(x='AGE', y='BMI', data=df, errorbar=None)
plt.title('BMI over Age')
plt.xlabel('Age')
plt.ylabel('BMI')
plt.show()

# A faixa etária de 19 a 30 anos tem os valores médios mais baixos de IMC,
# enquanto o IMC médio mais alto é encontrado na faixa etária de 65 a 79 anos. 
# Além disso, observe que o IMC médio para a maioria das faixas etárias está dentro do intervalo de sobrepeso.

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Análise de correlação
# 
# Vamos calcular as correlações entre diferentes recursos para entender seus relacionamentos e dependências.

# CELL ********************

# Use o ícone + Código abaixo da saída da célula para adicionar uma nova célula de código ao notebook e insira o seguinte código
df.corr(numeric_only=True)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# Um mapa de calor é uma ferramenta útil para visualizar rapidamente a força e a direção das relações entre pares de variáveis. Ele pode destacar correlações fortes, positivas ou negativas, e identificar pares que não tenham nenhuma correlação. Para criar um mapa de calor, adicione outra célula de código ao notebook e insira o seguinte código.

# CELL ********************

plt.figure(figsize=(15, 7))
sns.heatmap(df.corr(numeric_only=True), annot=True, vmin=-1, vmax=1, cmap="Blues")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# As variáveis S1 e S2 têm uma correlação positiva alta de 0,89, indicando que elas se movem na mesma direção. Quando S1 aumenta, S2 também tende a aumentar, e vice-versa. Além disso, S3 e S4 têm uma forte correlação negativa de -0,73. Isso significa que, à medida que S3 aumenta, S4 tende a diminuir.

# MARKDOWN ********************

# **O que significa quando os dados estão Ausentes de Forma Aleatória (MAR)?** 
# 
# Correto. Quando os dados estão Ausentes de Forma Aleatória (MAR), a falta de dados está relacionada aos valores de algumas outras variáveis, mas não aos dados ausentes em si. Por exemplo, se as mulheres são mais propensas a divulgar seu número de passos diários do que os homens, então os dados de passos diários são MAR.
# 
# **O que o coeficiente de correlação indica em uma análise de correlação?**
# 
# Correto. O coeficiente de correlação varia de -1 a 1. Um coeficiente de 1 indica uma correlação linear positiva perfeita, -1 indica uma correlação linear negativa perfeita e 0 não indica correlação linear.
# 
# **Qual é o intervalo de uma distribuição uniforme?**
# 
# Correto. O intervalo de uma distribuição uniforme é o intervalo [a, b], em que "a"é o valor mínimo e "b"é o valor máximo

