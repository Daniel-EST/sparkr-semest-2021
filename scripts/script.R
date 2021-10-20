################################################################################
############################### SEMESTE 12 #####################################
################################################################################
######## Utilizando o R para Big Data, uma introdução prática ao SparkR ########
################################################################################
############################### 20/10/2021 #####################################
################################################################################


#### Inicialização
# Checando instalação do Java
system("java -version")

# Definindo a localização do Spark no computador do usuário
SPARK_HOME = "D:/spark-3.1.2-bin-hadoop2.7"

# Definindo uma variável de ambiente SPARK_HOME
Sys.setenv(SPARK_HOME=SPARK_HOME)

# Carregando a biblioteca SparkR no caminho do Spark
library(SparkR,
        lib.loc=c(file.path(Sys.getenv("SPARK_HOME"), "R", "lib")))

# Iniciando uma sessão do 
sparkR.session(master = "local[*]",
               sparkConfig = list(spark.driver.memory = "1g"))

sparkR.version()

# Leitura dos dados
dados = read.df(
  path = "../dados/okcupid_profiles.csv",
  source = "csv",
  delimiter = ",",
  inferSchema = "true", # Inferir o tipo das colunas
  header = TRUE
)

### Conhecendo os dados
printSchema(dados)

nrow(dados)
ncol(dados)

View(head(dados, 5)) # Visualizar as 5 primeiras colunas

# Retirando colunas não desejadas
dados = drop(dados, "last_online")

abertas = paste0("essay", 0:9)

dados = drop(dados, abertas)

View(head(dados, 5))

# Contagem de gênero
gender_count = 
  collect(
    count(
      groupBy(dados, "sex")
      )
    )
print(gender_count)


# Também podesse usar consultas SQL 
createOrReplaceTempView(dados, "okcupid") # Cria uma tabela temporária para realização da query
collect(
  sql(
    "
    SELECT
      sex, COUNT(*) AS n
    FROM okcupid
    GROUP BY sex
    "
  )
)

require(ggplot2)
ggplot(gender_count) +
  geom_col(aes(x = sex, y = count))

# Separar a localização em cidade e estados
dados = transform(dados, location_city = regexp_replace(dados$location, ", .*", ""))
cidade_contagem = count(groupBy(dados, "location_city"))
collect(arrange(cidade_contagem, -cidade_contagem$count))
                
dados = transform(dados, location_state = regexp_replace(dados$location, ".*, ", ""))
estado_contagem = count(groupBy(dados, "location_state"))
collect(arrange(estado_contagem, -estado_contagem$count))

# Retirar coluna que não tinha tratamento
dados = drop(dados, dados$location)

collect(
  agg(
    groupBy(dados, dados$sex), # Agrupando dados por gênero
      max_age = max(dados$age), # Máximo
      min_age = min(dados$age), # Mínimo
      avg_age = avg(dados$age), # Média
      var_age = var(dados$age), # Variância
      med_age = percentile_approx(dados$age, .5) # Mediana
  )
)

status_count = 
  collect(
    arrange(
      count(
        groupBy(dados, "sex", "status")
      ), "sex", "count", decreasing = TRUE
    )
  )
print(status_count)

status_count$status = factor(status_count$status, unique(status_count$status))

ggplot(status_count) +
  geom_col(aes(x = sex, y = count, fill = status), position = "dodge2")

# Transformando em dados faltantes
dados = withColumn(dados, "income", ifelse(dados$income == -1, NA, dados$income))

# Contagem de dados faltantes na renda
NA_renda = filter(dados, isNull(dados$income)) # Filtrar dados faltantes na renda
percent_NA_renda = nrow(NA_renda)/nrow(dados)
print(percent_NA_renda)

# Coletando informações para realização do boxplot
boxplot_renda = collect(
  agg(
    groupBy(dados, dados$sex),
      med = percentile_approx(dados$income, percentage=.5),
      q1 = percentile_approx(dados$income, percentage=.25),
      q3 = percentile_approx(dados$income, percentage=.75))
)

options(scipen = 999)
ggplot(boxplot_renda,
       aes(x = sex,
           ymin = q1 - 1.5 * (q3 - q1),
           lower = q1,
           middle = med,
           upper = q3,
           ymax = q3 + 1.5 * (q3 - q1))) +
  geom_boxplot(stat = "identity") +
  ggtitle("Boxplot da Renda") +
  ylab("Renda") + xlab("") +
  theme_minimal() 

# Função para calcular histograma
hist_data = histogram(dados, dados$age, nbins = 16)
ggplot(hist_data) +
  geom_col(aes(x = centroids, y = counts))

# # Definir a quantidade de bins
# nbin = 30
# 
# # Calcular mínimo de x
# x_min = collect(agg(dados, min(dados$age)))
# # Calcular máximo de x
# x_max = collect(agg(dados, max(dados$age)))
# # Definir os intervalos para os bins de x
# x_bin = seq(floor(x_min[[1]]),
#             ceiling(x_max[[1]]),
#             length = nbin)
# 
# # Calcular tamanho do intervalo dos bins de x e y
# x_bin_width = x_bin[[2]] - x_bin[[1]]
# 
# # Calcular a qual bin pertece cada valor observado
# graph_data = withColumn(dados, "x_bin", ceiling((dados$age - x_min[[1]]) / x_bin_width))
# graph_data = mutate(graph_data, x_bin = ifelse(graph_data$x_bin == 0, 1, graph_data$x_bin))
# 
# graph_data = collect(agg(groupBy(graph_data, "x_bin"),
#                          count = n(graph_data$x_bin)))
# 
# ggplot(graph_data) +
#   geom_col(aes(x = x_bin, y = count))


caminho = "../dados/parquet"
# Salvar dados particionados
write.df(
  dados, caminho, "parquet", mode = "overwrite",
  partitionBy = "sex"
)

# Leitura dos dados particionados
dados_part = read.df(
  path = caminho,
  source = "parquet",
  delimiter = ",",
  inferSchema = "true", # Inferir o tipo das colunas
)

View(head(dados_part))

nrow(dados_part)
ncol(dados_part)


################################################################################
########################## Obrigado por participar! ############################
################################################################################
###################### (ﾉ◕ヮ◕)ﾉ*:･ﾟ✧ ✧ﾟ･: *ヽ(◕ヮ◕ヽ)  ########################
################################################################################
 