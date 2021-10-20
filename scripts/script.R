system("java -version")

SPARK_HOME = "D:/spark-3.1.2-bin-hadoop2.7"

Sys.setenv(SPARK_HOME=SPARK_HOME)


library(SparkR,
        lib.loc=c(file.path(Sys.getenv("SPARK_HOME"), "R", "lib")))


sparkR.session(master = "local[*]",
               sparkConfig = list(spark.driver.memory = "1g"))

sparkR.version()

dados = read.df(
  path = "../dados/okcupid_profiles.csv",
  source = "csv",
  delimiter = ",",
  inferSchema = "true",
  header = TRUE
)

nrow(dados)
ncol(dados)

printSchema(dados)

View(head(dados, 5))

# Retirando colunas
dados = drop(dados, "last_online")

abertas = paste0("essay", 0:9)

dados = drop(dados, abertas)

# Visualizando...
View(head(dados, 5))


gender_count = 
  collect(
    count(
      groupBy(dados, "sex")
      )
    )
print(gender_count)

createOrReplaceTempView(dados, "okcupid")
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

dados = withColumn(dados, "income", ifelse(dados$income == -1, NA, dados$income))

NA_renda = filter(dados, isNull(dados$income))
percent_NA_renda = nrow(NA_renda)/nrow(dados)
print(percent_NA_renda)

dados = transform(dados, location_city = regexp_replace(dados$location, ", .*", ""))
cidade_contagem = count(groupBy(dados, "location_city"))
collect(arrange(cidade_contagem, -cidade_contagem$count))
                
dados = transform(dados, location_state = regexp_replace(dados$location, ".*, ", ""))
estado_contagem = count(groupBy(dados, "location_state"))
collect(arrange(estado_contagem, -estado_contagem$count))


collect(
  agg(
    groupBy(dados, dados$sex),
      max_age = max(dados$age),
      min_age = min(dados$age),
      avg_age = avg(dados$age),
      med_age = percentile_approx(dados$age, .5)
  )
)


boxplot_age = collect(
  agg(
    groupBy(dados, dados$sex),
      med = percentile_approx(dados$income, percentage=.5),
      q1 = percentile_approx(dados$income, percentage=.25),
      q3 = percentile_approx(dados$income, percentage=.75))
)

options(scipen = 999)

ggplot(boxplot_age,
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

