install.packages('rJava')
install.packages('RJDBC')

library(rJava)
library(RJDBC)

URL <- 'https://s3.amazonaws.com/athena-downloads/drivers/AthenaJDBC41-1.1.0.jar'
fil <- basename(URL)
if (!file.exists(fil)) download.file(URL, fil)
drv <- JDBC(driverClass="com.amazonaws.athena.jdbc.AthenaDriver", fil, identifier.quote="'")
con <- jdbcConnection <- dbConnect(drv, 'jdbc:awsathena://athena.us-west-1.amazonaws.com:443/',
                                   s3_staging_dir="s3://clusteryourself-home-projects.amazonaws.com",
                                   user=Sys.getenv("ATHENA_ACCESS_KEY_ID"),
                                   password=Sys.getenv("ATHENA_SECRET_ACCESS_KEY"))

setMethod("dbGetQuery", signature(conn="JDBCConnection", statement="character"),  def=function(conn, statement, ...) {
  r <- dbSendQuery(conn, statement, ...)
  on.exit(.jcall(r@stat, "V", "close"))
  if (conn@jc %instanceof% "com.amazonaws.athena.jdbc.AthenaConnection") fetch(r, -1, 999) # Athena can only pull 999 rows at a time
  else fetch(r, -1)
})

items <- dbGetQuery(con, "SELECT * FROM takehome.yerdle_item_data")
views <- dbGetQuery(con, "SELECT * FROM takehome.yerdle_pageviews")

#descriptive statistics/data exploration
plot(items$msrp_new, items$used_list_price)
table(items$used_condition) #100% OK
table(items$category) #23 categories: Appliances, Bags, etc.
table(items$department) #7 departments: Furniture, Kids Furniture, etc.
table(is.na(items$last_known_retail_price_new))/nrow(items) #NA for 55%
table(items$first_ordered_date == '')/nrow(items) #31% not sold

#new variables of interest
items$used2full <- items$used_list_price/items$msrp_new

# pageviews as demand proxy