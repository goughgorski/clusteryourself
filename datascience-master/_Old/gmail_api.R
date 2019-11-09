#Clear Workspace
rm(list=ls())

#Load libraries

source('~/workspace/datascience/functions/load_libraries.R')
load_libraries('~/workspace/datascience/functions/')

#Set Seed
set.seed(123456789)

#Connect to Local PSQL Database
local_connection <- local_db_conect('development')
prod_connection <- heroku_connect_app_db('amitree')

#Get application credentials
credentials <- read.delim2('~/workspace/amitree/.env',sep = '=', header = FALSE, stringsAsFactors = FALSE)
gmail_client_id <- credentials[credentials[,1]=="GMAIL_CLIENT_ID",2]
gmail_client_secret <- credentials[credentials[,1]=="GMAIL_CLIENT_SECRET",2]
req <- GET("https://www.googleapis.com/",  config(token = check))
 
req <- GET("https://www.googleapis.com/oauth2/v1/userinfo?access_token=ya29.GlvoBFVx9DksoLD9uN2Hv50h5t9OXrNBXYuKu0nWg1w3PLUEukx9GsOc87AmHXt79EWDqkjPw0sYPsgdngzTacCl_PwVic3m6VlMMdW0oBQGKkQx4jnQw6eOoJ3l")

req <- GET("https://www.googleapis.com/gmail/v1/users/luke.amtriee@gmail.com/messages?access_token=ya29.GlvoBFVx9DksoLD9uN2Hv50h5t9OXrNBXYuKu0nWg1w3PLUEukx9GsOc87AmHXt79EWDqkjPw0sYPsgdngzTacCl_PwVic3m6VlMMdW0oBQGKkQx4jnQw6eOoJ3l")
GET https://www.googleapis.com/gmail/v1/users/userId/messages


 config(token = 'ya29.GlvoBFVx9DksoLD9uN2Hv50h5t9OXrNBXYuKu0nWg1w3PLUEukx9GsOc87AmHXt79EWDqkjPw0sYPsgdngzTacCl_PwVic3m6VlMMdW0oBQGKkQx4jnQw6eOoJ3l')

test <- message('15a3f047d06ba94e', user_id = "me", format = c("full"))


oauth2 <- gmail_auth(scope = "read_only", id = gmail_client_id, secret = gmail_client_secret, secret_file = NULL)


gmail_btoken_req <- POST(token_uri, body=body, add_headers(.headers = head), verbose())
access_token <- content(gmail_btoken_req)$access_token
names(access_token) <- c("Authorization")

https://accounts.google.com/o/oauth2/token 
client_id={ClientId}.apps.googleusercontent.com&client_secret={ClientSecret}&refresh_token=1/ffYmfI0sjR54Ft9oupubLzrJhD1hZS5tWQcyAvNECCA&grant_type=refresh_token 

auth_uri <- 'https://accounts.google.com/o/oauth2/auth'
token_uri <- 'https://accounts.google.com/o/oauth2/token'



https://www.googleapis.com/auth/gmail.readonly




url <- "https://www.googleapis.com/oauth2/v4/token"
refresh_token <- '1/vbnPnQQpRH8t1nbmrrwapvk6ukBGYF8XAYGwHeFfbKwVVbpX-F4FNei3jDu6WEAu'
body <- list(client_id = gmail_client_id, client_secret = gmail_client_secret, refresh_token = refresh_token, grant_type = 'refresh_token')
POST(url, body = body)


b2 <- "http://httpbin.org/post"
POST(b2, body = "A simple text string")


head <- c(
          "api.twitter.com",
          "PS Pipe",
          paste("Basic",key_secret_enc),
          "multipart/form-data", 
          "163", 
          "gzip"
          )

names(head) <- c(
  "Host",
  "User-Agent",
  "Authorization",
  "Content-Type",
  "Content-Length",
  "Accept-Encoding"
  )

url <- 'https://api.twitter.com/oauth2/token'
body <- list(grant_type = 'client_credentials')




user_email <- 'luke.amtriee@gmail.com'
message_id <- '1'
request <- paste('https://www.googleapis.com/gmail/v1/users/'
                    , user_email
                    , '/messages/'
                    , message_id, sep = '')


req <- GET("https://www.googleapis.com/oauth2/v1/userinfo",
  config(token = 'ya29.GlvoBFVx9DksoLD9uN2Hv50h5t9OXrNBXYuKu0nWg1w3PLUEukx9GsOc87AmHXt79EWDqkjPw0sYPsgdngzTacCl_PwVic3m6VlMMdW0oBQGKkQx4jnQw6eOoJ3l'))

google_oauth_endpoints <- oauth_endpoints("google")
GET 'https://www.googleapis.com/gmail/v1/users/userId/messages/id'


head <- c()

names(head) <- c(
  "Authorization",
  )

access_token <- 'ya29.GlvoBFVx9DksoLD9uN2Hv50h5t9OXrNBXYuKu0nWg1w3PLUEukx9GsOc87AmHXt79EWDqkjPw0sYPsgdngzTacCl_PwVic3m6VlMMdW0oBQGKkQx4jnQw6eOoJ3l'
refresh_token = '1/vbnPnQQpRH8t1nbmrrwapvk6ukBGYF8XAYGwHeFfbKwVVbpX-F4FNei3jDu6WEAu'




# 2. Register an application at https://cloud.google.com/console#/project
#    Replace key and secret below.
myapp <- oauth_app("google",
  key = "16795585089.apps.googleusercontent.com",
  secret = "hlJNgK73GjUXILBQvyvOyurl")

# 3. Get OAuth credentials
google_token <- oauth2.0_token(google_oauth_endpoints, myapp,
  scope = "https://www.googleapis.com/auth/userinfo.profile")


# 4. Use API
req <- GET("https://www.googleapis.com/oauth2/v1/userinfo",
  config(token = google_token))
stop_for_status(req)
content(req)



GET('https://www.googleapis.com/oauth2/v3/tokeninfo?id_token=ya29.GlvoBFVx9DksoLD9uN2Hv50h5t9OXrNBXYuKu0nWg1w3PLUEukx9GsOc87AmHXt79EWDqkjPw0sYPsgdngzTacCl_PwVic3m6VlMMdW0oBQGKkQx4jnQw6eOoJ3l')



endpoint <- oauth_endpoints("google")
secrets <- jsonlite::fromJSON("~/workspace/datascience/r_dev/client_secrets.json")
scope <- "https://www.googleapis.com/auth/messages.readonly"

token <- oauth_service_token(endpoint, secrets, scope)

add_head <- add_headers(
      Authorization = paste('Bearer', access_token)
    )



req <- GET("https://www.googleapis.com/auth/userinfo.profile", add_headers(Authorization = paste('Bearer', access_token)))
req <- GET("https://www.googleapis.com/gmail/v1/users/luke.amtriee@gmail.com/messages", add_headers(Authorization = paste('Bearer', access_token)))
req <- GET(paste("https://www.googleapis.com/oauth2/v4/token/",refresh_params,sep = ''))
req

client_id <- paste('client_id=',gmail_client_id,'&', sep = '')
client_secret <- paste('client_secret=',gmail_client_secret,'&', sep = '')
refresh_token <- paste('refresh_token=','1/vbnPnQQpRH8t1nbmrrwapvk6ukBGYF8XAYGwHeFfbKwVVbpX-F4FNei3jDu6WEAu','&', sep = '')
grant_type <- paste('grant_type=','refresh_token', sep = '')

refresh_params <- paste(client_id, client_secret, refresh_token, grant_type, sep = '')



