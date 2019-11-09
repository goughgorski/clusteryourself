REMCONPATH='/Users/luke/workspace/amitree'
GETEMAILPATH='/Users/luke/workspace/datascience/data_pipeline'
export TOKENID=${1}
export MESSAGEID='${2}'
export PART='${3}'
#export TOKENID=56882
#export MESSAGEID='15eed4ce2d9e3e5a'
#export PART='from'

cd
cd ${REMCONPATH}
${REMCONPATH}/bin/remcon -g prod << EORUBY > /tmp/tester.txt
puts hi
token = GmailToken.find(ENV['TOKENID'])
api = GmailAPI.new(token)
message = api.find_message(ENV['MESSAGEID'])	
out = message.send(ENV['PART'])
puts out

EORUBY 