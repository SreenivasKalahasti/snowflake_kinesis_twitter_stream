from tweepy import OAuthHandler, Stream, StreamListener
import hidden
import sys
import boto3
import json
import snowflake.connector
import snowconnector


# search terms for twitter stream
keywords = []
for arg in sys.argv[1:]:
    arg = str(arg)
    keywords.append(arg)
    print(arg)
# twitter creds
secrets = hidden.oauth()
client_id = secrets['consumer_key']
client_secret = secrets['consumer_secret']
token_key = secrets['token_key']
token_secret = secrets['token_secret']

# aws creds
aws = hidden.aws()
ID = aws['ID']
KEY = aws['KEY']
REGION = aws['REGION']


# snowflake creds
snow = snowconnector.letitsnow()
user = snow['userid']
passwd = snow['password']
acct = snow['account']

# ------------- user should go to AWS console and follow these instructions ----------
print("\n============ Please examine the following checklist =============\n")
print('''      a. make bucket in S3        -> copy bucket/folder ARN 
//             b. in IAM, make policy      -> copy snowdoc JSON, edit both "resource allocations" to bucket/folder ARN and bucket ARN respectively
//             c. in IAM, make role        -> put in Acct # and dummy External ID like "0000" (((UDPATE LATER))), copy role ARN
//             d. Snowflake Integration    -> paste role ARN & s3 bucket path, run, DESC INTEGRATION 'X', copy External ID & IAM_USER_ARN
//             e. in IAM, roles,           -> edit trust relationship in JSON with these two values from d.
//             f. Snowflake Stage          -> just integration and bucket/folder path will do.
//             g. Snowflake Pipe           -> auto_ingest = true, copy into from @stage, file_format important here. Run "show pipes;" copy "noti channel"
//             h. in S3, create Event      -> paste Notification Channel ARN
//             i. you got data, King. 
''')

# -------------- input for AWS -------------

role_arn = input('Please enter your role ARN\n')
s3_path = input('Please enter your S3 bucket path\n')
s3_path_folder = input('Please enter your S3 bucket path including the folder path data flows into\n')

# --------------- models ------------------

class snowcon():

    def __init__(self):
        self.conn = snowflake.connector.connect(user = user, password = passwd, account = acct)
        self.db = self.conn.cursor()
        
    def create_json_table(self):
        sql = '''CREATE TABLE TWITTER_DB.PUBLIC.tweets_json (tweet VARIANT);'''
        self.db.execute(sql)
    
    def create_integration(self):
        sql = f'''CREATE OR REPLACE STORAGE INTEGRATION twitter_integration
                 TYPE = EXTERNAL_STAGE
                 STORAGE_PROVIDER = S3
                 ENABLED = TRUE
                 STORAGE_AWS_ROLE_ARN = {role_arn}
                 STORAGE_ALLOWED_LOCATIONS =({s3_path});'''
        self.db.execute(sql)
    
    def create_stage(self):
        sql = f'''create or replace stage TWITTER_DB.PUBLIC.twitter_stage
                  url = {s3_path_folder}
                  storage_integration = twitter_integration;'''
        self.db.execute(sql)
    
    def create_pipe(self):
        sql = '''create or replace pipe TWITTER_DB.PUBLIC.twitter_pipe auto_ingest = true as
                 copy into TWITTER_DB.PUBLIC.tweets_json
                 from @TWITTER_DB.PUBLIC.twitter_stage
                 file_format = 'json';'''

    def select(self):
        sql = '''select * from TWITTER_DB.PUBLIC.tweets_json limit 10;'''
        self.db.execute(sql)
        return self.db.fetchall()

class aws_kinesis():

    def __init__(self):
        self.session = boto3.Session(aws_access_key_id = ID,aws_secret_access_key = KEY,region_name=REGION)

    def kinesis(self):
        kinesis = self.session.client('firehose')
        return kinesis

    def send_to_stream(self,data):
        response = self.kinesis().put_record(DeliveryStreamName = 'snowpipe_delivery_stream',Record = {'Data': data.encode()})   
        return response

class StreamTweets(StreamListener):

    def on_data(self,data):
        response = aws_kinesis.send_to_stream(data)
        print(json.dumps(data))
        return True
    
    def on_status(self,status):

        locations = ["Houston, TX", "New York, NY", "Los Angeles, CA", "San Francisco, CA", "Boston, MA"]

        if status.favorite_count is None or status.favorite_count < 10:
            return
        elif (status.retweeted) and ('RT @' in status.text):
            return
        elif status.user.location not in locations:
            return
    
    def on_error(self,status):
        if status == 420:
            return False

if len(keywords) == 0:
    kw = input("Please enter a keyword to search:\n")
    keywords.append(kw)

# --------- script ------------

aws_kinesis = aws_kinesis()
listener = StreamTweets()
auth = OAuthHandler(client_id,client_secret)
auth.set_access_token(token_key,token_secret)

    
stream = Stream(auth=auth, listener=listener)
stream.filter(track=keywords)

def snowpipe():
    snowcon = snowcon()
    snowcon.create_json_table()
    snowcon.create_integration()
    snowcon.create_stage()
    snowcon.create_pipe()
    test = snowcon.select()
    return print(test)

#snowpipe()
