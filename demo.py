import json
import boto3
import logging
import pymysql



secret = boto3.client('secretsmanager')
vault = secret.get_secret_value(SecretId='databasekey')
keeper=json.loads(vault['SecretString'])

rds_host  = keeper['host']
name = keeper['username']
password = keeper['password']
db_name = keeper['dbname']

logger = logging.getLogger()
logger.setLevel(logging.INFO)

try:
    conn = pymysql.connect(host=rds_host, user=name, passwd=password, db=db_name, connect_timeout=5)
    cursor=conn.cursor()
    cursor.execute("show tables")
    res=cursor.fetchall()
    print(res)
except pymysql.MySQLError as e:
    logger.error("ERROR: Unexpected error: Could not connect to MySQL instance.")
    logger.error(e)
    sys.exit()

logger.info("SUCCESS: Connection to RDS MySQL instance succeeded")



def lambda_handler(event, context):
    s3_obj =boto3.client('s3')
    dynamo=boto3.client('dynamodb')

    s3_clientobj = s3_obj.get_object(Bucket='demo159', Key='demo.json')
    s3_clientdata = s3_clientobj['Body'].read().decode('utf-8')
    data=json.loads(s3_clientdata)
    print(type(data))
    list_data=data['list']
    
    mysql_insert = '''insert ignore into weather (dt_txt,weather_description,temp,temp_min,temp_max,pressure,humidity,visibility,wind_speed,wind_dog,cloud,sunrise,sunset) 
    values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
    
    # dynamo.put_item(TableName='weather_bangalore',Item=data)
    print(type(data['list']))
    
    req_data={}
    my_list=[]
    i=2
    for item in list_data:
        
        req_data["dt_txt"]=item["dt_txt"]
        for item2 in item["weather"]:
            req_data["description"]=item2["description"]
        req_data["temp"]=item["main"]["temp"]
        req_data["temp_min"]=item["main"]["temp_min"]
        req_data["temp_max"]=item["main"]["temp_max"]
        req_data["pressure"]=item["main"]["pressure"]
        req_data["humidity"]=item["main"]["humidity"]
        req_data["visibility"]=item["visibility"]
        req_data["wind_speed"]=item["wind"]["speed"]
        req_data["wind_deg"]=item["wind"]["deg"]
        req_data["cloud"]=item["clouds"]["all"]
        req_data["sunrise"]=data["city"]["sunrise"]
        req_data["sunset"]=data["city"]["sunset"]
        for x in req_data.values():
            my_list.append(x)
        
        cursor.execute(mysql_insert,my_list)
        my_list.clear()
        
        
        
        
    # cursor.execute('''insert into demo(id,name,salary) values(2,"raghu",50000)''')
    # cursor.execute("select * from demo")
    # print(my_list)
    
    # cursor.executemany(mysql_insert,my_list)
    # cursor.execute("select * from weather")
    # res=cursor.fetchall()
    # print(res)
    conn.commit()

        
        
        
        
        
        
        # print(item["clouds"]["all"])
    # print(type(req_data[0]))
    # dynamo.put_item(TableName='weather_bangalore',Item={req_data})
    
    return {
        'statusCode': 200,
        'body': json.dumps(req_data)
        
    }
