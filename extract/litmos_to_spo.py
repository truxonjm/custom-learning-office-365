import lxml.etree as ET
from datetime import datetime
from flask import Flask, request, jsonify, make_response, Blueprint
from flask_restplus import Api, Resource
import os, json, pyodbc, platform
from functools import wraps 

from flask_restplus.apidoc import apidoc


#--------------------------------------------------------------------------------------------------------
from functools import wraps 
import logging, logging.handlers
smtp_handler = logging.handlers.SMTPHandler(mailhost=("smtp.cmog.org", 25),
                                            fromaddr="noreply@cmog.org", 
                                            toaddrs="truxonjm@cmog.org",
                                            subject=u"[ERROR] Litmos-SPO Export")
logger = logging.getLogger()
logger.addHandler(smtp_handler)


#--------------------------------------------------------------------------------------------------------
def exceptions_monitored( logger ): 
      
    def decorator(func): 
          
        @wraps(func) 
        def wrapper(*args, **kwargs): 
        
            try:
                return func(*args, **kwargs) 
            except Exception as e:
                tb = None
                try: tb = e.traceback.format_exc() 
                except: pass            
                issue = {"result":"Error", "function": f"{func.__name__}", "message":repr(e), "traceback":tb} 
                logger.exception(msg="[ERROR]", extra=issue)
            raise              
          
        return wrapper 
    return decorator 

def getEnv(name):
    result = ''
    try:
        result = os.environ[name]
    except:
        os.environ[name]=''
    return result

api_url_prefix=getEnv('API_URL_PREFIX')

apidoc.url_prefix = api_url_prefix

flask_app = Flask(__name__)

from werkzeug.middleware.proxy_fix import ProxyFix
flask_app.wsgi_app = ProxyFix(flask_app.wsgi_app, x_proto=1, x_host=1)

blueprint = Blueprint('api', __name__, url_prefix=f'{api_url_prefix}')

app = Api(app=blueprint 
            #,doc=f'/doc/'
            ,description='Main APIs'
            ,version='1.0'
            ,title='Litmos-SPO Exporter')

flask_app.register_blueprint(blueprint)

ns = app.namespace('api',description='Main APIs')


#this cannot appear inside a class - python interpreter doesn't like the **a, **b syntax to appear in that context for some reason
dictsum = lambda a, b: {**a,**b}
getpath = lambda filename: os.path.dirname(filename)


default_specs = {
    "database_name" : "Training_Portal"
    ,"server_name" : "cmogreport.cmog.org"
    ,"smtp_server_name" : "smtp.cmog.org"
    ,"output_root" : "."
}

#try to update default specs from environment, where supplied
for k in default_specs.keys():
    try:
        default_specs[k] = os.environ[k.upper()]
    except: pass
    
#########################################################################################
isnull = lambda value, default: value if value is not None else default
isempty = lambda value, default: value if value != '' else default
sql_escape = lambda sql: isnull(sql,'').replace("'","''") 
str2bool = lambda v: v.lower() in ("yes", "true", "t", "1")
def dict_concat(a, b): 
    #redundant, need to refactor to use dictsum directly or get rid of dictsum
    return dictsum(a,b)  
get_string = lambda fieldName: lambda obj, root: "'{}'".format(sql_escape(obj.find(fieldName).text)) if obj.find(fieldName) is not None else ''
get_value = lambda fieldName, default='null': lambda obj, root: isempty("{}".format(sql_escape(obj.find(fieldName).text)),default)
get_constant = lambda constValue: lambda obj, root: constValue
get_boolean_bit = lambda fieldName: lambda obj, root: 1 if str2bool(obj.find(fieldName).text) else 0
get_username_from_email = lambda fieldName: lambda obj, root: "'{}'".format(sql_escape(obj.find(fieldName).text.split('@')[0]))
get_parent_element = lambda depth, fieldFn: lambda obj, root: fieldFn(get_element_ancestry(root, obj)[depth], root)

placeholder = lambda objArray: ",".join(["{}" for i in range(len(objArray))])
simple_insert = lambda tableName, valueArray: "insert into {} values ({})".format(tableName,placeholder(valueArray)).format(*valueArray)
recordset = lambda rows, columnNames: [{col[0]:col[1] for col in zip(columnNames,row) } for row in rows ]


#---------------------------------------------------------------
def try_except(success, failure, *exceptions):
    try:
        return success()
    except exceptions or Exception:
        return failure() if callable(failure) else failure


#########################################################################################
def connection_string(database_name, server_name):
    if platform.system()=="Windows":
        cnx_str = 'DRIVER={{SQL Server}};Server={};Integrated Security=SSPI;Database={}'
    else:
        cnx_str = 'DRIVER={{ODBC Driver 17 for SQL Server}};Server={};Database={};UID='+os.environ["ODBC_UID"]+';PWD='+os.environ["ODBC_PWD"]
    return cnx_str.format(server_name, database_name)

def runsql(sqlstr, *params):
    import pyodbc
    cnx_str = connection_string(server_name = default_specs["server_name"], database_name = default_specs["database_name"])

    #print(cnx_str)

    _rows_and_columns = lambda cursor: (cursor.fetchall(), [column[0] for column in cursor.description])
    _runsql = lambda sql, params=None: _rows_and_columns(pyodbc.connect(cnx_str,autocommit=True).cursor().execute(sql,params if params is not None else ()))
    #_runsql = lambda sql, params=None: pyodbc.connect(cnx_str,autocommit=True).cursor().execute(sql,params if params is not None else ()).fetchall()
    result, columns = _runsql(sqlstr,*params)

    #print(result)
    return result, columns
        

def cmog_secret(keyid): 
    result, columns = runsql("select AppKeys.dbo.GetKeyValue(?) [ApiKey]",(keyid))[0][0]

    #print(result)
    return result

max_rate_per_minute = 50
ONE_MINUTE = 60


#--------------------------------------------------------------------------------------------------------
@exceptions_monitored( logger ) 
class Class_Extractor():
    def __init__(self, **kwargs):
        super().__init__()
        for k, v in kwargs.items():
            try:
                setattr(self, k, v)
            except:
                pass
                    
    def Extract_Courses(self):
        assets = recordset(*runsql("exec [REPORT_Extract_Courses_For_SharePoint] "))
        with open(f'{self.output_root}\\assets.json','w') as f:
            f.write(json.dumps(assets, indent=2))
        metadata={
                    "Technologies": recordset(*runsql("select * from SPO_Technologies")),
                    "Categories": [{**i, "SubCategories":[]} for i in recordset(*runsql("select * from SPO_Categories"))],
                    "Audiences": recordset(*runsql("select * from SPO_Audiences")),
                    "Sources": ["Litmos","Wombat"],
                    "Levels": recordset(*runsql("select * from SPO_Levels")),
                    "StatusTag": recordset(*runsql("select * from SPO_StatusTags"))
                } 
        with open(f'{self.output_root}\\metadata.json','w') as f:
            f.write(json.dumps(metadata, indent=2))
            
        return True
 
        
########################################################################################################################################################
@exceptions_monitored( logger ) 
@ns.route('/execute')
class Endpoint(Resource):
    def __init__(self, *args, **kwargs):
        for k, v in kwargs.items():
            try:
                setattr(self, k, v)
            except: pass
            
        super().__init__()

    def execute(self):
        worker = Class_Extractor(**default_specs) 
        result = ""
        try:
            result = worker.Extract_Courses()
           
        except Exception as e:
            print(f"Exception: {repr(e)}")
            tb = None
            try: tb = e.traceback.format_exc() 
            except: pass            
            return {"result":"Error", "message":repr(e), "traceback":tb}
        return result
                    
    def get(self):
        return self.execute()
    

if __name__ == "__main__":
    flask_app.run(debug=True,host='0.0.0.0',port=8080)



