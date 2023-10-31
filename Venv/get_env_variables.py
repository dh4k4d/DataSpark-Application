import os
import local_variables as localVar

os.environ['envn'] = 'DEV'
os.environ['header'] = 'True'
os.environ['inferSchema'] = 'True'

header = os.environ['header']
inferSchema = os.environ['inferSchema']
envn = os.environ['envn']

appName = 'Data Processing App'

current = os.getcwd()

src_olap = current + "/Source/olap"

src_oltp = current + "/Source/oltp"

# MYSQL Connection
# get db cred
jdbc_url = localVar.jdbc_url
table = localVar.table
connection_properties = localVar.connection_properties