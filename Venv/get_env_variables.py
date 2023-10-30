import os

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