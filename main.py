# Demo code sample. Not indended for production use.

# See instructions for installing Requests module for Python
# http://docs.python-requests.org/en/master/user/install/

import requests
import json
import boto3
from datetime import datetime

key = "kbxT5wfzUZXcLkbSzMYLeuZ2MLp5zdDr"
archivos = []
archivosListName = []
archivosArchivoMensual = []
archivosMasVendido = []
archivosPublicado = []
archivosMasPopulares = []

def executeListName():
  requestUrl = "https://api.nytimes.com/svc/books/v3/lists/names.json?api-key=" + key
  requestHeaders = {
    "Accept": "application/json"
  }

  request_list = requests.get(requestUrl, headers=requestHeaders)
  request_list_json = request_list.json()

  nombre_archivo = "List_name"
  archivos.append(nombre_archivo)

  with open("consultas/"+nombre_archivo+".json", "w") as file:
    json.dump(request_list_json, file, indent=4)

  request_Array = request_list_json["results"]
  lists_name = []
  for i in request_Array:
    lists_name.append(i["list_name_encoded"])
  return lists_name


def executeArchivo(year, month):
  requestUrl = "https://api.nytimes.com/svc/archive/v1/"+year+"/"+month+".json?api-key=" + key
  requestHeaders = {
    "Accept": "application/json"
  }

  request_archive = requests.get(requestUrl, headers=requestHeaders)
  request_archive_json = request_archive.json()

  nombre_archivo = "Archivo_"+year+"_"+month
  archivos.append(nombre_archivo)

  with open("consultas/"+nombre_archivo+".json", "w") as file:
    json.dump(request_archive_json, file, indent=4)


def executePublicados(date):
  requestUrl = "https://api.nytimes.com/svc/books/v3/lists/overview.json?published_date="+date+"&api-key=" + key
  requestHeaders = {
    "Accept": "application/json"
  }

  request_Publicados = requests.get(requestUrl, headers=requestHeaders)
  request_Publicados_json = request_Publicados.json()

  nombre_archivo = "Publicado_"+date
  archivos.append(nombre_archivo)

  with open("consultas/"+nombre_archivo+".json", "w") as file:
    json.dump(request_Publicados_json, file, indent=4)


def executeMasPopulares(periodo):

  requestHeaders = {
    "Accept": "application/json"
  }

  requestUrlEnviados = "https://api.nytimes.com/svc/mostpopular/v2/emailed/"+periodo+".json?api-key=" + key
  request_enviados = requests.get(requestUrlEnviados, headers=requestHeaders)
  request_enivados_json = request_enviados.json()

  nombre_archivo = "Mas_populares_enviados"
  archivos.append(nombre_archivo)

  with open("consultas/"+nombre_archivo+".json", "w") as file:
    json.dump(request_enivados_json, file, indent=4)

  requestUrlCompartidos = "https://api.nytimes.com/svc/mostpopular/v2/shared/30.json?api-key=" + key
  request_compartidos = requests.get(requestUrlCompartidos, headers=requestHeaders)
  request_compartidos_json = request_compartidos.json()

  nombre_archivo = "Mas_populares_compartidos"
  archivos.append(nombre_archivo)

  with open("consultas/"+nombre_archivo+".json", "w") as file:
    json.dump(request_compartidos_json, file, indent=4)

  requestUrlVistos = "https://api.nytimes.com/svc/mostpopular/v2/viewed/30.json?api-key=" + key
  request_vistos = requests.get(requestUrlVistos, headers=requestHeaders)
  request_vistos_json = request_vistos.json()

  nombre_archivo = "Mas_populares_vistos"
  archivos.append(nombre_archivo)

  with open("consultas/"+nombre_archivo+".json", "w") as file:
    json.dump(request_vistos_json, file, indent=4)


if __name__ == "__main__":
  limit_year = 2015
  limit_month = 7

  for i in range (2014,limit_year):
    for j in range (1,limit_month):
      executeArchivo(str(i), str(j))

  executeListName()

  ArrayDates = ["2013-05-20","2010-10-01","2009-06-05","2018-02-13","2017-12-24","2016-10-13","2009-12-15","2011-05-24","2019-08-08"]

  for i in ArrayDates:
    executePublicados(i)

  periodo = str(30)   #Periodo de 3, 7, 30 dias
  executeMasPopulares(periodo)

  dt = datetime.utcnow()

  #Conexion Amazon S3

  # Cliente con las credenciales
  client = boto3.client('s3', aws_access_key_id="AKIA6QHPHXNS7OJREYFG", aws_secret_access_key="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") #Credenciales IAM James
  #client = boto3.client('s3', aws_access_key_id="AKIA6QHPHXNSQAX55NEJ", aws_secret_access_key="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")  # Credenciales IAM Melissa

  # Upload a new file
  for i in archivos:
    # Informacion necesaria para cargar al bucket
    ruta = '/home/ubuntu/consultas/'
    name_bucket = 'my-bucket-prueba1'
    save_route = 'Cargas/'+str(dt)+'/'

    # Carga del archivo hacia el bucket
    client.upload_file(ruta+i+".json", name_bucket, save_route)
