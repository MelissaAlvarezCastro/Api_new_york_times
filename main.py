# Demo code sample. Not indended for production use.

# See instructions for installing Requests module for Python
# http://docs.python-requests.org/en/master/user/install/

import requests
import json

key = "kbxT5wfzUZXcLkbSzMYLeuZ2MLp5zdDr"

def executeListName():
  requestUrl = "https://api.nytimes.com/svc/books/v3/lists/names.json?api-key=" + key
  requestHeaders = {
    "Accept": "application/json"
  }

  request_list = requests.get(requestUrl, headers=requestHeaders)
  request_list_json = request_list.json()
  with open("List_name.json", "w") as file:
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
  with open("Archivo_"+year+"_"+month+".json", "w") as file:
    json.dump(request_archive_json, file, indent=4)


def executeMasVendido(lists_name, date):
  requestUrl = "https://api.nytimes.com/svc/books/v3/lists/"+date+"/"+lists_name+".json?offset=20&api-key=" + key
  requestHeaders = {
    "Accept": "application/json"
  }

  request_MasVendido = requests.get(requestUrl, headers=requestHeaders)
  request_json = request_MasVendido.json()
  with open("Libros_"+date+".json", "w") as file:
    json.dump(request_json, file, indent=4)


def executePublicados(date):
  requestUrl = "https://api.nytimes.com/svc/books/v3/lists/overview.json?published_date="+date+"&api-key=" + key
  requestHeaders = {
    "Accept": "application/json"
  }

  request_Publicados = requests.get(requestUrl, headers=requestHeaders)
  request_Publicados_json = request_Publicados.json()
  with open("Publicado_"+date+".json", "w") as file:
    json.dump(request_Publicados_json, file, indent=4)


def executeMasPopulares(periodo):

  requestHeaders = {
    "Accept": "application/json"
  }

  requestUrlEnviados = "https://api.nytimes.com/svc/mostpopular/v2/emailed/"+periodo+".json?api-key=" + key
  request_enviados = requests.get(requestUrlEnviados, headers=requestHeaders)
  request_enivados_json = request_enviados.json()
  with open("Mas_populares_enviados.json", "w") as file:
    json.dump(request_enivados_json, file, indent=4)

  requestUrlCompartidos = "https://api.nytimes.com/svc/mostpopular/v2/shared/30.json?api-key=" + key
  request_compartidos = requests.get(requestUrlCompartidos, headers=requestHeaders)
  request_compartidos_json = request_compartidos.json()
  with open("Mas_populares_compartidos.json", "w") as file:
    json.dump(request_compartidos_json, file, indent=4)

  requestUrlVistos = "https://api.nytimes.com/svc/mostpopular/v2/viewed/30.json?api-key=" + key
  request_vistos = requests.get(requestUrlVistos, headers=requestHeaders)
  request_vistos_json = request_vistos.json()
  with open("Mas_populares_vistos.json", "w") as file:
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