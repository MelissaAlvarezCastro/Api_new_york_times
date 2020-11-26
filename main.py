# Demo code sample. Not indended for production use.

# See instructions for installing Requests module for Python
# http://docs.python-requests.org/en/master/user/install/

import requests
import json

key = "kbxT5wfzUZXcLkbSzMYLeuZ2MLp5zdDr"

def executeArchivo(year, month):

  requestUrl = "https://api.nytimes.com/svc/archive/v1/"+year+"/"+month+".json?api-key=" + key
  requestHeaders = {
    "Accept": "application/json"
  }
  request = requests.get(requestUrl, headers=requestHeaders)
  request_json = request.json()
  with open("Archivo_"+year+"_"+month+".json", "w") as file:
    json.dump(request_json, file, indent=4)

if __name__ == "__main__":
  limit_year = 2020
  limit_month = 6

  for i in range (2019,limit_year):
    for j in range (1,limit_month):
      print (i, j)
      executeArchivo(str(i), str(j))
