import pandas as pd
import json
import datetime
import boto3
from sqlalchemy import create_engine

def executeArchivoMensual(client, name_bucket, list_mensual):
    array_json = []
    array_docs_json = []

    for ruta in list_mensual:
        s3_clientobj = client.get_object(Bucket=name_bucket, Key=ruta['Key'])

        file_content = s3_clientobj['Body'].read().decode('utf-8')
        data = json.loads(file_content)

        date = datetime.datetime.now()

        data.update({
            'end_point': 'https://api.nytimes.com/svc/archive/v1/{year}/{month}.json',
            'origin': 'New York Times APIs',
            'date_query': ruta['LastModified'], #Asociar hora bucket
            'date_warehouse': date,
            'num_docs': len(data['response']['docs']),
        })

        data_docs = data['response']['docs']
        for i in data_docs:
            i.update({
                'end_point': 'https://api.nytimes.com/svc/archive/v1/{year}/{month}.json',
                'origin': 'New York Times APIs',
                'date_query': ruta['LastModified'], #Asociar hora bucket
                'date_warehouse': date,
            })

        array_json.append(data)
        for item in data_docs:
            array_docs_json.append(item)

    data_df_archivosMensual = pd.DataFrame(array_json)
    data_to_db_archivosMensual = data_df_archivosMensual[['end_point','origin','date_query','date_warehouse','copyright','num_docs']]

    data_df_docs = pd.DataFrame(array_docs_json)
    data_to_db_docs = data_df_docs[['end_point','origin','date_query','date_warehouse','abstract','web_url','pub_date','document_type','section_name','uri']]

    return data_to_db_archivosMensual, data_to_db_docs

def executeListName(client, name_bucket, list_name):
    array_json = []
    array_docs_json = []

    for ruta in list_name:
        s3_clientobj = client.get_object(Bucket=name_bucket, Key=ruta['Key'])

        file_content = s3_clientobj['Body'].read().decode('utf-8')
        data = json.loads(file_content)

        date = datetime.datetime.now()

        data.update({
            'end_point': 'https://api.nytimes.com/svc/books/v3/lists/names.json',
            'origin': 'New York Times APIs',
            'date_query': ruta['LastModified'], #Asociar hora bucket
            'date_warehouse': date,
        })

        data_docs = data['results']
        for i in data_docs:
            i.update({
                'end_point': 'https://api.nytimes.com/svc/archive/v1/{year}/{month}.json',
                'origin': 'New York Times APIs',
                'date_query': ruta['LastModified'], #Asociar hora bucket
                'date_warehouse': date,
            })

        array_json.append(data)
        for item in data_docs:
            array_docs_json.append(item)

    data_df_archivosMensual = pd.DataFrame(array_json)
    data_to_db_archivosMensual = data_df_archivosMensual[['end_point', 'origin', 'date_query', 'date_warehouse', 'copyright', 'status', 'num_results']]

    data_df_docs = pd.DataFrame(array_docs_json)
    data_to_db_docs = data_df_docs[['end_point', 'origin', 'date_query', 'date_warehouse', 'list_name', 'display_name', 'list_name_encoded','newest_published_date', 'updated']]

    return data_to_db_archivosMensual, data_to_db_docs

def executeMasPopulares(client, name_bucket, list_populares):
    array_json = []
    array_docs_json = []

    for ruta in list_populares:
        s3_clientobj = client.get_object(Bucket=name_bucket, Key=ruta['Key'])

        file_content = s3_clientobj['Body'].read().decode('utf-8')
        data = json.loads(file_content)

        date = datetime.datetime.now()

        data.update({
            'end_point': 'https://api.nytimes.com/svc/mostpopular/v2/{emailed||shared}.json',
            'origin': 'New York Times APIs',
            'date_query': ruta['LastModified'], #Asociar hora bucket
            'date_warehouse': date,
        })

        data_docs = data['results']
        for i in data_docs:
            i.update({
                'end_point': 'https://api.nytimes.com/svc/mostpopular/v2/{emailed||shared}.json',
                'origin': 'New York Times APIs',
                'date_query': ruta['LastModified'], #Asociar hora bucket
                'date_warehouse': date,
            })

        array_json.append(data)
        for item in data_docs:
            array_docs_json.append(item)

    data_df_archivosMensual = pd.DataFrame(array_json)
    data_to_db_archivosMensual = data_df_archivosMensual[['end_point', 'origin', 'date_query', 'date_warehouse', 'copyright', 'status', 'num_results']]

    data_df_docs = pd.DataFrame(array_docs_json)
    data_to_db_docs = data_df_docs[['end_point', 'origin', 'date_query', 'date_warehouse', 'uri', 'id', 'published_date','type', 'title', 'abstract']]

    return data_to_db_archivosMensual, data_to_db_docs

def executePublicados(client, name_bucket, list_publicados):
    array_json = []
    array_docs_json = []
    array_lists_json = []
    array_books_json = []

    for ruta in list_publicados:
        s3_clientobj = client.get_object(Bucket=name_bucket, Key=ruta['Key'])

        file_content = s3_clientobj['Body'].read().decode('utf-8')
        data = json.loads(file_content)

        date = datetime.datetime.now()

        data.update({
            'end_point': 'https://api.nytimes.com/svc/books/v3/lists/overview.json',
            'origin': 'New York Times APIs',
            'date_query': ruta['LastModified'], #Asociar hora bucket
            'date_warehouse': date,
        })

        data_docs = data['results']
        data_docs.update({
            'end_point': 'https://api.nytimes.com/svc/books/v3/lists/overview.json',
            'origin': 'New York Times APIs',
            'date_query': ruta['LastModified'], #Asociar hora bucket
            'date_warehouse': date,
        })

        data_list = data['results']['lists']
        for i in data_list:
            i.update({
                'end_point': 'https://api.nytimes.com/svc/books/v3/lists/overview.json',
                'origin': 'New York Times APIs',
                'date_query': ruta['LastModified'], #Asociar hora bucket
                'date_warehouse': list,
                'num_books': len(i['books']),
            })

            data_books = i['books']
            for j in data_books:
                j.update({
                    'end_point': 'https://api.nytimes.com/svc/books/v3/lists/overview.json',
                    'origin': 'New York Times APIs',
                    'date_query': ruta['LastModified'], #Asociar hora bucket
                    'date_warehouse': list,
                })

        array_json.append(data)
        array_docs_json.append(data_docs)
        for item in data_list:
            array_lists_json.append(item)
        for item in data_books:
            array_books_json.append(item)

    data_df_archivosMensual = pd.DataFrame(array_json)
    data_to_db_archivosMensual = data_df_archivosMensual[['end_point', 'origin', 'date_query', 'date_warehouse', 'copyright', 'status', 'num_results']]

    data_df_docs = pd.DataFrame(array_docs_json)
    data_to_db_docs = data_df_docs[['end_point', 'origin', 'date_query', 'date_warehouse', 'bestsellers_date', 'published_date', 'published_date_description','previous_published_date', 'next_published_date']]

    data_df_lists = pd.DataFrame(array_lists_json)
    data_to_db_lists = data_df_lists[['end_point', 'origin', 'date_query', 'date_warehouse', 'list_id', 'list_name_encoded','list_image', 'num_books']]

    data_df_books = pd.DataFrame(array_books_json)
    data_to_db_books = data_df_books[['end_point', 'origin', 'date_query', 'date_warehouse', 'author', 'contributor', 'created_date','book_uri','title','primary_isbn10','primary_isbn13']]

    return data_to_db_archivosMensual, data_to_db_docs, data_to_db_lists, data_to_db_books

if __name__ == "__main__":
    # Conexion Amazon S3

    # Cliente con las credenciales
    # client = boto3.client('s3', aws_access_key_id="AKIARUOSMCVG3L6WLJ7A", aws_secret_access_key="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") #Credenciales IAM James
    client = boto3.client('s3', aws_access_key_id="AKIARUOSMCVGUR6ETZ73", aws_secret_access_key="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")  # Credenciales IAM Melissa

    ruta = 'Consultas/archivosListName/'
    name_bucket = 'my-bucket-prueba3'

    list_mensual = []
    list_name = []
    list_populares = []
    list_publicados = []

    all_objects = client.list_objects(Bucket=name_bucket)
    contents = all_objects['Contents']
    for i in contents:
        if i['Key'].find ('archivosArchivoMensual') >= 0:
            list_mensual.append(i)
        elif i['Key'].find ('archivosListName') >= 0:
            list_name.append(i)
        elif i['Key'].find ('archivosMasPopulares') >= 0:
            list_populares.append(i)
        elif i['Key'].find ('archivosPublicado') >= 0:
            list_publicados.append(i)

    engine = create_engine('postgresql://postgres:12345678@database.cqsybfowumzl.us-east-2.rds.amazonaws.com:5432/postgres')

    listName, listName_results = executeListName(client, name_bucket, list_name)
    listName.to_sql('name_length', con=engine, index=False)
    listName_results.to_sql('book_name', con=engine, index=False)

    masPopulares, masPopulares_results = executeMasPopulares(client, name_bucket, list_populares)
    masPopulares.to_sql('popular_length', con=engine, index=False)
    masPopulares_results.to_sql('arcticle_popular', con=engine, index=False)

    publicado, publicado_results, publicado_results_lists, publicado_results_lists_books = executePublicados(client,name_bucket,list_publicados)
    publicado.to_sql('publish_length', con=engine, index=False)
    publicado_results.to_sql('publish_date', con=engine, index=False)
    publicado_results_lists.to_sql('book_list', con=engine, index=False)
    publicado_results_lists_books.to_sql('publish_books', con=engine, index=False)

    archivosMensual, archivosMensual_docs = executeArchivoMensual(client, name_bucket, list_mensual)
    archivosMensual.to_sql('archive_length', con=engine, index=False)
    archivosMensual_docs.to_sql('docs', con=engine, index=False)