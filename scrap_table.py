import requests
from bs4 import BeautifulSoup
import boto3
import uuid

def lambda_handler(event, context):
    # URL de la página web que contiene la tabla
    url = "https://ultimosismo.igp.gob.pe/api/ultimo-sismo/ajaxb/2024"

    # Realizar la solicitud HTTP a la página web
    response = requests.get(url)
    if response.status_code != 200:
        return {
            'statusCode': response.status_code,
            'body': 'Error al acceder a la página web'
        }

    # Parsear el contenido HTML de la página web
    data = response.json()

    print(data)

    # Encontrar la tabla en el HTML
    l_sismos = sorted(data, key=lambda sis: sis['createdAt'], reverse=True)

    res = l_sismos[:10]    

    print(res)

    # Guardar los datos en DynamoDB
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('TablaWebScrapping-IGP')

    # Eliminar todos los elementos de la tabla antes de agregar los nuevos
    scan = table.scan()
    with table.batch_writer() as batch:
        for each in scan['Items']:
            batch.delete_item(
                Key={
                    'id': each['id']
                }
            )

    
    for i,sismo in range(len(res)):
        sismo['num'] = i
        sismo['id'] = str(uuid.uuid4())
        table.put_item(Item=sismo)


    # Retornar el resultado como JSON
    return {
        'statusCode': 200,
        'body': res
    }
