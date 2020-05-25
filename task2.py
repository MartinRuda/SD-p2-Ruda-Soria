import pywren_ibm_cloud as pywren
import time
import re
import json
import sys

BUCKET_NAME = 'mutex'
N_SLAVES = 0
config = {
    'pywren' : {'storage_bucket' : BUCKET_NAME},
    'ibm_cf':  {'endpoint': 'ENDPOINT', 
                'namespace': 'NAMESPACE', 
                'api_key': 'API_KEY'}, 
    'ibm_cos': {
                'private_endpoint': 'PRIVATE_ENDPOINT',
                'endpoint': 'ENDPOINT',
                "access_key": "ACCESS_KEY", 
                "secret_key": "SECRET_KEY"
                }
    }

def resetData(id, x, ibm_cos):

    for ibmObject in ibm_cos.list_objects(Bucket=BUCKET_NAME)['Contents']:
        ibm_cos.delete_object(Bucket=BUCKET_NAME, Key=ibmObject['Key'])

def master(x, ibm_cos):

    write_permission_list = []

    #Creamos inicialmente el objeto result.json vacío
    ibm_cos.put_object(Bucket=BUCKET_NAME, Key='result.json')
    
    while (True):
        # 1. monitor COS bucket each X seconds
        time.sleep(1)
        # 2. List all "p_write_{id}" files
        try:
            p_write_list = ibm_cos.list_objects(Bucket=BUCKET_NAME, Prefix='p_write_')['Contents']
        except:
            break
            
        # 3. Order objects by time of creation
        p_write_list.sort(key=lambda x: x['LastModified'])

        while (len(p_write_list) > 0):

            # 4. Pop first object of the list "p_write_{id}"
            p_write_id = p_write_list.pop(0)['Key']
            #Usamos la libreria re para buscar el número (\d) dentro del literal p_write_{id}
            id = re.findall("\d+", p_write_id)[0]

            # 5. Write empty "write_{id}" object into COS
            
            #Usaremos el key 'LastModified' para comparar si ha cambiado el archivo (y así no comparar todo el archivo)
            resultFile = ibm_cos.get_object(Bucket=BUCKET_NAME, Key='result.json')['LastModified']
            ibm_cos.put_object(Bucket=BUCKET_NAME, Key='write_' + id)

            # 6. Delete from COS "p_write_{id}", save {id} in write_permission_list
            ibm_cos.delete_object(Bucket=BUCKET_NAME, Key=p_write_id)
            write_permission_list.append(id)

            # 7. Monitor "result.json" object each X seconds until it is updated
            while (resultFile == ibm_cos.get_object(Bucket=BUCKET_NAME, Key='result.json')['LastModified']):
                time.sleep(1)
            
            # 8. Delete from COS “write_{id}”
            ibm_cos.delete_object(Bucket=BUCKET_NAME, Key='write_' + id)
            
        # 8. Back to step 1 until no "p_write_{id}" objects in the bucket

    return write_permission_list

def slave(id, x, ibm_cos):

    # 1. Write empty "p_write_{id}" object into COS    
    ibm_cos.put_object(Bucket=BUCKET_NAME, Key='p_write_' + str(id))

    # 2. Monitor COS bucket each X seconds until it finds a file called "write_{id}"
    while (True):
        try:
            ibm_cos.get_object(Bucket=BUCKET_NAME, Key='write_' + str(id))
            break
        except:
            time.sleep(1)

    # 3. If write_{id} is in COS: get result.json, append {id}, and put back to COS result.json
    
    resultFile = ibm_cos.get_object(Bucket=BUCKET_NAME, Key='result.json')['Body'].read().decode('utf-8')
    try:
        resultFile = json.loads(resultFile)
        resultFile.append(str(id))
        resultFile = json.dumps(resultFile)
    except:
        resultFile = "[\"" + str(id) + "\"]"


    ibm_cos.put_object(Bucket=BUCKET_NAME, Key='result.json', Body=resultFile.encode())

if __name__ == '__main__':
    if(len(sys.argv) != 2 or sys.argv[1] == '-h'):
        print("Se necesita 1 argumento -- task2.py <N_SLAVES>")
        sys.exit()

    N_SLAVES = int(sys.argv[1])
    if (N_SLAVES > 100):
        print("El número de slaves no puede ser superior a 100")
        sys.exit()

    pw = pywren.ibm_cf_executor(config=config)  
    pw.map(slave, range(N_SLAVES))
    pw.call_async(master, 0)
    write_permission_list = pw.get_result()
    # Get result.json
    resultFile = pw.internal_storage.get_client().get_object(Bucket=BUCKET_NAME, Key='result.json')['Body'].read().decode()
    resultFile = json.loads(resultFile)

    # check if content of result.json == write_permission_list
    print('\nObjeto result.json:\t')
    print(resultFile)
    print('Resultados lista permisos:\t\t')
    print(write_permission_list)
    
    if (resultFile == write_permission_list):
        print("----------------------------")
        print("Los resultados son correctos")
        print("----------------------------")

    # Vaciar bucket
    pw.call_async(resetData, 0)