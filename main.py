import aiohttp
import asyncio
from datetime import datetime
import json

async def fetchDataFromGeoAPI_Callback(data, endpoint):
    
    async with aiohttp.ClientSession() as session:

        geoAPI_url = "https://shamba-gateway-staging-2ycmet71.ew.gateway.dev/geoapi/v1/" + endpoint
        
        async with session.post(url=geoAPI_url, json=data) as resp:

            response_data = await resp.json()
            print(response_data)
            if resp.status == 200:
                response_data['status'] = 200
                return response_data
             
            else:
                if 'message' in response_data:
                    message = response_data['message']
                else:
                    message = ''

                return {
                    "status": resp.status,
                    "data": {
                        "status": "errored",
                        "error": {
                            "name": "AdapterError",
                            "message": message
                        },
                        "statusCode": resp.status
                    }
                }


async def fetchDataFromGeoAPI(data, endpoint):
    return await fetchDataFromGeoAPI_Callback(data, endpoint)

async def submitDataToWeb3Storage_Callback(web3_data):
    token = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJkaWQ6ZXRocjoweDE0YzM3YURkOUU0OEI3NzJiOTdlN2I1OUM3NzVjRmMxOUI3QUI4NEUiLCJpc3MiOiJ3ZWIzLXN0b3JhZ2UiLCJpYXQiOjE2NTEyMjE2MDk2MTIsIm5hbWUiOiJPcmFjbGVBY2MydG9rZW4ifQ.tlEisv1V_iJlzbPGusx7QXKnVSY6nmOfX9oA9sj1V3g' 
    header = {'Authorization': 'Bearer ' + token}
    
    async with aiohttp.ClientSession(headers=header) as session:

        web3API_url = 'https://api.web3.storage/upload'
        
        async with session.post(url=web3API_url, json=web3_data) as resp:
            response_data = await resp.json()

            if resp.status == 200:
                response_data['status'] = 200
                return response_data
            else:
                return {
                    "status": resp.status,
                    "data": {
                        "status": "errored",
                        "error": {
                            "name": "Unable to upload data to web3 store",
                        },
                        "statusCode": resp.status
                    }
                }

            

async def submitDataToWeb3Storage(web3_data):
    return await submitDataToWeb3Storage_Callback(web3_data)

async def getCidUrlsFromWeb3_Callback():
    token = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJkaWQ6ZXRocjoweDQ4MjM3QjA4RTZBQTYzMTVGYTI0MmQ1NTNCQmExNTEzODU0QjEyRjQiLCJpc3MiOiJ3ZWIzLXN0b3JhZ2UiLCJpYXQiOjE2NDY2Njk3NTA3MjIsIm5hbWUiOiJzaGFtYmEtdG9rZW4ifQ.OCFKBexDShFdDkS1_7nsd40PwzhSH4PYoXxq7BCCro0' 
    header = {'Authorization': 'Bearer ' + token}
    cid_url_list = []
    async with aiohttp.ClientSession(headers=header) as session:

        api_url = 'https://api.web3.storage/user/uploads'
        
        async with session.get(api_url) as resp:
            data_list = await resp.json()
            

            for data in data_list:
                cid_url_list.append("https://dweb.link/ipfs/" + data['cid'])
    return {'urls': cid_url_list}

async def getCidUrlsFromWeb3():
    return await getCidUrlsFromWeb3_Callback()

def main(request):

    request_json = request.get_json()
    
    if request.args and 'message' in request.args:
        return request.args.get('message')
    elif request_json:
        jobRunID = request_json['id']
        tx_hash = ''
        contract_address = ''
        operator_address = ''
        agg_x = ''
        endpoint = 'fire-analysis'

        if ('tx_hash' in request_json):
            tx_hash = request_json['tx_hash']


        if ('contract_address' in request_json):
            contract_address = request_json['contract_address']


        if ('operator_address' in request_json):
            operator_address = request_json['operator_address']


        if (isinstance(request_json['data'], str)):
            request_json['data'] = json.loads(request_json['data'])


        dataset_code = request_json['data']['dataset_code']
        selected_band = request_json['data']['selected_band']
        geometry = request_json['data']['geometry']
        start_date = request_json['data']['start_date']
        end_date = request_json['data']['end_date']
        image_scale = request_json['data']['image_scale']

        if ('agg_x' in request_json['data']):
            agg_x = request_json['data']['agg_x']
            endpoint = 'statistics'


        url = "https://shamba-gateway-staging-2ycmet71.ew.gateway.dev/geoapi/v1/" + endpoint

        data = {
            'dataset_code': dataset_code,
            'selected_band': selected_band,
            'geometry': geometry,
            'start_date': start_date,
            'end_date': end_date,
            'image_scale': image_scale,
        }

        geoAPI_data = asyncio.run(fetchDataFromGeoAPI(data, endpoint))

        geoAPI_data['data']['jobRunID'] = jobRunID

        if geoAPI_data['status'] == 200:
            
            geoAPI_data['data']['statusCode'] = 200
            final_result = ''
            if (endpoint == 'statistics'):
                final_result = int(format(geoAPI_data['data'][agg_x] * (10 ** 18), '.53g'))
                geoAPI_data['data']['data'] = {
                    agg_x: final_result,
                    "result": final_result
                }

            else:
                final_result = []
                agg_fire_detected = geoAPI_data['data']['detection']

                for i in agg_fire_detected:
                    
                    if i['fire_detected']:
                        final_result.append(1)
                    else:
                        final_result.append(9)
            
            

            geoAPI_data['result'] = final_result

            metadata_cid = geoAPI_data['metadata']['ipfs_cid']


            del geoAPI_data['success']
            del geoAPI_data['error']
            del geoAPI_data['data_token']
            del geoAPI_data['duration']
            del geoAPI_data['metadata']


            web3_json_data = {
                "request": {
                    "dataset_code": dataset_code,
                    "selected_band": selected_band,
                    "geometry": geometry,
                    "start_date": start_date,
                    "end_date": end_date,
                    "image_scale": image_scale
                },
                "response": {
                    "datetime": str(datetime.now()),
                    "result": final_result,
                    "contract_address": contract_address,
                    "operator_address": operator_address,
                    "tx_hash": tx_hash
                }
            }

            if (endpoint == 'statistics'):
                web3_json_data['request']['agg_x'] = agg_x
                web3_json_data['response'][agg_x] = final_result
            
            web3API_data = asyncio.run(submitDataToWeb3Storage(web3_json_data))
            web3API_data['jobRunID'] = jobRunID

            if web3API_data['status'] == 200:
                cid = web3API_data['cid']
                return {
                        "jobRunID": jobRunID,
                        "status": "success",
                        "result": { "cid": cid, "result": final_result },
                        "message": 'Data successfully uploaded to https://dweb.link/ipfs/' + cid,
                        "statusCode": 200
                    }
            else:
                return web3API_data
                

        else:
            return geoAPI_data
    else:
        return asyncio.run(getCidUrlsFromWeb3())