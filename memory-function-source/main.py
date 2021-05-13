import base64
import json

from google.cloud import iot_v1
from google.api_core.exceptions import FailedPrecondition


client = iot_v1.DeviceManagerClient()
last3 = []

def combine_arrays(event, context):
    """Triggered from a message on a Cloud Pub/Sub topic.
    Args:
        event (dict): Event payload.
        context (google.cloud.functions.Context): Metadata for the event.
    """
    global last3

    data = json.loads(base64.b64decode(event['data']).decode('utf-8'))
    attr = event['attributes']
    project_id = attr['projectId']
    registry_id = attr['deviceRegistryId']
    device_id = attr['deviceId']
    region = attr['deviceRegistryLocation']

    last3.extend(data)
    
    if len(last3) >= 30:
        data = json.dumps(last3[0:30]).encode('utf-8')
        del last3[0:30]
        device_name = ('projects/{}/locations/{}/registries/{}/'
                        'devices/{}'.format(
                            project_id,
                            region,
                            registry_id,
                            device_id))
        try:
            client.send_command_to_device(request={"name": device_name, "binary_data": data})
        except FailedPrecondition:
            print('Failed. Device not connected.')
