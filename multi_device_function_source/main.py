import base64
import json
from collections import defaultdict

from google.cloud import iot_v1
from google.api_core.exceptions import FailedPrecondition


client = iot_v1.DeviceManagerClient()
recent = defaultdict(list)

def combine_arrays(event, context):
    """Triggered from a message on a Cloud Pub/Sub topic.
    Args:
        event (dict): Event payload.
        context (google.cloud.functions.Context): Metadata for the event.
    """
    data = json.loads(base64.b64decode(event['data']).decode('utf-8'))
    attr = event['attributes']
    project_id = attr['projectId']
    registry_id = attr['deviceRegistryId']
    device_id = attr['deviceId']
    region = attr['deviceRegistryLocation']

    hist = recent[device_id]

    hist.extend(data)
    
    if len(hist) >= 30:
        data = json.dumps(hist[0:30]).encode('utf-8')
        del hist[0:30]
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
