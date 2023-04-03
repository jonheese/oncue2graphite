import aiohttp
import asyncio
import json
import socket
import time
import traceback

from aiooncue import Oncue
from datetime import datetime

MAX_RETRIES = 5
PARAMETER_IDS = [
    4, 5, 6, 7, 11, 18, 20, 26, 33, 34, 36, 37, 38, 39, 40, 41, 42, 43,
    44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60,
    65, 66, 67, 68, 69, 114, 115, 116, 118, 119, 123, 124, 125, 128, 129
] 
PARAMETERS= [
    "devicestate",
    "EngineSpeed",
    "EngineOilPressure",
    "BatteryVoltage",
    "LubeOilTemperature",
    "GensetControllerTemperature",
    "GeneratorTrueTotalPower",
    "GeneratorTruePercentOfRatedPower",
    "GeneratorVoltageAverageLineToLine",
    "GeneratorFrequency",
    "GensetControllerTotalOperationTime",
    "EngineTotalRunTime",
]

class Oncue2Graphite:
    def __init__(self):
        with open('./config.json', 'r') as f:
            config = json.load(f)
            self.user = config.get('login')
            self.password = config.get('password')
            self.carbon_server = config.get('carbon_server')
            self.carbon_port = config.get('carbon_port')
            self.parameters = config.get('parameters', PARAMETERS)
            self.parameter_ids = config.get('parameter_ids', PARAMETER_IDS)

    async def get_data(self):
        data = {}
        success = False
        retry_count = 0
        last_exception = None
        while not success:
            try:
                websession = aiohttp.ClientSession()
                oncue = Oncue(self.user, self.password, websession)
                await oncue.async_login()
                devices = await oncue.async_list_devices()
                for device in devices:
                    serialnumber = device["serialnumber"]
                    data[serialnumber] =  await oncue.async_device_details(
                        serialnumber,
                        parameters=self.parameter_ids,
                    )
                success = True
            except Exception as e:
                if last_exception and traceback.format_exc() != last_exception:
                    last_exception += '\n' + traceback.format_exc()
                else:
                    last_exception = traceback.format_exc()
            finally:
                await websession.close()
            retry_count += 1
            time.sleep(1)
            if retry_count > MAX_RETRIES:
                #if last_exception:
                #    print(last_exception)
                break
        return data

    def get_parameter_value(self, data, target_parameter):
        if target_parameter == "devicestate":
            state = data.get("devicestate")
            if state == "Stopping" or state == "Crank On" or state == "-" or state == "--":
                return 0.5
            elif state == "Performing Unloaded Full Speed Exercise" or state == "Running":
                return 1
            elif state == "Standby":
                return 0
            elif state == "Off":
                return -1
            print(f"Unknown state encountered: {state}")
            return 1
        if target_parameter in data.keys():
            return data[target_parameter]
        for parameter in data.get("parameters"):
            if parameter.get("name") == target_parameter:
                return parameter.get("value")
        return None

    def insert_data(self, offset=0):
        timestamp = datetime.now().replace(second=offset)
        while datetime.now() < timestamp:
            time.sleep(0.5)
        loop = asyncio.get_event_loop()
        data = loop.run_until_complete(self.get_data())
        #print(json.dumps(data, indent=2))
        if not data:
            raise RuntimeError("API data not found")
        for device in data.keys():
            for parameter in self.parameters:
                value = self.get_parameter_value(data[device][0], parameter)
                if value is None:
                    continue
                parameter = parameter.replace(" ", "_").replace(".", "_").replace("/", "_")
                metric = f'gen.{parameter}.{device}'
                self.send_to_graphite(metric, value, timestamp)

    def send_to_graphite(self, metric, value, timestamp):
        if isinstance(value, bool):
            value = int(value)
        sock = socket.socket()
        sock.connect((self.carbon_server, self.carbon_port))
        s = f'{metric} {value} {round(timestamp.timestamp())}\n'
        #print(s)
        sock.send(s.encode())


if __name__ == '__main__':
    oncue2graphite = Oncue2Graphite()
    #for offset in [0, 15, 30, 45]:
    for offset in [0]:
        try:
            oncue2graphite.insert_data(offset)
        except Exception as e:
            #traceback.print_exc()
            pass
