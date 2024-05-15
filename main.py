import datetime
import logging
import platform
import sys
import time
import ssl
import pathlib

import yaml
#MN To interact with Cumulocity IoT devices.
from c8y.c8y_device import c8yDevice
#MN To read "holding registers" in a Modbus device.
from modbus.modbus_client import read_hr

#MN Logging to the console is enabled
log_console = True
if log_console:
    root = logging.getLogger()
    root.setLevel(logging.DEBUG)

    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(logging.DEBUG)
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    handler.setFormatter(formatter)
    root.addHandler(handler)


if __name__ == "__main__":
    logging.basicConfig(
        format="%(asctime)-15s %(levelname)s %(name)-18s %(message)s",
        level=logging.DEBUG,
    )

    if "Ubuntu" in platform.version():
        CONFIG_FILE = "ubuntu/config.yaml"
        CERTS_PATH = "ubuntu/certificates"
    else:
        CONFIG_FILE = "/home/root/myapp/modbus2cumulocity/config.yaml"
        CERTS_PATH = "/home/root/myapp/modbus2cumulocity/certificates"
#MN Opens and reads a YAML configuration file. The file's contents are loaded into the dictionary s using yaml.load, which safely parses the YAML file.
    with open(CONFIG_FILE) as f:
        s = yaml.load(f, Loader=yaml.FullLoader)

    server_cert_required = s["cumulocity"]["server_cert_required"]

    certfile = f"{CERTS_PATH}/{s['cumulocity']['device_id']}_deviceCertChain.pem"
    keyfile = f"{CERTS_PATH}/{s['cumulocity']['device_id']}_deviceKey.pem"

    cert_file_list = [certfile, keyfile]

    if server_cert_required:
        ca_certs = f"{CERTS_PATH}/{s['cumulocity']['url']}_serverCertChain.pem"
        cert_file_list += [ca_certs]
        cert_reqs = ssl.CERT_REQUIRED
    else:
        ca_certs = None
        cert_reqs = ssl.CERT_NONE

    for cert_file in cert_file_list:
        pathlib_path = pathlib.Path(cert_file)
        assert (
            pathlib_path.is_file()
        ), f"The file {pathlib_path} is inaccessible or not there!"

    client = c8yDevice(
        url=s["cumulocity"]["url"],
        tenant=s["cumulocity"]["tenant"],
        device_id=str(s["cumulocity"]["device_id"]),
        device_type=s["cumulocity"]["device_type"],
        measurement_qos=s["cumulocity"]["measurement_qos"],
        ca_certs=ca_certs,
        certfile=certfile,
        keyfile=keyfile,
        cert_reqs=cert_reqs,
    )
    client.start()

    # remove this for production
    # while True:
    #     time.sleep(10)

    #1 200CV MBF	'Temp~DEGC	MBF 32bits float
	#2 202CV MBF	'Turbidity~NTU	MBF 32bits float
	#3 204CV MBF	'ph~ph 	MBF 32bits float
	#4 206CV MBF	'Depth~m 	MBF 32bits float
	#5 208CV MBF	'cond~uS/cm	MBF 32bits float
	#6 210CV MBF	'nlfcond~uS/cm	MBF 32bits float
	#7 212CV MBF	'DO SAT~%sat	MBF 32bits float
	#8 214CV MBF	'DO CB ~%cb	MBF 32bits float
	#9 216CV MBF	'DO MGL~mg/L	MBF 32bits float
	#10 218CV MBF	'ORP~mV	MBF 32bits float
	#11 220CV MBF	'PSI ~psia	MBF 32bits float
	#12 222CV MBF	'SAL ~psu  	MBF 32bits float
	#13 224CV MBF	'SP cond~uS/cm	MBF 32bits float
	#14 226CV MBF	'TDS~mg/L	MBF 32bits float
	#15 228CV MBF	'TSS~mg/L	MBF 32bits float
	#16 230CV MBF	'PH MV~mV	MBF 32bits float
	#17 232CV MBF	'CABLE POWER~volt	MBF 32bits float
	#18 234CV MBF	'Stores average Battery Voltage ~V	MBF 32bits float
	#19 236CV MBI 1	 Level (Low = 0, High = 1) MBI 16bits integer


    # Totally 19 registers will be polled
    # Temperature, Turbidity, Battery Voltage + rest of above are polled by FX30 every 6 minutes
    # pushed to Cumulocity around the same interval.
    # Level switch is polled by FX30 every 10 seconds, and pushed to
    # Cumulocity as changed or the first reading
    last_read = None
    last_read_time = None

    SEND_INTERVAL = 6

    try:
        while True:
            server_ip = s["modbus"]["slave_ip"]
            
            values1 = read_hr(
                holding_register=199,
                size=36,
                server_ip=server_ip,
                format="32bit_float",
                word_order="reverse",
            )
            logging.info(f"values output: {values1}")
            
            
            values1 = [round(v, 4) for v in values1]
            logging.info(f"rounded values: {values1}")
            values2 = read_hr(
                holding_register=235,
                size=1,
                server_ip=server_ip,
                format="16bit_integer",
            )
            logging.info(f"values output: {values2}")         
            
            values = values1 + values2
            if len(values) == 19:
                if last_read is None:
                    logging.info("first push, all 19 measurements")
                    client.send(
                        "datalogger",
                        "Temp(200)",
                        values[0],
                        "degC",
                        datetime.datetime.utcnow(),
                    )
                    client.send(
                        "datalogger",
                        "TURBIDITY(202)",
                        values[1],
                        "NTU",
                        datetime.datetime.utcnow(),
                    )
                    client.send(
                        "datalogger",
                        "PH(204)",
                        values[2],
                        "ph",
                        datetime.datetime.utcnow(),
                    )
                    client.send(
                        "datalogger",
                        "DEPTH(206)",
                        values[3],
                        "m",
                        datetime.datetime.utcnow(),
                    )
                    client.send(
                        "datalogger",
                        "COND(208)",
                        values[4],
                        "uS/cm",
                        datetime.datetime.utcnow(),
                    )
                    client.send(
                        "datalogger",
                        "nLfCond(210)",
                        values[5],
                        "uS/cm",
                        datetime.datetime.utcnow(),
                    )
                    client.send(
                        "datalogger",
                        "DO(212)",
                        values[6],
                        "%sat",
                        datetime.datetime.utcnow(),
                    )
                    client.send(
                        "datalogger",
                        "DO(214)",
                        values[7],
                        "%cb",
                        datetime.datetime.utcnow(),
                    )
                    client.send(
                        "datalogger",
                        "DO(216)",
                        values[8],
                        "mg/L",
                        datetime.datetime.utcnow(),
                    )
                    client.send(
                        "datalogger",
                        "ORP(218)",
                        values[9],
                        "mV",
                        datetime.datetime.utcnow(),
                    )
                    client.send(
                        "datalogger",
                        "PRESSURE(220)",
                        values[10],
                        "psia",
                        datetime.datetime.utcnow(),
                    )
                    client.send(
                        "datalogger",
                        "SAL(222)",
                        values[11],
                        "psu",
                        datetime.datetime.utcnow(),
                    )
                    client.send(
                        "datalogger",
                        "Sp Cond(224)",
                        values[12],
                        "uS/cm",
                        datetime.datetime.utcnow(),
                    )
                    client.send(
                        "datalogger",
                        "TDS(226)",
                        values[13],
                        "mg/L",
                        datetime.datetime.utcnow(),
                    )
                    client.send(
                        "datalogger",
                        "TSS(228)",
                        values[14],
                        "mg/L",
                        datetime.datetime.utcnow(),
                    )
                    client.send(
                        "datalogger",
                        "PH(230)",
                        values[15],
                        "mV",
                        datetime.datetime.utcnow(),
                    )
                    client.send(
                        "datalogger",
                        "CABLEPOWER(232)",
                        values[16],
                        "volt",
                        datetime.datetime.utcnow(),
                    )
                    client.send(
                        "datalogger",
                        "Bat(234)",
                        values[17],
                        "V",
                        datetime.datetime.utcnow(),
                    )
                    client.send(
                        "datalogger",
                        "Level(236)",
                        values[18],
                        "",
                        datetime.datetime.utcnow(),
                    )
                    last_read = values
                    last_read_time = time.time()
                else:
                    if time.time() - last_read_time > 60 * SEND_INTERVAL:
                        logging.info(
                            f"{SEND_INTERVAL} mins reached, push 19 measurements"
                        )
                    client.send(
                        "datalogger",
                        "Temp(200)",
                        values[0],
                        "DEGC",
                        datetime.datetime.utcnow(),
                    )
                    client.send(
                        "datalogger",
                        "TURBIDITY(202)",
                        values[1],
                        "NTU",
                        datetime.datetime.utcnow(),
                    )
                    client.send(
                        "datalogger",
                        "PH(204)",
                        values[2],
                        "ph",
                        datetime.datetime.utcnow(),
                    )
                    client.send(
                        "datalogger",
                        "DEPTH(206)",
                        values[3],
                        "m",
                        datetime.datetime.utcnow(),
                    )
                    client.send(
                        "datalogger",
                        "COND(208)",
                        values[4],
                        "uS/cm",
                        datetime.datetime.utcnow(),
                    )
                    client.send(
                        "datalogger",
                        "nLfCond(210)",
                        values[5],
                        "uS/cm",
                        datetime.datetime.utcnow(),
                    )
                    client.send(
                        "datalogger",
                        "DO(212)",
                        values[6],
                        "%sat",
                        datetime.datetime.utcnow(),
                    )
                    client.send(
                        "datalogger",
                        "DO(214)",
                        values[7],
                        "%cb",
                        datetime.datetime.utcnow(),
                    )
                    client.send(
                        "datalogger",
                        "DO(216)",
                        values[8],
                        "mg/L",
                        datetime.datetime.utcnow(),
                    )
                    client.send(
                        "datalogger",
                        "ORP(218)",
                        values[9],
                        "mV",
                        datetime.datetime.utcnow(),
                    )
                    client.send(
                        "datalogger",
                        "PRESSURE(220)",
                        values[10],
                        "psia",
                        datetime.datetime.utcnow(),
                    )
                    client.send(
                        "datalogger",
                        "SAL(222)",
                        values[11],
                        "psu",
                        datetime.datetime.utcnow(),
                    )
                    client.send(
                        "datalogger",
                        "Sp Cond(224)",
                        values[12],
                        "uS/cm",
                        datetime.datetime.utcnow(),
                    )
                    client.send(
                        "datalogger",
                        "TDS(226)",
                        values[13],
                        "mg/L",
                        datetime.datetime.utcnow(),
                    )
                    client.send(
                        "datalogger",
                        "TSS(228)",
                        values[14],
                        "mg/L",
                        datetime.datetime.utcnow(),
                    )
                    client.send(
                        "datalogger",
                        "PH(230)",
                        values[15],
                        "mV",
                        datetime.datetime.utcnow(),
                    )
                    client.send(
                        "datalogger",
                        "CABLEPOWER(232)",
                        values[16],
                        "volt",
                        datetime.datetime.utcnow(),
                    )
                    client.send(
                        "datalogger",
                        "Bat(234)",
                        values[17],
                        "V",
                        datetime.datetime.utcnow(),
                    )
                    client.send(
                        "datalogger",
                        "Level(236)",
                        values[18],
                        "",
                        datetime.datetime.utcnow(),
                    )
                    last_read_time = time.time()
                if values[18] != last_read[18]:
                    logging.info("leve changed, push")
                    client.send(
                        "datalogger",
                        "Level(236)",
                        values[18],
                        "",
                        datetime.datetime.utcnow(),
                    )
                    last_read = values

            time.sleep(10)
    except (KeyboardInterrupt, SystemExit):
        logging.info("Received keyboard interrupt, quitting ...")
        client.on = False
        exit(0)
