import logging
import socket
import sys
from time import sleep
import struct

from pymodbus.client import ModbusTcpClient


def read_hr(
    holding_register,
    size,
    server_ip="127.0.0.1",
    format="16bit_integer",
    word_order="standard",
):
    """
    Function to read holding register from Modbus TCP Server
    :param holding_register: HR address
    :param size: number of registers to read
    :param server_ip: address of server
    :format: 16bit_integer(default),
             32bit_integer
             32bit_float, a single precision IEEE-754 floating point number
    :word_order: only apply to 32bit_integer and 32bit_float
             standard(default), upper 16 bits are in Modbus register n, lower 16 bits are in register n+1.
             reverse, lower 16 bits are in Modbus register n, upper 16 bits are in register n+1.
    :return: list of values
    """
    result = None
    read_output = None
    # initialise client, default ports and modbus address
    client = ModbusTcpClient(server_ip)

    # read all 4 registers
    try:
        result = client.read_holding_registers(
            address=holding_register, count=size, slave=1
        )
        logging.debug(result)
    except Exception as e:
        logging.error(e)
        client.close()

    # clean up
    client.close()

    logging.debug(result)

    modbus_data = []
    # check if result data received, build results data
    if format == "16bit_integer":
        try:
            for j in range(size):
                # get param from result
                modbus_register = result.getRegister(j)
                # check if register needs conversion for signed value
                modbus_register = twos_comp(modbus_register)
                # append to result array
                modbus_data.append(modbus_register)
            logging.debug("Valid modbus data received")

        except AttributeError as e:
            logging.warning(f"No data received, error: {e}")
    elif format == "32bit_integer":
        if (size % 2) == 1:
            logging.warning(
                f"format is 32bit_integer, but number of registers to read is odd number: {size}"
            )
        try:
            for j in range(0, size, 2):
                # get param from result
                if word_order == "standard":
                    upper_16bits = result.getRegister(j)
                    lower_16bits = result.getRegister(j + 1)
                else:
                    upper_16bits = result.getRegister(j + 1)
                    lower_16bits = result.getRegister(j)
                combined_value = (upper_16bits << 16) | lower_16bits
                # check if combined_value needs conversion for signed value
                combined_value = twos_comp_32bit(combined_value)
                # append to result array
                modbus_data.append(combined_value)
            logging.debug("Valid modbus data received")

        except AttributeError as e:
            logging.warning(f"No data received, error: {e}")
    elif format == "32bit_float":
        if (size % 2) == 1:
            logging.warning(
                f"format is 32bit_float, but number of registers to read is odd number: {size}"
            )
        try:
            for j in range(0, size, 2):
                # get param from result
                if word_order == "standard":
                    upper_16bits = result.getRegister(j)
                    lower_16bits = result.getRegister(j + 1)
                else:
                    upper_16bits = result.getRegister(j + 1)
                    lower_16bits = result.getRegister(j)
                combined_value = (upper_16bits << 16) | lower_16bits
                # convert IEEE-754 floating point number into float
                combined_value = convert_to_float(combined_value)
                # append to result array
                modbus_data.append(combined_value)
            logging.debug("Valid modbus data received")

        except AttributeError as e:
            logging.warning(f"No data received, error: {e}")
    return modbus_data


# All modbus registers stored as 16bit signed words. Convert to integer
def twos_comp(input_word):
    if input_word > 0x7FFF:
        logging.debug(f"calculating 2's compliment of {input_word}")
        input_word = input_word - int((input_word << 1) & 2**16)
        logging.debug(f"New value: {input_word}")

    return input_word


def twos_comp_32bit(input_word):
    if input_word > 0x7FFFFFFF:
        logging.debug(f"Calculating 2's complement of {input_word}")
        input_word = input_word - ((input_word << 1) & 0xFFFFFFFF)
        logging.debug(f"New value: {input_word}")

    return input_word


# convert integer into a 32-bit IEEE-754 single precision float
def convert_to_float(combined_value):
    combined_value_in_bytes = combined_value.to_bytes(4, byteorder="big")
    float_value = struct.unpack("!f", combined_value_in_bytes)[0]

    return float_value


if __name__ == "__main__":
    log_level = logging.INFO
    root = logging.getLogger()
    root.setLevel(log_level)

    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(log_level)
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    handler.setFormatter(formatter)
    root.addHandler(handler)

    server_ip = "192.168.13.100"
    # server_ip = "127.0.0.1"
    while True:
        values = read_hr(
            holding_register=199,
            size=6,
            server_ip=server_ip,
            format="32bit_float",
            word_order="reverse",
        )
        logging.info(f"values output: {values}")
        values = read_hr(
            holding_register=205,
            size=2,
            server_ip=server_ip,
            format="32bit_integer",
            word_order="reverse",
        )
        logging.info(f"values output: {values}")

        logging.info("sleep...")
        sleep(5)
