from bitmex_rest import bitmex
import logging
import logging.handlers
import json

def setup_logger():
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)  # Change this to DEBUG if you want a lot more info
    ch = logging.StreamHandler()
    formatter = logging.Formatter("%(asctime)s - Thread-%(threadName)s - %(levelname)s - %(message)s")
    ch.setFormatter(formatter)
    logger.addHandler(ch)
    return logger


if __name__ == "__main__":
    setup_logger()
    contract_name = 'XBTUSD'
    test = False
    api_key = 'RbKv_8cp9-EM5sPKhM2-tcIh'
    api_secret = '6VEYWM7x6Cg5Uo3iTNAgD997tvgYT4711eXqTXgU6dj7cCoB'
    test_url = 'https://testnet.bitmex.com/api/v1'
    product_url = 'https://www.bitmex.com/api/v1'
    cli = bitmex(test=test, api_key=api_key, api_secret=api_secret)
    print(dir(cli.Execution))
    filter = {
        "execType": "Funding"
    }
    res = cli.Execution.Execution_getTradeHistory(reverse=True, symbol=contract_name, syfilter=json.dumps(filter)).result()
    print(res)