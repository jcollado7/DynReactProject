from spade import quit_spade
import time
import datetime
from spade.agent import Agent
from spade.behaviour import CyclicBehaviour, PeriodicBehaviour
from spade.template import Template
from spade.message import Message
import pandas as pd
import logging
import argparse
import assistant_functions as asf
import os
import logging.handlers as handlers
import re
import json


class LogAgent(Agent):
    class LogBehav(CyclicBehaviour):
        async def run(self):
            global wait_msg_time, logger, log_status_var
            if log_status_var == "on":
                msg = await self.receive(timeout=wait_msg_time)  # wait for a message for 20 seconds
                if msg:
                    print(f"received msg number {self.counter}")
                    self.counter += 1
                    logger.info(msg.body)
                    msg_sender_jid0 = str(msg.sender)
                    msg_sender_jid = msg_sender_jid0[:-31]
                    x = re.search("won auction to process", msg.body)
                    if x:
                        auction = msg.body.split(" ")
                        coil_id = auction[0]
                        status = auction[13]
                        o = asf.checkFile2Existance()
                        if o == True:
                            asf.update_coil_status(coil_id, status)
                        logger.info(msg.body)
                        print("Coil status updated")
                    elif msg_sender_jid == "dynrct_r00":
                        print("MSG", msg)
                        launcher_df = pd.read_json(msg.body)
                        asf.change_warehouse(launcher_df, my_dir)
                        coils = launcher_df.loc[0,'list_coils']
                        locations = launcher_df.loc[0, 'list_ware']
                        code = launcher_df.loc[0, 'order_code']
                        order = f' Order Code: {code} with code coils: [{coils}] registered in locations [{locations}].'
                        order = json.dumps(order)
                        logger.info(order)
                    elif msg_sender_jid == "browser":
                        x = re.search("{", msg.body)
                        if x:
                            browser_df = pd.read_json(msg.body)
                            if 'purpose' in browser_df.columns:
                                if browser_df.loc[0, 'purpose'] == "location_coil":
                                    msg = asf.loc_of_coil(browser_df)
                                    msg_to_br = asf.msg_to_br(msg, my_dir)
                                    await self.send(msg_to_br)
                else:
                    msg = f"Log_agent didn't receive any msg in the last {wait_msg_time}s"
                    msg = json.dumps(msg)
                    logger.debug(msg)
            elif log_status_var == "stand-by":
                status_msg = f"Log agent status: {log_status_var}"
                status_msg = json.dumps(status_msg)
                logger.debug(status_msg)

                log_status_var = "on"
                on_msg = f"Log agent status: {log_status_var}"
                on_msg = json.dumps(on_msg)
                logger.debug(on_msg)
            else:
                logger.debug(f"Log agent status: {log_status_var}")
                log_status_var = "stand-by"
                logger.debug(f"Log agent status: {log_status_var}")

        async def on_end(self):
            await self.agent.stop()

        async def on_start(self):
            self.counter = 1

    async def setup(self):
        b = self.LogBehav()
        template = Template()
        template.metadata = {"performative": "inform"}
        self.add_behaviour(b, template)


if __name__ == "__main__":
    """Parser parameters"""
    parser = argparse.ArgumentParser(description='Log parser')
    parser.add_argument('-an', '--agent_number', type=int, metavar='', required=False, default=1, help='agent_number: 1,2,3,4..')
    parser.add_argument('-v', '--verbose', type=str, choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'], metavar='', required=False, default='DEBUG', help='verbose: amount of information saved')
    parser.add_argument('-w', '--wait_msg_time', type=int, metavar='', required=False, default=120, help='wait_msg_time: time in seconds to wait for a msg. Purpose of system monitoring')
    parser.add_argument('-st', '--stop_time', type=int, metavar='', required=False, default=84600, help='stop_time: time in seconds where agent isnt asleep')
    parser.add_argument('-do', '--delete_order', type=str, metavar='', required=False, default='No', help='Order to delete') #29/04
    args = parser.parse_args()
    my_dir = os.getcwd()
    my_name = os.path.basename(__file__)[:-3]
    delete_order = args.delete_order
    my_full_name = asf.my_full_name(my_name, args.agent_number)
    wait_msg_time = args.wait_msg_time
    log_status_var = "stand-by"


    """Logger info"""
    logger = logging.getLogger(__name__)
    formatter = logging.Formatter('%(asctime)s;%(levelname)s;%(name)s;%(pathname)s;%(message)s')  # parameters saved to log file. message will be the *.json
    file_handler = logging.FileHandler(f'{my_dir}/{my_name}.log')
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    if args.verbose == "DEBUG":
        logger.setLevel(logging.DEBUG)
    elif args.verbose == "INFO":
        logger.setLevel(logging.INFO)
    elif args.verbose == "WARNING":
        logger.setLevel(logging.WARNING)
    elif args.verbose == "ERROR":
        logger.setLevel(logging.ERROR)
    elif args.verbose == "CRITICAL":
        logger.setLevel(logging.CRITICAL)
    else:
        print('not valid verbosity')

    start_msg = f"{my_name}_agent started"
    start_msg = json.dumps(start_msg)
    logger.debug(start_msg)

    """XMPP info"""
    log_jid = asf.agent_jid(my_dir, my_full_name)
    log_passwd = asf.agent_passwd(my_dir, my_full_name)
    log_agent = LogAgent(log_jid, log_passwd)
    future = log_agent.start(auto_register=True)
    future.result()

    """Counter"""
    stop_time = datetime.datetime.now() + datetime.timedelta(seconds=args.stop_time)
    while datetime.datetime.now() < stop_time:
        time.sleep(1)
    else:
        log_agent.stop()
        log_status_var = "off"
        stop_msg_log = f"{my_full_name}_agent stopped, coil_status_var: {log_status_var}"
        stop_msg_log = json.dumps(stop_msg_log)
        logger.critical(stop_msg_log)
        quit_spade()
 
