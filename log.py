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
                    #logger.info(msg.body)
                    msg_sender_jid0 = str(msg.sender)
                    msg_sender_jid = msg_sender_jid0[:-31]
                    fileh = logging.FileHandler(f'{my_dir}/{my_name}.log')
                    formatter = logging.Formatter(f'%(asctime)s;%(levelname)s;{msg_sender_jid};%(pathname)s;%(message)s')
                    fileh.setFormatter(formatter)
                    log = logging.getLogger()  # root logger
                    for hdlr in log.handlers[:]:  # remove all old handlers
                        log.removeHandler(hdlr)
                    log.addHandler(fileh)
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
                    msg_2 = pd.read_json(msg.body)
                    if msg_2.loc[0, 'purpose'] == 'inform error':
                        logger.warning(msg.body)
                    elif msg_2.loc[0, 'purpose'] == 'inform change':
                        logger.debug(msg.body)
                    else:
                        logger.info(msg.body)
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
                    elif msg_sender_jid == "launcher":
                        launcher_df = pd.read_json(msg.body)
                        asf.change_warehouse(launcher_df, my_dir)
                        coils = launcher_df.loc[0,'list_coils']
                        locations = launcher_df.loc[0, 'list_ware']
                        code = launcher_df.loc[0, 'order_code']
                        order = asf.order_register(my_full_name, code, coils, locations)
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
                    msg = asf.inform_error(msg)
                    logger.debug(msg)
            elif log_status_var == "stand-by":
                status_log = asf.log_status(my_full_name, log_status_var)
                logger.debug(status_log)

                log_status_var = "on"
                status_log = asf.log_status(my_full_name, log_status_var)
                logger.debug(status_log)
            else:
                status_log = asf.log_status(my_full_name, log_status_var)
                logger.debug(status_log)
                log_status_var = "stand-by"
                status_log = asf.log_status(my_full_name, log_status_var)
                logger.debug(status_log)

        async def on_end(self):
            await self.agent.stop()

        async def on_start(self):
            self.counter = 1

    async def setup(self):
        b = self.LogBehav()
        template = Template()
        template.metadata = {"performative": "inform"}
        self.add_behaviour(b, template)
        fileh = logging.FileHandler(f'{my_dir}/{my_name}.log')
        formatter = logging.Formatter(f'%(asctime)s;%(levelname)s;{my_full_name};%(pathname)s;%(message)s')
        fileh.setFormatter(formatter)
        log = logging.getLogger()  # root logger
        for hdlr in log.handlers[:]:  # remove all old handlers
            log.removeHandler(hdlr)
        log.addHandler(fileh)
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
        start_msg = asf.send_activation_finish(my_full_name, 'start')
        logger.debug(start_msg)


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
