from spade import quit_spade
import argparse
import assistant_functions as asf
import pandas as pd
import os
import time
import datetime
from spade.agent import Agent
from spade.behaviour import OneShotBehaviour
from spade.template import Template
import json


class LaunchAgent(Agent):
    class LABehav(OneShotBehaviour):
        async def run(self):
            global la_status_var, my_full_name, la_started_at, stop_time, my_dir, wait_msg_time, list_ware, string_operations
            """inform log of status"""
            '''la_activation_json = asf.activation_df(my_full_name, la_started_at)
            la_msg_log = asf.msg_to_log(la_activation_json, my_dir)
            await self.send(la_msg_log)'''
            """Send new order to log"""
            if order_code != "No":
                la_inform_log_json = asf.order_file(my_full_name, order_code, steel_grade, thickness, width_coils,
                                                    num_coils, list_coils, each_coil_price, list_ware, string_operations)
                la_order_log = asf.order_to_log(la_inform_log_json, my_dir)
                await self.send(la_order_log)

            """Send searching code to browser"""
            if la_search != "No":
                la_search_browser = asf.order_to_search(la_search, my_full_name, my_dir)
                await self.send(la_search_browser)

    class ReceiverBehav(OneShotBehaviour):
        async def run(self):
            await self.agent.b.join()
            """Receive message"""
            msg = await self.receive(timeout=2)  # wait for a message for 5 seconds
            if msg:
                single = msg.body.split(":")
                if single[0] == "Alive":
                    msg_aa_response = f'ActiveAgent: agent_name:{my_full_name}, active_time:{la_started_at}'
                    response_active = asf.msg_to_log(msg_aa_response, my_dir)
                    await self.send(response_active)

        async def on_end(self):
            """Inform log """
            '''la_msg_end = f'{my_full_name} agent ended'
            la_msg_end = json.dumps(la_msg_end)
            la_msg_end = asf.msg_to_log(la_msg_end, my_dir)
            await self.send(la_msg_end)'''
            await self.agent.stop()

        async def on_start(self):
            self.counter = 1
            """Inform log """
            '''la_msg_start = f'{my_full_name} agent started'
            la_msg_start = json.dumps(la_msg_start)
            la_msg_start = asf.msg_to_log(la_msg_start, my_dir)
            await self.send(la_msg_start)'''

    async def setup(self):
        self.b = self.LABehav()
        template = Template()
        template.metadata = {"performative": "inform"}
        self.add_behaviour(self.b, template)
        self.b2 = self.ReceiverBehav()
        template2 = Template()
        template2.metadata = {"performative": "inform"}
        self.add_behaviour(self.b2, template2)


if __name__ == "__main__":
    """Parser parameters"""
    parser = argparse.ArgumentParser(description='wh parser')
    parser.add_argument('-an', '--agent_number', type=int, metavar='', required=False, default=1,
                        help='agent_number: 1,2,3,4..')
    parser.add_argument('-w', '--wait_msg_time', type=int, metavar='', required=False, default=10,
                        help='wait_msg_time: time in seconds to wait for a msg')
    parser.add_argument('-st', '--stop_time', type=int, metavar='', required=False, default=10,
                        help='stop_time: time in seconds where agent')
    parser.add_argument('-s', '--status', type=str, metavar='', required=False, default='stand-by',
                        help='status_var: on, stand-by, Off')
    parser.add_argument('--search', type=str, metavar='', required=False, default='No',
                        help='Search order by code. Writte depending on your case: oc (order_code),sg(steel_grade),at(average_thickness), wi(width_coils), ic(id_coil), so(string_operations), date.Example: --search oc = 987')
    parser.add_argument('-oc', '--order_code', type=str, metavar='', required=False, default='No',
                        help='Specify the number code of the order. Write between "x"')
    parser.add_argument('-sg', '--steel_grade', type=str, metavar='', required=False, default='1',
                        help='Number which specifies the type of steel used for coils in an order.Write between "x"')
    parser.add_argument('-at', '--average_thickness', type=float, metavar='', required=False, default='0.4',
                        help='Specify the thickness for coils ordered')
    parser.add_argument('-wi', '--width_coils', type=int, metavar='', required=False, default='950',
                        help='Specify the width for coils ordered')
    parser.add_argument('-nc', '--number_coils', type=int, metavar='', required=False, default='1',
                        help='Number of coils involved in the order')
    parser.add_argument('-lc', '--list_coils', type=str, metavar='', required=False, default='No',
                        help='List of codes of coils involved in the order.Write between "x"')
    parser.add_argument('-po', '--price_order', type=float, metavar='', required=False, default='1',
                        help='Price given to the order')
    parser.add_argument('-lp', '--list_position', type=str, metavar='', required=False, default='No',
                        help='Coil warehouses.Write between ",".Format:K,L,K')
    parser.add_argument('-so', '--string_operations', type=str, metavar='', required=False, default='No',
                        help='Sequence of operations needed.Write between "x".Format:VA_08,VA_09')
    args = parser.parse_args()
    my_dir = os.getcwd()
    my_name = os.path.basename(__file__)[:-3]
    my_full_name = asf.my_full_name(my_name, args.agent_number)
    wait_msg_time = args.wait_msg_time
    la_started_at = datetime.datetime.now()
    la_status_var = args.status
    la_search = args.search
    order_code = args.order_code
    steel_grade = args.steel_grade
    thickness = args.average_thickness
    width_coils = args.width_coils
    num_coils = args.number_coils
    each_coil_price = round((args.price_order / args.number_coils), 2)
    list_coils = args.list_coils
    list_ware = args.list_position
    string_operations = args.string_operations
    """Save to csv who I am"""
    la_data_df = asf.set_agent_parameters(my_dir, my_name, my_full_name)
    """XMPP info"""
    la_jid = asf.agent_jid(my_dir, my_full_name)
    la_passwd = asf.agent_passwd(my_dir, my_full_name)
    la_agent = LaunchAgent(la_jid, la_passwd)
    future = la_agent.start(auto_register=True)
    future.result()
    la_agent.b2.join()
    """Counter"""
    stop_time = datetime.datetime.now() + datetime.timedelta(seconds=args.stop_time)
    while la_agent.is_alive():
        try:
            time.sleep(1)
        except KeyboardInterrupt:
            la_status_var = "off"
            la_agent.stop()
    quit_spade()

