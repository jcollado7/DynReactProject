import assistant_functions as asf
import time
import datetime
from spade.agent import Agent
from spade.behaviour import PeriodicBehaviour
from spade.template import Template
import pandas as pd
import os
import argparse
from spade import quit_spade
import json
import sys
import socket

class CoilAgent(Agent):
    class CoilBehav(PeriodicBehaviour):
        async def run(self):
            global my_full_name, my_dir, wait_msg_time, coil_status_var, coil_started_at, stop_time, refresh_time, coil_agent, coil_df, bid_register_df, number_auction, auction_finish_at, ip_machine, seq_coil
            if coil_status_var == "auction":
                """inform log of status"""
                to_do = "search_auction"
                coil_inform_json = asf.inform_log_df(my_full_name, coil_started_at, coil_status_var, coil_df, to_do).to_json(orient="records")
                coil_msg_log = asf.msg_to_log(coil_inform_json, my_dir)
                await self.send(coil_msg_log)
                # it will wait here for va's that are auctionable.
                va_coil_msg = await self.receive(timeout=auction_time)
                if va_coil_msg:
                    seq_coil = seq_coil + 1
                    msg_sender_jid = str(va_coil_msg.sender)
                    msg_sender_jid = msg_sender_jid[:-33]
                    if msg_sender_jid == "va":
                        """Inform log """
                        va_coil_msg_sender = va_coil_msg.sender
                        coil_msg_log_sender = str(va_coil_msg.sender)
                        va_coil_msg_df = pd.read_json(va_coil_msg.body)
                        '''Evaluate whether to enter the auction, asking Browser the location'''
                        coil_request_type = "my location"
                        coil_msg_br_body = asf.req_active_users_loc_times_coil(coil_df, seq_coil, coil_request_type).to_json()  # returns a json with request info to browser
                        coil_msg_br = asf.msg_to_br(coil_msg_br_body, my_dir)
                        # returns a msg object with request info to browser and message setup
                        await self.send(coil_msg_br)
                        """Inform log """
                        coil_msg_log_body = asf.send_coil(my_full_name, seq_coil)
                        coil_msg_log_body = coil_msg_log_body.to_json(orient="records")
                        coil_msg_log = asf.msg_to_log(coil_msg_log_body, my_dir)
                        await self.send(coil_msg_log)
                        i = 0
                        while i < 4:
                            i = i + 1
                            br_coil_msg_json = await self.receive(timeout=wait_msg_time)
                            if br_coil_msg_json:
                                msg_sender_jid = str(br_coil_msg_json.sender)
                                msg_sender_jid_0 = msg_sender_jid[:-9]
                                msg_sender_jid = msg_sender_jid[:-31]
                                if msg_sender_jid == "browser":
                                    br_coil_msg_df = pd.read_json(br_coil_msg_json.body)
                                    coil_enter = asf.auction_entry(va_coil_msg_df,coil_df,number_auction,br_coil_msg_df)
                                    loc = br_coil_msg_df.loc[0, 'location']
                                    if coil_enter == 1:
                                        '''Create bid'''
                                        bid_mean = va_coil_msg_df.loc[0, 'bid_mean']
                                        bid = asf.create_bid(coil_df, bid_mean)
                                        coil_df['bid'] = bid
                                        coil_df['budget_remaining'] = coil_df.loc[0,'budget'] - coil_df.loc[0,'bid']
                                        coil_va_msg = asf.msg_to_sender(va_coil_msg)
                                        coil_va_msg.body = coil_df.to_json()
                                        await self.send(coil_va_msg)
                                        """Inform log"""
                                        coil_msg_log_body = asf.send_to_va_msg(my_full_name, bid, coil_msg_log_sender[:-9], '1')
                                        coil_msg_log_body = coil_msg_log_body.to_json(orient="records")
                                        coil_msg_log = asf.msg_to_log(coil_msg_log_body, my_dir)
                                        await self.send(coil_msg_log)
                                        """Receive request to counterbid"""
                                        i = 0
                                        while i < 3:
                                            i = i + 1
                                            va_coil_msg2 = await self.receive(timeout=30)
                                            if va_coil_msg2:
                                                va_coil_msg_df = pd.read_json(va_coil_msg2.body)
                                                if va_coil_msg2.sender == va_coil_msg_sender:  # checks if communication comes from last sender
                                                    a = va_coil_msg_df.at[0, 'bid_status']
                                                    if va_coil_msg_df.at[0, 'bid_status'] == 'extrabid':
                                                        """Create extra Bid"""
                                                        counterbid = asf.create_counterbid(va_coil_msg_df,coil_df)
                                                        """Send bid to va"""
                                                        coil_va_msg = asf.msg_to_sender(va_coil_msg2)
                                                        coil_df.loc[0, 'counterbid'] = counterbid
                                                        coil_df['User_name_va'] = str(va_coil_msg2.sender)
                                                        coil_df['budget_remaining'] = coil_df.loc[0, 'budget'] - coil_df.loc[0, 'counterbid']
                                                        coil_va_msg.body = coil_df.to_json()
                                                        await self.send(coil_va_msg)
                                                        """Inform log """
                                                        coil_msg_log_body = asf.send_to_va_msg(my_full_name, counterbid,
                                                                                               coil_msg_log_sender[:-9], '2')
                                                        coil_msg_log_body = coil_msg_log_body.to_json(orient="records")
                                                        coil_msg_log = asf.msg_to_log(coil_msg_log_body, my_dir)
                                                        await self.send(coil_msg_log)
                                                        """Wait to receive acceptance"""
                                                        va_coil_msg3 = await self.receive(timeout=wait_msg_time)
                                                        if va_coil_msg3:
                                                            va_coil_msg_df = pd.read_json(va_coil_msg3.body)
                                                            if va_coil_msg3.sender == va_coil_msg_sender:  # checks if communication comes from last sender
                                                                if va_coil_msg_df.at[0, 'bid_status'] == 'acceptedbid':
                                                                    """Store accepted Bid from ca agent"""
                                                                    bid_register_df = bid_register_df.append(va_coil_msg_df)
                                                                    """Confirm or deny assignation"""
                                                                    accepted_jid = asf.compare_va(va_coil_msg_df, bid_register_df)
                                                                    accepted_jid = accepted_jid[:-9]
                                                                    va_coil_msg_sender_f = str(va_coil_msg_sender)[:-9]
                                                                    accepted_jid = str(accepted_jid)
                                                                    if accepted_jid == va_coil_msg_sender_f:
                                                                        coil_va_msg = asf.msg_to_sender(va_coil_msg)
                                                                        coil_df.loc[0, 'bid_status'] = 'acceptedbid'
                                                                        coil_va_msg.body = coil_df.to_json()
                                                                        await self.send(coil_va_msg)
                                                                        """inform log of auction won"""
                                                                        auction_finish_at = datetime.datetime.now()
                                                                        coil_msg_log_body = asf.won_auction(my_full_name, va_coil_msg_sender_f, auction_finish_at)
                                                                        coil_msg_log = asf.msg_to_log(coil_msg_log_body, my_dir)
                                                                        await self.send(coil_msg_log)
                                                                        coil_status_var = 'sleep'
                                                                        coil_msg_log_body = asf.coil_status(my_full_name)
                                                                        coil_msg_log = asf.msg_to_log(coil_msg_log_body, my_dir)
                                                                        await self.send(coil_msg_log)
                                                                        break
                                                                    else:
                                                                        """inform log of issue"""
                                                                        va_id = va_coil_msg_df.loc[0, 'id']
                                                                        coil_msg_log_body = f'{my_full_name} did not accept to process in {va_id} in final acceptance'
                                                                        coil_msg_log_body = asf.inform_error(coil_msg_log_body)
                                                                        coil_msg_log = asf.msg_to_log(coil_msg_log_body, my_dir)
                                                                        await self.send(coil_msg_log)
                                                                else:
                                                                    """inform log of issue"""
                                                                    va_id = va_coil_msg_df.loc[0, 'id']
                                                                    coil_msg_log_body = f'{va_id} did not accept to process in {my_full_name} in final acceptance'
                                                                    coil_msg_log_body = asf.inform_error(coil_msg_log_body)
                                                                    coil_msg_log = asf.msg_to_log(coil_msg_log_body, my_dir)
                                                                    await self.send(coil_msg_log)
                                                            else:
                                                                """inform log of issue"""
                                                                coil_msg_log_body = f'incorrect sender'
                                                                coil_msg_log_body = asf.inform_error(coil_msg_log_body)
                                                                coil_msg_log = asf.msg_to_log(coil_msg_log_body, my_dir)
                                                                await self.send(coil_msg_log)
                                                        else:
                                                            """inform log"""
                                                            coil_msg_log_body = f'{my_full_name} did not receive any msg in the last {wait_msg_time}s at {coil_status_var} at last auction level'
                                                            coil_msg_log_body = asf.inform_error(coil_msg_log_body)
                                                            coil_msg_log = asf.msg_to_log(coil_msg_log_body, my_dir)
                                                            await self.send(coil_msg_log)
                                                    else:
                                                        """inform log"""
                                                        coil_msg_log_body = f'{my_full_name} did not receive valuation for the offer in the last {wait_msg_time}s at {coil_status_var} at last auction level'
                                                        coil_msg_log_body = asf.inform_error(coil_msg_log_body)
                                                        coil_msg_log = asf.msg_to_log(coil_msg_log_body, my_dir)
                                                        await self.send(coil_msg_log)
                                                else:
                                                    """inform log of issue"""
                                                    coil_msg_log_body = f'incorrect sender'
                                                    coil_msg_log_body = asf.inform_error(coil_msg_log_body)
                                                    coil_msg_log = asf.msg_to_log(coil_msg_log_body, my_dir)
                                                    await self.send(coil_msg_log)
                                            else:
                                                """inform log"""
                                                coil_msg_log_body = f'{my_full_name} did not receive any msg in the last {wait_msg_time}s at {coil_status_var} at last auction level'
                                                coil_msg_log_body = asf.inform_error(coil_msg_log_body)
                                                coil_msg_log = asf.msg_to_log(coil_msg_log_body, my_dir)
                                                await self.send(coil_msg_log)
                                    else:
                                        """inform log of status"""
                                        to_do = "search_auction"
                                        va_id = va_coil_msg_df.loc[0, 'id']
                                        number_auction += int(1)
                                        number_auction_str = f'{my_full_name} did not enter {va_id} auction because configuration measures was too high. Not_entered auction number: {number_auction}'
                                        coil_msg_log_body = asf.inform_error(number_auction_str)
                                        coil_msg_log = asf.msg_to_log(coil_msg_log_body, my_dir)
                                        await self.send(coil_msg_log)
                                else:
                                    """inform log"""
                                    coil_msg_log_body = f'{my_full_name} did not receive any msg from Browser Agent in the last {wait_msg_time}s at {coil_status_var}'
                                    coil_msg_log_body = asf.inform_error(coil_msg_log_body)
                                    coil_msg_log = asf.msg_to_log(coil_msg_log_body, my_dir)
                                    await self.send(coil_msg_log)
                            else:
                                """inform log"""
                                coil_msg_log_body = f'{my_full_name} did not receive any msg in the last {wait_msg_time}s at {coil_status_var}'
                                coil_msg_log_body = asf.inform_error(coil_msg_log_body)
                                coil_msg_log = asf.msg_to_log(coil_msg_log_body, my_dir)
                                await self.send(coil_msg_log)
                    else:
                        """inform log"""
                        coil_msg_log_body = f'{my_full_name} did not receive any msg from VA Agent in the last {wait_msg_time}s at {coil_status_var}'
                        coil_msg_log_body = asf.inform_error(coil_msg_log_body)
                        coil_msg_log = asf.msg_to_log(coil_msg_log_body, my_dir)
                        await self.send(coil_msg_log)
                else:
                    """inform log"""
                    coil_msg_log_body = f'{my_full_name} did not receive any msg in the last {auction_time}s at {coil_status_var}'
                    coil_msg_log_body = asf.inform_error(coil_msg_log_body)
                    coil_msg_log = asf.msg_to_log(coil_msg_log_body, my_dir)
                    await self.send(coil_msg_log)
            elif coil_status_var == "sleep":
                """wait for message from in case fabrication was interrupted"""
                interrupted_fab_msg = await self.receive(timeout=wait_msg_time)
                if interrupted_fab_msg:
                    interrupted_fab_msg_sender = interrupted_fab_msg.sender
                    if interrupted_fab_msg_sender[:-33] == "bro":
                        interrupted_fab_msg_df = pd.read_json(interrupted_fab_msg)
                        if interrupted_fab_msg_df.loc[0, 'int_fab'] == 1:
                            coil_df.loc[0, 'int_fab'] = 1
                            coil_status_var = "stand-by"
                            """inform log of issue"""
                            this_time = datetime.datetime.now()
                            coil_msg_log_body = f'{my_full_name} interrupted fab. Received that msg at {this_time}'
                            coil_msg_log_body = json.dumps(coil_msg_log_body)
                            coil_msg_log = asf.msg_to_log(coil_msg_log_body, my_dir)
                            await self.send(coil_msg_log)
                    else:
                        """inform log"""
                        time.sleep(5)
                        coil_msg_log_body = f'{my_full_name} receive msg at {coil_status_var}, but not from browser'
                        coil_msg_log_body = asf.inform_error(coil_msg_log_body)
                        coil_msg_log = asf.msg_to_log(coil_msg_log_body, my_dir)
                        await self.send(coil_msg_log)
                else:
                    """inform log"""
                    time.sleep(20)
                    coil_msg_log_body = f'{my_full_name} did not receive any msg in the last {wait_msg_time}s at {coil_status_var}'
                    coil_msg_log_body = asf.inform_error(coil_msg_log_body)
                    coil_msg_log = asf.msg_to_log(coil_msg_log_body, my_dir)
                    await self.send(coil_msg_log)
                    now_time = datetime.datetime.now()
                    tiempo = now_time - auction_finish_at
                    segundos = tiempo.seconds
                    if segundos > 1500:
                        asf.change_agent(my_full_name, my_dir)
                        sys.exit()
            elif coil_status_var == "stand-by":
                """inform log of status"""
                coil_inform_json = asf.inform_log_df(my_full_name, coil_started_at, coil_status_var, coil_df).to_json(orient="records")
                coil_msg_log = asf.msg_to_log(coil_inform_json, my_dir)
                await self.send(coil_msg_log)
                # now it just changes directly to auction
                coil_status_var = "auction"
            else:
                """inform log of status"""
                coil_inform_json = asf.inform_log_df(my_full_name, coil_started_at, coil_status_var, coil_df).to_json(orient="records")
                coil_msg_log = asf.msg_to_log(coil_inform_json, my_dir)
                await self.send(coil_msg_log)
                coil_status_var = "stand-by"

        async def on_end(self):
            print({self.counter})
            """Inform log """
            coil_msg_end = asf.send_activation_finish(my_full_name, ip_machine, 'end')
            va_msg_log = asf.msg_to_log(coil_msg_end, my_dir)
            await self.send(va_msg_log)

        async def on_start(self):
            """inform log of start"""
            coil_msg_start = asf.send_activation_finish(my_full_name, ip_machine, 'start')
            coil_msg_start = asf.msg_to_log(coil_msg_start, my_dir)
            await self.send(coil_msg_start)
            self.counter = 1

    async def setup(self):
        start_at = datetime.datetime.now() + datetime.timedelta(seconds=3)
        b = self.CoilBehav(period=3, start_at=start_at)  # periodic sender
        template = Template()
        template.metadata = {"performative": "inform"}
        self.add_behaviour(b)

if __name__ == "__main__":
    """Parser parameters"""
    parser = argparse.ArgumentParser(description='coil parser')
    parser.add_argument('-an', '--agent_number', type=int, metavar='', required=False, default=3, help='agent_number: 1,2,3,4..')
    parser.add_argument('-v', '--wait_msg_time', type=int, metavar='', required=False, default=20, help='wait_msg_time: time in seconds to wait for a msg')
    parser.add_argument('-w', '--wait_auction_time', type=int, metavar='', required=False, default=500,
                        help='wait_msg_time: time in seconds to wait for a msg')
    parser.add_argument('-st', '--stop_time', type=int, metavar='', required=False, default=84600, help='stop_time: time in seconds where agent')
    parser.add_argument('-s', '--status', type=str, metavar='', required=False, default='stand-by', help='status_var: on, stand-by, off')
    parser.add_argument('-b', '--budget', type=int, metavar='', required=False, default=200, help='budget: in case of needed, budget can be increased')
    parser.add_argument('-l', '--location', type=str, metavar='', required=False, default='K',
                        help='location: K')
    parser.add_argument('-c', '--code', type=str, metavar='', required=False, default='cO202106101',
                        help='code: cO202106101')
    args = parser.parse_args()
    my_dir = os.getcwd()
    my_name = os.path.basename(__file__)[:-3]
    my_full_name = asf.my_full_name(my_name, args.agent_number)
    wait_msg_time = args.wait_msg_time
    auction_time = args.wait_auction_time
    coil_started_at = datetime.datetime.now()
    coil_status_var = args.status
    location = args.location
    code = args.code
    refresh_time = datetime.datetime.now() + datetime.timedelta(seconds=1)
    auction_finish_at = ""
    """Save to csv who I am"""
    coil_df = asf.set_agent_parameters_coil(my_dir, my_name, my_full_name, location, code)
    coil_df.at[0, 'budget'] = args.budget
    budget = coil_df.loc[0, 'budget']
    bid_register_df = asf.bid_register(my_name, my_full_name)
    number_auction = int(0)
    seq_coil = int(200)
    "IP"
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.connect(("8.8.8.8", 80))
    ip_machine = s.getsockname()[0]
    """XMPP info"""
    coil_jid = asf.agent_jid(my_dir, my_full_name)
    coil_passwd = asf.agent_passwd(my_dir, my_full_name)
    coil_agent = CoilAgent(coil_jid, coil_passwd)
    future = coil_agent.start(auto_register=True)
    future.result()
    """Counter"""
    stop_time = datetime.datetime.now() + datetime.timedelta(seconds=args.stop_time)
    while datetime.datetime.now() < stop_time:
        time.sleep(1)
    else:
        coil_status_var = "off"
        coil_agent.stop()
        quit_spade()

