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


class CoilAgent(Agent):
    class CoilBehav(PeriodicBehaviour):
        async def run(self):
            global my_full_name, my_dir, wait_msg_time, coil_status_var, coil_started_at, stop_time, refresh_time, coil_agent, coil_df, bid_register_df, number_auction
            """inform log of status"""
            '''coil_activation_json = asf.activation_df(my_full_name, coil_started_at)
            coil_msg_log = asf.msg_to_log(coil_activation_json, my_dir)
            await self.send(coil_msg_log)'''
            if coil_status_var == "auction":
                """inform log of status"""
                to_do = "search-auction"
                coil_inform_json = asf.inform_log_df(my_full_name, coil_started_at, coil_status_var, to_do).to_json()
                coil_msg_log = asf.msg_to_log(coil_inform_json, my_dir)
                await self.send(coil_msg_log)
                # it will wait here for ca's that are auctionable.
                va_coil_msg = await self.receive(timeout=20)
                if va_coil_msg:
                    """Inform log """
                    va_coil_msg_sender = va_coil_msg.sender
                    coil_msg_log_body = f'{my_full_name} receives auction from {va_coil_msg_sender}'
                    coil_msg_log = asf.msg_to_log(coil_msg_log_body, my_dir)
                    await self.send(coil_msg_log)
                    va_coil_msg_df = pd.read_json(va_coil_msg.body)
                    '''Evaluate whether to enter the auction'''
                    coil_enter = asf.auction_entry(va_coil_msg_df,coil_df,number_auction)
                    if coil_enter == 1:
                        '''Create bid'''
                        bid_mean = va_coil_msg_df.loc[0, 'bid_mean']
                        print("BID_MEAN ", bid_mean)
                        bid = asf.create_bid(coil_df, bid_mean)
                        coil_df['bid'] = bid
                        coil_df['budget_remaining'] = coil_df.loc[0,'budget'] - coil_df.loc[0,'bid']
                        coil_va_msg = asf.msg_to_sender(va_coil_msg)
                        coil_va_msg.body = coil_df.to_json()
                        await self.send(coil_va_msg)
                        """Inform log """
                        coil_msg_log_body = f'{my_full_name} sends bid to {va_coil_msg_sender}. Bid: {bid}'
                        coil_msg_log = asf.msg_to_log(coil_msg_log_body, my_dir)
                        await self.send(coil_msg_log)
                        """Receive request to counterbid"""
                        i = 0
                        while i < 3:
                            i = i + 1
                            va_coil_msg2 = await self.receive(timeout=20)
                            if va_coil_msg2:
                                va_coil_msg_df = pd.read_json(va_coil_msg2.body)
                                if va_coil_msg2.sender == va_coil_msg_sender:  # checks if communication comes from last sender
                                    a = va_coil_msg_df.at[0, 'bid_status']
                                    print(f'{a}')
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
                                        coil_msg_log_body = f'{my_full_name} sends counter-bid to {va_coil_msg_sender}. Bid: {counterbid}'
                                        coil_msg_log = asf.msg_to_log(coil_msg_log_body, my_dir)
                                        await self.send(coil_msg_log)
                                        """Wait to receive acceptance"""
                                        va_coil_msg3 = await self.receive(timeout=20)
                                        if va_coil_msg3:
                                            va_coil_msg_df = pd.read_json(va_coil_msg3.body)
                                            if va_coil_msg3.sender == va_coil_msg_sender:  # checks if communication comes from last sender
                                                a = va_coil_msg_df.at[0, 'bid_status']
                                                if va_coil_msg_df.at[0, 'bid_status'] == 'acceptedbid':
                                                    """Store accepted Bid from ca agent"""
                                                    bid_register_df = bid_register_df.append(va_coil_msg_df)
                                                    """Confirm or deny assignation"""
                                                    accepted_jid = asf.compare_va(va_coil_msg_df, bid_register_df)
                                                    accepted_jid = accepted_jid[:-9]
                                                    print(f'accepted jid: {accepted_jid}')
                                                    va_coil_msg_sender_f = str(va_coil_msg_sender)[:-9]
                                                    print(f'va_coil_msg_sender jid: {va_coil_msg_sender_f}')
                                                    accepted_jid = str(accepted_jid)
                                                    if accepted_jid == va_coil_msg_sender_f:
                                                        coil_va_msg = asf.msg_to_sender(va_coil_msg)
                                                        coil_df.loc[0, 'bid_status'] = 'acceptedbid'
                                                        coil_va_msg.body = coil_df.to_json()
                                                        await self.send(coil_va_msg)
                                                        """inform log of auction won"""
                                                        this_time = datetime.datetime.now()
                                                        coil_msg_log_body = f'{my_full_name} won auction to process in {va_coil_msg_sender_f} at {this_time}. Change status to sleep.'
                                                        coil_msg_log = asf.msg_to_log(coil_msg_log_body, my_dir)
                                                        await self.send(coil_msg_log)
                                                        coil_status_var = 'sleep'
                                                        coil_msg_log_body = f'{my_full_name} changes status to {coil_status_var}.'
                                                        coil_msg_log = asf.msg_to_log(coil_msg_log_body, my_dir)
                                                        await self.send(coil_msg_log)
                                                        break
                                                    else:
                                                        """inform log of issue"""
                                                        va_id = va_coil_msg_df.loc[0, 'id']
                                                        coil_msg_log_body = f'{my_full_name} did not accept to process in {va_id} in final acceptance'
                                                        coil_msg_log = asf.msg_to_log(coil_msg_log_body, my_dir)
                                                        await self.send(coil_msg_log)
                                                else:
                                                    """inform log of issue"""
                                                    va_id = va_coil_msg_df.loc[0, 'id']
                                                    coil_msg_log_body = f'{va_id} did not accept to process in {my_full_name} in final acceptance'
                                                    coil_msg_log = asf.msg_to_log(coil_msg_log_body, my_dir)
                                                    await self.send(coil_msg_log)
                                            else:
                                                """inform log of issue"""
                                                coil_msg_log_body = f'incorrect sender'
                                                coil_msg_log = asf.msg_to_log(coil_msg_log_body, my_dir)
                                                await self.send(coil_msg_log)
                                        else:
                                            """inform log"""
                                            coil_msg_log_body = f'{my_full_name} did not receive any msg in the last {wait_msg_time}s at {coil_status_var} at last auction level'
                                            coil_msg_log = asf.msg_to_log(coil_msg_log_body, my_dir)
                                            await self.send(coil_msg_log)
                                    else:
                                        """inform log"""
                                        coil_msg_log_body = f'{my_full_name} did not receive valuation for the offer in the last {wait_msg_time}s at {coil_status_var} at last auction level'
                                        coil_msg_log = asf.msg_to_log(coil_msg_log_body, my_dir)
                                        await self.send(coil_msg_log)
                                else:
                                    """inform log of issue"""
                                    coil_msg_log_body = f'incorrect sender'
                                    coil_msg_log = asf.msg_to_log(coil_msg_log_body, my_dir)
                                    await self.send(coil_msg_log)
                            else:
                                """inform log"""
                                coil_msg_log_body = f'{my_full_name} did not receive any msg in the last {wait_msg_time}s at {coil_status_var} at last auction level'
                                coil_msg_log = asf.msg_to_log(coil_msg_log_body, my_dir)
                                await self.send(coil_msg_log)
                    else:
                        """inform log of status"""
                        to_do = "search-auction"
                        va_id = va_coil_msg_df.loc[0, 'id']
                        number_auction += int(1)
                        number_auction_str = f'{my_full_name} did not enter {va_id} auction because configuration measures was too high. Not_entered auction number: {number_auction}'
                        coil_inform_json = asf.inform_log_df(my_full_name, coil_started_at, coil_status_var, to_do,number_auction_str).to_json()
                        coil_msg_log = asf.msg_to_log(coil_inform_json, my_dir)
                        await self.send(coil_msg_log)
                else:
                    """inform log"""
                    coil_msg_log_body = f'{my_full_name} did not receive any msg in the last 20s at {coil_status_var}'
                    coil_msg_log = asf.msg_to_log(coil_msg_log_body, my_dir)
                    await self.send(coil_msg_log)
            elif coil_status_var == "sleep":
                """wait for message from in case fabrication was interrupted"""
                interrupted_fab_msg = await self.receive(timeout=20)
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
                            coil_msg_log = asf.msg_to_log(coil_msg_log_body, my_dir)
                            await self.send(coil_msg_log)
                            print(coil_msg_log_body)
                    else:
                        """inform log"""
                        time.sleep(5)
                        coil_msg_log_body = f'{my_full_name} receive msg at {coil_status_var}, but not from browser'
                        coil_msg_log = asf.msg_to_log(coil_msg_log_body, my_dir)
                        await self.send(coil_msg_log)
                else:
                    """inform log"""
                    time.sleep(5)
                    coil_msg_log_body = f'{my_full_name} did not receive any msg in the last {wait_msg_time}s at {coil_status_var}'
                    coil_msg_log = asf.msg_to_log(coil_msg_log_body, my_dir)
                    await self.send(coil_msg_log)
            elif coil_status_var == "stand-by":  # stand-by status for BR is not very useful, just in case we need the agent to be alive, but not operative. At the moment, it won´t change to stand-by.
                """inform log of status"""
                coil_inform_json = asf.inform_log_df(my_full_name, coil_started_at, coil_status_var).to_json()
                coil_msg_log = asf.msg_to_log(coil_inform_json, my_dir)
                await self.send(coil_msg_log)
                # now it just changes directly to auction
                """inform log of status"""
                coil_status_var = "auction"
            else:
                """inform log of status"""
                coil_inform_json = asf.inform_log_df(my_full_name, coil_started_at, coil_status_var).to_json()
                coil_msg_log = asf.msg_to_log(coil_inform_json, my_dir)
                await self.send(coil_msg_log)
                coil_status_var = "stand-by"

        async def on_end(self):
            print({self.counter})

        async def on_start(self):
            """inform log of status"""
            coil_activation_json = asf.activation_df(my_full_name, coil_started_at)
            coil_msg_log = asf.msg_to_log(coil_activation_json, my_dir)
            await self.send(coil_msg_log)
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
    parser.add_argument('-w', '--wait_msg_time', type=int, metavar='', required=False, default=20, help='wait_msg_time: time in seconds to wait for a msg')
    parser.add_argument('-st', '--stop_time', type=int, metavar='', required=False, default=84600, help='stop_time: time in seconds where agent')
    parser.add_argument('-s', '--status', type=str, metavar='', required=False, default='stand-by', help='status_var: on, stand-by, off')
    parser.add_argument('-b', '--budget', type=int, metavar='', required=False, default=200, help='budget: in case of needed, budget can be increased')
    args = parser.parse_args()
    my_dir = os.getcwd()
    my_name = os.path.basename(__file__)[:-3]
    my_full_name = asf.my_full_name(my_name, args.agent_number)
    wait_msg_time = args.wait_msg_time
    coil_started_at = datetime.datetime.now().time()
    coil_status_var = args.status
    refresh_time = datetime.datetime.now() + datetime.timedelta(seconds=1)
    """Save to csv who I am"""
    asf.set_agent_parameters(my_dir, my_name, my_full_name)  # Crea un COIL_0XX.csv
    coil_df = pd.read_csv(f'{my_full_name}.csv', header=0, delimiter=",", engine='python')
    coil_df.at[0, 'budget'] = args.budget
    budget = coil_df.loc[0, 'budget']
    print(f'budget:{budget}')
    bid_register_df = asf.bid_register(my_name, my_full_name)
    number_auction = int(0)
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
