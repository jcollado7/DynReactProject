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
import socket

class VA(Agent):
    class VABehav(PeriodicBehaviour):
        async def run(self):
            global process_df, va_status_var, my_full_name, va_status_started_at, stop_time, my_dir, wait_msg_time, va_data_df, conf_va_df, auction_df, fab_started_at, leeway, op_times_df, auction_start, va_to_tr_df, coil_msgs_df, medias_list, ip_machine, seq_va, list_coils
            auction_start = datetime.datetime.now()
            if va_status_var == "pre-auction":
                seq_va = seq_va + 1
                pre_auction_start = datetime.datetime.now()
                auction_df.at[0, 'pre_auction_start'] = pre_auction_start
                """inform log of status"""
                va_inform_json = asf.inform_log_df(my_full_name, va_status_started_at, va_status_var, va_data_df).to_json(orient="records")
                #va_inform_json = json.dumps(va_inform)
                va_msg_log = asf.msg_to_log(va_inform_json, my_dir)
                await self.send(va_msg_log)
                """Asks browser for active coils and locations"""
                #  Builds msg to br
                va_request_type = "coils"
                va_msg_br_body = asf.req_active_users_loc_times(va_data_df, seq_va, list_coils, va_request_type)
                va_msg_br_body_json = va_msg_br_body.to_json() #returns a json with request info to browser
                va_msg_br = asf.msg_to_br(va_msg_br_body_json, my_dir)
                # returns a msg object with request info to browser and message setup
                await self.send(va_msg_br)
                """Inform log """
                va_req_br = asf.request_browser(va_msg_br_body, seq_va, list_coils).to_json(orient="records")
                va_req_br = asf.msg_to_log(va_req_br, my_dir)
                await self.send(va_req_br)
                br_msg = await self.receive(timeout=20)
                if br_msg:
                    br_data_df = pd.read_json(br_msg.body)
                    br_jid = asf.br_jid(my_dir)
                    msg_sender_jid = str(br_msg.sender)
                    msg_sender_jid = msg_sender_jid[:-9]
                    """Send a message to all active coils presenting auction and ideal conditions"""
                    if msg_sender_jid == br_jid:
                        if not br_data_df.empty:
                            #closest_coils_df = asf.get_coil_list(br_data_df)
                            auction_df.at[0, 'active_coils'] = [str(br_data_df['agent'].to_list())]  # Save information to auction df
                            va_data_df.at[0, 'auction_level'] = 1  # initial auction level
                            va_data_df.at[0, 'bid_status'] = 'bid'
                            bid_mean = asf.bids_mean(medias_list)
                            va_data_df.at[0, 'bid_mean'] = float(bid_mean)
                            va_to_coils_df = asf.va_to_coils_initial_df(va_data_df, conf_va_df)
                            va_to_coils_json = va_to_coils_df.to_json()  # json to send to coils with auction info including last temperatures
                            # Create a loop to inform of auctionable resource to willing to be fab coils.
                            jid_list = br_data_df['agent'].tolist()
                            jid_list_msg = str(jid_list)
                            auction_df.at[0, 'number_preauction'] = auction_df.at[0, 'number_preauction'] + 1
                            number = int(auction_df.at[0, 'number_preauction'])
                            """Inform log """
                            va_msg_log_body = asf.send_va(my_full_name, number, va_data_df.at[0, 'auction_level'], jid_list_msg)
                            va_msg_log_body = va_msg_log_body.to_json(orient="records")
                            va_msg_log = asf.msg_to_log(va_msg_log_body, my_dir)
                            await self.send(va_msg_log)
                            va_msg_to_coils = asf.va_msg_to(va_to_coils_json)
                            for z in jid_list:
                                coil_data_df = pd.read_csv(f'agents.csv', header=0, delimiter=",", engine='python')
                                row_df = coil_data_df.loc[coil_data_df['Name'] == z]
                                row_df = row_df.reset_index(drop=True)
                                jid_name = row_df.loc[0, 'User name']
                                #print("Message sent to: ", jid_name)
                                va_msg_to_coils.to = jid_name
                                await self.send(va_msg_to_coils)
                            """Create a loop to receive all* the messages"""
                            coil_msgs_df = pd.DataFrame()
                            for i in range(len(jid_list)):  # number of messages that enter auction
                                coil_msg = await self.receive(timeout=10)
                                if coil_msg:
                                    coil_jid = str(coil_msg.sender)
                                    msg_sender_jid = coil_jid[:-33]
                                    if msg_sender_jid == "c0":
                                        coil_msg_df = pd.read_json(coil_msg.body)
                                        coil_jid = str(coil_msg.sender)
                                        msg_sender = coil_jid[:-9]
                                        coil_msg_df.at[0, 'coil_jid'] = msg_sender
                                        coil_msgs_df = coil_msgs_df.append(coil_msg_df)  # received msgs
                                        va_status_var = "auction"
                                    else:
                                        """inform log of issue"""
                                        va_msg_log_body = f'{my_full_name} received a msg from {msg_sender_jid} rather than coil'
                                        va_msg_log_body = asf.inform_error(va_msg_log_body)
                                        va_msg_log = asf.msg_to_log(va_msg_log_body, my_dir)
                                        await self.send(va_msg_log)
                                else:
                                    """Inform log """
                                    va_msg_log_body = f'{my_full_name} did not receive answer from coil'
                                    va_msg_log_body = asf.inform_error(va_msg_log_body)
                                    va_msg_log = asf.msg_to_log(va_msg_log_body, my_dir)
                                    await self.send(va_msg_log)
                        else:
                            """Inform log """
                            va_msg_log_body = f'{my_full_name} did not receive answer from browser. Coils not available'
                            va_msg_log_body = asf.inform_error(va_msg_log_body)
                            va_msg_log = asf.msg_to_log(va_msg_log_body, my_dir)
                            await self.send(va_msg_log)
            if va_status_var == "auction":
                if not coil_msgs_df.empty:
                    auction_start = datetime.datetime.now()
                    auction_df.at[0, 'auction_start'] = auction_start
                    auction_df.at[0, 'number_auction'] = auction_df.at[0, 'number_auction'] + 1
                    number = int(auction_df.at[0, 'number_auction'])
                    bid_list = coil_msgs_df['id'].tolist()
                    bid_list_msg = str(bid_list)
                    va_data_df.at[0, 'auction_level'] = 2
                    """Inform log """
                    va_msg_log_body = asf.send_va(my_full_name, number, va_data_df.at[0, 'auction_level'], bid_list_msg)
                    va_msg_log_body = va_msg_log_body.to_json(orient="records")
                    va_msg_log = asf.msg_to_log(va_msg_log_body, my_dir)
                    await self.send(va_msg_log)
                    coil_msgs_df = coil_msgs_df.reset_index(drop=True)
                    auction_df.at[0, 'auction_coils'] = [str(coil_msgs_df['id'].to_list())]  # Send info to log
                    bid_coil = asf.bid_evaluation(coil_msgs_df, va_data_df)
                    bid_coil['bid_status'] = 'extrabid'
                    jid_list = bid_coil['coil_jid'].tolist()
                    result = asf.result(bid_coil, jid_list)
                    for i in jid_list:
                        """Ask for extra bid"""
                        va_data_df.at[0, 'bid_status'] = 'extrabid'
                        va_coil_extra_msg = asf.va_msg_to(bid_coil.to_json())
                        va_coil_extra_msg.to = i
                        await self.send(va_coil_extra_msg)
                    """Create a loop to receive all the messages"""
                    coil_msgs_df_2 = pd.DataFrame()
                    for i in range(len(jid_list)):
                        coil_msg = await self.receive(timeout=10)
                        if coil_msg:
                            coil_jid = str(coil_msg.sender)
                            msg_sender_jid = coil_jid[:-33]
                            if msg_sender_jid == "c0":
                                coil_msg_df = pd.read_json(coil_msg.body)
                                coil_jid = str(coil_msg.sender)
                                coil_msg_df.at[0, 'coil_jid'] = coil_jid
                                coil_msgs_df_2 = coil_msgs_df_2.append(coil_msg_df)
                            else:
                                """inform log of issue"""
                                va_msg_log_body = f'{my_full_name} received a msg from {msg_sender_jid} rather than coil'
                                va_msg_log_body = asf.inform_error(va_msg_log_body)
                                va_msg_log = asf.msg_to_log(va_msg_log_body, my_dir)
                                await self.send(va_msg_log)
                        else:
                            """Inform log """
                            va_msg_log_body = f'{my_full_name} did not receive answer from any coil'
                            va_msg_log_body = asf.inform_error(va_msg_log_body)
                            va_msg_log = asf.msg_to_log(va_msg_log_body, my_dir)
                            await self.send(va_msg_log)
                    if not coil_msgs_df_2.empty:
                        coil_msgs_df_2 = coil_msgs_df_2.reset_index(drop=True)
                        """Evaluate extra bids and give a rating"""
                        va_data_df.loc[0, 'auction_level'] = 3  # third level
                        counterbid_coil = asf.counterbid_evaluation(coil_msgs_df_2, va_data_df)
                        """Inform coil of assignation and agree on assignation"""
                        jid_list = counterbid_coil['coil_jid'].tolist()
                        results_2 = asf.results_2(counterbid_coil, jid_list)
                        for i in range(len(jid_list)):
                            coil_jid_winner_f = counterbid_coil.loc[i, 'coil_jid']
                            coil_jid_winner = coil_jid_winner_f[:-9]
                            winner_df = counterbid_coil.loc[counterbid_coil['coil_jid'] ==
                                                                  coil_jid_winner_f]
                            profit = winner_df.loc[i, 'profit']
                            if profit >= 0.5:
                                winner_df.at[0, 'bid_status'] = 'acceptedbid'
                                winner_df = winner_df.reset_index(drop=True)
                                coil_jid_winner = str(coil_jid_winner)
                                va_data_df.at[0, 'bid_status'] = 'acceptedbid'
                                va_data_df['accumulated_profit'] = va_data_df['accumulated_profit'] + winner_df.loc[0, 'profit']
                                print(winner_df)
                                va_coil_winner_msg = asf.va_msg_to(winner_df.to_json())
                                va_coil_winner_msg.to = coil_jid_winner
                                await self.send(va_coil_winner_msg)
                                """Inform log """
                                va_data_df.at[0, 'auction_level'] = 3
                                va_msg_log_body = asf.send_va(my_full_name, number, va_data_df.at[0, 'auction_level'],
                                                              coil_jid_winner)
                                va_msg_log_body = va_msg_log_body.to_json(orient="records")
                                va_msg_log = asf.msg_to_log(va_msg_log_body, my_dir)
                                await self.send(va_msg_log)
                                coil_msg = await self.receive(timeout=20)
                                if coil_msg:
                                    coil_jid = str(coil_msg.sender)
                                    msg_sender_jid = coil_jid[:-33]
                                    if msg_sender_jid == "c0":
                                        coil_msg_df = pd.read_json(coil_msg.body)
                                        coil_jid = coil_msg.sender
                                        coil_msg_df.at[0, 'id'] = coil_jid
                                        if coil_msg_df.loc[0, 'bid_status'] == 'acceptedbid':
                                            """Save winner information"""
                                            auction_df.at[0, 'coil_ratings'] = [counterbid_coil.to_dict(orient="records")]  # Save information to auction df
                                            """Calculate processing time"""
                                            process_df = asf.process_df(process_df, winner_df)
                                            """Inform log of assignation and auction KPIs"""
                                            counterbid_win = winner_df.loc[0, 'counterbid']
                                            medias_list.append(counterbid_win)
                                            auction_df.at[0, 'number_auction_completed'] = auction_df.at[0, 'number_auction_completed'] + 1
                                            number = int(auction_df.at[0, 'number_auction_completed'])
                                            va_msg_log_body = asf.auction_kpis(va_data_df, auction_df, process_df, winner_df)
                                            print("Results_1: \n", result)
                                            print("Results_2: \n", results_2)
                                            va_msg_log = asf.msg_to_log_2(va_msg_log_body.to_json(orient="records"), my_dir)
                                            time_wh = va_msg_log_body.loc[0,'time_wh']
                                            coil_id = va_msg_log_body.loc[0,'id_winner_coil']
                                            print(va_msg_log_body)
                                            await self.send(va_msg_log)
                                            va_status_var = "stand_by"
                                            """Inform log """
                                            va_msg_log_body = f' {coil_id } will be at {time_wh} in wh Terminado.'
                                            va_msg_log_body = asf.inform_finish(va_msg_log_body)
                                            va_msg_log = asf.msg_to_log(va_msg_log_body, my_dir)
                                            await self.send(va_msg_log)
                                            """Inform log """
                                            va_msg_log_body = asf.finish_va_auction(my_full_name,number)
                                            va_msg_log = asf.msg_to_log(va_msg_log_body, my_dir)
                                            await self.send(va_msg_log)
                                            break
                                        else:
                                            """Inform log """
                                            va_msg_log_body = f'{my_full_name} did not receive answer from finalist coil'
                                            va_msg_log_body = asf.inform_error(va_msg_log_body)
                                            va_msg_log = asf.msg_to_log(va_msg_log_body, my_dir)
                                            await self.send(va_msg_log)

                                    else:
                                        """inform log of issue"""
                                        va_msg_log_body = f'{my_full_name} received a msg from {msg_sender_jid} rather than coil'
                                        va_msg_log_body = asf.inform_error(va_msg_log_body)
                                        va_msg_log = asf.msg_to_log(va_msg_log_body, my_dir)
                                        await self.send(va_msg_log)
                                else:
                                    """Inform log """
                                    va_msg_log_body = f'{my_full_name} did not receive answer from coil'
                                    va_msg_log_body = asf.inform_error(va_msg_log_body)
                                    va_msg_log = asf.msg_to_log(va_msg_log_body, my_dir)
                                    await self.send(va_msg_log)
                            else:
                                """inform log of issue"""
                                va_msg_log_body = f'coils does not bring positive benefit to {my_full_name}'
                                va_msg_log_body = asf.inform_error(va_msg_log_body)
                                va_msg_log = asf.msg_to_log(va_msg_log_body, my_dir)
                                await self.send(va_msg_log)

                        else:
                            """Inform log """
                            va_msg_log_body = f'{my_full_name} did not receive answer from any coil'
                            va_msg_log_body = asf.inform_error(va_msg_log_body)
                            va_msg_log = asf.msg_to_log(va_msg_log_body, my_dir)
                            await self.send(va_msg_log)
                    else:
                        """Inform log """
                        va_msg_log_body = f'{my_full_name} did not receive answer from any coil. coils_msgs_df is empty'
                        va_msg_log_body = asf.inform_error(va_msg_log_body)
                        va_msg_log = asf.msg_to_log(va_msg_log_body, my_dir)
                        await self.send(va_msg_log)
                        va_status_var = 'pre-auction'
                else:
                    va_status_var = 'pre-auction'
            elif va_status_var == "stand-by": # stand-by status for VA is very useful. It changes to pre-auction, when there are 3 minutes left to the end of current processing.
                """inform log of status"""
                """Starts next auction when there is some time left before current fab ends"""
                if process_df['start_next_auction_at'].iloc[-1] < datetime.datetime.now():
                    va_status_var = 'pre-auction'
            else:
                """inform log of status"""
                va_inform_json = asf.inform_log_df(my_full_name, va_status_started_at, va_status_var, va_data_df).to_json(orient="records")
                va_msg_log = asf.msg_to_log(va_inform_json, my_dir)
                await self.send(va_msg_log)
                va_status_var = "stand-by"

        async def on_end(self):
            print({self.counter})
            """Inform log """
            '''va_msg_end = asf.send_activation_finish(my_full_name, ip_machine, 'end')
            va_msg_log = asf.msg_to_log(va_msg_end, my_dir)
            await self.send(va_msg_log)'''

        async def on_start(self):
            self.counter = 1
            """Inform log """
            va_msg_start = asf.send_activation_finish(my_full_name, ip_machine, 'start')
            va_msg_log = asf.msg_to_log(va_msg_start, my_dir)
            await self.send(va_msg_log)

    async def setup(self):
        start_at = datetime.datetime.now() + datetime.timedelta(seconds=3)
        b = self.VABehav(period=3, start_at=start_at)  # periodic sender
        template = Template()
        template.metadata = {"performative": "inform"}
        self.add_behaviour(b)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='wh parser')
    parser.add_argument('-an', '--agent_number', type=int, metavar='', required=False, default=8, help='agent_number: 8, 9, 10, 11, 12')
    parser.add_argument('-s', '--status', type=str, metavar='', required=False, default='pre-auction', help='status_var: pre-auction, auction, stand-by, Off')
    parser.add_argument('-st', '--stop_time', type=int, metavar='', required=False, default=84600, help='stop_time: time in seconds where agent')
    parser.add_argument('-sab', '--start_auction_before', type=int, metavar='', required=False, default=10, help='start_auction_before: seconds to start auction prior to current fab ends')
    args = parser.parse_args()
    my_dir = os.getcwd()
    my_name = os.path.basename(__file__)[:-3]
    my_full_name = asf.my_full_name(my_name, args.agent_number)
    va_status_started_at = datetime.datetime.now()  #datetime.datetime.now().time()
    va_status_refresh = datetime.datetime.now() + datetime.timedelta(seconds=5)
    va_status_var = args.status
    start_auction_before = args.start_auction_before
    """Save to csv who I am"""
    va_data_df = asf.set_agent_parameters(my_dir, my_name, my_full_name)
    conf_va_df = va_data_df[['ancho', 'largo', 'espesor']]
    va_data_df['accumulated_profit'] = 0
    va_data_df.at[0,'wh_available'] = "K, L, M, N"
    auction_df = asf.auction_blank_df()
    auction_df.at[0, 'number_preauction'] = 0
    auction_df.at[0, 'number_auction'] = 0
    auction_df.at[0, 'number_auction_completed'] = 0
    process_df = pd.DataFrame([], columns=['fab_start', 'processing_time', 'start_auction_before', 'start_next_auction_at', 'ancho', 'largo', 'espesor'])
    process_df.at[0, 'start_next_auction_at'] = datetime.datetime.now() + datetime.timedelta(seconds=start_auction_before)
    medias_list = [140]
    fab_started_at = datetime.datetime.now()
    leeway = datetime.timedelta(minutes=int(2))
    op_times_df = pd.DataFrame([], columns=['AVG(ca_op_time)', 'AVG(tr_op_time)'])
    ca_to_tr_df = pd.DataFrame()
    seq_va = int(100)
    list_coils = ['K', 'L', 'M', 'N']
    "IP"
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.connect(("8.8.8.8", 80))
    ip_machine = s.getsockname()[0]

    """XMPP info"""
    va_jid = asf.agent_jid(my_dir, my_full_name)
    va_passwd = asf.agent_passwd(my_dir, my_full_name)
    va_agent = VA(va_jid, va_passwd)
    future = va_agent.start(auto_register=True)
    future.result()
    """Counter"""
    stop_time = datetime.datetime.now() + datetime.timedelta(seconds=args.stop_time)
    while datetime.datetime.now() < stop_time:
        time.sleep(1)
    else:
        va_status_var = "off"
        va_agent.stop()
        quit_spade()
        
