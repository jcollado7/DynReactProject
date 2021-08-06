import pandas as pd
import os
import datetime
from random import randrange
from spade.message import Message
import math
import json
from random import random
import numpy as np
import statistics as stats
from datetime import timedelta,date
import subprocess
import time


def agents_data():
    agents_data_df = pd.read_csv(f'agents.csv', header=0, delimiter=",", engine='python')
    return agents_data_df

def auction_blank_df():
    """Returns df column structure with all necessary information to evaluate auction performance"""
    df = pd.DataFrame([], columns=['agent_type', 'active_coils', 'auction_coils', 'fab_start', 'coil_ratings',
                                   'pre_auction_duration', 'auction_duration'])

    '''df = pd.DataFrame([], columns=['id', 'agent_type', 'location_1', 'location_2', 'location',
                                   'coil_auction_winner', 'coil_length', 'coil_width', 'coil_thickness', 'coil_weight',
                                   'int_fab', 'bid', 'budget', 'ship_date', 'ship_date_rating',
                                   'setup_speed',
                                   'bid_rating', 'int_fab_priority', 'int_fab_rating', 'rating', 'rating_dif',
                                   'negotiation',
                                   'pre_auction_start', 'auction_start', 'auction_finish',
                                   'active_tr_slot_1', 'active_tr_slot_2', 'tr_booking_confirmation_at', 'active_wh',
                                   'wh_booking_confirmation_at', 'wh_location', 'active_coils', 'auction_coils',
                                   'brAVG(tr_op_time)', 'brAVG(va_op_time)', 'AVG(tr_op_time)', 'AVG(va_op_time)',
                                   'fab_start'
                                   'slot_1_start', 'slot_1_end', 'slot_2_start', 'slot_2_end', 'delivered_to_wh',
                                   'handling_cost_slot_1', 'handling_cost_slot_2',
                                   'coil_ratings_1', 'coil_ratings_2',
                                   'pre_auction_duration', 'auction_duration',
                                   'gantt', 'location_diagram'
                                   ])'''
    return df


def info_browser():
    df = pd.DataFrame()
    df['location'] = ['K', 'L', 'M', '', 'A', 'K', 'L', 'M', 'N', 'A']
    df['agents'] = ['c01', 'c004@apiict03.etsii.upm.es', 'c003@apiict03.etsii.upm.es', 'br', 'c01', 'c07', 'c03', 'c04', 'c09', 'c01']
    df['id'] = ['coil_001', 'coil_004', 'coil_003', 'browser', 'coil_001', 'coil_007', 'coil_003', 'coil_004', 'coil_009', 'coil_001']
    df['time'] = [57, 72, 47, 69, 57, 68, 72, 47, 69, 57]
    df['status'] = ['on', 'on', 'on', 'on', 'on', 'on', 'stand-by', 'on', 'on', 'on']
    df['From'] = ['NWW1', 'NWW1', 'NWW4', '', 'PA_04', 'NWW1', 'NWW3', 'NWW1', 'NWW3', 'NWW4']
    return df

def browser_util(df):
    op_times_df = pd.DataFrame([], columns=['AVG(tr_op_time)', 'AVG(va_op_time)'])
    op_times_df.at[0, 'AVG(tr_op_time)'] = 72
    op_times_df.at[0, 'AVG(va_op_time)'] = 89

    sorted_df = df.sort_values(by=['time'])
    sorted_df = sorted_df.loc[df['status'] == "on"]  # Solo los que estén activos
    active_time = 70
    sorted_df = sorted_df.loc[sorted_df['time'] < active_time]
    sorted_df = sorted_df.loc[:, ['location', 'id', 'From', 'agents']]
    sorted_df = sorted_df.drop_duplicates(subset=['id'])
    sorted_df = sorted_df.rename(columns={'id': 'agent'})
    sorted_df = sorted_df.reset_index(drop=True)
    for i in range(len(sorted_df['agent'])):
        slice = sorted_df.loc[i, 'agent'][:-3]
        if slice == 'coil_':
            sorted_df.at[i, 'agent_type'] = sorted_df.loc[i, 'agent'][:-4]  # Crea otra columna con agent type
        elif slice == 'brow':
            sorted_df.at[i, 'agent_type'] = sorted_df.loc[i, 'agent']
        else:
            sorted_df.at[i, 'agent_type'] = sorted_df.loc[i, 'agent'][:-3]
    sorted_df = sorted_df.join(op_times_df)
    return sorted_df

def br_jid(agent_directory):
    """Returns str with browser jid"""
    agents_df = agents_data()
    agents_df = agents_df.loc[agents_df['Name'] == "browser"]
    jid = agents_df['User name'].iloc[-1]
    return jid

def msg_to_br(msg_body, agent_directory):
    """Returns msg object to send to browser agent"""
    agents_df = agents_data()
    agents_df = agents_df.loc[agents_df['Name'] == "browser"]
    jid = agents_df['User name'].iloc[-1]
    msg_br = Message(to=jid)
    msg_br.body = msg_body
    msg_br.set_metadata("performative", "inform")
    return msg_br

def get_coil_list(browser_df, list):
    browser_df = browser_df.loc[browser_df['agent_type'] == "coil"]
    browser_df = browser_df.reset_index(drop=True)
    coil_df_nww = pd.DataFrame()
    for i in list:
        row_df = browser_df.loc[browser_df['location'] == i]
        coil_df_nww = coil_df_nww.append(row_df)
    coil_df_nww = coil_df_nww.sort_index()
    coil_df_nww = coil_df_nww.reset_index(drop=True)
    return coil_df_nww

def va_to_coils_initial_df(agent_df, conf_va_df):
    """Builds df to send to coils with auction information made by agent_df and last plc temperatures"""
    agent_df.at[0, 'ancho'] = conf_va_df.loc[0, 'ancho']
    agent_df.at[0, 'largo'] = conf_va_df.loc[0, 'largo']
    agent_df.at[0, 'espesor'] = conf_va_df.loc[0, 'espesor']
    return agent_df

def conf_medidas(agent_df, configuracion_med):
    agent_df.at[0, 'ancho'] = configuracion_med.loc[0, 'ancho']
    agent_df.at[0, 'largo'] = configuracion_med.loc[0, 'largo']
    agent_df.at[0, 'espesor'] = configuracion_med.loc[0, 'espesor']
    return agent_df

def production_cost(configuracion_df,coil_df, i):
    z = coil_df.loc[i,'ancho'] - configuracion_df.loc[0,'ancho']
    m = coil_df.loc[i,'largo'] - configuracion_df.loc[0,'largo']
    n = coil_df.loc[i,'espesor'] - configuracion_df.loc[0,'espesor']
    cost = float(z * 4 + m * 2.5 + n * 2)
    return cost

def transport_cost(to):
    costes_df = pd.DataFrame()
    costes_df['From'] = ['NWW1', 'NWW1', 'NWW1','NWW1','NWW1','NWW3','NWW3','NWW3','NWW3','NWW3','NWW4','NWW4','NWW4','NWW4','NWW4']
    costes_df['CrossTransport'] = [24.6, 24.6, 0, 0, 55.6, 74.8, 74.8, 50.2, 50.2, 32.3, 71.5, 71.5, 46.9,46.9, 0]
    costes_df['Supply'] = [24.6, 24.6, 21.1, 21.1, 5.7, 24.6, 24.6, 21.1, 21.1, 5.7, 24.6, 24.6, 21.1, 21.1, 5.7]
    costes_df['To'] = ['va_08', 'va_09', 'va_10','va_11','va_12','va_08','va_09','va_10','va_11','va_12','va_08','va_09','va_10','va_11','va_12']
    costes_df = costes_df.loc[costes_df['To'] == to]
    costes_df = costes_df.reset_index(drop=True)
    return costes_df

def bid_evaluation(coil_msgs_df, va_data_df):
    key = []
    transport_cost_df = transport_cost(va_data_df.loc[0,'id'])
    for i in range(transport_cost_df.shape[0]): #.shape[0], devuelve las n filas
        m = transport_cost_df.loc[i, 'CrossTransport']
        n = transport_cost_df.loc[i, 'Supply']
        key.append(n+m)
    transport_cost_df['transport_cost'] = key
    transport_cost_df = transport_cost_df.loc[:, ['From', 'To', 'transport_cost']]
    for i in range(coil_msgs_df.shape[0]):
        coil_msgs_df.at[i, 'production_cost'] = production_cost(va_data_df, coil_msgs_df, i)
    coil_msgs_df = coil_msgs_df.loc[:, ['From', 'id', 'agent_type', 'coil_jid', 'location', 'bid', 'production_cost', 'ancho', 'largo', 'espesor', 'ship_date', 'budget_remaining']]
    coil_msgs_df = coil_msgs_df.reset_index(drop=True)
    coil_msgs_df = coil_msgs_df.merge(transport_cost_df, on='From', sort=False)
    for i in range(coil_msgs_df.shape[0]):
        m = coil_msgs_df.loc[i, 'production_cost']
        n = coil_msgs_df.loc[i, 'transport_cost']
        coil_msgs_df.loc[i, 'minimum_price'] = m + n
    for i in range(coil_msgs_df.shape[0]):
        m = coil_msgs_df.loc[i, 'minimum_price']
        n = coil_msgs_df.loc[i, 'bid']
        coil_msgs_df.loc[i, 'difference'] = m - n
    results = coil_msgs_df.loc[:,['agent_type', 'id', 'coil_jid', 'bid', 'minimum_price', 'difference', 'ancho', 'largo', 'espesor', 'ship_date', 'budget_remaining']]
    results = results.sort_values(by=['difference'])
    results = results.reset_index(drop=True)
    value = []
    for i in range(results.shape[0]):
        value.append(i+1)
    results.insert(loc=0, column='position', value=value)
    return results

def counterbid_evaluation(coil_msgs_df, va_data_df):
    key = []
    transport_cost_df = transport_cost(va_data_df.loc[0,'id'])
    for i in range(transport_cost_df.shape[0]):
        m = transport_cost_df.loc[i, 'CrossTransport']
        n = transport_cost_df.loc[i, 'Supply']
        key.append(n + m)
    transport_cost_df['transport_cost'] = key
    transport_cost_df = transport_cost_df.loc[:, ['From', 'To', 'transport_cost']]
    for i in range(coil_msgs_df.shape[0]):
        coil_msgs_df.at[i, 'production_cost'] = production_cost(va_data_df, coil_msgs_df, i)
    coil_msgs_df = coil_msgs_df.loc[:, ['From', 'id', 'agent_type', 'coil_jid', 'location', 'counterbid', 'bid', 'production_cost', 'User_name_va', 'ancho', 'largo', 'espesor', 'budget_remaining', 'ship_date']]
    coil_msgs_df = coil_msgs_df.reset_index(drop=True)
    coil_msgs_df = coil_msgs_df.merge(transport_cost_df, on='From', sort=False)
    for i in range(coil_msgs_df.shape[0]):
        m = coil_msgs_df.loc[i, 'production_cost']
        n = coil_msgs_df.loc[i, 'transport_cost']
        coil_msgs_df.loc[i, 'minimum_price'] = m + n
    for i in range(coil_msgs_df.shape[0]):
        m = coil_msgs_df.loc[i, 'minimum_price']
        n = coil_msgs_df.loc[i, 'counterbid']
        coil_msgs_df.loc[i, 'profit'] = n - m
    results = coil_msgs_df.loc[:, ['agent_type', 'location', 'id', 'coil_jid', 'bid', 'counterbid', 'minimum_price', 'profit', 'User_name_va', 'ancho', 'largo', 'espesor', 'budget_remaining', 'ship_date']]
    results = results.sort_values(by=['profit'], ascending = False)
    results = results.reset_index(drop=True)
    value = []
    for i in range(results.shape[0]):
        value.append(i + 1)
    results.insert(loc=0, column='position', value=value)
    return results

def auction_kpis(va_data_df, auction_df, process_df, winner_df):
    df = auction_blank_df()
    #va
    df.at[0, 'purpose'] = 'inform'
    df.at[0, 'id_va'] = va_data_df.loc[0, 'id']
    df.at[0, 'accumulated_profit_va'] = va_data_df.loc[0, 'accumulated_profit']
    #coil_winner
    df.at[0, 'profit_va_auction'] = winner_df.loc[0, 'profit']
    df.at[0, 'id_winner_coil'] = winner_df.loc[0, 'id']
    df.at[0, 'coil_location_winner'] = winner_df.loc[0, 'location']
    df.at[0, 'minimum_price'] = winner_df.loc[0, 'minimum_price']
    df.at[0, 'bid_winner_coil'] = winner_df.loc[0, 'bid']
    df.at[0, 'counterbid_winner_coil'] = winner_df.loc[0, 'counterbid']
    df.at[0, 'budget_remaining_winner'] = winner_df.loc[0, 'budget_remaining']
    df.at[0, 'ship_date_winner'] = winner_df.loc[0, 'ship_date']
    df.at[0, 'coil_ancho_winner'] = winner_df.loc[0, 'ancho']
    df.at[0, 'coil_largo_winner'] = winner_df.loc[0, 'largo']
    df.at[0, 'coil_espesor_winner'] = winner_df.loc[0, 'espesor']

    df.at[0, 'pre_auction_start'] = auction_df.loc[0, 'pre_auction_start']
    df.at[0, 'pre_auction_duration'] = auction_df.loc[0, 'auction_start'] - auction_df.loc[0, 'pre_auction_start']
    df.at[0, 'auction_start'] = auction_df.loc[0, 'auction_start']
    df.at[0, 'auction_finish'] = datetime.datetime.now()
    df.at[0, 'auction_duration'] = df.loc[0, 'auction_finish'] - auction_df.loc[0, 'auction_start']
    df.at[0, 'fab_start'] = process_df['fab_start'].iloc[-1]
    df.at[0, 'fab_end'] = process_df['fab_end'].iloc[-1]
    df.at[0, 'time_wh'] = df.loc[0, 'fab_end'] + datetime.timedelta(seconds=30)

    df.at[0, 'active_coils'] = auction_df.loc[0, 'active_coils']
    df.at[0, 'auction_coils'] = auction_df.loc[0, 'auction_coils']
    df.at[0, 'active_coils'] = auction_df.loc[0, 'active_coils']
    df.at[0, 'number_preauction'] = auction_df.at[0, 'number_preauction']
    df.at[0, 'number_auction'] = auction_df.loc[0, 'number_auction']
    df.at[0, 'number_auction_completed'] = auction_df.loc[0, 'number_auction_completed']

    #df.at[0, 'coil_ratings_1'] = auction_df.loc[0, 'coil_ratings_1']
    df.at[0, 'coil_ratings'] = auction_df.loc[0, 'coil_ratings']

    #gantt_df = gantt(df)
    #df.at[0, 'gantt'] = gantt_df.to_dict()
    '''location_diagram_df = location_diagram(df)
    df.at[0, 'location_diagram'] = location_diagram_df.to_dict()'''
    return df

def gantt(auction_kpis_df):
    df = pd.DataFrame([], columns=['number_auction', 'task_id', 'task_name', 'duration', 'start', 'resource'])
    number = [auction_kpis_df.loc[0, 'number_auction_completed'], auction_kpis_df.loc[0, 'number_auction_completed'], auction_kpis_df.loc[0, 'number_auction_completed']]
    task_id = [1, 2, 3]
    task_name = ['pre_auction', 'auction', 'processing']
    duration = [auction_kpis_df.loc[0, 'pre_auction_duration'], auction_kpis_df.loc[0, 'auction_duration'], auction_kpis_df.loc[0, 'fab_end']-auction_kpis_df.loc[0, 'fab_start']]
    start = [auction_kpis_df.loc[0, 'pre_auction_start'], auction_kpis_df.loc[0, 'auction_start'], auction_kpis_df.loc[0, 'fab_start']]
    finish = [auction_kpis_df.loc[0, 'auction_start'], auction_kpis_df.loc[0, 'auction_finish'], auction_kpis_df.loc[0, 'fab_end']]
    resource = [auction_kpis_df.loc[0, 'active_coils'], auction_kpis_df.loc[0, 'auction_coils'], auction_kpis_df.loc[0, 'id_va']]

    df['number_auction'] = number
    df['task_id'] = task_id
    df['task_name'] = task_name
    df['duration'] = duration
    df['start'] = start
    df['finish'] = finish
    df['resource'] = resource

    print("GANTT: \n", df)
    return df

def change_agent(my_full_name, my_dir):
    df = pd.read_csv('agents.csv', header=0, delimiter=",", engine='python')
    df.loc[df.Name == my_full_name, 'Code'] = ''
    df.to_csv(f'{my_dir}''/''agents.csv', index=False, header=True)

def auction_entry(va_data_df, coil_df,number,location_df):
    dif_ancho = coil_df.loc[0,'ancho'] - va_data_df.loc[0, 'ancho']
    dif_largo = coil_df.loc[0, 'largo'] - va_data_df.loc[0, 'largo']
    dif_espesor = coil_df.loc[0, 'espesor'] - va_data_df.loc[0, 'espesor']
    dif_total = float(dif_ancho + dif_largo + dif_espesor)
    if (dif_total <= 250) or (number >= 5):
        if (va_data_df.loc[0, 'id'] == 'va_08' or va_data_df.loc[0, 'id'] == 'va_09') and (
                location_df.loc[0, 'location'] == 'K'):
            answer = 1
        elif (va_data_df.loc[0, 'id'] == 'va_10' or va_data_df.loc[0, 'id'] == 'va_11') and (location_df.loc[0, 'location'] == 'L'):
            answer = 1
        elif (va_data_df.loc[0, 'id'] == 'va_12') and (
                location_df.loc[0, 'location'] == 'M' or location_df.loc[0, 'location'] == 'N'):
            answer = 1
        else:
            answer = 0
    else:
        answer = 0
    return answer

def create_bid(coil_df, bid_mean):

    if coil_df.loc[0, 'number_auction'] <= 3:
        valor_1 = 0.15 * coil_df.loc[0, 'budget']
    elif coil_df.loc[0, 'number_auction'] > 3 and coil_df.loc[0, 'number_auction'] <= 7:
        valor_1 = 0.23 * coil_df.loc[0, 'budget']
    else:
        valor_1 = 0.4 * coil_df.loc[0, 'budget']
    if coil_df.loc[0, 'ship_date'] <= 25:
        valor_2 = 0.15 * coil_df.loc[0, 'budget']
    else:
        valor_2 = 0.2 * coil_df.loc[0, 'budget']
    oferta = 0.5 * bid_mean + valor_1 + valor_2
    if oferta > coil_df.loc[0, 'budget']:
        oferta = coil_df.loc[0, 'budget']
    return oferta

def create_counterbid(msg_va, coil_df):

    if msg_va.loc[0,'position'] <= 3:
        valor_1 = 0.7 * coil_df.loc[0, 'budget_remaining']
    else:
        valor_1 = 0.8 * coil_df.loc[0, 'budget_remaining']
    contraoferta = valor_1 + coil_df.loc[0, 'bid']
    return contraoferta

def compare_va(va_coil_msg_df, bid_register_df):

    va_coil_msg_df['winning_auction'] = va_coil_msg_df['counterbid']
    results = pd.concat([bid_register_df, va_coil_msg_df])
    results = results.sort_values(by=['winning_auction'])
    results = results.reset_index(drop=True)
    coil_name_winner = results.loc[0, 'User_name_va']
    return coil_name_winner

def va_msg_to(msg_body):
    """Returns msg object without destination"""
    msg_tr = Message()
    msg_tr.body = msg_body
    msg_tr.set_metadata("performative", "inform")
    return msg_tr

def msg_to_sender(received_msg):
    """Returns msg to send without msg.body"""
    msg_reply = Message()
    msg_reply.to = str(received_msg.sender)
    msg_reply.set_metadata("performative", "inform")
    return msg_reply

def process_df(df, coil_winner_df):
    process_df = df
    if pd.isnull(process_df['fab_start'].iloc[-1]):
        new_line_df = pd.Series(
            [np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan],
            index=['fab_start', 'processing_time', 'start_auction_before', 'start_next_auction_at', 'fab_end',
                   'setup_speed', 'ancho', 'largo', 'espesor'])
        process_df = process_df.append(new_line_df, ignore_index=True)
        process_df = process_df.reset_index(drop=True)
        processing_time = 30 #600
        process_df['start_auction_before'].iloc[-1] = 1.5 * 60
        start_auction_before = process_df['start_auction_before'].iloc[-1]
        process_df['processing_time'].iloc[-1] = processing_time
        process_df['fab_start'].iloc[-1] = (datetime.datetime.now() + datetime.timedelta(minutes=5) - datetime.timedelta(minutes=2.5))
        process_df['fab_end'].iloc[-1] = process_df['fab_start'].iloc[-1] + datetime.timedelta(seconds=processing_time)
        start_next_auction_at = process_df['fab_end'].iloc[-1] - datetime.timedelta(seconds=start_auction_before)
        process_df['start_next_auction_at'].iloc[-1] = start_next_auction_at
        a = process_df['fab_start'].iloc[-1]
    else:
        process_df.loc[process_df.index.max() + 1, 'start_auction_before'] = ""
        processing_time = 100
        process_df['processing_time'].iloc[-1] = processing_time
        process_df['fab_start'].iloc[-1] = process_df['fab_end'].iloc[-2] #Empieza la última, cuando acaba la penúltima
        process_df['fab_start'] = pd.to_datetime(process_df['fab_start'])
        process_df['fab_end'].iloc[-1] = process_df['fab_start'].iloc[-1] + datetime.timedelta(seconds=processing_time)
        process_df['start_auction_before'].iloc[-1] = process_df['start_auction_before'].iloc[-2]
        start_next_auction_at = process_df['fab_end'].iloc[-1] - datetime.timedelta(seconds=process_df['start_auction_before'].iloc[-1])
        process_df['start_next_auction_at'].iloc[-1] = start_next_auction_at
        a = process_df['fab_start'].iloc[-1]
    process_df['ancho'].iloc[-1] = coil_winner_df.loc[0, 'ancho']
    process_df['largo'].iloc[-1] = coil_winner_df.loc[0, 'largo']
    process_df['espesor'].iloc[-1] = coil_winner_df.loc[0, 'espesor']
    return process_df

def my_full_name(agent_name, agent_number):
    decimal = ""
    if agent_name == "coil":
        if len(str(agent_number)) == 1:
            decimal = str("00")
        elif len(str(agent_number)) == 2:
            decimal = str(0)
        full_name = str(agent_name) + str("_") + decimal + str(agent_number)
    elif agent_name == "log":
        full_name = agent_name
    elif agent_name == "browser":
        full_name = agent_name
    elif agent_name == "launcher":
        full_name = agent_name
    else:
        if len(str(agent_number)) == 1:
            decimal = str(0)
        elif len(str(agent_number)) == 2:
            decimal = ""
        full_name = str(agent_name) + str("_") + decimal + str(agent_number)
    return full_name

def set_agent_parameters_coil(my_dir, agent_name, agent_full_name, location, code):
    agent_data = pd.DataFrame([], columns=['id', 'agent_type','location', 'purpose', 'request_type', 'time', 'activation_time', 'int_fab'])
    agent_data.at[0, 'id'] = agent_full_name
    agent_data.at[0, 'agent_type'] = agent_name
    agents_df = agents_data()
    agents_df.loc[agents_df.Name == agent_full_name, 'location'] = location
    agents_df.loc[agents_df.Name == agent_full_name, 'Code'] = code
    agents_df.to_csv(f'{my_dir}''/''agents.csv', index=False, header=True)
    agents_df = agents_df.loc[agents_df['Name'] == agent_full_name]
    agents_df = agents_df.reset_index(drop=True)
    if agent_name == 'va':
        agent_data = agent_data.reindex(columns=['id', 'agent_type', 'purpose', 'request_type', 'time', 'activation_time', 'setup_speed', 'ancho', 'largo', 'espesor']) #Los valores ya existentes, se mantienen
        agent_data = va_parameters(agent_data, agents_df, agent_name)
    elif agent_name == "coil":
        agent_data = agent_data.reindex(
            columns=['id', 'agent_type', 'location', 'From', 'Code', 'purpose', 'request_type', 'time', 'activation_time', 'to_do', 'plant', 'number_auction', 'int_fab', 'bid', 'bid_status', 'ancho', 'largo', 'espesor', 'budget'])
        agent_data = coil_parameters(agent_data, agents_df, agent_name)
    else: #log,browser..
        agents_df = agents_data()
        df = agents_df.loc[agents_df['Name'] == agent_name]
        df = df.reset_index(drop=True)
        #agent_data.at[0, 'location'] = df.loc[0, 'Location']
    return agent_data

def set_agent_parameters(my_dir, agent_name, agent_full_name):
    agent_data = pd.DataFrame([], columns=['id', 'agent_type','location', 'purpose', 'request_type', 'time', 'activation_time', 'int_fab'])
    agent_data.at[0, 'id'] = agent_full_name
    agent_data.at[0, 'agent_type'] = agent_name
    agents_df = agents_data()
    agents_df = agents_df.loc[agents_df['Name'] == agent_full_name]
    agents_df = agents_df.reset_index(drop=True)
    if agent_name == 'va':
        agent_data = agent_data.reindex(columns=['id', 'agent_type', 'purpose', 'request_type', 'time', 'activation_time', 'setup_speed', 'ancho', 'largo', 'espesor']) #Los valores ya existentes, se mantienen
        agent_data = va_parameters(agent_data, agents_df, agent_name)
    elif agent_name == "coil":
        agent_data = agent_data.reindex(
            columns=['id', 'agent_type', 'location', 'From', 'Code', 'purpose', 'request_type', 'time', 'activation_time', 'to_do', 'plant', 'number_auction', 'int_fab', 'bid', 'bid_status', 'ancho', 'largo', 'espesor', 'budget'])
        agent_data = coil_parameters(agent_data, agents_df, agent_name)
    else: #log,browser..
        agents_df = agents_data()
        df = agents_df.loc[agents_df['Name'] == agent_name]
        df = df.reset_index(drop=True)
        #agent_data.at[0, 'location'] = df.loc[0, 'Location']
    return agent_data

def agent_jid(agent_directory, agent_full_name):
    agents_df = agents_data()
    agents_df = agents_df.loc[agents_df['Name'] == agent_full_name]
    agents_df = agents_df.reset_index(drop=True)
    jid_direction = agents_df.loc[agents_df.Name == agent_full_name, 'User name']
    jid_direction = jid_direction.values
    jid_direction = jid_direction[0]
    return jid_direction

def agent_passwd(agent_directory, agent_full_name):
    agents_df = agents_data()
    agents_df = agents_df.loc[agents_df['Name'] == agent_full_name]
    password = agents_df['Password'].iloc[-1]
    return password

def coil_parameters(agent_data, agents_df, agent_name):
    """Sets pseudo random parameters"""
    rn = random()
    agent_data.at[0, 'int_fab'] = 0
    agent_data.at[0, 'location'] = agents_df.loc[0, 'location']
    agent_data.loc[0, 'From'] = agents_df.loc[0, 'From']
    agent_data.loc[0, 'Code'] = agents_df.loc[0, 'Code']
    agent_data.loc[0, 'to_do'] = "search_auction"
    agent_data.loc[0, 'plant'] = "VA"
    agent_data.at[0, 'ancho'] = 12 + (rn * 10)  # between 12-22
    agent_data.at[0, 'largo'] = 13 + (rn * 10)  # between 13-16
    agent_data.at[0, 'espesor'] = 14 + (rn * 10)  # between 14-17
    agent_data.at[0, 'ship_date'] = 1 + (rn * 40)  # Planning: between now and 41
    agent_data.at[0, 'number_auction'] = 0 + (rn * 10)  # 0 y 10
    if rn < 0.15:
        agent_data.at[0, 'budget'] = 200 + (20 * random())
    else:
        agent_data.at[0, 'budget'] = 200
    return agent_data

def random_date(start, end):
    """
    This function will return a random datetime between two datetime
    objects.
    """
    delta = end - start
    int_delta = (delta.days * 24 * 60 * 60) + delta.seconds
    random_second = randrange(int_delta)
    return start + timedelta(seconds=random_second)

def va_parameters(agent_data, agents_df, agent_name):
    """Sets pseudo random parameters"""
    rn = random()
    agent_data.at[0, 'ancho'] = 5 + (rn * 10)  # between 5-15
    agent_data.at[0, 'largo'] = 6 + (rn * 10)  # between 6-16
    agent_data.at[0, 'espesor'] = 7 + (rn * 10)  # between 7-17
    return agent_data

def bid_register(agent_name, agent_full_name):
    """Creates bid register"""
    df = pd.DataFrame([], columns=['id', 'agent_type', 'auction_owner', 'initial_bid', 'second_bid', 'won_bid', 'accepted_bid'])
    #df.at[0, 'id'] = agent_full_name
    #df.at[0, 'agent_type'] = agent_name
    return df

def msg_to_log(msg_body, agent_directory):
    agents_df = agents_data()
    agents_df = agents_df.loc[agents_df['Name'] == "log"]
    log_jid = agents_df['User name'].iloc[-1]
    msg_log = Message(to=log_jid)
    msg_log.body = msg_body
    msg_log.set_metadata("performative", "inform")
    return msg_log

def msg_to_log_2(msg_body, agent_directory):
    agents_df = agents_data()
    agents_df = agents_df.loc[agents_df['Name'] == "log"]
    log_jid = agents_df['User name'].iloc[-1]
    msg_log = Message(to=log_jid)
    print(f'msg_body:{msg_body}')
    msg_log.body = msg_body
    msg_log.set_metadata("performative", "inform")
    return msg_log

def activation_df(agent_full_name, status_started_at, df, *args):
    act_df = df.loc[:, 'id':'activation_time']
    act_df = act_df.astype(str)
    act_df.at[0, 'purpose'] = "inform"
    act_df.at[0, 'request_type'] = ""
    act_df.at[0, 'time'] = datetime.datetime.now()
    act_df.at[0, 'status'] = "on"
    act_df.at[0, 'activation_time'] = status_started_at
    if act_df.at[0, 'id'] == 'browser':
        act_df.drop(['location'], axis=1)
    if args:
        df = args[0]
        act_df = act_df.join(df)
    act_json = act_df.to_json(orient="records")
    return act_json

def inform_log_df(agent_full_name, status_started_at, status, df, *args, **kwargs):
    """Inform of agent status"""
    inf_df = df.loc[:, 'id':'activation_time']
    inf_df = inf_df.astype(str)
    inf_df.at[0, 'id'] = inf_df.at[0, 'id']
    inf_df.at[0, 'agent_type'] = inf_df.at[0, 'agent_type']
    inf_df.at[0, 'purpose'] = "inform"
    inf_df.at[0, 'request_type'] = ""
    inf_df.at[0, 'time'] = datetime.datetime.now()
    inf_df.at[0, 'status'] = status
    inf_df.at[0, 'activation_time'] = status_started_at
    if inf_df.at[0, 'id'] == 'browser':
        inf_df.drop(['location'], axis=1)
    if args:
        inf_df.at[0, 'to_do'] = args[0]
        inf_df.loc[0, 'plant'] = "VA"
    if kwargs:  # in case did not enter auction
        inf_df.at[0, 'entered_auction'] = kwargs[0]  # "No, temp difference out of limit"
    return inf_df

def op_times(p_df, ca_data_df):
    df = ca_data_df
    df.at[0, 'AVG(ca_op_time)'] = p_df['processing_time'].iloc[-1]
    df.at[0, 'AVG(tr_op_time)'] = (3 + random()) * 60  # between 3 and 4
    return df

def result(coil_ofertas_df, jid_list):
    df = pd.DataFrame([], columns=['Coil', 'Minimum_price', 'Bid', 'Difference', 'Budget_remaining'])
    for i in range(len(jid_list)):
        df.at[i, 'Coil'] = coil_ofertas_df.loc[i, 'id']
        df.at[i, 'Minimum_price'] = coil_ofertas_df.loc[i, 'minimum_price']
        df.at[i, 'Bid'] = coil_ofertas_df.loc[i, 'bid']
        df.at[i, 'Difference'] = coil_ofertas_df.loc[i, 'difference']
        df.at[i, 'Budget_remaining'] = coil_ofertas_df.loc[i, 'budget_remaining']
    return df

def results_2 (coil_contraofertas_df, jid_list):
    df = pd.DataFrame([], columns=['Coil', 'Minimum_price', 'Counterbid', 'Profit'])
    for i in range(len(jid_list)):
        df.at[i, 'Coil'] = coil_contraofertas_df.loc[i, 'id']
        df.at[i, 'Minimum_price'] = coil_contraofertas_df.loc[i, 'minimum_price']
        df.at[i, 'Counterbid'] = coil_contraofertas_df.loc[i, 'counterbid']
        df.at[i, 'Profit'] = coil_contraofertas_df.loc[i, 'profit']
    return df

def check_active_users_loc_times(va_data_df, agent_name, *args):
    """Returns a json with va averages operation time"""
    if args == "coils":
        df = br_get_requested_df(agent_name, args)
    else:
        df = br_get_requested_df(agent_name)
    # Calculate means
    df['time'] = pd.to_datetime(df['time'])
    '''df['AVG(ca_op_time)'] = pd.to_datetime(df['AVG(ca_op_time)'], unit='ms')
    va_avg = df['AVG(ca_op_time)'].mean()  # avg(operation_time_ca)
    if pd.isnull(va_avg):
        va_avg = 9
    else:
        va_avg = va_avg - datetime.datetime(1970, 1, 1)
        va_avg = va_avg.total_seconds() / 60
    op_times_df = pd.DataFrame([], columns=['AVG(va_op_time)'])
    op_times_df.at[0, 'AVG(va_op_time)'] = va_avg'''
    # Check active users locations
    sorted_df = df.sort_values(by=['time'])
    sorted_df = sorted_df.loc[sorted_df['status'] == "auction"]
    active_time = datetime.datetime.now() - datetime.timedelta(seconds=300)
    sorted_df = sorted_df.loc[sorted_df['time'] < active_time]
    uniques = sorted_df['id']
    uniques = uniques.drop_duplicates()
    uniques = uniques.tolist()
    values = []
    keys = []
    for i in uniques:
        a = sorted_df.loc[sorted_df['id'] == i]
        last_id = a.loc[a.index[-1], 'id']
        last_location = a.loc[a.index[-1], 'location']
        keys.append(last_id)
        values.append(last_location)
    users_location = dict(zip(keys, values))
    users_location_df = pd.DataFrame([users_location])
    users_location_df = users_location_df.T
    indexes = users_location_df.index.values.tolist()
    users_location_df.insert(loc=0, column='agent', value=indexes)
    users_location_df = users_location_df.rename(columns={0: "location"})
    users_location_df = users_location_df.reset_index(drop=True)
    for i in range(len(users_location_df['agent'])):
        slice = users_location_df.loc[i, 'agent'][:-3]
        if slice == 'coil_':
            users_location_df.at[i, 'agent_type'] = users_location_df.loc[i, 'agent'][:-4]
        elif slice == 'brow':
            users_location_df.at[i, 'agent_type'] = users_location_df.loc[i, 'agent']
        else:
            users_location_df.at[i, 'agent_type'] = users_location_df.loc[i, 'agent'][:-3]
    # Joins information
    #users_location_df = users_location_df.join(op_times_df)
    users_location_df = users_location_df.loc[users_location_df['agent_type'] == "coil"]
    users_location_df = users_location_df.reset_index(drop=True)
    coil_df = pd.DataFrame()
    z = va_data_df.loc[0, 'wh_available']
    for i in z:
        row_df = users_location_df.loc[users_location_df['location'] == i]
        coil_df = coil_df.append(row_df)
    coil_df = coil_df.sort_index()
    coil_df = coil_df.reset_index(drop=True)
    coil_df = get_coil_list(coil_df, va_data_df.loc[0, 'list_coils'])
    return coil_df

def br_get_requested_df(agent_name, *args):
    """Returns a df in which calculations can be done"""
    df = pd.DataFrame()
    if args == "coils":
        search_str = '{"id":{"0":"' + "coil" + '_'  # tiene que encontrar todas las coil que quieran fabricarse y como mucho los últimos 1000 registros.
    else:
        search_str = "activation_time"  # takes every record with this. Each agent is sending that info while alive communicating to log.
    l = []
    N = 1000
    with open(r"log.log") as f:
        for line in f.readlines()[-N:]:  # from the last 1000 lines
            if search_str in line:  # find search_str
                n = line.find("{")
                a = line[n:]
                l.append(a)
    df_0 = pd.DataFrame(l, columns=['register'])
    for ind in df_0.index:
        if ind == 0:
            element = df_0.loc[ind, 'register']
            for x in range(len(element)):
                element = element.replace("]", "")
            y = json.loads(element)
            df = pd.DataFrame(y, index=[0])
        else:
            element = df_0.loc[ind, 'register']
            for x in range(len(element)):
                element = element.replace("]", "")
            y = json.loads(element)
            b = pd.DataFrame(y, index=[0])
            df = df.append(b)
    df = df.reset_index(drop=True)
    if args == "coils":  # if ca is requesting
        df = df.loc[0, 'to_do'] == "search_auction"
    return df

def req_active_users_loc_times(agent_df, seq, list, *args):
    """Returns msg body to send to browser as a json"""
    va_request_df = agent_df #.loc[:, 'id':'time']
    va_request_df = va_request_df.astype(str)
    va_request_df.at[0, 'purpose'] = "request"
    this_time = datetime.datetime.now()
    va_request_df.at[0, 'time'] = this_time
    va_request_df.at[0, 'seq'] = seq
    va_request_df.loc[0, 'list_coils'] = str(list)
    if args:
        va_request_df.at[0, 'request_type'] = args[0]
    else:
        va_request_df.at[0, 'request_type'] = "active users location & op_time"
    return va_request_df

def req_active_users_loc_times_coil(agent_df, seq, *args):
    """Returns msg body to send to browser as a json"""
    va_request_df = agent_df #.loc[:, 'id':'time']
    va_request_df = va_request_df.astype(str)
    va_request_df.at[0, 'purpose'] = "request"
    this_time = datetime.datetime.now()
    va_request_df.at[0, 'time'] = this_time
    va_request_df.at[0, 'seq'] = seq
    va_request_df.loc[0, 'list_coils'] = str(list)
    if args:
        va_request_df.at[0, 'request_type'] = args[0]
    else:
        va_request_df.at[0, 'request_type'] = "active users location & op_time"
    return va_request_df

def req_coil_loc(agent_df, *args):
    """Returns msg body to send to browser as a json"""
    coil_request_df = agent_df #.loc[:, 'id':'time']
    coil_request_df = coil_request_df.astype(str)
    coil_request_df.at[0, 'purpose'] = "request"
    this_time = datetime.datetime.now()
    coil_request_df.at[0, 'time'] = this_time
    if args:
        coil_request_df.at[0, 'request_type'] = args[0]
    else:
        coil_request_df.at[0, 'request_type'] = "my location"
    return coil_request_df.to_json()

def bids_mean(medias_list):
    if len(medias_list) > 3:
        medias_list = medias_list[-3:]
    medias_list = stats.mean(medias_list)
    return medias_list

def msg_to_browser(order_body, agent_directory):
    agents_df = agents_data()
    agents_df = agents_df.loc[agents_df['Name'] == "browser"]
    log_jid = agents_df['User name'].iloc[-1]
    order_msg = Message(to=log_jid)
    order_msg.body = order_body
    order_msg.set_metadata("performative", "inform")
    return order_msg

def change_warehouse(launcher_df, my_dir):
    va = launcher_df.loc[0, 'list_ware'].split(',')
    lc = launcher_df.loc[0, 'list_coils'].split(',')
    wait_time = launcher_df.loc[0, 'wait_time']
    #df = pd.read_csv('agents.csv', header=0, delimiter=",", engine='python')
    j = 0
    my_dir = os.getcwd()
    for z in lc:
        number = 1
        name = 'coil_00' + str(number)
        df = pd.read_csv('agents.csv', header=0, delimiter=",", engine='python')
        for i in range(11):
            if df.loc[df.Name == name, 'Code'].isnull().any().any():
                cmd = f'python3 coil.py -an {str(number)} -l {va[j]} -c{z} -w{wait_time}'
                subprocess.Popen(cmd, stdout=None, stdin=None, stderr=None, close_fds=True, shell=True)
                break
            elif df.loc[df.Name == name, 'Code'].values == z:
                cmd = f'python3 coil.py -an {str(number)} -l {va[j]} -c{z} -w{wait_time}'
                subprocess.Popen(cmd, stdout=None, stdin=None, stderr=None, close_fds=True, shell=True)
                break
            else:
                number = number + 1
                name = 'coil_00' + str(number)
        time.sleep(3)
        j = j + 1

def order_file(agent_full_name, order_code, steel_grade, thickness, width_coils, num_coils, list_coils, each_coil_price,
               list_ware, string_operations, wait_time):
    order_msg_log = pd.DataFrame([], columns=['id', 'order_code', 'steel_grade', 'thickness_coils', 'width_coils',
                                              'num_coils', 'list_coils', 'each_coil_price', 'string_operations',
                                              'date'])
    order_msg_log.at[0, 'id'] = agent_full_name
    order_msg_log.at[0, 'purpose'] = 'setup'
    order_msg_log.at[0, 'msg'] = 'new order'
    order_msg_log.at[0, 'order_code'] = order_code
    order_msg_log.at[0, 'steel_grade'] = steel_grade
    order_msg_log.at[0, 'thickness_coils'] = thickness
    order_msg_log.at[0, 'width_coils'] = width_coils
    order_msg_log.at[0, 'num_coils'] = num_coils
    order_msg_log.at[0, 'list_coils'] = list_coils
    order_msg_log.at[0, 'each_coil_price'] = each_coil_price
    order_msg_log.at[0, 'list_ware'] = list_ware
    order_msg_log.at[0, 'string_operations'] = string_operations
    order_msg_log.at[0, 'date'] = date.today().strftime('%Y-%m-%d')
    order_msg_log.at[0, 'to'] = 'log'
    order_msg_log.at[0, 'wait_time'] = wait_time
    return order_msg_log

def order_code_log(coil_code, df, my_full_name):
    order_coil_df = pd.DataFrame([], columns = ['Code'])
    order_coil_df.at[0, 'Code'] = coil_code
    order_coil_df.loc[0, 'purpose'] = "location_coil"
    order_coil_df.loc[0, 'id'] = my_full_name
    order_coil_df.loc[0, 'to'] = 'log@apiict00.etsii.upm.es'
    order_coil_df.loc[0, 'msg'] = df.loc[0, 'seq']
    order_coil_df = order_coil_df[['id', 'Code', 'purpose', 'msg', 'to']]
    return order_coil_df

def loc_of_coil(coil_df):
    loc_df = pd.DataFrame([], columns = ['location'])
    df = pd.read_csv('agents.csv', header=0, delimiter=",", engine='python')
    code = coil_df.loc[0, "Code"]
    location = df.loc[df.Code == code, 'location']
    location = location.values
    location = location[0]
    loc_df.loc[0, 'location'] = location
    return loc_df






'''Functions to improve readability in messages. Improve functions'''

def request_browser(df, seq, list):
    df.loc[:, 'id':'request_type']
    df.loc[0, 'to'] = 'browser@apiict00.etsii.upm.es'
    df.loc[0, 'msg'] = seq
    df.loc[0, 'coils'] = str(list)
    df = df[['id', 'purpose', 'request_type', 'msg', 'to']]
    return df

def answer_va(df_br, sender, df_va, coils, location):
    df = pd.DataFrame()
    df.loc[0, 'msg'] = df_va.loc[0, 'seq']
    df.loc[0, "id"] = 'browser'
    df.loc[0, "coils"] = coils
    df.loc[0, "location"] = location
    df.loc[0, "purpose"] = 'answer'
    df.loc[0, "to"] = sender
    df = df[['id', 'purpose', 'msg', 'coils', 'location', 'to']]
    return df

def answer_coil(df, sender, seq_df):
    df.loc[0, 'msg'] = seq_df.loc[0, 'msg']
    df.loc[0, "id"] = 'browser'
    df.loc[0, "purpose"] = 'answer'
    df.loc[0, "to"] = sender
    df = df[['id', 'purpose', 'msg','location', 'to']]
    return df

def send_va(my_full_name, number, auction_level, jid_list):
    df = pd.DataFrame()
    df.loc[0, 'id'] = my_full_name
    df.loc[0, 'purpose'] = 'inform'
    if auction_level == 1:
        df.loc[0, 'msg'] = 'send pre-auction'
    elif auction_level == 2:
        df.loc[0, 'msg'] = 'send auction'
    elif auction_level == 3:
        df.loc[0, 'msg'] = 'send acceptance'
    df.loc[0, 'number'] = number
    df.loc[0, 'to'] = jid_list
    return df

def send_coil(my_full_name, seq):
    df = pd.DataFrame()
    df.loc[0, 'id'] = my_full_name
    df.loc[0, 'agent_type'] = 'coil'
    df.loc[0, 'purpose'] = 'request'
    df.loc[0, 'request_type'] = 'my location'
    df.loc[0, 'msg'] = seq
    df.loc[0, 'to'] = 'browser@apiict00.etsii.upm.es'
    return df

def send_br_log(df, df_br, my_full_name):
    df.loc[0, 'id'] = my_full_name
    df.loc[0, 'purpose'] = 'answer'
    df.loc[0, 'msg'] = df_br.loc[0, 'msg']
    df.loc[0, 'to'] = 'browser@apiict00.etsii.upm.es'
    df = df[['id', 'purpose', 'msg', 'location', 'to']]
    return df.to_json(orient="records")

def send_to_va_msg(my_full_name, bid, to, level):
    df = pd.DataFrame()
    df.loc[0, 'id'] = my_full_name
    df.loc[0, 'agent_type'] = 'coil'
    df.loc[0, 'purpose'] = 'inform'
    if level == '1':
        df.loc[0, 'msg'] = 'send bid'
        df.loc[0, 'Bid'] = bid
    elif level == 2:
        df.loc[0, 'msg'] = 'send counterbid'
        df.loc[0, 'counterbid'] = bid
    df.loc[0, 'to'] = to
    return df

def send_activation_finish(my_full_name, ip_machine, level):
    df = pd.DataFrame()
    df.loc[0, 'id'] = my_full_name
    df.loc[0, 'purpose'] = 'inform'
    df.loc[0, 'msg'] = 'change status'
    if level == 'start':
        df.loc[0, 'status'] = 'started'
    elif level == 'end':
        df.loc[0, 'status'] = 'ended'
    df.loc[0, 'IP'] = ip_machine
    return df.to_json(orient="records")

def inform_error(msg):
    df = pd.DataFrame()
    df.loc[0, 'purpose'] = 'inform error'
    df.loc[0, 'msg'] = msg
    return df.to_json(orient="records")

def inform_finish(msg):
    df = pd.DataFrame()
    df.loc[0, 'purpose'] = 'inform'
    df.loc[0, 'msg'] = msg
    return df.to_json(orient="records")

def won_auction(my_full_name, va_coil_msg_sender_f, this_time):
    df = pd.DataFrame()
    df.loc[0, 'id'] = my_full_name
    df.loc[0, 'purpose'] = 'inform'
    df.loc[0, 'msg'] = f'won auction in {va_coil_msg_sender_f}'
    df.loc[0, 'time'] = this_time
    return df.to_json(orient="records")

def finish_va_auction(my_full_name, number):
    df = pd.DataFrame()
    df.loc[0, 'id'] = my_full_name
    df.loc[0, 'purpose'] = 'inform'
    df.loc[0, 'msg'] = f'finish auction number: {number}.'
    return df.to_json(orient="records")

def order_register(my_full_name, code, coils, locations):
    df = pd.DataFrame()
    df.loc[0, 'id'] = my_full_name
    df.loc[0, 'purpose'] = 'inform'
    df.loc[0, 'msg'] = 'new order from launcher'
    df.loc[0, 'code'] = code
    df.loc[0, 'coils'] = coils
    df.loc[0, 'locations'] = locations
    return df.to_json(orient="records")

def log_status(my_full_name, status, ip_machine):
    df = pd.DataFrame()
    df.loc[0, 'id'] = my_full_name
    df.loc[0, 'purpose'] = 'inform'
    df.loc[0, 'msg'] = 'change status'
    df.loc[0, 'status'] = status
    df.loc[0, 'IP'] = ip_machine
    return df.to_json(orient="records")

def coil_status(my_full_name):
    df = pd.DataFrame()
    df.loc[0, 'id'] = my_full_name
    df.loc[0, 'purpose'] = 'inform'
    df.loc[0, 'msg'] = 'change status'
    df.loc[0, 'status'] = 'sleep'
    return df.to_json(orient="records")





'''funciones Monica'''
def order_to_search(search_body, agent_full_name, agent_directory):
    agents_df = agents_data()
    agents_df = agents_df.loc[agents_df['Name'] == "browser"]
    browser_jid = agents_df['User name'].iloc[-1]
    search_msg = Message(to=browser_jid)
    search_msg.body = 'Search:' + search_body + ':' + agent_full_name
    search_msg.set_metadata("performative", "inform")
    return search_msg

def br_int_fab_df(agent_df):
    """Returns df to send to interrupted fab coil"""
    agent_df.at[0, 'int_fab'] = 1
    return agent_df

def br_msg_to(msg_body):
    """Returns msg object without destination"""
    msg = Message()
    msg.body = msg_body
    msg.set_metadata("performative", "inform")
    return msg

def order_searched(filter, agent_request, agent_directory):
    agents_df = agents_data()
    agents_df = agents_df.loc[agents_df['Name'] == agent_request]
    launcher_jid = agents_df['User name'].iloc[-1]
    order_searched_msg = Message(to=launcher_jid)
    order_searched_msg.body = 'Order searched:' + filter
    order_searched_msg.set_metadata("performative", "inform")
    return order_searched_msg

def order_to_log(order_body, agent_directory):
    agents_df = agents_data()
    agents_df = agents_df.loc[agents_df['Name'] == "log"]
    log_jid = agents_df['User name'].iloc[-1]
    order_msg = Message(to=log_jid)
    order_msg.body = order_body
    order_msg.set_metadata("performative", "inform")
    return order_msg

def checkFileExistance():
    try:
        with open('ActiveAgents.csv', 'r') as f:
            return True
    except FileNotFoundError as e:
        return False
    except IOError as e:
        return False

def alive_agent(agent_jid):
    msg_alive = Message(to=agent_jid)
    msg_alive.body = "Alive: Agent"
    msg_alive.set_metadata("performative", "inform")
    return msg_alive

def aa_type(id):
    t = id.split('@')
    type = t[0]
    if type[:-2] == "l":
        s = "log"
    elif type[:-2] == "brows":
        s = "browser"
    elif type[:-2] == "ca":
        s = "ca"
    elif type[:-2] == "wh":
        s = "wh"
    elif type[:-2] == "tc":
        s = "tc"
    elif type[:-2] == "launch":
        s = "launcher"
    else:
        s = "coil"
    return s

def checkFile2Existance():
    try:
        with open('RegisterOrders.csv', 'r') as f:
            return True
    except FileNotFoundError as e:
        return False
    except IOError as e:
        return False

def update_coil_status(coil_id, status):
    df = pd.read_csv('RegisterOrders.csv', header=0, delimiter=",", engine='python')
    df.loc[(df.ID_coil.isin([coil_id])), 'coil_status'] = status
    df.to_csv('RegisterOrders.csv', index=False)

def save_order(msg):
    s = msg.split(':')
    code = s[4].split('"')
    steel = s[6].split('"')
    thick = s[8].split('}')
    width = s[10].split('}')
    num = s[12].split('}')
    list = s[14].split('"')
    id_coil = list[1].split(',')
    price = s[16].split('}')
    dat = msg.split('"')
    string_operations = s[18].split('"')
    status = string_operations[1].split(';')
    i = 0
    n = int(num[0])
    while (i < n):
        lista_total = []
        lista_total.append({
            'Date': dat[51],
            'Order_code': code[1],
            'Steel_grade': steel[1],
            'Thickness': thick[0],
            'Width_coils': width[0],
            'Number_coils': num[0],
            'ID_coil': id_coil[i],
            'Price_coils': price[0],
            'Operations': string_operations[1],
            'coil_status': status[0]
        })
        columns = ['Date', 'Order_code', 'Steel_grade', 'Thickness', 'Width_coils', 'Number_coils', 'ID_coil',
                   'Price_coils', 'Operations', 'coil_status']
        df = pd.DataFrame(lista_total, columns=columns)
        with open('RegisterOrders.csv', 'a') as f:
            if os.path.getsize('RegisterOrders.csv') == 0:
                df.to_csv(f, header=True, index=False)
            else:
                df.to_csv(f, header=False, index=False)
        i += 1

def msg_to_launcher(msg, agent_directory):
    """Returns msg object to send to launcher agent"""
    agents_df = agents_data()
    agents_df = agents_df.loc[agents_df['Name'] == "launcher"]
    jid = agents_df['User name'].iloc[-1]
    msg_la = Message(to=jid)
    msg_la.body = msg
    msg_la.set_metadata("performative", "inform")
    return msg_la
