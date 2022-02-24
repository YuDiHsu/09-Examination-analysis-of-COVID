import datetime
import math
import multiprocessing as mp
import os
from datetime import timedelta
import cx_Oracle
import numpy as np
import pandas as pd
import xlsxwriter
from smtp import SMTP
import time
from subprocess import Popen
import glob
from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive
import re


def download_raw_data(file_name, code):
    dsn = cx_Oracle.makedsn('192.168.170.52', '1561', service_name='DW')

    conn = cx_Oracle.connect(
        user='sas',
        password='ueCr5brAD6u4rAs62t9a',
        dsn=dsn,
        encoding='UTF8',
        nencoding='UTF8'
    )

    c = conn.cursor()

    c.execute(code)

    desc = c.description
    col_name_list = []
    for s in desc:
        col_name_list.append(s[0])

    data_list = c.fetchall()
    conn.close()

    df = pd.DataFrame(data_list, columns=col_name_list)

    df.to_csv(os.path.join('.', 'imported_data', 'huang', f'{file_name}.csv'), encoding='utf-8', index=False)


def calculate(df_data):
    # receive calculate
    today = datetime.datetime.now()
    date_m1 = ''
    date_m2 = ''
    if today.hour in range(15, 20):
        date_m1 = today.replace(hour=13, minute=59, second=59)
        date_m2 = today.replace(hour=14, minute=0, second=0) - timedelta(days=1)
    if today.hour in range(0, 5):
        date_m1 = today.replace(hour=23, minute=59, second=59) - timedelta(days=1)
        date_m2 = today.replace(hour=0, minute=0, second=0) - timedelta(days=1)


    cdc_unit = ['南區實驗室(高雄)', '昆陽單一窗口', '中區管制中心']
    # 全部件數
    re_num_all = 0
    # 指定單位
    assigned = 0
    # 非指定
    cdc = 0
    # 指定中實際收件不等於送驗單位
    assigned_not_exam = 0

    efficiency_real_re_units_dict = {}
    efficiency_re_source_units_dict = {}

    # 檢驗結果通知時間-單一窗口檢體收件時間 <=0
    inform_receive_small_than_zero = 0

    for row in df_data.itertuples():
        if row.單一窗口檢體收件時間 != 'nan':
            if date_m2 <= datetime.datetime.strptime(row.單一窗口檢體收件時間, '%Y-%m-%d %H:%M:%S') <= date_m1:
                re_num_all += 1
                if row.實際收件單位 not in cdc_unit:
                    assigned += 1
                    if row.實際收件單位 != row.送驗單位:
                        assigned_not_exam += 1
                else:
                    cdc += 1

            if row.檢驗結果通知時間 != 'nan':
                re_time = datetime.datetime.strptime(df_data.loc[row.Index, '單一窗口檢體收件時間'], '%Y-%m-%d %H:%M:%S')
                info_time = datetime.datetime.strptime(df_data.loc[row.Index, '檢驗結果通知時間'], '%Y-%m-%d %H:%M:%S')
                dt = round((info_time - re_time).total_seconds() / 3600, 1)
                df_data.loc[row.Index, '檢驗結果通知時間_單一窗口檢體收件時間'] = dt
                if dt > 0:
                    df_data.loc[row.Index, '檢驗結果通知時間_單一窗口檢體收件時間'] = dt

                else:
                    df_data.loc[row.Index, '檢驗結果通知時間_單一窗口檢體收件時間'] = 'Error'
            else:
                df_data.loc[row.Index, '檢驗結果通知時間'] = 'None'

            if row.送驗單建檔時間 != 'nan':
                re_time = datetime.datetime.strptime(df_data.loc[row.Index, '單一窗口檢體收件時間'], '%Y-%m-%d %H:%M:%S')
                star_time = datetime.datetime.strptime(df_data.loc[row.Index, '送驗單建檔時間'], '%Y-%m-%d %H:%M:%S')
                dt = round((re_time - star_time).total_seconds() / 3600, 1)
                if dt >= 0:
                    df_data.loc[row.Index, '單一窗口檢體收件時間_送驗單建檔時間'] = dt

                else:
                    df_data.loc[row.Index, '單一窗口檢體收件時間_送驗單建檔時間'] = 'Error'
            else:
                df_data.loc[row.Index, '送驗單建檔時間'] = 'None'

        else:
            df_data.loc[row.Index, '單一窗口檢體收件時間'] = 'None'

        if row.單一窗口檢體收件時間 != 'nan':
            if date_m2 <= datetime.datetime.strptime(row.單一窗口檢體收件時間, '%Y-%m-%d %H:%M:%S') <= date_m1:
                if row.單一窗口檢體收件時間 != 'None' and row.檢驗結果通知時間 != 'None':
                    if row.實際收件單位 not in efficiency_real_re_units_dict:
                        efficiency_real_re_units_dict[row.實際收件單位] = [0, [], 0]
                    efficiency_real_re_units_dict[row.實際收件單位][0] += 1

                    if df_data.loc[row.Index, '檢驗結果通知時間_單一窗口檢體收件時間'] != 'Error' and df_data.loc[row.Index, '檢驗結果通知時間_單一窗口檢體收件時間'] != 'None':
                        efficiency_real_re_units_dict[row.實際收件單位][1].append(df_data.loc[row.Index, '檢驗結果通知時間_單一窗口檢體收件時間'])
                    # 檢驗結果通知時間-單一窗口檢體收件時間 <=0
                    if df_data.loc[row.Index, '檢驗結果通知時間_單一窗口檢體收件時間'] == 'Error':
                        efficiency_real_re_units_dict[row.實際收件單位][2] += 1

                if row.送驗單建檔時間 != 'None' and row.單一窗口檢體收件時間 != 'None':
                    if row.送驗單位 not in efficiency_re_source_units_dict:
                        efficiency_re_source_units_dict[row.送驗單位] = [0, []]
                    efficiency_re_source_units_dict[row.送驗單位][0] += 1

                    if df_data.loc[row.Index, '單一窗口檢體收件時間_送驗單建檔時間'] != 'Error' and df_data.loc[row.Index, '單一窗口檢體收件時間_送驗單建檔時間'] != 'None':
                        efficiency_re_source_units_dict[row.送驗單位][1].append(df_data.loc[row.Index, '單一窗口檢體收件時間_送驗單建檔時間'])

    if re_num_all != 0 and assigned != 0:
        return [df_data, dict(總件數=re_num_all, 指定單位收件數=assigned, 非指定單位收件數=cdc, 指定百分比='{0:.2f} %'.format((assigned / re_num_all * 100)),
                              非指定百分比='{0:.2f} %'.format((cdc / re_num_all * 100)), 送驗不等於實際件數=assigned_not_exam,
                              送驗不等於實際百分比='{0:.2f} %'.format((assigned_not_exam / assigned * 100))), efficiency_real_re_units_dict, efficiency_re_source_units_dict]
    elif re_num_all != 0 and assigned == 0:
        return [df_data, dict(總件數=re_num_all, 指定單位收件數=assigned, 非指定單位收件數=cdc, 指定百分比='{0:.2f} %'.format((assigned / re_num_all * 100)),
                              非指定百分比='{0:.2f} %'.format((cdc / re_num_all * 100)), 送驗不等於實際件數=assigned_not_exam,
                              送驗不等於實際百分比='0 %'), efficiency_real_re_units_dict, efficiency_re_source_units_dict]
    elif re_num_all == 0 and assigned != 0:
        return [df_data, dict(總件數=re_num_all, 指定單位收件數=assigned, 非指定單位收件數=cdc, 指定百分比='{0:.2f} %'.format((assigned / re_num_all * 100)),
                              非指定百分比='0 %', 送驗不等於實際件數=assigned_not_exam,
                              送驗不等於實際百分比='{0:.2f} %'.format((assigned_not_exam / assigned * 100))), efficiency_real_re_units_dict, efficiency_re_source_units_dict]
    else:
        return [df_data, dict(總件數=re_num_all, 指定單位收件數=assigned, 非指定單位收件數=cdc, 指定百分比='0 %',
                              非指定百分比='0 %', 送驗不等於實際件數=assigned_not_exam,
                              送驗不等於實際百分比='0 %'), efficiency_real_re_units_dict, efficiency_re_source_units_dict]


def seg(get):  # add
    c_c = min(mp.cpu_count(), len(get))
    m_c = math.ceil(len(get) / c_c)
    m_c_d = len(get) % c_c
    return [get[m_c * i - (i - m_c_d):m_c * (i + 1) - (i + 1 - m_c_d)] if i >= m_c_d and m_c_d else get[m_c * i:m_c * (i + 1)] for i in range(0, c_c)]


def merge_list(get):
    finish = []

    for in_receive_19cov in get:
        if not finish:
            finish = in_receive_19cov
        else:
            for num_in_receive_19cov in range(len(in_receive_19cov)):
                if type(in_receive_19cov[num_in_receive_19cov]) is dict:
                    for k, v in in_receive_19cov[num_in_receive_19cov].items():
                        if type(v) is int:
                            finish[num_in_receive_19cov][k] += v
                        elif type(v) is list:

                            if k not in finish[num_in_receive_19cov]:
                                finish[num_in_receive_19cov][k] = v
                                # print(finish[num_in_receive_19cov][k])
                            else:
                                finish[num_in_receive_19cov][k][0] += v[0]
                                finish[num_in_receive_19cov][k][1] += v[1]
                                if len(finish[num_in_receive_19cov][k]) == 3:
                                    finish[num_in_receive_19cov][k][2] += v[2]
                elif type(finish[num_in_receive_19cov]) is pd.core.frame.DataFrame:

                    finish[num_in_receive_19cov] = pd.concat([finish[num_in_receive_19cov], in_receive_19cov[num_in_receive_19cov]], axis=0)

    finish[1]['指定百分比'] = '{0:.2f} %'.format((finish[1]['指定單位收件數'] / finish[1]['總件數'] * 100))
    finish[1]['非指定百分比'] = '{0:.2f} %'.format((finish[1]['非指定單位收件數'] / finish[1]['總件數'] * 100))
    finish[1]['送驗不等於實際百分比'] = '{0:.2f} %'.format((finish[1]['送驗不等於實際件數'] / finish[1]['指定單位收件數'] * 100))

    finish_list = []

    for k, v in dict(sorted(finish[2].items())).items():
        finish_list.append(dict(實際收件單位=k, 平均時效=max(0, round(sum(v[1]) / v[0], 2)), 最長時效=max(v[1]) if v[1] else '無資料', 報告未發=v[2]))
    finish[2] = finish_list
    # print(finish_list)
    finish_list = []
    for k, v in dict(sorted(finish[3].items())).items():

        finish_list.append(dict(送驗單位=k, 平均時效=max(0, round(sum(v[1]) / v[0], 2)), 最長時效=max(v[1]) if v[1] else '無資料'))

    finish[3] = finish_list
    return finish


def analysis():
    data_dict = dict(raw_data_all=[[], ''], raw_data_range=[[], ''], data_19cov=[[], ''], data_sicv2=[[], ''], data_sicov=[[], ''],
                     case_receive='', re_unit_barcode='', send_unit_barcode='', time_efficiency_1='', time_efficiency_2='',
                     positive_all_barcode='', first_positive='')
    case_receive_list = []

    pool = mp.Pool()  # add

    df = pd.read_csv(os.path.join('.', 'imported_data', 'huang', 'raw_data_all.csv')).drop(columns=['送驗至結果時間']).drop_duplicates(subset="BARCODE編號", keep='first')

    sheet_7_8_df = df.copy()
    df.loc[:, '檢驗結果通知時間_單一窗口檢體收件時間'] = None
    df.loc[:, '單一窗口檢體收件時間_送驗單建檔時間'] = None

    # sheet_1
    sheet_1_header = list(df.columns)
    df_sheet_1 = df.copy().apply(lambda x: x.replace('nan', '')).fillna('').values

    data_dict['raw_data_all'] = [df_sheet_1, sheet_1_header]

    # sheet_2
    today = datetime.datetime.now()
    date_m1 = ''
    date_m2 = ''
    if today.hour in range(15, 20):
        date_m1 = today.replace(hour=13, minute=59, second=59)
        date_m2 = today.replace(hour=14, minute=0, second=0) - timedelta(days=1)
    if today.hour in range(0, 5):
        date_m1 = today.replace(hour=23, minute=59, second=59) - timedelta(days=1)
        date_m2 = today.replace(hour=0, minute=0, second=0) - timedelta(days=1)
    # date_m1 = today.replace(hour=13, minute=59, second=59) - timedelta(days=1)
    # date_m2 = today.replace(hour=13, minute=59, second=59) - timedelta(days=2)

    df['單一窗口檢體收件時間'] = pd.to_datetime(df['單一窗口檢體收件時間'])
    con = (df['單一窗口檢體收件時間'] >= date_m2) & (df['單一窗口檢體收件時間'] <= date_m1)

    df = df.loc[con].reset_index(drop=True)
    sheet_2_header = list(df.columns)
    df_sheet_2 = df.copy().astype(str).apply(lambda x: x.replace('nan', '')).fillna('').values

    data_dict['raw_data_range'] = [df_sheet_2, sheet_2_header]

    # 全部入口不含血清
    df = df.astype(str)
    df_all = df[df['檢體種類'] != '血清'].reset_index(drop=True)
    if not df_all.empty:
        seg_df_all = seg(df_all.copy())  # add
        receive_all = pool.imap(calculate, seg_df_all)  # add
        receive_all = merge_list(receive_all)

        # sheet_9 header: ['實際收件單位', '平均時效', '最長時效', '報告未發']
        sheet_9_header = ['實際收件單位', '平均時效', '最長時效', '報告未發']
        data_dict['time_efficiency_1'] = \
            [pd.DataFrame(receive_all[2], columns=['實際收件單位', '平均時效', '最長時效', '報告未發']).sort_values(['平均時效', '最長時效', '報告未發'], ascending=False).values, sheet_9_header]
        # sheet_10 header: ['送驗單位', '平均時效', '最長時效']
        sheet_10_header = ['送驗單位', '平均時效', '最長時效']
        data_dict['time_efficiency_2'] = \
            [pd.DataFrame(receive_all[3], columns=['送驗單位', '平均時效', '最長時效']).sort_values(['平均時效', '最長時效'], ascending=False).values, sheet_10_header]

    # 嚴肺入口 19cov
    df_19cov = df[(df['檢體種類'] != '血清') & (df['送驗疾病'] == '19CoV')].reset_index(drop=True)
    if not df_19cov.empty:
        seg_df_19cov = seg(df_19cov.copy())  # add
        receive_19cov = pool.imap(calculate, seg_df_19cov)  # add
        receive_19cov = merge_list(receive_19cov)  # add

        # sheet_3
        sheet_3_header = list(receive_19cov[0].columns)
        data_dict['data_19cov'] = [receive_19cov[0].apply(lambda x: x.replace('nan', '')).fillna('').values, sheet_3_header]

        # sheet_6 part 19cov
        case_receive_list.append(['嚴肺入口_19_cov'] + list(receive_19cov[1].values()))

    # 疑似新冠 sicv2
    df_sicv2 = df[(df['檢體種類'] != '血清') & (df['送驗疾病'] == 'SICV2')].reset_index(drop=True)

    if not df_sicv2.empty:
        seg_df_sicv2 = seg(df_sicv2.copy())  # add
        receive_sicv2 = pool.imap(calculate, seg_df_sicv2)  # add
        receive_sicv2 = merge_list(receive_sicv2)  # add

        # sheet_4
        sheet_4_header = list(receive_sicv2[0].columns)
        data_dict['data_sicv2'] = [receive_sicv2[0].apply(lambda x: x.replace('nan', '')).fillna('').values, sheet_4_header]

        # sheet_6 part sicv2
        case_receive_list.append(['疑似新冠_sicv2'] + list(receive_sicv2[1].values()))

    # 居家檢疫轉嚴肺 sicv2
    df_sicov = df[(df['檢體種類'] != '血清') & (df['送驗疾病'] == 'SICoV')].reset_index(drop=True)

    if not df_sicov.empty:
        seg_df_sicov = seg(df_sicov.copy())  # add
        receive_sicov = pool.imap(calculate, seg_df_sicov)  # add
        receive_sicov = merge_list(receive_sicov)  # add

        # sheet_5
        sheet_5_header = list(receive_sicov[0].columns)
        data_dict['data_sicov'] = [receive_sicov[0].apply(lambda x: x.replace('nan', '')).fillna('').values, sheet_5_header]

        # sheet_6 part sicv2
        case_receive_list.append(['居家檢疫轉嚴肺_sicov'] + list(receive_sicov[1].values()))

    # sheet_6 ['', '總件數', '指定單位收件數', '非指定單位收件數', '指定百分比', '非指定百分比', '送驗不等於實際件數', '送驗不等於實際百分比']
    # print(case_receive_list)
    if case_receive_list:
        sheet_6_header = [' ', '總件數', '指定單位收件數', '非指定單位收件數', '指定百分比', '非指定百分比', '送驗不等於實際件數', '送驗不等於實際百分比']
        case_receive_list = np.array(case_receive_list)
        data_dict['case_receive'] = [case_receive_list, sheet_6_header]

    # sheet 7_8 sheet_7 header: ['實際收件單位', '件數'] ; sheet_8 header: ['送驗單位', '件數']
    sheet_7_8_df['單一窗口檢體收件時間'] = pd.to_datetime(sheet_7_8_df['單一窗口檢體收件時間'])
    sheet_7_8_df = sheet_7_8_df.loc[con].reset_index(drop=True)

    sheet_7_8_df = sheet_7_8_df[sheet_7_8_df['檢體種類'] != '血清']
    sheet_7_df = sheet_7_8_df.groupby('實際收件單位').size().reset_index(name='件數').sort_values(by=['件數'], ascending=False).reset_index(drop=True)
    sheet_8_df = sheet_7_8_df.groupby('送驗單位').size().reset_index(name='件數').sort_values(by=['件數'], ascending=False).reset_index(drop=True)

    if not sheet_7_df.empty:
        sheet_7_header = ['實際收件單位', '件數']
        data_dict['re_unit_barcode'] = [sheet_7_df.dropna(0).values, sheet_7_header]

    if not sheet_8_df.empty:
        sheet_8_header = ['送驗單位', '件數']
        data_dict['send_unit_barcode'] = [sheet_8_df.dropna(0).values, sheet_8_header]

    # sheet_11 header:
    sheet_11_df = pd.read_csv(os.path.join('.', 'imported_data', 'huang', 'requirement_11.csv'))
    if not sheet_11_df.empty:
        sheet_11_header = list(sheet_11_df.drop(columns=['送驗至結果時間']).columns)
        data_dict['positive_all_barcode'] = [sheet_11_df.drop(columns=['送驗至結果時間']).astype(str).apply(lambda x: x.replace('nan', '')).fillna('').values, sheet_11_header]

    # sheet_12 header:
    sheet_12_df = pd.read_csv(os.path.join('.', 'imported_data', 'huang', 'requirement_12.csv'))
    if not sheet_12_df.empty:
        sheet_12_header = list(sheet_12_df.drop(columns=['送驗至結果時間']).columns)
        data_dict['first_positive'] = [sheet_12_df.drop(columns=['送驗至結果時間']).astype(str).apply(lambda x: x.replace('nan', '')).fillna('').values, sheet_12_header]

    wx = WriteXlsx()
    #     print(data_dict)
    f_n = wx.create_sheet(data_dict)

    return f_n


class WriteXlsx():
    def __init__(self):
        self.t_date = datetime.datetime.now()
        self.workbook = xlsxwriter.Workbook(os.path.join('.', 'exported_data', 'huang', f"cdc_labs_{self.t_date.strftime('%Y-%m-%d %H')}點版.xlsx"))

        #         self.workbook = xlsxwriter.Workbook(os.path.join('/media/sf_Eic03-2/研檢資料', 'cdc_labs.xlsx'))

        # old sheets
        # self.worksheet_dict = {'raw_data_all': 'worksheet_1', 'raw_data_range': 'worksheet_2', 'data_19cov': 'worksheet_3', 'data_sicv2': 'worksheet_4', 'data_sicov': 'worksheet_5',
        #                                'case_receive': 'worksheet_6', 're_unit_barcode': 'worksheet_7', 'send_unit_barcode': 'worksheet_8', 'time_efficiency_1': 'worksheet_9',
        #                                'time_efficiency_2': 'worksheet_10', 'positive_all_barcode': 'worksheet_11', 'first_positive': 'worksheet_12'}

        self.worksheet_dict = {'raw_data_all': 'worksheet_1', 'case_receive': 'worksheet_2',
                               're_unit_barcode': 'worksheet_3', 'send_unit_barcode': 'worksheet_4',
                               'positive_all_barcode': 'worksheet_5', 'first_positive': 'worksheet_6',
                               'time_efficiency_1': 'worksheet_7', 'time_efficiency_2': 'worksheet_8'}

        for k, v in self.worksheet_dict.items():
            self.worksheet_dict[k] = self.workbook.add_worksheet(k)

        self.table_format = self.workbook.add_format({'align': 'center',
                                                      'valign': 'vcenter',
                                                      'size': 12, 'color': 'black'})

        self.title_format = self.workbook.add_format({'align': 'center',
                                                      'valign': 'vcenter',
                                                      'size': 12, 'bold': 4,
                                                      'color': 'white',
                                                      })

    def param_option(self, columns_list):
        param_list = []
        for column in columns_list:
            param = {'header': column, 'format': self.table_format}
            param_list.append(param)
        return param_list

    def create_sheet(self, data_dict):
        # start = time.time()
        write_sh_list = ['raw_data_all', 'case_receive', 're_unit_barcode', 'send_unit_barcode', 'positive_all_barcode', 'first_positive', 'time_efficiency_1', 'time_efficiency_2']

        for sh, data in data_dict.items():
            if sh in write_sh_list:

                add_content = {'data': data[0], 'autofilter': False, 'columns': WriteXlsx.param_option(self, data[1])}

                if type(data[0]) is not list and type(data[0]) is not str:
                    r_len = len(data[0])
                    c_len = len(data[1])

                    if sh != 'case_receive':
                        self.worksheet_dict[sh].set_column(0, c_len, 40)
                    if sh == 'case_receive':
                        self.worksheet_dict[sh].set_column(0, c_len, 25)

                    self.worksheet_dict[sh].add_table(0, 0, r_len + 1, c_len - 1, add_content)

                    for i in range(r_len + 1):
                        if i == 0:
                            self.worksheet_dict[sh].set_row(i, 30, self.title_format)
                        else:
                            self.worksheet_dict[sh].set_row(i, 20)
            # print(time.time()-start)
        self.workbook.close()

        return f"cdc_labs_{self.t_date.strftime('%Y-%m-%d %H')}點版.xlsx"


def upload(zip_name, hour_):
    gauth = GoogleAuth()
    # Try to load saved client credentials
    gauth.LoadCredentialsFile(os.path.abspath(os.path.join('.', 'mycreds.txt')))
    if gauth.credentials is None:
        # Authenticate if they're not there
        gauth.LocalWebserverAuth()
    elif gauth.access_token_expired:
        # Refresh them if expired
        gauth.Refresh()
    else:
        # Initialize the saved creds
        gauth.Authorize()

    # Save the current credentials to a file
    gauth.SaveCredentialsFile(os.path.abspath(os.path.join('.', 'mycreds.txt')))

    drive = GoogleDrive(gauth)

    path_list = glob.glob((os.path.abspath(os.path.join('.', 'exported_data', 'huang', zip_name))))

    folder_list = drive.ListFile({'q': "'1C9VqxWCwoVoTj0STGxc1d9iz6L94RFb3' in parents and trashed=False"}).GetList()

    delete_date = datetime.datetime.strptime(zip_name.split(' ')[0].split('cdc_labs_')[1], '%Y-%m-%d').date() - timedelta(days=1)
    delete_file_name = f"cdc_labs_{delete_date} {hour_}點版.zip"

    base_name_list = []

    for path in path_list:
        base_name_list.append(os.path.basename(path))

    file_info_list = []
    for gdf in folder_list:
        # get the info of the file with same name for further replace
        if gdf['title'] in base_name_list:
            file_info_list.append(dict(
                title=gdf['title'], id=gdf['id']
            ))
        # delete file in google drive
        if gdf['title'] == delete_file_name:
            gdf.Delete()

    if file_info_list:
        for file_info in file_info_list:
            for path in path_list:
                if file_info['title'] == os.path.basename(path):
                    file = drive.CreateFile({'parents': [{'id': '1C9VqxWCwoVoTj0STGxc1d9iz6L94RFb3'}], 'title': file_info['title'], 'id': file_info['id']})
                    file.SetContentFile(path)
                    # url = f"https://drive.google.com/thumbnail?id={img_info['id']}&sz=w1920-h1080"
                    file.Upload()
                    # print(file['title'], url)
    #                     print(file['title'] + '---Renew and Upload success')
    else:
        for path in path_list:
            file = drive.CreateFile({'parents': [{'id': '1C9VqxWCwoVoTj0STGxc1d9iz6L94RFb3'}], 'title': os.path.basename(path)})
            file.SetContentFile(path)
            file.Upload()

            # print(file['title'] + '---Upload success')


def main():
    sql_code = dict(raw_data_all="Select * from cdcdw.V_EXAMINATION_REPORT_EIC",
                    # requirement_6="select t2.LAB_UNIT_NAME as 實際收件單位,count(t2.LAB_UNIT_NAME) as 件數 from cdcdw.dws_sample_detail t1 "
                    #               "left join cdcdw.DIM_LAB_UNIT t2 on t1.CDC_RECEIVED_UNIT = t2.LAB_UNIT where t1.SAMPLE_CDC_RECEIVED_DATETIME >= trunc(sysdate-2)+14/24 "
                    #               "and t1.SAMPLE_CDC_RECEIVED_DATETIME <= trunc(sysdate-1)+839/1440 group by t2.LAB_UNIT_NAME order by count(t2.LAB_UNIT_NAME) desc",
                    requirement_11="select tt2.* from (select distinct t2.IDNO from CDCDW.DWS_SAMPLE_DETAIL t1 left join (select * from CDCDW.USV_INDIVIDUAL_SAS where SICK_DATE >= TO_DATE('2020-01-01','YYYY-MM-DD')) t2 "
                                   "on t1.INDIVIDUAL = t2.INDIVIDUAL where t1.DISEASE in ('19CoV','SICV2','SICoV') and t1.RESULT = 5)tt1 left join (select t2.IDNO,t1.* from cdcdw.V_EXAMINATION_REPORT_EIC t1 left join ("
                                   "select t1.DISEASE,t1.SAMPLE,t1.SAMPLE_DATE,t2.IDNO from CDCDW.DWS_SAMPLE_DETAIL t1 left join (select * from CDCDW.USV_INDIVIDUAL_SAS "
                                   "where SICK_DATE >= TO_DATE('2020-01-01','YYYY-MM-DD')) t2 on t1.INDIVIDUAL = t2.INDIVIDUAL where t1.DISEASE in ('19CoV','SICV2','SICoV')) t2  "
                                   "on t1.BARCODE編號 = t2.SAMPLE and t1.送驗疾病 = t2.DISEASE)tt2 on tt1.IDNO = tt2.IDNO",
                    # requirement_10="select t2.HOSPITAL_NAME as 送驗單位,count(t2.HOSPITAL_NAME) as 件數 from cdcdw.dws_sample_detail t1 left join CDCDW.DIM_HOSPITAL t2 on t1.SAMPLE_HOSPITAL = t2.HOSPITAL "
                    #                "where t1.SAMPLE_CDC_RECEIVED_DATETIME >= trunc(sysdate-2)+14/24 and t1.SAMPLE_CDC_RECEIVED_DATETIME <= trunc(sysdate-1)+839/1440 "
                    #                "group by t2.HOSPITAL_NAME order by count(t2.HOSPITAL_NAME) desc",
                    requirement_12="Select t2.IDNO,t2.RANKING,t1.* from cdcdw.V_EXAMINATION_REPORT_EIC t1 left join (select t1.DISEASE,t1.SAMPLE,t1.SAMPLE_DATE,t2.IDNO,RANK()OVER(PARTITION BY t2.IDNO "
                                   "ORDER BY t1.SAMPLE_DATE,t1.SAMPLE ,t1.DISEASE desc ) RANKING from CDCDW.DWS_SAMPLE_DETAIL t1 left join (select * from CDCDW.USV_INDIVIDUAL_SAS "
                                   "where SICK_DATE >= TO_DATE('2020-01-01','YYYY-MM-DD')) t2 on t1.INDIVIDUAL = t2.INDIVIDUAL where t1.DISEASE in ('19CoV','SICV2','SICoV')) t2 "
                                   "on t1.BARCODE編號 = t2.SAMPLE and t1.送驗疾病 = t2.DISEASE where t1.綜合檢驗結果 = '陽性' order by t2.RANKING asc")

    t_date = datetime.datetime.now()

    # 刪除資料
    for file_extension in ['xlsx', 'zip']:
        delete_file_list = glob.glob((os.path.abspath(os.path.join('.', 'exported_data', 'huang', f'*.{file_extension}'))))
        for f in delete_file_list:
            file_name = os.path.basename(f)
            if re.search('\d{4}-\d{2}-\d{2}', file_name):
                f_date = datetime.datetime.strptime(file_name.split(' ')[0].split('cdc_labs_')[1], '%Y-%m-%d').date()
                if t_date.day - f_date.day > 10:
                    os.remove(f)
    print('File check to delete is completed.')
    print('-' * 100)

    # 下載db資料
    # for k, v in sql_code.items():
    #     download_raw_data(k, v)
    # print('Data download is completed')
    # print('-' * 100)

    # 分析
    xlsx_name = analysis()
    print('Analysis is successful.')
    print('-' * 100)

    file_path = os.path.join('.', 'exported_data', 'huang', xlsx_name)
    # xlsx_name = 'cdc_labs_2021-05-17 11點版.xlsx'
    # file_path = os.path.join('.', 'exported_data', 'huang', 'cdc_labs_2021-05-17 00點版.xlsx')

    e_mail_list = ['syl@cdc.gov.tw', 'mtliu@cdc.gov.tw', 'ggyang@cdc.gov.tw', 'hjteng@cdc.gov.tw', 'jouhan@cdc.gov.tw',
                   'fang@cdc.gov.tw', 'littleka@cdc.gov.tw', 'neo811016@cdc.gov.tw', 'yudihsu@cdc.gov.tw']

    test_mail = ['yudihsu@cdc.gov.tw']

    w_file_path = os.path.abspath(file_path)

    zip_name = os.path.abspath(os.path.join('.', 'exported_data', 'huang', f"{xlsx_name.split('.xlsx')[0]}.zip"))

    Popen(['zip', '-j', zip_name, w_file_path, '-P', '27850513@858'])
    print('Zip file is completed.')
    print('-' * 100)

    while True:
        if os.path.isfile(zip_name):
            break
        time.sleep(0.1)

    upload(f"{xlsx_name.split('.xlsx')[0]}.zip", t_date.hour)
    print('Upload to Google Drive successfully.')
    print('-' * 100)

    mail = SMTP(receiver=test_mail,
                attachment=[],
                subject=f'{os.path.splitext(os.path.basename(zip_name))[0]}',
                content=f'{os.path.splitext(os.path.basename(zip_name))[0]}.zip upload success.\nPlease access this url: https://drive.google.com/drive/folders/1C9VqxWCwoVoTj0STGxc1d9iz6L94RFb3?usp=sharing',
                sender='e-exam_analysis@service.cdc.gov.tw')

    # mail.send()
    print('Successfully send mail.')
    print('-' * 100)


if __name__ == '__main__':
    main()
