#!/usr/bin/env python3
# 0.1e
import argparse
import requests
import json
import sys
import os
import sys
import os.path as path
import time
import re

from copy import copy
from typing import Union
from collections import defaultdict, namedtuple

class ERP:

    main_page = ''
    # main_page = 'http://192.168.1.43:2223/'  # test server
    payload_server = ''  # 数据系统，基础分析 http://113.98.96.228:9005/

    # {'LoginUserInfo':'ced38a37-922d-4556-a7a4-f5ea00dce77b',
    # 'wftoken':'kGXSz5ZP3W/CdAp6a1P8AeVYWJYFKccls5soT4kxTmicegDjoDQDMzC6AN1e9WRlgWVykS06sBa15gCkdPuv4tJc'}
    token_dict = {}

    #
    # {storage_file_id/fileId:
    #    {'file_id': '6d4be7803ae5433ea3d50062855ec557',
    #     'storage_id': '61d7ddbc2c89630dd5edd1fa',
    #     'file_name': 'M2000-352-102162-1002-LSL-EXON-41_L01_2.fq.gz',
    #     'file_md5': 'e7a5875ee83afbc7d0c9f4f34c0db10e',
    #     'file_md5_read': 'e7a5875ee83afbc7d0c9f4f34c0db10e',
    #     'file_verify_md5': 'e7a5875ee83afbc7d0c9f4f34c0db10e',
    #     'file_md5_pass': int,
    #     'file_time': '2021-12-26 00:16:50',
    #     'do_id': 'c66e075249ed4f6c89cbfd714629b07f',
    #     'do_code': 数据单号
    #     'r_code': 申请单号
    #     'file_path': '/mfs/osstore/LAB/HUADA/M2000-361/....',
    #     'file_url':'http://172.20.0.32:9009/opengk/data/1651077749179/117613ZS-9710-CNVseqPLUS-CNV_all_negative_CNV_plot.png'
    #     'file_url_WAN':'http://113.98.96.228:9005/mio/opengk/data/1651077749179/117613ZS-9710-CNVseqPLUS-CNV_all_negative_CNV_plot.png'
    #     'file_size': 12345678,
    #     'file_update_time': str,
    #     'file_name_update_flag': 0,
    #     'file_original_name': None,
    #     'file_url':str,
    #     'file_type':str,
    #     'BarCode':str,  # 检测号
    #     'BatchNo':str, # 批次号
    #     'Direction':int (1/2), 1 单端，2 双端
    #     'GrpEncode':str,
    #     'GrpItem':str,
    #     'PatName':str
    #
    #     ......},
    #  ......}
    #  默认值为None， 如果键存在值，则应为defaultdict
    files_dict = defaultdict(lambda: defaultdict(lambda: None))    # dict[dict]

    # {'106036ZS-9610': {
    #  'rid': 'a0aefa9c-3fa7-42b6-bc8f-952e7b404f45',
    #  'r_code':  '1000145153'  # 申请单号
    #  'bar_sn': '10175038' # 样本ID
    #  'BatchNo': 'ZS2000-004',
    #  'BarCode': '106036ZS',
    #  'p_family_no': '48544',
    #  'PatName': 'WS21351张田然',
    #  'sex_recorded': '女',
    #  'family_relation': '先证者'，
    #  'GrpItem': '高精度临床外显-单项',
    #  'GrpEncode':'9610',
    #  'lab_name'：'广州核心实验室',
    #  'org_name': '济南市妇幼保健院'
    #  'IsHasFamilyGroup':  0 / 1
    #  'CombinedFlag': 0 / 1 # 是否是组合单
    #  'DataType': 'MES',
    #  'sp_name': '全血',
    #  'CreateUserName': '张子钰',
    #  'QcUserName': '刘晓莹',
    #  'req_born': '产前',
    #  'urgent': '加急'
    #  'QcResult': '合格'
    #  'is_need_inner_qc': True # 是否需要内质检
    #  'innerQCFlag': True  # 内质检是否合格 （可以为None， 表示还未进行质检）
    #  'Data_time': '2022-01-04 15:12:51',  # 实验室推送时间 （_api_SearchByPage）
    #  'Request_time': '2022-01-03 14:53' # 申请单生成时间 （_api_SearchRequestNoteList）
    #  'TaskCreateTime': '2022-01-03 14:53' # 推送openGK时间 （_api_SearchPushListByPage）
    #  'ETL_time': '2022-01-04 12:42:08',  # ETL推送时间(opengk运行完毕，推送到质控模块的时间， _api_SearchByPage)
    #  'Push_time': '2022-01-04 15:12:51',   # 推送时间（推送到医学部的时间， _api_SearchByPage）
    #  'data_id':'1641199365781',  # 用来提取混样数据
    #  'is_return': True # 是否为退样，
    #  'return_reason': '变更项目',
    #  'return_user_name': '李兰飞'}

    #   .......}
    rid_dict = defaultdict(lambda: defaultdict(lambda: None))  # dict[defaultdict(lambda :'-')]

    #
    #   {
    #   bed:str
    #   raw_base:float
    #   q30:float
    #   on_target_rate: float
    #   depthAvg: float
    #   depthSD: float
    #   coverage:float
    #   insert_size_average:float
    #   error_rate: float
    #   CNV_total:int
    #   CNVSeq_totalSample: int
    #   CNVSeq_totalAbnormal: int
    #   CNVSeqPlus_100K:int
    #   CNVSeqPlus_Trip:str
    #   sex_calculate:str
    #   sex_record:str
    #   P_count:int
    #   M_count:int
    #   PvsM:float
    #   deNovo:int}}
    qc_dict = defaultdict(lambda: None)  #

    def __init__(self, username: str, password: str, addr = 'Login/Login', main_server = 'http://www.dna.gz.cn/', payload_server = 'http://113.98.96.228:9005/'):
        '''
        initinalize in the ERP system

        Parameters:
            **username**: str
                ERP user name

            **password**: str
                ERP password

        '''
        self.main_page = main_server
        self.payload_server = payload_server

        api_addr = self.main_page + addr
        payload = {'user_loginid': username,
                   'user_password': password}

        r = requests.post(api_addr, data = payload)
        if r.status_code == 200:
            token_dict = {'LoginUserInfo': r.cookies.get('LoginUserInfo'), 'wftoken': r.cookies.get('wftoken'), }
            self.token_dict = token_dict
            return None
        else:
            message = 'Attempt to login ERP at {api_addr} with {uname}/{pwd}, get return code {code}\n{text} '.format(addr = api_addr,
                                                                                                                uname = username,
                                                                                                                pwd = password,
                                                                                                                code = r.status_code,
                                                                                                                text = r.text)
            raise ConnectionRefusedError(message)

    def _api_AddDataOrder(self, files: list[dict], batch_no = '', bar_code = [], grp_encoded = [], p_name = [], server_folder = '', do_sequencing_platform = 'illumina', do_sequencing_platform_type = 'Hiseq500', do_transfer_method = '快递', do_transfer_mode = '硬盘', addr = 'etlApi/dataorder/AddDataOrder') -> tuple:
        '''
        数据申请单 -> 全部 -> 新增

        Parameters:
            **files**: list
                the files you want to add to an order
                [ {'file_md5':'xxxxxxxxxxxx', 'file_name':'afaeuifgaefa.fq.gz'}, .......]

            **batch_no**: str
                批次号

            **bar_code**: list
                检测号

            **grp_encoded**: list
                项目号

            **p_name**: list
                患者姓名

            **do_sequencing_platform**: str
                测序平台('illumina'， '华大'， '其他')

            **do_sequencing_platform_type**: str
                测序平台具体型号

            **do_transfer_method**: str
                传输方法('快递'，'5G'，'专线'，'网盘')

            **do_transfer_mode**: str
                传输模式('硬盘'，'云盘')

        Returns:
            **do_id**: str
                do_id, 内部使用

            **do_code**: int
                数据单号

            **files_dict**: file_dict type
                Standard files_dict type
        '''
        files_dict = copy(self.files_dict)

        api_addr = self.main_page + addr
        payload = {'do_sequencing_platform': do_sequencing_platform,
                   'do_sequencing_platform_type': do_sequencing_platform_type,
                   'do_transfer_method': do_transfer_method,
                   'do_transfer_mode': do_transfer_mode,
                   'files': files,
                   # 'do_code': '',
                   # 'do_remarks': '',
                   # 'transfer': None,
                   # 'do_id': None,
                   }
        r = requests.post(api_addr, json = payload, cookies = self.token_dict)
        if r.status_code != 200 or not json.loads(r.text)['success']:
            message = 'Fail to AddDataOrder at {addr}, get return code {code}\n{text} '.format(addr = api_addr,
                                                                                                         code = r.status_code,
                                                                                                         text = json.loads(r.text))
            raise ConnectionRefusedError(message)

        if len(json.loads(r.text)['data']['files']) == 0:
            message = 'Data order added successfully, but no files returned'
            raise ValueError(message)

        data_dict = json.loads(r.text)['data']
        do_id = data_dict['do_id']
        do_code = data_dict['do_code']

        # set bar_code etc.
        file_id_dict = self._api_GetDataOrderFiles(do_id)  # file_id_dict is dict[dict]
        i = 0
        for file_id, d in file_id_dict.items():
            '''
            file_id is '6a6d36f4f977462fafea77ce5fa8015d'
            d is {'do_id': '732d21e441f240d198802b61d5809ce4',
                'file_id': '6a6d36f4f977462fafea77ce5fa8015d',
                'file_md5': '12345',
                'file_md5_pass': 0,
                'file_md5_read': None,
                'file_name': 'merged_R1.fq.gz',
                'file_path': None,
                'file_size': None}
            '''
            files_dict[file_id] = copy(d)
            files_dict[file_id]['do_code'] = do_code

            try:
                auto_get_tu = self._get_server_path(d['file_name'])
            except Exception:
                auto_get_tu = ()

            # set batch_no
            if batch_no != '':
                files_dict[file_id]['BatchNo'] = batch_no
            elif auto_get_tu != ():
                files_dict[file_id]['BatchNo'] = auto_get_tu[0]
            else:
                message = 'Can not set BatchNo for {}'.format(d)
                raise ValueError(message)

            # set bar_code
            if bar_code != []:
                files_dict[file_id]['BarCode'] = bar_code[i]
            elif auto_get_tu != ():
                files_dict[file_id]['BarCode'] = auto_get_tu[1]
            else:
                message = 'Can not set BarCode for {}'.format(d)
                raise ValueError(message)

            # set GrpEncode
            if grp_encoded != []:
                files_dict[file_id]['GrpEncode'] = grp_encoded[i]
            elif auto_get_tu != ():
                files_dict[file_id]['GrpEncode'] = auto_get_tu[2]
            else:
                message = 'Can not set GrpEncode for {}'.format(d)
                raise ValueError(message)

            # set PatName
            if p_name != []:
                files_dict[file_id]['PatName'] = p_name[i]
            elif auto_get_tu != ():
                files_dict[file_id]['PatName'] = auto_get_tu[3]
            else:
                message = 'Can not set Patient Name for {}'.format(d)
                raise ValueError(message)

            # set Direction
            if d['file_name'].endswith('_R1.fq.gz') or d['file_name'].endswith('_1.fq.gz'):
                files_dict[file_id]['Direction'] = 1
            elif d['file_name'].endswith('_R2.fq.gz') or d['file_name'].endswith('_2.fq.gz'):
                files_dict[file_id]['Direction'] = 2
            else:
                message = 'Can not set Direction for {}'.format(d)
                raise ValueError(message)

            # set file_path
            if server_folder != '':
                files_dict[file_id]['file_path'] = server_folder + '/' + d['file_name']
            elif auto_get_tu != ():
                files_dict[file_id]['file_path'] = auto_get_tu[5]
            else:
                message = 'Can not set file path for {}'.format(d)
                raise ValueError(message)

            i += 1


        self.files_dict = files_dict
        return do_id, do_code, files_dict

    def _api_SubmitDataOrder(self, do_id: Union[str, list[str]], addr = 'etlApi/dataorder/SubmitDataOrder') -> bool:
        '''
        数据申请单 -> 待提交 -> 提交

        Parameters:
            **do_id**: str or a string list
                do_id

        Returns:
            **is_sucessful**: boolean
                是否提交成功?
        '''

        api_addr = self.main_page + addr
        payload = []

        if isinstance(do_id, str):
            do_id = [do_id]
        elif isinstance(do_id, list):
            for s in do_id:
                if not isinstance(s, str):
                    message = ''
                    raise TypeError(message)
        else:
            message = ''
            raise TypeError(message)

        for s in do_id:
            temp = {'do_id': s, 'remarks': '脚本自动提交', }
            payload.append(temp)

        r = requests.post(api_addr, json = payload, cookies = self.token_dict)
        if r.status_code != 200 or not json.loads(r.text)['success']:
            message = 'Fail to SubmitDataOrder at {addr}, get return code {code}\n\n{text} '.format(addr = api_addr,
                                                                                                         code = r.status_code,
                                                                                                         text = json.loads(r.text))
            raise ConnectionRefusedError(message)

        return True

    def _api_ReceiveData(self, do_id: Union[str, list[str]], addr = 'etlApi/dataorder/ReceiveData') -> bool:
        '''
        数据申请单 -> 待接收 -> 接收

        Parameters:
            **do_id**: str or a string list
                do_id

        Returns:
            **is_sucessful**: boolean
                是否接收成功?
        '''

        api_addr = self.main_page + addr
        payload = []

        if isinstance(do_id, str):
            do_id = [do_id]
        elif isinstance(do_id, list):
            for s in do_id:
                if not isinstance(s, str):
                    message = ''
                    raise TypeError(message)
        else:
            message = ''
            raise TypeError(message)

        for s in do_id:
            temp = {'do_id': s, 'remarks': '', }
            payload.append(temp)

        r = requests.post(api_addr, json = payload, cookies = self.token_dict)
        if r.status_code == 200 and json.loads(r.text)['success']:
            return True
        else:
            message = 'Fail to ReceiveData at {addr} with , get return code {code}\n{text} '.format(addr = api_addr,
                                                                                                        code = r.status_code,
                                                                                                        text = json.loads(r.text))
            raise ConnectionRefusedError(message)

    def _api_FileList(self, path = '/', addr = 'http://192.168.1.243:18080/ini/filelist') -> list:
        '''
        数据处理申请单 -> 待签收 -> 点击一个申请单的MD5按钮 -> 选择文件
        用于取得给定批次文件在服务器的位置，进而使用_api_GetMD5()函数计算MD5值

        Parameters:
            **path**: 服务器路径
                例如：'/mfs/osstore/LAB/HUADA/M2000-406/'

            **addr**: 默认路径为核心实验室储存路径，其他可能有变

        Returns:
            **file_list**: list
            [ {'updateTime': '2022-02-23 05:30:39', (文件创建时间)
                'IsFile': True,
                'fileSize': 2297098709,
                'fileName': 'M2000-406-87947-4100-PTC-EXON-72_L04_2.fq.gz',
                'file_path': '/mfs/osstore/LAB/HUADA/M2000-406/M2000-406-87947-4100-PTC-EXON-72_L04_2.fq.gz'},
               {'updateTime': '2022-02-23 05:30:39',
                'IsFile': True,
                'fileSize': 2256287645,
                'fileName': 'M2000-406-87947-4100-PTC-EXON-72_L04_1.fq.gz',
                'file_path': '/mfs/osstore/LAB/HUADA/M2000-406/M2000-406-87947-4100-PTC-EXON-72_L04_1.fq.gz'},
               {'updateTime': '2022-02-23 05:02:02',
                'IsFile': True,
                'fileSize': 2275837130,
                'fileName': 'M2000-406-87947-4100-PTC-EXON-72_L03_2.fq.gz',
                'file_path': '/mfs/osstore/LAB/HUADA/M2000-406/M2000-406-87947-4100-PTC-EXON-72_L03_2.fq.gz'},...]
        '''

        api_addr = addr
        payload = {'fs': 'local',
                   'path': path, }

        r = requests.get(api_addr, params = payload, cookies = self.token_dict)
        if r.status_code != 200 or not json.loads(r.text)['success']:
            message = 'Fail to FileList at {addr} with {payload}, get return code {code}\n{text} '.format(addr = api_addr,
                                                                                                          payload = payload,
                                                                                                          code = r.status_code,
                                                                                                          text = json.loads(r.text))
            raise ConnectionRefusedError(message)

        if len(json.loads(r.text)['data']) == 0:
            message = '{} Connection successfully, but no file list return in {}.\n{}'.format(api_addr,
                                                                                              payload['path'],
                                                                                              json.loads(r.text))
            raise FileNotFoundError(message)

        return json.loads(r.text)['data']

    def _api_SigninData(self, do_id: Union[str, list[str]], addr = 'etlApi/dataorder/SigninData') -> bool:
        '''
        数据申请单 -> 待签收 -> 签收

        Parameters:
            **do_id**: str or a string list
                do_id

        Returns:
            **is_sucessful**: boolean
                是否签收成功?
        '''

        api_addr = self.main_page + addr
        payload = []

        if isinstance(do_id, str):
            do_id = [do_id]
        elif isinstance(do_id, list):
            for s in do_id:
                if not isinstance(s, str):
                    message = ''
                    raise TypeError(message)
        else:
            message = ''
            raise TypeError(message)

        for s in do_id:
            temp = {'do_id': s, 'remarks': '', }
            payload.append(temp)

        r = requests.post(api_addr, json = payload, cookies = self.token_dict)
        if r.status_code != 200 or not json.loads(r.text)['success']:
            message = 'Fail to SigninData at {addr}, get return code {code}\n\n{text} '.format(addr = api_addr,
                                                                                                        code = r.status_code,
                                                                                                        text = json.loads(r.text))
            raise ConnectionRefusedError(message)

        return True

    def _api_GetMD5(self, file_names: list[str], addr = 'etlApi/dataorder/GetMD5') -> list[dict]:
        '''
        Send a signal to make server to calculate MD5 for files on server.
        file_names must be the path in server
        set the 'file_verify_md5' flag of files_dict

        Parameters:
            **file_names**: list[str]
                 ['/mfs/osstore/LAB/HUADA/M2000-406/M2000-406-87947-4100-PTC-EXON-72_L04_1.fq.gz',
                 '/mfs/osstore/LAB/HUADA/M2000-406/M2000-406-87947-4100-PTC-EXON-72_L04_2.fq.gz',]

        Returns:
            **data_list**: list
            [{'Path':'XXXX', 'FileMD5':'XXXXXX', 'VerifiedMD5':'XXXXXX'/None},
            .......
            ]
        '''
        api_addr = self.main_page + addr
        payload = {'fs': 'local', }

        if not isinstance(file_names, list):
            message = ''
            raise TypeError(message)

        for s in file_names:
            if not isinstance(s, str):
                message = ''
                raise TypeError(message)

        # ========================
        paths = []
        for s in file_names:
            paths.append(s)
        payload['paths'] = paths

        r = requests.post(api_addr, json = payload, cookies = self.token_dict)
        if r.status_code != 200 or not json.loads(r.text)['success']:
            message = 'Fail to GetMD5 at {addr} with , get return code {code}\n{text} '.format(addr = api_addr,
                                                                                                        code = r.status_code,
                                                                                                        text = json.loads(r.text))
            raise ConnectionRefusedError(message)

        data_list = json.loads(r.text)['data']  # data_list is a list[dict]

        return data_list

    def _api_GetDataOrderFiles(self, do_id: str, addr = 'etlApi/dataorder/GetDataOrderFiles') -> dict[dict]:
        '''
        数据申请单 -> 待签收 -> 申请单的MD5按钮
        主要用来获取指定申请单的所有file的file_id

        Parameters:
            **do_id**: str
                do_id

        Returns:
            **files_dict**: dict
                'file_id': {'do_id':'xxxxxxxxxxx',
                  'file_id':'XXXXXXXXXX',
                  'file_md5':'xxxxxxxxx',
                  'file_md5_pass':int
                  'file_md5_read':'xxxxxxxxx',
                  'file_name':'afaeuifgaefa.fq.gz',
                  'file_path':'/mfs/osstore/.......'
                  'file_verified_md5':'XXXXXXXX',
                  'file_size':NNNNNN},
                .......
        '''
        if not isinstance(do_id, str):
            message = ''
            raise TypeError(message)

        api_addr = self.main_page + addr
        payload = {'do_id': do_id, }

        r = requests.get(api_addr, params = payload, cookies = self.token_dict)
        if r.status_code != 200 or not json.loads(r.text)['success']:
            message = 'Fail to GetDataOrderFiles at {addr} with , get return code {code}\n\n{text} '.format(addr = api_addr,
                                                                                                            code = r.status_code,
                                                                                                            text = json.loads(r.text))
            raise ConnectionRefusedError(message)

        data_list = json.loads(r.text)['data']
        files_dict = copy(self.files_dict)
        for d in data_list:
            # {'do_id':'xxxxxxxxxxx',
            #  'file_id':'XXXXXXXXXX',
            #  'file_md5':'xxxxxxxxx',
            #  'file_md5_pass':int
            #  'file_md5_read':'xxxxxxxxx',
            #  'file_name':'afaeuifgaefa.fq.gz',
            #  'file_path':'/mfs/osstore/.......'
            #  'file_verified_md5':'XXXXXXXX',
            #  'file_size':NNNNNN}
            file_id = d['file_id']

            files_dict[file_id]['do_id'] = d['do_id']
            files_dict[file_id]['file_id'] = d['file_id']
            files_dict[file_id]['file_md5'] = d['file_md5']
            files_dict[file_id]['file_md5_pass'] = d['file_md5_pass']
            files_dict[file_id]['file_md5_read'] = d['file_md5_read']
            files_dict[file_id]['file_name'] = d['file_name']
            files_dict[file_id]['file_path'] = d['file_path']
            files_dict[file_id]['file_size'] = d['file_size']

        self.files_dict = files_dict

        return files_dict

    def _api_verifyMD5(self, do_id: str, verbose = True, addr = 'etlApi/dataorder/VerifyMd5') -> bool:
        '''
        文件列表 -> 选择文件 -> 文件校验 -> 提交
        Parameters:
            **do_id**: str
                do_id

        Returns:
            **bool**: boolean
                是否成功
        '''
        if not isinstance(do_id, str):
            message = ''
            raise TypeError(message)

        # ====
        files_pass_lst = []
        files_fail_lst = []

        i = 0
        for file_id, d in self.files_dict.items():
            #{'do_id': 'dded0439f4e94414b53622b3e4d31469',
            #'file_id': '3ae30174674b4fb5bd22c521df13fd4d',
            #'file_md5': '2e4827380c5138b30788484068a93230',
            #'file_md5_pass': 0,
            #'file_md5_read': None,
            #'file_name': 'merged_R2.fq.gz',
            #'file_path': '/mfs/osstore/LAB/RESEARCH/NCBI-002/merged_R2.fq.gz',
            #'file_size': None,
            #'do_code': '1000003352',
            #'BatchNo': 'KY-002',
            #'BarCode': 'HG001A',
            #'GrpEncode': '4100',
            #'PatName': 'HG001',
            #'Direction': 2}
            i += 1
            file_name = d['file_name']
            remote_name = d['file_path']
            message = '{}/{} {} 校验中 ... '.format(i, len(self.files_dict), file_name)
            print(message, end = '')

            md5_str = self._api_GetMD5([remote_name])[0]['FileMD5']

            if md5_str == d['file_md5']:
                print('成功')
                file_md5_pass_int = 2
                temp_dict = {'file_id': file_id,
                             'file_md5_pass': file_md5_pass_int,
                             'file_md5_read': d['file_md5'],
                             'file_name': d['file_name'],
                             'file_path': remote_name,
                             'file_veryfy_md5': md5_str,
                             'file_size': None,
                             'file_update_time': None,
                             }
                files_pass_lst.append(temp_dict)
            else:
                file_md5_pass_int = 1
                message = '失败, 本地值 {}，服务器值 {}'.format(d['file_md5'], md5_str)
                print(message)
                files_fail_lst.append(file_name)
                continue

        if len(files_fail_lst) != 0:
            message = '以下文件MD5校验失败\n{}'.format(files_fail_lst)
            raise ValueError(message)

        api_addr = self.main_page + addr
        payload = {'do_id': do_id,
                   'do_md5_remarks': '脚本自动校验',
                   'do_skip_verity_md5': 0,
                   'do_verify_md5': 2,
                   'files': files_pass_lst, }
        r = requests.post(api_addr, json = payload, cookies = self.token_dict)
        if r.status_code != 200 or not json.loads(r.text)['success']:
            message = 'Fail to verifyMD5 at {addr}, get return code {code}\n\n{text} '.format(addr = api_addr,
                                                                                                            code = r.status_code,
                                                                                                            text = json.loads(r.text))
            raise ConnectionRefusedError(message)

        return True

    def _api_SetFileCheckStatus(self, file_id: Union[str, list[str]], do_id: str, file_status = 1, addr = 'etlApi/DownloadSourceFile/FilesCheck') -> bool:
        '''
        文件列表 -> 选择文件 -> 文件校验 -> 提交

        Parameters:
            **file_id_list**: list[str]
                由file_id组成的列表

            **do_id**: str
                do_id

            **file_status**: int (0 or 1, 0 不合格，，1 合格)
                MD5校验状态

        Returns:
            **bool**: boolean
                是否成功
        '''
        api_addr = self.main_page + addr
        payload = []

        if isinstance(file_id, str):
            file_id_list = [file_id]
        elif isinstance(file_id, list):
            for s in file_id:
                if not isinstance(s, str):
                    message = ''
                    raise TypeError(message)
            file_id_list = file_id
        else:
            message = ''
            raise TypeError(message)

        if isinstance(file_status, int) and file_status in [0, 1]:
            file_status_str = str(file_status)
        else:
            message = ''
            raise TypeError(message)

        # ========format check finish==========
        for file_id in file_id_list:
            temp = {'file_id': file_id, 'do_id': do_id, 'file_status': file_status_str, 'file_status_remarks': '自动提交'}
            payload.append(temp)

        r = requests.post(api_addr, json = payload, cookies = self.token_dict)
        if r.status_code != 200 or not json.loads(r.text)['success']:
            message = 'Fail to SetFileCheckStatus at {addr} with , get return code {code}\n{text} '.format(addr = api_addr,
                                                                                                               code = r.status_code,
                                                                                                               text = json.loads(r.text))
            raise ConnectionRefusedError(message)

        return True

    def _api_MarkFile(self, testType = '标准', addr = 'etlApi/DataStore/MarkFile') -> bool:
        '''
        数据清洗，数据标记， 提交

        Parameters:
            **testType**: str
                '标准'，'重测'，'补测'

        Returns:
            **is_sucessful**: boolean
                是否提交成功?
        '''
        api_addr = self.main_page + addr
        payload = []

        if testType not in ['标准', '重测', '补测']:
            message = "'The testType must be one of '标准'，'重测'，'补测'"
            raise ValueError(message)
        # ========format check finish==========

        payload = []
        for d in self.files_dict.values():
            temp_dict = {}
            temp_dict['BarCode'] = d['BarCode']
            temp_dict['BatchNo'] = d['BatchNo']
            temp_dict['Direction'] = d['Direction']
            temp_dict['DirectionLimit'] = 0
            temp_dict['FileID'] = d['file_id']
            temp_dict['FileName'] = d['file_name']
            temp_dict['FilePath'] = d['file_path']
            temp_dict['GrpEncode'] = d['GrpEncode']
            temp_dict['SplitSize'] = 0
            temp_dict['TestType'] = testType

            payload.append(temp_dict)

        r = requests.post(api_addr, json = payload, cookies = self.token_dict)
        if r.status_code != 200 or not json.loads(r.text)['success']:
            message = 'Fail to MarkFile at {addr} with {payload}, get return code {code}\n\n{text} '.format(addr = api_addr,
                                                                                                            payload = payload,
                                                                                                            code = r.status_code,
                                                                                                            text = json.loads(r.text))
            raise ConnectionRefusedError(message)

        return True

    def _api_StoreFiles(self, sd_dict, retry = 999, addr = 'etlApi/DataStore/StoreFiles') -> bool:
        '''
        数据系统 -> 数据储存 -> 储存

        Parameters:
            **sd_dict**: dictionary
                由_api_SearchStoreFilesByPage返回的字典

            **retry**: int
                最大重试次数
        Returns:
            **successful**: index_list
                成功存储的 检测号和项目号
        '''
        MEANING = {1000: '未储存',
                    1600: '存储失败后重试',
                    1601: '等待存储',
                    1602: '正在存储',
                    2000: '存储成功',
                    5000: '存储失败', }

        if not isinstance(sd_dict, dict):
            message = ''
            raise TypeError(message)
        # ========format check finish==========
        api_addr = self.main_page + addr

        n = 0
        while n <= retry:
            n += 1
            successful_list = []
            i = 0
            for index, d in sd_dict.items():
                time.sleep(0.5)
                i += 1
                # sd_dict：{'102163-1002':
                # {'sd_id': '05d05764b1bf44098ea0396118bae6a8',
                # 'sd_status': 1000,
                # 'sd_status_time': '2022-01-29 11:38:27',
                # 'bar_code': '102163',
                # 'grp_encoded': '1002',
                # 'batch_no': 'M2000-352',
                # 'sequencing_type': '标准',
                # 'split_size': 0.0,
                # 'folder': '/mfs/osstore/LAB/HUADA/M2000-352',
                # 'latest': 1,
                # 'mark_time': '2022-01-29 11:38:27',
                # 'upload_id': None,
                # 'storage_id': None,
                # 'storage_time': None,
                # 'sn': '5717088875561529293',
                # 'direction_limit': 0,
                # 'no_verify_md5': 0,
                # 'AddStatus': -1,
                # 'AddTime': None,
                # 'set_data_flag': 0}, ''......
                # }
                sn = d['sn']

                query_dict = self._api_SearchStoreFilesByPage(sn = sn)
                if len(query_dict) != 1:
                    message = ''
                    raise ValueError(message)

                bar_code = query_dict[index]['bar_code']
                grp_encoded = query_dict[index]['grp_encoded']
                sd_status = query_dict[index]['sd_status']
                sd_status_time = query_dict[index]['sd_status_time']
                storage_id = query_dict[index]['storage_id']

                if sd_status in MEANING.keys():
                    meaning_str = MEANING[sd_status]
                else:
                    meaning_str = '未知'

                message = '{i}/{total} {bar_code}-{grp_encoded} {meaning}(sd_status:{sd_status})'.format(i = i,
                                                                                                         total = len(sd_dict),
                                                                                                         bar_code = bar_code,
                                                                                                         grp_encoded = grp_encoded,
                                                                                                         meaning = meaning_str,
                                                                                                         sd_status = sd_status)
                print(message, end = ' ')

                # 已成功储存
                if sd_status == 2000 and storage_id is not None:
                    successful_list.append(index)
                    message = 'storage_id:{storage_id}'.format(storage_id = storage_id)
                    print(message)
                    continue

                # 正在储存
                if sd_status == 1602:
                    message = '... 跳过！'
                    print(message)
                    continue

                # 尝试储存
                sd_id = d['sd_id']
                payload = [sd_id]
                r = requests.post(api_addr, json = payload, cookies = self.token_dict)

                if r.status_code != 200 or not json.loads(r.text)['success']:
                    message = 'Fail to StoreFiles at {addr} with {sn}, get return code {code}\n\n{text} '.format(addr = api_addr,
                                                                                                                 sn = sn,
                                                                                                                 code = r.status_code,
                                                                                                                 text = json.loads(r.text))
                    print(message)
                    continue

                message = '成功发送储存信号'
                print(message)

            if len(successful_list) == len(sd_dict):  # 所有都储存成功了
                break
            else:
                time.sleep(60)
                print('==========================================')

        return successful_list

    def _api_CreateBatchRequest(self, bar_code: Union[str, list[str]]) -> dict:
        '''
        数据系统 -> 数据任务 -> 批量生产申请单 -> 生成申请单

        结合loadErpRequestInfo查询功能和createBatchRequest生成功能, 生成给定bar_code的任务申请单

        Parameters:
            **bar_code**:
                检测号，或者由检测号组成的列表

        Returns:
            **successful_dict**: dict
                格式 {'BarCode': [], 'GrpEncoded': [], }, 成功提交的检测号和项目号
        '''
        MODEL_API = 'etlApi/RequestNoteApi/GetAnalysisModel'
        QUERY_API = 'etlApi/RequestNote/loadErpRequestInfo'
        CREATE_API = 'etlApi/RequestNote/createBatchRequest'

        if isinstance(bar_code, str):
            bar_code_lst = [bar_code]
        elif isinstance(bar_code, list):
            for s in bar_code:
                if not isinstance(s, str):
                    message = ''
                    raise TypeError(message)
            bar_code_list = bar_code
        else:
            message = ''
            raise TypeError(message)

        # get analysis model
        api_addr = self.main_page + MODEL_API
        r = requests.get(api_addr, cookies = self.token_dict)
        if r.status_code != 200 and not json.loads(r.text)['success']:
            message = 'Fail to GetAnalysisModel at {addr} with , get return code {code}\n\n{text} '.format(addr = api_addr,
                                                                                                         code = r.status_code,
                                                                                                         text = json.loads(r.text))
            raise ConnectionRefusedError(message)

        task_lst = json.loads(r.text)['data']

        # 高精度临床外显夫妻联合筛查 SPOUSE 700
        # 线粒体家系分析 MITO-PEDIGREE 1400
        # 线粒体分析 MITO 100
        # 一代分析 FIRSTEQ-MUTATION 1500
        # 家系分析 PEDIGREE 500
        # 数据格式转换 DATA-FORMAT 1000
        # 存储服务 STORAGE 900
        # 点变异分析 POINT-MUTATION 800
        # 单人分析 Singer -10000
        # 不做AI分析 NoAnalysis -10001
        task_code_dict = defaultdict(lambda: {})
        task_files_dict = defaultdict(lambda: {})
        for d in task_lst:
            task_code_dict[d['taskType']]['root'] = d['taskCode']
            task_files_dict[d['taskType']]['root'] = d['files']
            for temp in d['auTask']:  # d['auTask'] is a list
                # temp is dictionary
                # {'auTask': [],
                # 'files': ['CNVSEQPLUS-Annotated10K',
                #        'CNVSEQPLUS-Filtered10K',
                #        'CNVSEQPLUS-Bed10k',
                #        'CNVSEQPLUS-Plot10k',
                #        'CNVSEQPLUS-All10k',
                #        'AOH',
                #        'AOH_all'],
                # 'id': '609927c6bb78000027007a03',
                # 'name': 'CNVseqPLUS分析',
                # 'taskCode': '1700',
                # 'taskType': 'CNVseqPLUS',
                # 'type': None},
                task_code_dict[d['taskType']][temp['taskType']] = temp['taskCode']
                task_files_dict[d['taskType']][temp['taskType']] = temp['files']

        # query quest list
        api_addr = self.main_page + QUERY_API
        payload = {'pageSize': 10000,
                   'sortName': 'IsGroup',
                   'sortOrder': 'asc',
                   # 'RequestFlag': 0,  # 0 未开单， 1 已开单
                   'BarCode': ','.join(bar_code_list)
                   }
        print('正在查询申请单.....', end = ' ')
        r = requests.get(api_addr, params = payload, cookies = self.token_dict)
        if r.status_code != 200 or not json.loads(r.text)['success']:
            message = '\nFail to loadErpRequestInfo at {addr} with {payload}, get return code {code}\n\n{text} '.format(addr = api_addr,
                                                                                                                      payload = payload,
                                                                                                                      code = r.status_code,
                                                                                                                      text = json.loads(r.text)['message'])
            raise ConnectionRefusedError(message)

        quest_lst = json.loads(r.text)['data']['rows']
        print('{} 条. OK!'.format(len(quest_lst)))

        # 把从etlApi/RequestNote/loadErpRequestInfo返回的quest，转化成可以提交的request
        def __make_batch_request(quest: dict, task_code_dict, task_files_dict) -> dict:
            if quest['ProductType'] not in task_code_dict.keys():  # 'SPOUSE', 'MITO-PEDIGREE', 'MITO', 'PEDIGREE', 'POINT-MUTATION, 'FIRSTEQ-MUTATION', 'DATA-FORMAT', 'STORAGE', 'Singer', 'NoAnalysis'
                message = 'Can not make a request ({bar_code}-{grp_encode} {p_name} {relation}). The queryed ProductType is {q_type}, only {t_type} are support.'.format(bar_code = quest['BarCode'],
                                                                                                                                                                         grp_encode = quest['GrpEncoded'],
                                                                                                                                                                         p_name = quest['PatName'],
                                                                                                                                                                         relation = quest['relation'],
                                                                                                                                                                         q_type = quest['ProductType'],
                                                                                                                                                                         t_type = list(task_code_dict.keys()))
                raise TypeError(message)

            request = {}

            request['AnalysisModel'] = quest['AnalysisModel']
            request['AnalysisType'] = '临床分析'
            request['BarCode'] = quest['BarCode']
            request['BarSn'] = quest['BarSn']
            request['CoverDegree'] = None

            product_type_str = quest['ProductType']

            request['DataDeliveryFileType'] = ','.join(task_files_dict[product_type_str]['root'])

            files_str = ''
            models_lst = []
            if quest['AnalysisModel'] is not None:
                for model in quest['AnalysisModel'].split(','):  # model = 'CNV' or 'SNV' ....
                    files_str = files_str + ',' + ','.join(task_files_dict[product_type_str][model])
                    models_lst.append({'mType': model, 'mcode': task_code_dict[product_type_str][model], })
                request['DataDeliveryFileType'] += files_str

            request['Models'] = models_lst

            request['DataFile'] = None if quest['IsGroup'] else 'FastQ/Fasta'
            request['DataType'] = quest['DataType']
            request['Groups'] = quest['Groups']
            request['GrpEncoded'] = quest['GrpEncoded']
            request['IsGroup'] = quest['IsGroup']
            request['OrganID'] = '14aee279-dbe5-9f20-331a-77740ec58977'
            request['OrganName'] = '广州嘉检医学检测有限公司'
            request['ProductType'] = quest['ProductType']
            request['ProductTypeCode'] = task_code_dict[product_type_str]['root']
            request['RCode'] = quest['RCode']
            request['RType'] = 'report' if quest['isReport'] == '1' else 'datafile'

            # the following items are not necessary for submitting a request
            # only for human readablity
            request['GrpItem'] = quest['GrpItem']
            request['PatName'] = quest['PatName']
            request['relation'] = quest['relation']

            return request

        request_dict = {}
        for quest in quest_lst:
            try:
                request = __make_batch_request(quest, task_code_dict, task_files_dict)
            except Exception as ex:
                print(ex)
                continue

            request_dict[request['BarSn']] = request

        # submit request dict
        successful_dict = {'BarCode': [], 'GrpEncoded': [], 'BarSn': [], 'RCode': [], }
        api_addr = self.main_page + CREATE_API
        while True and len(request_dict) > 0:
            submit_at_least_one_request = False
            key_lst = list(request_dict.keys())
            for key in key_lst:
                payload = request_dict[key]
                r = requests.post(api_addr, json = [payload], cookies = self.token_dict)  # [payload] is necessary. Payload must be a list, even it only has one item
                if r.status_code != 200 or not json.loads(r.text)['success']:
                    message = 'Fail to createBatchRequest at {addr}, get return code {code}\n\n{text} '.format(addr = api_addr,
                                                                                                               code = r.status_code,
                                                                                                               text = json.loads(r.text))
                    raise ConnectionRefusedError(message)

                if json.loads(r.text)['data'][0]['BarSn'] != payload['BarSn']:
                    message = 'The request BarSn {} is not the same as server return value {}.'.format(payload['BarSn'], json.loads(r.text)['data'][0]['BarSn'])
                    raise ValueError(message)

                if json.loads(r.text)['data'][0]['IsSuccess'] and payload['RCode'] is not None:
                    submit_at_least_one_request = True
                    request_dict.pop(key)
                    successful_dict['BarCode'].append(payload['BarCode'])
                    successful_dict['GrpEncoded'].append(payload['GrpEncoded'])
                    successful_dict['BarSn'].append(payload['BarSn'])
                    successful_dict['RCode'].append(payload['RCode'])
                    message = '成功生成申请单 申请单号：{RCode} {bar_code}-{grp_encode} {p_name} {GrpItem} {relation}.'.format(bar_code = payload['BarCode'],
                                                                                                         grp_encode = payload['GrpEncoded'],
                                                                                                         p_name = payload['PatName'],
                                                                                                         GrpItem = payload['GrpItem'],
                                                                                                         relation = payload['relation'],
                                                                                                         #BarSn = payload['BarSn'],
                                                                                                         RCode = payload['RCode'])
                    print(message)

            if not submit_at_least_one_request or len(request_dict) == 0:
                break

        if len(request_dict) != 0:
            print('以下申请单生成失败：')
        for request in request_dict.values():
            payload = request
            r = requests.post(api_addr, json = request, cookies = self.token_dict)
            if r.status_code != 200 or not json.loads(r.text)['success']:
                message = 'Fail to createBatchRequest at {addr}, get return code {code}\n\n{text} '.format(addr = api_addr,
                                                                                                             code = r.status_code,
                                                                                                             text = json.loads(r.text))
                raise ConnectionRefusedError(message)

            if not json.loads(r.text)['data'][0]['IsSuccess']:
                message = '申请单号：{RCode} {bar_code}-{grp_encode} {p_name} {GrpItem} {relation}. 原因: 先生成以下子单 {error_code} '.format(bar_code = payload['BarCode'],
                                                                                                                                     grp_encode = payload['GrpEncoded'],
                                                                                                            p_name = payload['PatName'],
                    GrpItem = payload['GrpItem'],
                    relation = payload['relation'],
                    RCode = payload['RCode'],
                    error_code = json.loads(r.text)['data'][0]['ErrorBarCode'])
                print(message)

        return successful_dict

    def _api_SubmitRequestNote(self, rid_dict: dict, addr = 'etlApi/RequestNote/SubmitRequestNote') -> bool:
        '''
        数据系统 -> 数据任务 -> 申请单管理 -> 提交

        Parameters:
            **rid_dict**: dict
                通常使用_api_SearchRequestNoteList返回的结果

        Returns:
            **successful**: list
                成功递交的申请单号
        '''
        if not isinstance(rid_dict, dict):
            message = ''
            raise TypeError(message)
        # =========format check=========
        api_addr = self.main_page + addr

        successful_lst = []  # 成功提交的单号
        rid = copy(rid_dict)
        while True:
            submit_at_least_one = False
            index_lst = list(rid.keys())
            # index is '106846-T4300'
            for index in index_lst:
                d = rid[index]
                # d is
                # {'BarCode': '106848',
                # 'BatchNo': None,
                # 'CreateUserName': '姚天然',
                # 'DataType': 'MES',
                # 'GrpEncode': '4100',
                # 'GrpItem': '高精度临床外显-单项',
                # 'IsHasFamilyGroup': 0,
                # 'PatName': '孙祎诺母',
                # 'family_relation': '母',
                # 'p_family_no': '48932',
                # 'r_code': '1000145169',
                # 'rid': '7cac13d3-78d4-4356-b2ff-423f2d8d88a7'}

                payload = []
                temp = {'BarCode': d['BarCode'],
                        'GrpEncode': d['GrpEncode'],
                        'IsHasFamilyGroup': d['IsHasFamilyGroup'],
                        'RCode': d['r_code'],
                        'RID': d['rid'],
                        }
                payload.append(temp)
                message = '提交申请单 {RCode}：{bar_code}-{grp_encode} {p_name} {GrpItem} {relation}.'.format(bar_code = d['BarCode'],
                                                                                                                               grp_encode = d['GrpEncode'],
                                                                                                                           p_name = d['PatName'],
                                                                                                                           GrpItem = d['GrpItem'],
                                                                                                                           relation = d['family_relation'],
                                                                                                                           RCode = d['r_code']
                                                                                                        )
                message += ' ' * 100
                print(message[:90], end = ' ')
                r = requests.post(api_addr, json = payload, cookies = self.token_dict)
                if r.status_code == 200 and json.loads(r.text)['success']:
                    successful_lst.append(d['rid'])
                    submit_at_least_one = True
                    rid.pop(index)
                    message = '成功！'
                    print(message)

            if not submit_at_least_one or len(rid) == 0:
                break

        if len(rid) != 0:
            print('以下申请单提交失败：')
        for request in rid.values():
            payload = request
            r = requests.post(api_addr, json = request, cookies = self.token_dict)
            if r.status_code != 200 or not json.loads(r.text)['success']:
                message = json.loads(r.text)['message']
                print(message)

        message = '共成功递交 {} 申请单'.format(len(successful_lst))
        print(message)

        return successful_lst

    def _api_ReceiveRequestNote(self, rid: Union[str, list[str]], addr = 'etlApi/RequestNote/ReceiveRequestNote') -> bool:
        '''
        数据系统 -> 数据任务 -> 申请单管理 -> 已提交 -> 接收

        Parameters:
            **rid**: str or list[str]
                由rid组成的列表

        Returns:
            **successful**: bool
                是否成功
        '''
        if isinstance(rid, str):
            rid_lst = [rid]
        elif isinstance(rid, list):
            for s in rid:
                if not isinstance(s, str):
                    message = ''
                    raise TypeError(message)
            rid_lst = rid
        else:
            message = ''
            raise TypeError(message)
        # ========format check finish==========
        api_addr = self.main_page + addr

        for rid in rid_lst:
            payload = []
            payload.append({'RID': rid,
                            'DataSendWay': '5G',
                            'Remarks': '脚本自动提交',
                            'DataSendCompany': "",
                            'DataSendCourierNumber': "",
                            'DataSendLinkman': "",
                            'DataSendNetaddress': "",
                            'DataSendPassword': "",
                            'DataSendPhone': "",
                            'DataSendUserAccount': "",
                            'SocialType': ""})

        r = requests.post(api_addr, json = payload, cookies = self.token_dict)
        if r.status_code != 200 or not json.loads(r.text)['success']:
            message = 'Fail to ReceiveRequestNote at {addr}, get return code {code}\n\n{text} '.format(addr = api_addr,
                                                                                           code = r.status_code,
                                                                                           text = json.loads(r.text)['message'])
            raise ConnectionRefusedError(message)

        return True

    # 数据系统 -> 数据维护 -> OSS文件下载（内网）
    def _api_download(self, folder, url, prefix = '', inner = True, addr = 'etlApi/datafile/download') -> bool:
        '''
        从OSS数据维护系统中下载内网文件, 外网无效

        Parameters:
            **folder**: str
                下载目录

            **url**: str
                url， 通常为位于172.20.0.32, 例如 http://172.20.0.32:9009/opengk/data/1641949522789/107836-9130-WES-Coverage_qc.txt

            **prefix**: str
                添加的文件前缀

            **inner**: boolean
                是否内网下载

        Returns:
            **bool**：boolean
                成功
        '''
        import shutil
        import os.path as path

        if inner:
            api_addr = self.main_page + addr
            payload = {'url': url}
            r = requests.get(api_addr, params = payload, cookies = self.token_dict, stream = True)
        else:
            headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.102 Safari/537.36',
                       'Referer': 'http://www.dna.gz.cn/',
                       'Host': '113.98.96.228:9005', }
            r = requests.get(url, headers = headers, cookies = self.token_dict, stream=True)

        if r.status_code != 200:
            message = 'Fail to download at {addr} with {payload}, get return code {code}\n\n{text} '.format(addr = api_addr,
                                                                                                           payload = payload,
                                                                                                           code = r.status_code,
                                                                                                           text = json.loads(r.text))
            raise ConnectionRefusedError(message)

        if prefix != '':
            local_filename = path.join(folder, prefix + '_' + url.split('/')[-1])
        else:
            local_filename = path.join(folder, url.split('/')[-1])

        with open(local_filename, 'wb') as f:
            shutil.copyfileobj(r.raw, f)

        return True

    # 数据系统 -> 数据存储 -> 设置数据
    def _api_SetData(self, rid_dict: dict, addr = 'etlApi/DataStore/SetData') -> list:
        '''
        数据系统 -> 数据存储 -> 设置数据

        Parameters:
            **rid_dict**: dict
                通常使用_api_SearchRequestNoteList返回并且成功提交的申请单结果

        Returns:
            **successful**: list
                [{'rid':'.........', 'sd_id':'..........', 'Success':True}]
        '''
        if not isinstance(rid_dict, dict):
            message = ''
            raise TypeError(message)
        # ========format check finish==========

        api_addr = self.main_page + addr
        payload = []
        for d in rid_dict.values():
            # d is
            # {'BarCode': '106848',
            # 'BatchNo': None,
            # 'CreateUserName': '姚天然',
            # 'DataType': 'MES',
            # 'GrpEncode': '4100',
            # 'GrpItem': '高精度临床外显-单项',
            # 'IsHasFamilyGroup': 0,
            # 'PatName': '孙祎诺母',
            # 'family_relation': '母',
            # 'p_family_no': '48932',
            # 'r_code': '1000145169',
            # 'rid': '7cac13d3-78d4-4356-b2ff-423f2d8d88a7'}
            message = ''

            bar_code = d['BarCode']
            grp_encode = d['GrpEncode']
            rid = d['rid']

            sd_dict = self._api_SearchStoreFilesByPage(bar_code = bar_code, grp_encoded = grp_encode)
            if len(sd_dict) == 1:
                sd_id = list(sd_dict.values())[0]['sd_id']
                payload.append({'RID': rid,
                                'sd_id': sd_id,
                                'Remarks': '脚本自动设置', })
                message = '找到申请单对应数据  RID: {}， sd_id: {}'.format(rid, sd_id)
                print(message)
            else:
                message = '多个申请单对应数据  RID: {}， {}-{}'.format(rid, bar_code, grp_encode)
                print(message)

        # payload has been made, post it
        message = '设置 {} 申请单数据'.format(len(payload))
        print(message)
        r = requests.post(api_addr, json = payload, cookies = self.token_dict)
        if r.status_code != 200:
            message = 'Fail to SubmitDataOrder at {addr}, get return code {code}'.format(addr = api_addr,
                                                                                         code = r.status_code)
            raise ConnectionRefusedError(message)

        # check the results
        successful_lst = []
        data_lst = json.loads(r.text)['data']
        for data in data_lst:
            #  data is a dict
            #  Errormsg: str
            #  RID:str
            #  sd_id: str
            #  Success: bool
            if data['Success']:
                successful_lst.append({'sd_id': data['sd_id'], 'RID': data['RID'], })
                message = '设置数据成功， rid: {}'.format(data['RID'])
                print(message)
            else:
                message = '设置数据失败， rid: {}, sd_id: {}. {}'.format(data['RID'], data['sd_id'], data['Errormsg'])
                print(message)

        return successful_lst

    # 数据系统 -> 数据签收 -> 数据储存 -> 查询
    def _api_SearchStoreFilesByPage(self, batch_no = '', bar_code = '', grp_encoded = '', sn = '', addr = 'etlApi/DataStore/SearchStoreFilesByPage') -> list:
        '''
        数据系统 -> 数据签收 -> 数据储存 -> 查询
        用于搜索 sd_id， 可用于_api_StoreFiles储存数据

        Parameters:
            **batch_no**: str
                批次号

            **bar_code**: str
                检测号

            **grp_encoded**: str
                项目号

            **sn**: str
                数据序列号

        Returns:
            **sd_id dict**: dict
                sd_id dictionary.
                Keys are the {BarCode}-{GrpEncode} for each item.
        '''
        api_addr = self.main_page + addr
        payload = {}

        if not isinstance(batch_no, str):
            message = ''
            raise TypeError(message)

        if not isinstance(bar_code, str):
            message = ''
            raise TypeError(message)

        if not isinstance(grp_encoded, str):
            message = ''
            raise TypeError(message)

        # ======format check finish======

        payload['pageSize'] = 10000
        payload['batch_no'] = batch_no
        payload['bar_code'] = bar_code
        payload['grp_encoded'] = grp_encoded
        payload['sn'] = sn

        r = requests.get(api_addr, params = payload, cookies = self.token_dict)
        if r.status_code != 200 and not json.loads(r.text)['success']:
            message = 'Fail to SearchStoreFilesByPage at {api_addr} with , get return code {code}\n\n{text} '.format(addr = api_addr,
                                                                                                                   code = r.status_code,
                                                                                                                   text = json.loads(r.text))
            raise ConnectionRefusedError(message)

        sd_id_dict = {}

        row_lst = json.loads(r.text)['data']['rows']
        for d in row_lst:
            ''' d
            {AddStatus: -1
            AddTime: null
            bar_code: "102163"
            batch_no: "M2000-352"
            direction_limit: 0
            folder: "/mfs/osstore/LAB/HUADA/M2000-352"
            grp_encoded: "1002"
            latest: 1
            mark_time: "2021-12-14 09:24:00"
            no_verify_md5: 0
            sd_id: "943647bfe2fc4877aba2cd03c79a812e"
            sd_status: 2000
            sd_status_time: "2021-12-14 09:28:53"
            sequencing_type: "标准"
            set_data_flag: 1
            sn: "4803461111747065779"
            split_size: 0
            storage_id: "61b7f3557c434914ba08e380"
            storage_time: "2021-12-14 09:28:53"
            upload_id: "9b8ba6040a404def80c71df21236982e"
            '''
            index = '{}-{}'.format(d['bar_code'], d['grp_encoded'])
            sd_id_dict[index] = d

        return sd_id_dict

    def _api_SearchDataSourceFileByPage(self, batch_no = '', bar_code = '', grp_encoded = '', direction = 0, addr = 'etlApi/datafile/SearchDataSourceFileByPage') -> dict[dict]:
        '''
        数据系统 -> 数据维护 -> 文件管理 -> 数据中心-源文件下载

        Parameters:
            **batch_no**: str
                批次号

            **bar_code**: str
                检测号

            **grp_encoded**: str
                项目号

            **direction**: int
                1 单端， 2 双端, 其他无限制

        Returns:
            **files_dict**: defaultdict(lambda: defaultdict(dict))    # dict[dict]
                storage_file_id:
                文件列表
        '''
        api_addr = self.main_page + addr
        payload = {}

        if not isinstance(batch_no, str):
            message = ''
            raise TypeError(message)

        if not isinstance(bar_code, str):
            message = ''
            raise TypeError(message)

        if not isinstance(grp_encoded, str):
            message = ''
            raise TypeError(message)

        if direction not in [0, 1, 2]:
            message = ''
            raise ValueError(message)
        # format check finish

        payload['pageSize'] = 950
        payload['batch_no'] = batch_no
        payload['bar_code'] = bar_code
        payload['grp_encoded'] = grp_encoded
        payload['direction'] = str(direction)
        payload['incl'] = '-1'

        r = requests.get(api_addr, params = payload, cookies = self.token_dict)
        if r.status_code == 200 and json.loads(r.text)['success']:
            files_dict = copy(self.files_dict)
            row_lst = json.loads(r.text)['data']['rows']
            if len(row_lst) == 0:
                message = ''
                raise IndexError(message)

            for d in row_lst:
                ''' d
                {'file_id': 'cfef901f1f10477c9cc1e17b76e7024a',
                 'sn': '4893512870200604563',
                 'bar_code': '107250',
                 'grp_encoded': '4100',
                 'batch_no': 'M2000-372',
                 'sequencing_type': '标准',
                 'latest': 1,
                 'mark_time': '2022-01-07 14:25:11',
                 'storage_time': '2022-01-07 14:29:16',
                 'storage_id': '61d7ddbc2c89630dd5edd1fa',
                 'fileName': 'M2000-372-107250-4100-ZZP-EXON-41_L02_2.fq.gz',
                 'direction': 2,
                 'storage_file_id': '61d7ddbca032aa41f2770230',
                 'url': 'http://172.20.0.25:31268/file/downfileId/online?token=token&fileId=61d7ddbca032aa41f2770230',
                 'combine': 0,
                 'ext': '.fq.gz',
                 'file_type': 'Exon',
                 'aid': None,
                 'set_data_time': None,
                 'latest_set_data': -1,
                 'rcode': None}
                '''
                file_id = d['file_id']

                files_dict[file_id]['file_id'] = d['file_id']
                files_dict[file_id]['storage_id'] = d['storage_id']
                files_dict[file_id]['file_name'] = d['fileName']
                files_dict[file_id]['file_time'] = d['storage_time']
                files_dict[file_id]['file_url'] = d['url']
                files_dict[file_id]['BarCode'] = d['bar_code']
                files_dict[file_id]['BatchNo'] = d['batch_no']
                files_dict[file_id]['Direction'] = d['direction']
                files_dict[file_id]['GrpEncode'] = d['grp_encoded']

            self.files_dict = files_dict
            return files_dict

        else:
            message = 'Fail to SearchDataSourceFileByPage at {api_addr} with , get return code {code}\n{text} '.format(addr = api_addr,
                                                                                                                   code = r.status_code,
                                                                                                                   text = json.loads(r.text))
            raise ConnectionRefusedError(message)

        return

    def _api_SearchOssReportByPage(self, batch_no = '', bar_code = '', grp_encoded = '', direction = 0, addr = 'etlApi/datafile/SearchOssReportByPage') -> dict[dict]:
        '''
        数据系统 -> 数据维护 -> 文件管理 -> 数据中心-OSS下载

        如果无结果返回，raise IndexError

        Parameters:
            **batch_no**: str
                批次号

            **bar_code**: str
                检测号

            **grp_encoded**: str
                项目号

            **direction**: int
                1 单端， 2 双端, 其他无限制

        Returns:
            **files_dict**: defaultdict(lambda: defaultdict(dict))    # dict[dict]
                storage_file_id:
                文件列表
        '''
        api_addr = self.main_page + addr
        payload = {}

        if not isinstance(batch_no, str):
            message = ''
            raise TypeError(message)

        if not isinstance(bar_code, str):
            message = ''
            raise TypeError(message)

        if not isinstance(grp_encoded, str):
            message = ''
            raise TypeError(message)
        # format check finish

        payload['pageSize'] = 100000
        payload['batch_no'] = batch_no
        payload['bar_code'] = bar_code
        payload['grp_encoded'] = grp_encoded
        payload['inp'] = str(direction - 1)
        payload['sortName'] = 'createTime'

        r = requests.get(api_addr, params = payload, cookies = self.token_dict)
        if r.status_code != 200 or not json.loads(r.text)['success']:
            message = 'Fail to SearchOssReportByPage at {addr} with , get return code {code}\n{text} '.format(addr = api_addr,
                                                                                                                   code = r.status_code,
                                                                                                                   text = json.loads(r.text))
            raise ConnectionRefusedError(message)


        files_dict = copy(self.files_dict)
        row_lst = json.loads(r.text)['data']['rows']
        if len(row_lst) == 0:
            message = 'No oss files returned.\nPayload {}'.format(payload)
            raise IndexError(message)

        for d in row_lst:
            ''' d
            {'fileId': '61d8114f17ade57dccc844a7',
             'fileSize': 123014868,
             'fileFormat': 'Snv',
             'createTime': '2022-01-07 18:01:02',
             'filePath': '/data/1641542270493/107250-4100-MES-AllData.Snv',
             'fileName': '107250-4100-MES-AllData.Snv',
             'dataId': '1641542270493',
             'fileSystemType': 'minio',
             'md5': '848172033a1041d03a5f2d061ddb17e7',
             'fileUrl': 'http://172.20.0.32:9009/opengk/data/1641542270493/107250-4100-MES-AllData.Snv',
             'fileOuterDown': 'http://113.98.96.228:9005/mio/opengk/data/1641542270493/107250-4100-MES-AllData.Snv',
             'inp': 1,
             'ext': '.Snv',
             'fileType': None,
             'EtlTaskId': '5a3b47a193fc49d7ae5763cc64e051c8',
             'AnalyseCompleteTime': '2022-01-08 13:27:25',
             'NDID': '6f0ec884260740fcb0494c53e79b7125',
             'dataTime': '2022/1/7 15:53:25',
             'curTask': 1,
             'curData': 1,
             'groupflag': 0,
             'rCode': '1000141708',
             'batchNo': 'M2000-372',
             'barCode': '107250',
             'grpEncoded': '4100',
             'grpItem': '高精度临床外显-单项'}
            '''

            file_id = d['fileId']

            files_dict[file_id]['file_id'] = file_id
            files_dict[file_id]['file_name'] = d['fileName']
            files_dict[file_id]['file_time'] = d['createTime']
            files_dict[file_id]['file_url'] = d['fileUrl']
            files_dict[file_id]['file_url_WAN'] = d['fileOuterDown']
            files_dict[file_id]['BarCode'] = d['barCode']
            files_dict[file_id]['BatchNo'] = d['batchNo']
            files_dict[file_id]['Direction'] = d['inp'] + 1
            files_dict[file_id]['GrpEncode'] = d['grpEncoded']
            files_dict[file_id]['GrpItem'] = d['grpItem']
            files_dict[file_id]['file_md5'] = d['md5']
            files_dict[file_id]['file_type'] = d['fileType']
            files_dict[file_id]['r_code'] = d['rCode']

        self.files_dict = files_dict
        return files_dict

    def _api_SearchRequestNoteList(self, batch_no = '', bar_code = '', grp_encoded = '', r_code = '', data_type = '', p_family_no = '', is_set_data = '1', addr = 'etlApi/RequestNote/SearchRequestNoteList') -> dict[dict]:
        '''
        数据系统 -> 数据任务 -> 申请单管理 -> 查询

        Parameters:
            **batch_no**: str
                批次号, 多个批次号用逗号分割

            **bar_code**: str
                批次号, 多个批次号用逗号分割

            **grp_encoded**:str
                项目号

            **r_code**: str
                申请单号， 多个申请单号用逗号分割

            **data_type**: str
                数据类型： '', 'WGS', 'WES', 'MES', 'MITO', 'Panel', 'Sanger', 'PCRNGS', 'CNVSEQ', 'CNVSEQPLUS', 'Other'

            **is_set_data**: str
                '1': 已设置数据， '0':未设置数据

        Returns:
            **rid_dict**: defaultdict(lambda: defaultdict(dict))    # dict[dict]
        '''
        if not isinstance(batch_no, str):
            message = ''
            raise TypeError(message)

        if not isinstance(bar_code, str):
            message = ''
            raise TypeError(message)

        if not isinstance(grp_encoded, str):
            message = ''
            raise TypeError(message)

        if not isinstance(r_code, str):
            message = ''
            raise TypeError(message)

        if data_type not in ['', 'WGS', 'WES', 'MES', 'MITO', 'Panel', 'Sanger', 'PCRNGS', 'CNVSEQ', 'CNVSEQPLUS', 'Other']:
            message = ''
            raise TypeError(message)
        # =============format check finish==============

        # api_addr = self.main_page + addr

        api_addr = '{main_page}{addr}?sortName=CreateTime&sortOrder=desc&pageSize=100000&search={{"BatchNo":"{batch_no}", "RCode":"{r_code}", "BarCode":"{bar_code}", "GrpEncode":"{grp_encoded}", "DataType":"{data_type}", "SetDataFlag":"{SetDataFlag}", "p_family_no":"{p_family_no}"}}'.format(main_page = self.main_page,
                                                                                                                                                                                                                                               addr = addr,
                                                                                                                                                                                                                                               batch_no = batch_no,
                                                                                                                                                                                                                                               r_code = r_code,
                                                                                                                                                                                                                                               bar_code = bar_code,
                                                                                                                                                                                                                                               grp_encoded = grp_encoded,
                                                                                                                                                                                                                                               data_type = data_type,
                                                                                                                                                                                                                                               SetDataFlag = is_set_data,
                                                                                                                                                                                                                                               p_family_no = p_family_no
                                                                                                                                                                                                                                                                                                    )


        # r = requests.get(api_addr, json = payload, cookies = self.token_dict)

        r = requests.get(api_addr, cookies = self.token_dict)
        if r.status_code != 200 or not json.loads(r.text)['success']:
            message = 'Fail to SearchRequestNoteList at {addr}, get return code {code}\n\n{text} '.format(addr = api_addr,
                                                                                                        code = r.status_code,
                                                                                                        text = json.loads(r.text))
            raise ConnectionRefusedError(message)

        rid_list = json.loads(r.text)['data']['rows']
        rid_dict = copy(self.rid_dict)
        for d in rid_list:
            '''
            {'RID': '04f0251a-bf96-4381-aab9-35f32a684b75',
            'RCode': '1000140917',
            'StatusFlag': 5,
            'StatusTime': '2022-01-03 15:00',
            'AnalysisType': '临床分析',
            'AnalysisModel': 'CNV-SEQ,CNVseqPLUS',
            'XQMS': None,
            'JFJG': None,
            'BarSn': '10162137',
            'BarCode': '107613',
            'GrpEncode': '9610',
            'DataSources': '实验室',
            'CreateUserName': '张子钰',
            'CreateTime': '2022-01-03 14:53',
            'DataToRmsFlag': 1,
            'DataToRmsTime': '2022-01-07 10:44',
            'DataFile': 'FastQ/Fasta',
            'DataType': 'CNVSEQ',
            'DataDeliveryFileType': 'QualityTesting,Coverage,MutationVCF,v2CNVSEQdata-All,v2CNVSEQdata-Filtered,CNVSEQPLUS-Annotated10K,CNVSEQPLUS-Filtered10K,CNVSEQPLUS-Bed10k,CNVSEQPLUS-Plot10k,CNVSEQPLUS-All10k,AOH,AOH_all',
            'IsDataCleaned': 1,
            'BatchNo': 'M2000-370',
            'GrpItem': '全基因组拷贝数变异-单项',
            'ProductType': 'POINT-MUTATION',
            'ProductTypeCode': '800',
            'IsHasFamilyGroup': 0,
            'DataSendWay': '5G',
            'DataSendLinkman': None,
            'DataSendPhone': None,
            'DataSendCompany': None,
            'DataSendAddress': None,
            'DataSendCourierNumber': None,
            'DataSendEmail': None,
            'DataSendNetaddress': None,
            'DataSendUserAccount': None,
            'DataSendPassword': None,
            'SetDataFlag': 1,
            'SetDataTime': '2022-01-03 16:41',
            'NDID': '9c12216d519947b1a1b7e990bacc6690',
            'AID': '61d2b6c234b5fc3eca49bc6e',
            'DataId': '1641199365781',
            'sn': '5069775468310483845',
            'AllowSetDataFlag': 1,
            'IsGroup': 0,
            'RType': 'report',
            'FamilyRelation': '先证者',
            'PatName': '冯琬桐',
            'p_family_no': '49251'}
            '''

            id_str = '{}-{}'.format(d['BarCode'], d['GrpEncode'])

            rid_dict[id_str]['rid'] = d['RID']
            rid_dict[id_str]['r_code'] = d['RCode']
            rid_dict[id_str]['bar_sn'] = d['BarSn']
            rid_dict[id_str]['BatchNo'] = d['BatchNo']
            rid_dict[id_str]['BarCode'] = d['BarCode']
            rid_dict[id_str]['GrpEncode'] = d['GrpEncode']
            rid_dict[id_str]['PatName'] = d['PatName']
            rid_dict[id_str]['family_relation'] = d['FamilyRelation']
            rid_dict[id_str]['GrpItem'] = d['GrpItem']
            rid_dict[id_str]['p_family_no'] = d['p_family_no']
            rid_dict[id_str]['IsHasFamilyGroup'] = d['IsHasFamilyGroup']
            rid_dict[id_str]['DataType'] = d['DataType']
            rid_dict[id_str]['CreateUserName'] = d['CreateUserName']
            rid_dict[id_str]['Request_time'] = d['CreateTime']
            rid_dict[id_str]['Push_time'] = d['DataToRmsTime']

        self.rid_dict = rid_dict
        return rid_dict

    def _api_SearchByPage(self, batch_no = '', bar_code = '', grp_encoded = '', p_family_no = '', qc_time_begin = None, qc_time_end = None, push_time_begin = None, push_time_end = None, addr = 'lab20Api/DataQc/SearchByPage') -> dict[dict]:
        '''
        实验室系统 -> 二代检测数据质控 -> 查询 (可用于查询生产状态和rid)

        Parameters:
            **batch_no**: str
                批次号

            **bar_code**: str
                检测号

            **grp_encoded**: str
                项目号

            **p_family_no**: str
                家庭号

            **qc_time_begin, qc_time_end, push_time_begin, push_time_end**:
                质控时间查询范围起始/结束，推送时间查询范围起始/结束， 例如'2022-03-01'


        Returns:
            if no item was found, raise IndexError
            **rid_dict**: defaultdict(lambda: defaultdic        t(dict))    # dict[dict]
        '''
        if not isinstance(batch_no, str):
            message = ''
            raise TypeError(message)

        if not isinstance(bar_code, str):
            message = ''
            raise TypeError(message)

        if not isinstance(grp_encoded, str):
            message = ''
            raise TypeError(message)

        if not isinstance(p_family_no, str):
            message = ''
            raise TypeError(message)
        # format check finish

        api_addr = self.main_page + addr

        payload = {}
        payload['pageSize'] = 100000
        payload['batch_no'] = batch_no
        payload['bar_code'] = bar_code
        payload['grp_encoded'] = grp_encoded
        payload['pat_familyno'] = p_family_no
        payload['qc_time_begin'] = qc_time_begin
        payload['qc_time_end'] = qc_time_end
        payload['push_time_begin'] = push_time_begin
        payload['push_time_end'] = push_time_end

        r = requests.get(api_addr, params = payload, cookies = self.token_dict)
        if r.status_code != 200 or not json.loads(r.text)['success']:
            message = 'Fail to SearchByPage at {addr}, get return code {code}\n\n{text} '.format(addr = api_addr,
                                                                                            code = r.status_code,
                                                                                            text = json.loads(r.text))
            raise ConnectionRefusedError(message)

        rid_dict = copy(self.rid_dict)
        row_lst = json.loads(r.text)['data']['rows']
        if len(row_lst) == 0:
            message = 'No items found in current search keyword: {} {}-{} {}'.format(batch_no, bar_code, grp_encoded, p_family_no)
            raise IndexError(message)

        for d in row_lst:
            '''
            {'__i__': 10,
            'DID': 'd22dd1aa42d64d3bab44a224dfe7ef3c',
            'bar_code': '107613',
            'grp_encoded': '9610',
            'RCode': '1000140917',
            'PlatformFlag': 1,
            'RID': '04f0251a-bf96-4381-aab9-35f32a684b75',
            'TaskId': '33ed813ea47d496595b1dd3773f23508',
            'CombinedFlag': 0,
            'ParentRID': None,
            'DataId': '1641199365781',
            'DataType': 'CNVSEQ',
            'RType': 'report',
            'PushFlag': 200,
            'PushTime': '2022-01-04 15:12:51',   # PushTime 推送时间（推送到医学部的时间）
            'PushUserID': '689',
            'PushUserName': '刘晓莹',
            'QcFlag': 1,
            'QcTime': '2022-01-04 15:12:48',  # 质控时间(点击质控按钮，确定质控结果的时间)
            'QcUserID': '689',
            'QcUserName': '刘晓莹',
            'QcResult': '质控合格',
            'QcFailReason': None,
            'CreateTime': '2022-01-04 12:42:08',   # ETL_time, ETL推送时间(opengk运行完毕，推送到质控模块的时间)
            'DelFlag': 0,
            'NeedQc': 1,
            'ItemFlag': 1000,
            'Source': 101,
            'grp_item': '全基因组拷贝数变异CNVseq-单人',
            'pat_familyno': '49251',
            'pat_name': '冯琬桐',
            'pat_family_relation': '先证者',
            'sp_name': '全血',
            'batch_no': 'M2000-370',
            'HasTaskFlag': 1,
            'sample_priority': '常规',
            'CanPush': 1,
            'reportTask': 1,
            'bar_sn': '10162137',
            'org_name': '首都医科大学附属北京儿童医院',
            'depart_name': '神经内科      ',
            'qc_flag': 0,
            'sam_sign_time': '2021-12-25 11:05:11',   # 样品签收时间
            'YuCompleteTime': '2022-01-03 09:07:16',  # Data_time 数据下机时间
            'req_born_flag': 1,
            'req_born_flag_text': '产后'}
            '''
            id_str = '{}-{}'.format(d['bar_code'], d['grp_encoded'])

            rid_dict[id_str]['rid'] = d['RID']
            rid_dict[id_str]['r_code'] = d['RCode']
            rid_dict[id_str]['bar_sn'] = d['bar_sn']
            rid_dict[id_str]['BatchNo'] = d['batch_no']
            rid_dict[id_str]['BarCode'] = d['bar_code']
            rid_dict[id_str]['GrpEncode'] = d['grp_encoded']
            try:
                rid_dict[id_str]['GrpItem'] = d['grp_item'].strip()
            except:
                rid_dict[id_str]['GrpItem'] = d['grp_item']

            rid_dict[id_str]['DataType'] = d['DataType']
            rid_dict[id_str]['CombinedFlag'] = d['CombinedFlag']

            rid_dict[id_str]['is_need_inner_qc'] = True if d['IsNeedInnerQc'] == 1 else False
            if d['qc_flag'] == 20:
                rid_dict[id_str]['innerQCFlag'] = True
            elif d['qc_flag'] is None:
                rid_dict[id_str]['innerQCFlag'] = None
            else:
                rid_dict[id_str]['innerQCFlag'] = False

            try:
                rid_dict[id_str]['PatName'] = d['pat_name'].strip()
            except:
                rid_dict[id_str]['PatName'] = d['pat_name']

            rid_dict[id_str]['p_family_no'] = d['pat_familyno']
            rid_dict[id_str]['family_relation'] = d['pat_family_relation']
            rid_dict[id_str]['req_born'] = d['req_born_flag_text']
            rid_dict[id_str]['QcUserName'] = d['QcUserName']

            try:
                rid_dict[id_str]['QcResult'] = d['QcResult'].strip()
            except:
                rid_dict[id_str]['QcResult'] = d['QcResult']

            rid_dict[id_str]['sp_name'] = d['sp_name']
            rid_dict[id_str]['ETL_time'] = d['CreateTime']
            rid_dict[id_str]['Push_time'] = d['PushTime']
            rid_dict[id_str]['Data_time'] = d['YuCompleteTime']
            rid_dict[id_str]['data_id'] = d['DataId']


        self.rid_dict = rid_dict
        return rid_dict


    # 数据系统 -> 数据交付 -> 查询 (是否可以推送质控环节主要用于获得开始质控时间 TaskCreateTime)
    def _api_SearchPushListByPage(self, batch_no = '', bar_code = '', grp_encoded = '', p_family_no = '', CombinedFlag = None, CanPush = None, DataToRmsFlag = None, addr = 'etlApi/DataTreating/SearchPushListByPage'):
        '''
        数据系统 -> 数据交付 -> 查询 (是否可以推送质控环节主要用于获得开始质控时间 TaskCreateTime)

        Parameters:
            **batch_no**: str
                批次号

            **bar_code**: str
                检测号

            **grp_encoded**: str
                项目号

            **p_family_no**: str
                家庭号

            **CombinedFlag**: int
                申请单类型， 0子单， 1组合单

            **CanPush**: int
                是否可以推送至质控阶段， 0不可以， 1可以

            **DataToRmsFlag**: int
                是否已推送至质控阶段， 0未推送， 1已推送

        Returns:
            **rid_dict**: defaultdict(lambda: defaultdict(dict))            # dict[dict]
        '''

        if not isinstance(batch_no, str):
            message = ''
            raise TypeError(message)

        if not isinstance(bar_code, str):
            message = ''
            raise TypeError(message)

        if not isinstance(grp_encoded, str):
            message = ''
            raise TypeError(message)

        if not isinstance(p_family_no, str):
            message = ''
            raise TypeError(message)

        # =============format check finish==============
        api_addr = self.main_page + addr
        payload = {}

        payload['BatchNo'] = batch_no
        payload['BarCode'] = bar_code
        payload['GrpEncode'] = grp_encoded
        payload['p_family_no'] = p_family_no
        payload['pageSize'] = 100000
        payload['pageNumber'] = 1
        payload['sortOrder'] = 'asc'

        if DataToRmsFlag is not None:
            payload['DataToRmsFlag'] = DataToRmsFlag


        # 当全部参数为空时可能返回空结果，未避免此情况，当全部为空参数时我们将所有子单和组合单合并，作为最终结果
        if payload['BatchNo'] == '' and payload['BarCode'] == '' and payload['GrpEncode'] == '' and payload['p_family_no'] == '' and CombinedFlag is None and CanPush is None and DataToRmsFlag is None:
            payload['CombinedFlag'] = 0
            r = requests.get(api_addr, params = payload, cookies = self.token_dict)
            if r.status_code != 200 or not json.loads(r.text)['success']:
                message = 'Fail to SearchRequestNoteList at {addr} with {payload}, get return code {code}\n\n{text} '.format(addr = api_addr,
                                                                                                                             payload = payload,
                                                                                                                             code = r.status_code,
                                                                                                                             text = json.loads(r.text))
                raise ConnectionRefusedError(message)

            rid_solo_list = json.loads(r.text)['data']['rows']

            payload['CombinedFlag'] = 1
            r = requests.get(api_addr, params = payload, cookies = self.token_dict)
            if r.status_code != 200 or not json.loads(r.text)['success']:
                message = 'Fail to SearchRequestNoteList at {addr} with {payload}, get return code {code}\n\n{text} '.format(addr = api_addr,
                                                                                                                             payload =  payload,
                                                                                                                             code = r.status_code,
                                                                                                                             text = json.loads(r.text))
                raise ConnectionRefusedError(message)

            rid_comb_list = json.loads(r.text)['data']['rows']

            rid_list = rid_solo_list + rid_comb_list

        else:
            r = requests.get(api_addr, params = payload, cookies = self.token_dict)
            if r.status_code != 200 or not json.loads(r.text)['success']:
                message = 'Fail to SearchRequestNoteList at {addr}, get return code {code}\n\n{text} '.format(addr = api_addr,
                                                                                                            code = r.status_code,
                                                                                                            text = json.loads(r.text))
                raise ConnectionRefusedError(message)

            rid_list = json.loads(r.text)['data']['rows']


        if rid_list is None:
            message = 'Server responded, but the returned value is empty.'
            raise ValueError(message)

        rid_dict = copy(self.rid_dict)
        for d in rid_list:   # 可能有多条 相同 barcode-grpencode 条目：目前只能返回总后一个，不合理，缓解方法：尽量返回HasTaskFlag 为1的条目
            '''
            {'HasTaskFlag': 1,
            'TaskId': 'e6834aedf3e54c4386a11aef75dd15c0',
            'CombinedFlag': 0,
            'TaskCreateTime': '2021-11-01 09:41:58',
            'IsAnalyseComplete': 1,
            'AnalyseCompleteTime': '2021-11-01 14:11:01',
            'RID': 'f096438b-2b41-497e-af00-d0226319763f',
            'combinedTaskId': None,
            'DataId': '1635730918796',
            'AID': '617f451a0e421f5486b5149e',
            'NDID': 'dbb8d2dc9d8c4892bc6722564ac8c1cb',
            'RCode': '1000130722',
            'ProductType': 'POINT-MUTATION',
            'ProductTypeCode': '800',
            'AnalysisModel': 'SNV,CNV,CNV-WES',
            'BarSn': '10151506',
            'DataType': 'MES',
            'DataToRmsFlag': 1,
            'DataToRmsTime': '2021-11-08 08:49:45',
            'DataToRmsTaskId': None,
            'RType': 'report',
            'BarCode': '101524',
            'BatchNo': 'M2000-317',
            'GrpEncode': '4110',
            'GrpItem': '高精度临床外显携带者筛查-单人',
            'p_name': '曾艳军',
            'p_family_no': '46348',
            'AllowToAiFlag': 1,
            'FamilyTies': '先证者',
            'CanPush': 1,
            'ParentFlag': -1,
            'ParentRID': None}
            '''
            if d['HasTaskFlag'] == 0:
                continue

            id_str = '{}-{}'.format(d['BarCode'], d['GrpEncode'])

            rid_dict[id_str]['rid'] = d['RID']
            rid_dict[id_str]['aid'] = d['AID']
            rid_dict[id_str]['r_code'] = d['RCode']
            rid_dict[id_str]['bar_sn'] = d['BarSn']
            rid_dict[id_str]['BatchNo'] = d['BatchNo']
            rid_dict[id_str]['BarCode'] = d['BarCode']
            rid_dict[id_str]['GrpEncode'] = d['GrpEncode']
            rid_dict[id_str]['PatName'] = d['p_name']
            rid_dict[id_str]['family_relation'] = d['FamilyTies']
            rid_dict[id_str]['GrpItem'] = d['GrpItem']
            rid_dict[id_str]['p_family_no'] = d['p_family_no']
            rid_dict[id_str]['DataType'] = d['DataType']
            rid_dict[id_str]['CombinedFlag'] = d['CombinedFlag']
            rid_dict[id_str]['TaskCreateTime'] = d['TaskCreateTime']
            rid_dict[id_str]['AnalyseCompleteTime'] = d['AnalyseCompleteTime']
            rid_dict[id_str]['DataToRmsTime'] = d['DataToRmsTime']

            self.rid_dict = rid_dict
        return rid_dict


    # 数据系统 -> 数据任务 -> 提交分析任务单 -> 查询(未提交的分析任务) （主要用于获取未提交的任务分析单的aid，用于提交分析）
    def _api_SearchSubmitTaskList(self, batch_no = '', bar_code = '', grp_encoded = '', p_family_no = '', CombinedFlag = 0, SubmitReadyFlag = None, addr = 'etlApi/DataStore/SearchSubmitTaskList'):
        '''
        数据系统 -> 数据任务 -> 提交分析任务单 -> 查询(未提交的分析任务) （主要用于获取未提交的任务分析单的aid，用于提交分析）
        通常只需查询子单即可，因为似乎只有子单存在AID

        Parameters:
            **batch_no**: str
                批次号

            **bar_code**: str
                检测号

            **grp_encoded**: str
                项目号

            **p_family_no**: str
                家庭号

            **CombinedFlag**: int
                订单类型, 0子单，1组合单，其他 未设置

            **SubmitReadyFlag:**: int
                是否可以提交， 0否， 1是，其他 未设置

        Returns:
            **rid_dict**: defaultdict(lambda: defaultdict(dict))            # dict[dict]
        '''
        if not isinstance(batch_no, str):
            message = ''
            raise TypeError(message)

        if not isinstance(bar_code, str):
            message = ''
            raise TypeError(message)

        if not isinstance(grp_encoded, str):
            message = ''
            raise TypeError(message)

        if not isinstance(p_family_no, str):
            message = ''
            raise TypeError(message)

        if not isinstance(CombinedFlag, int):
            message = ''
            raise TypeError(message)

        if not isinstance(SubmitReadyFlag, int):
            message = ''
            raise TypeError(message)
        # format check finish

        api_addr = self.main_page + addr

        payload = {}
        payload['pageSize'] = 100000
        payload['pageNumber'] = 1
        payload['sortOrder'] = 'asc'

        payload['batch_no'] = batch_no
        payload['bar_code'] = bar_code
        payload['grp_encoded'] = grp_encoded
        payload['pat_familyno'] = p_family_no

        if CombinedFlag in [0, 1]:
            payload['CombinedFlag'] = CombinedFlag

        if SubmitReadyFlag in [0, 1]:
            payload['SubmitReadyFlag'] = SubmitReadyFlag

        r = requests.get(api_addr, params = payload, cookies = self.token_dict)
        if r.status_code != 200 or not json.loads(r.text)['success']:
            message = 'Fail to SearchSubmitTaskList at {addr} with {payload}, get return code {code}\n\n{text} '.format(addr = api_addr,
                                                                                                                payload = payload,
                                                                                                                code = r.status_code,
                                                                                                                text = json.loads(r.text))
            raise ConnectionRefusedError(message)

        rid_dict = copy(self.rid_dict)
        row_lst = json.loads(r.text)['data']['rows']
        if len(row_lst) == 0:
            message = 'No items found in current search keyword: {} {}-{} {}'.format(batch_no, bar_code, grp_encoded, p_family_no)
            raise IndexError(message)

        return


    def _api_SearchSampleByPage(self, batch_no = '', bar_code = '', grp_encoded = '', p_family_no = '', startTime = '', endTime = '', is_return = False, verbose = False, addr = 'lab20Api/Sample/SearchSampleByPage'):
        '''
        实验室系统 -> 样本查询 -> 样本查询 -> 查询 （主要用于获取所有的样本信息等）

        Parameters:
            **batch_no**: str
                批次号

            **bar_code**: str
                检测号

            **grp_encoded**: str
                项目号

            **p_family_no**: str
                家庭号

            **startTime**, **endTime**:str
                起始，终止日期，例如2022-03-23

            **is_return**: bool
                是否获取退样样本

        Returns:
            **rid_dict**: defaultdict(lambda: defaultdict(dict))            # dict[dict]

        lab_id:1 (核心)   1774 (2022-02-28 ~ 2022-03-28 样本数)
        lab_id:2 (重庆妇幼)  1214
        lab_id:3 (华西)   0
        lab_id:4 (新疆)   0
        lab_id:6 (湘雅)   95
        lab_id:7 (郑大附三)  214
        lab_id:8 (西北妇女)   27
        lab_id:9 (北京协和)   8
        lab_id:10 (郑大附一)  12
        '''
        if not isinstance(batch_no, str):
            message = ''
            raise TypeError(message)

        if not isinstance(bar_code, str):
            message = ''
            raise TypeError(message)

        if not isinstance(grp_encoded, str):
            message = ''
            raise TypeError(message)

        if not isinstance(p_family_no, str):
            message = ''
            raise TypeError(message)

        if not isinstance(startTime, str):
            message = ''
            raise TypeError(message)

        if not isinstance(endTime, str):
            message = ''
            raise TypeError(message)
        # format check finish

        api_addr = self.main_page + addr


        payload = {}
        payload['pageSize'] = 1000000
        payload['pageNumber'] = 1
        payload['sortOrder'] = 'asc'

        payload['batch_no'] = batch_no
        payload['bar_code'] = bar_code
        payload['grp_encoded'] = grp_encoded
        payload['pat_familyno'] = p_family_no
        payload['BeginTime'] = startTime
        payload['EndTime'] = endTime


        rid_list = []
        for lab_id in range(1, 11):  # 必须分段获取，否则服务器可能报错
            payload['lab_id'] = lab_id
            r = requests.get(api_addr, params = payload, cookies = self.token_dict)
            if r.status_code != 200 or not json.loads(r.text)['success']:
                message = 'Fail to SearchSubmitTaskList at {addr} with {payload}, get return code {code}\n\n{text} '.format(addr = api_addr,
                                                                                                                    payload = payload,
                                                                                                                    code = r.status_code,
                                                                                                                    text = json.loads(r.text))
                raise ConnectionRefusedError(message)

            if json.loads(r.text)['data']['total'] > payload['pageSize']:
                message = '样本未获取完全，全部样本为 {}'.format(json.loads(r.text)['data']['total'])
                print(message)

            temp_list = json.loads(r.text)['data']['rows']
            if verbose:
                print('lab_id: {}, count: {}'.format(lab_id, len(temp_list)))
            rid_list = rid_list + temp_list


        rid_dict = copy(self.rid_dict)
        for d in rid_list:
            '''
            Step: 3
            bar_code: "115836"
            bar_con_num: 1
            bar_item_name: "高精度临床外显-单项"
            bar_serial_num: "00247820"
            bar_sn: "10177884"
            batch_no: null
            deliver_flag: 1
            deliver_remarks: "实验"
            deliver_time: "2022-03-25 15:19"
            deliver_user_id: "839"
            deliver_user_name: "熊秀玲"
            dep_name: "销售部"
            exp_anotherName: "医学外显捕获测序"
            exp_code: "MES"
            exp_name: "MES"
            grp_code: "4209"
            grp_encoded: "4100"
            grp_item: "高精度临床外显-单项"
            is_return: false
            is_urgent: true
            lab_id: "1"
            lab_name: "广州核心实验室"
            orderNumberCode: "202200012560"
            org_name: "济南市妇幼保健院"
            pat_familyno: "53501"
            pat_name: "杨江山之孩"
            pat_sex: 0
            pat_sex_text: "未知"
            rPlatform: "3.0"
            relation: "先证者"
            req_code: "10130843"
            req_user_id: "308"
            req_user_name: "李俊英"
            return_reason: null
            return_time: null
            return_user_id: null
            return_user_name: null
            sam_sign_time: "2022-03-25 13:51"
            sam_sign_user_name: "黎芷雅"
            samp_id: "397a5dcf9fd44f0c8b928e3da5843953"
            sample_priority: "加急"
            sp_id: "47"
            sp_name: "羊水"
            urgent_time: "2022-03-25 15:19"
            '''
            id_str = '{}-{}'.format(d['bar_code'], d['grp_encoded'])

            if is_return:
                rid_dict[id_str]['is_return'] = d['is_return']
                rid_dict[id_str]['return_user_name'] = d['return_user_name']
                rid_dict[id_str]['return_reason'] = d['return_reason']
            else:
                if d['is_return']:  # 为退样样本
                    continue


            rid_dict[id_str]['BarCode'] = d['bar_code']
            rid_dict[id_str]['bar_sn'] = d['bar_sn']
            rid_dict[id_str]['BatchNo'] = d['batch_no']
            rid_dict[id_str]['GrpEncode'] = d['grp_encoded']
            rid_dict[id_str]['GrpItem'] = d['grp_item']

            rid_dict[id_str]['lab_name'] = d['lab_name']
            rid_dict[id_str]['org_name'] = d['org_name']
            rid_dict[id_str]['p_family_no'] = d['pat_familyno']
            rid_dict[id_str]['PatName'] = d['pat_name']
            rid_dict[id_str]['sex_recorded'] = d['pat_sex_text']
            rid_dict[id_str]['family_relation'] = d['relation']
            rid_dict[id_str]['urgent'] = d['sample_priority']
            rid_dict[id_str]['sp_name'] = d['sp_name']



        self.rid_dict = rid_dict
        return rid_dict

    def _api_GetQcInfo(self, rid: str, addr = 'lab20Api/DataQc/GetQcInfo') -> dict[dict]:
        '''
        实验室系统 -> 二代检测数据质控 -> (条目按钮)质控信息查看

        Parameters:
            **rid**: str
                rid


        Returns:
            if no result return raise ConnectionRefusedError
            **rid_dict**: defaultdict(lambda: defaultdict(dict))    # dict[dict]

        # result_dict:
        #   family_relation:str
        #   raw_base:float
        #   q30:float
        #   on_target_rate: float
        #   depthAvg: float
        #   depthSD: float
        #   coverage:float
        #   insert_size_average:float
        #   error_rate: float
        #   CNV_total:int
        #   ploidy:str
        #   trisome:str
        #   CNVSeq_totalSample: int
        #   CNVSeq_totalAbnormal: int
        #   CNVSeqPlus_100K:int
        #   CNVSeqPlus_Trip:str
        #   sex_record:str
        #   sexInfo:list
        #   sex_calculate:str
        #   P_count:int
        #   M_count:int
        #   PvsM:float
        #   deNovo:int
        '''
        payload = {'rid': rid}
        search_addr = self.main_page + addr
        # result_dict = defaultdict(lambda: None)
        result_dict = copy(self.qc_dict)

        r = requests.get(search_addr, params = payload, cookies = self.token_dict)
        if r.status_code != 200 or not json.loads(r.text)['success']:
            message = 'Attempt to get RID at {addr}, get return code {re}'.format(addr = search_addr,
                                                                                  re = r.status_code)
            raise ConnectionRefusedError(message)

        data_dict = json.loads(r.text)['data']
        data_type_str = data_dict['DataType']
        qc_dict = data_dict['QcData']
        result_dict['bed'] = qc_dict['benFile']
        try:
            family_relation_str = data_dict['FamilyRelation']  # CNVSEQ, CNVseqPLUS, MES, MITO, WES, Panel, PCRNGS
            result_dict['family_relation'] = family_relation_str
        except:
            pass

        try:
            result_dict['raw_base'] = round(int(qc_dict['fastq']['total_bases']) / 1000000000, 2)
        except ValueError:
            result_dict['raw_base'] = qc_dict['fastq']['total_bases']
        except:
            pass

        try:
            result_dict['q30'] = round(float(qc_dict['fastq']['q30_rate']), 3)
        except ValueError:
            result_dict['q30'] = qc_dict['fastq']['q30_rate']
        except:
            pass

        try:
            result_dict['on_target_rate'] = round(float(qc_dict['coverage']['on_target_rate']), 3)
        except ValueError:
            result_dict['on_target_rate'] = qc_dict['coverage']['on_target_rate']
        except:
            pass

        try:
            result_dict['depthAvg'] = round(float(qc_dict['coverage']['depth_left']), 2)
        except ValueError:
            result_dict['depthAvg'] = qc_dict['coverage']['depth_left']
        except:
            pass

        try:
            result_dict['depthSD'] = round(float(qc_dict['coverage']['depth_right']), 2)
        except ValueError:
            result_dict['depthSD'] = qc_dict['coverage']['depth_right']
        except:
            pass

        try:
            result_dict['coverage'] = round(float(qc_dict['coverage']['coverage5000']), 2) if data_type_str == 'MITO' else round(float(qc_dict['coverage']['section20']), 2)
            # result_dict['coverage20X'] = float(qc_dict['coverage']['coverage20x'])
        except ValueError:
            result_dict['coverage'] = qc_dict['coverage']['coverage5000'] if data_type_str == 'MITO' else qc_dict['coverage']['section20']
        except:
            pass

        try:
            result_dict['insert_size_average'] = float(qc_dict['maping']['insert_size_average'])
        except ValueError:
            result_dict['insert_size_average'] = qc_dict['maping']['insert_size_average']
        except:
            pass

        try:
            result_dict['error_rate'] = float(qc_dict['maping']['error_rate'])
        except ValueError:
            result_dict['error_rate'] = qc_dict['maping']['error_rate']
        except:
            pass

        try:
            if 'cnv' in qc_dict.keys():
                scale13_int = int(qc_dict['cnv']['scale13'])
                scale07_int = int(qc_dict['cnv']['scale107'])
                result_dict['CNV_total'] = scale13_int + scale07_int
            elif 'clinCnv' in qc_dict.keys():
                result_dict['CNV_total'] = int(qc_dict['clinCnv']['totalCnv'])
            else:
                raise KeyError
        except ValueError:
            if 'cnv' in qc_dict.keys():
                result_dict['CNV_total'] = qc_dict['cnv']['scale13'] + qc_dict['cnv']['scale107']
            elif 'clinCnv' in qc_dict.keys():
                result_dict['CNV_total'] = qc_dict['clinCnv']['totalCnv']
            else:
                raise KeyError
        except:
            pass

        try:
            result_dict['ploidy'] = qc_dict['triploid']['ploidy']
        except:
            pass

        try:
            result_dict['trisome'] = qc_dict['triploid']['trisome']
        except:
            pass

        try:
            result_dict['CNVSeq_totalSample'] = int(qc_dict['cnvSeq']['totalSample'])
            result_dict['CNVSeq_totalAbnormal'] = int(qc_dict['cnvSeq']['totalAbnormal'])
        except ValueError:
            result_dict['CNVSeq_totalSample'] = qc_dict['cnvSeq']['totalSample']
            result_dict['CNVSeq_totalAbnormal'] = qc_dict['cnvSeq']['totalAbnormal']
        except:
            pass

        try:
            result_dict['CNVSeqPlus_100K'] = int(qc_dict['cnvSeqPlus']['k10'])
            result_dict['CNVSeqPlus_Trip'] = '-' if qc_dict['cnvSeqPlus']['k10Trip'] == [] else qc_dict['cnvSeqPlus']['k10Trip']
        except ValueError:
            result_dict['CNVSeqPlus_100K'] = qc_dict['cnvSeqPlus']['k10']
            result_dict['CNVSeqPlus_Trip'] = qc_dict['cnvSeqPlus']['k10Trip']
        except:
            pass

        # sexInfo
        try:
            sry_float = float(qc_dict['coverage']['sry_rate'])
            usp9y_float = float(qc_dict['coverage']['usp9y_rate'])

            chrX_float = float(qc_dict['coverage']['chrX_avgdepth'])
            autosome_float = float(qc_dict['coverage']['depth_autosome'])
            chrX_rate_float = chrX_float / autosome_float

            if sry_float >= 0.1 or usp9y_float >= 0.1:
                if chrX_rate_float >= 0.8:
                    sexInfo_lst = ['男', 'XXY']
                elif chrX_rate_float >= 0.3 and chrX_rate_float <= 0.7:
                    sexInfo_lst = ['男', 'XY']
                elif chrX_rate_float <= 0.1:
                    sexInfo_lst = ['男', 'XY with X deletion']
                else:
                    sexInfo_lst = ['男', 'XY with abnormal X']
            else:
                if chrX_rate_float >= 1.35:
                    sexInfo_lst = ['女', 'XXX']
                elif chrX_rate_float >= 0.8 and chrX_rate_float <= 1.35:
                    sexInfo_lst = ['女', 'XX']
                elif chrX_rate_float >= 0.3 and chrX_rate_float <= 0.7:
                    sexInfo_lst = ['女', 'XX with X deletion']
                else:
                    sexInfo_lst = ['女', 'XX with abnormal X']

            result_dict['sexInfo'] = sexInfo_lst

        except:
            pass

        # sex_calculate
        try:
            if qc_dict['coverage']['gender'] not in ['0', '1']:
                raise ValueError
            elif data_type_str == 'MITO':
                raise KeyError
            else:
                result_dict['sex_calculate'] = '男' if qc_dict['coverage']['gender'] == '1' else '女'
        except ValueError:
            result_dict['sex_calculate'] = qc_dict['coverage']['gender']
        except:
            pass

        # sex_record
        try:
            # result_dict['sex_record'] = qc_dict['sex'].strip()
            result_dict['sex_record'] = self._api_GetRequestNote(rid)['sex_recorded']
        except:
            pass

        try:
            if qc_dict['heritableVariation']['frareAltCount'] != '-1':
                result_dict['P_count'] = int(qc_dict['heritableVariation']['frareAltCount'])
            else:
                raise KeyError
        except ValueError:
            result_dict['P_count'] = qc_dict['heritableVariation']['frareAltCount']
        except:
            pass

        try:
            if qc_dict['heritableVariation']['mrareAltCount'] != '-1':
                result_dict['M_count'] = int(qc_dict['heritableVariation']['mrareAltCount'])
            else:
                raise KeyError
        except ValueError:
            result_dict['M_count'] = qc_dict['heritableVariation']['mrareAltCount']
        except:
            pass

        try:
            if qc_dict['heritableVariation']['maf_avf'] == '-1':
                raise KeyError
            temp = qc_dict['heritableVariation']['maf_avf'].split(':')  # ['48.51', '49.39']
            result_dict['PvsM'] = round(float(temp[0]) / float(temp[1]), 2)
        # except ValueError:
        except (ValueError, ZeroDivisionError) as e:
            result_dict['PvsM'] = qc_dict['heritableVariation']['maf_avf']
        except:
            pass

        try:
            if qc_dict['heritableVariation']['newAltCount'] != '-1':
                result_dict['deNovo'] = int(qc_dict['heritableVariation']['newAltCount'])
            else:
                raise KeyError
        except ValueError:
            result_dict['deNovo'] = qc_dict['heritableVariation']['newAltCount']
        except:
            pass

        self.qc_dict = result_dict



        return result_dict

    def _api_GetMixSampleQc(self, data_id: str, addr = 'lab20Api/DataQc/GetMixSampleQc') -> list:
        '''
        实验室系统 -> 数据质控 -> 二代数据 -> 详情（混样图数据）

        Parameters:
            **data_id**: str
                通常由_api_SearchByPage提供

        Returns:
            **list**:
                [99.4, 92.2, .......]
        '''
        if not isinstance(data_id, str):
            message = ''
            raise TypeError(message)
        # ============format check finish============

        api_addr = self.main_page + addr

        payload = {}
        payload['DataId'] = data_id
        payload['inp'] = 1

        r = requests.get(api_addr, params = payload, cookies = self.token_dict)
        if r.status_code != 200 or not json.loads(r.text)['success']:
            message = 'Fail to GetMixSampleQc at {addr}, get return code {code}\n\n{text} '.format(addr = api_addr,
                                                                                                        code = r.status_code,
                                                                                                        text = json.loads(r.text))
            raise ConnectionRefusedError(message)

        data_lst = json.loads(r.text)['data']
        if len(data_lst) == 0:
            message = 'Request successful, but no MixSampleQc content returned.'
            raise ValueError(message)

        result_lst = []
        for l in data_lst:
            #  l is a list
            #  [11854457, 99.84]
            result_lst.append(l[1])

        return result_lst


    def _api_GetRequestNote(self, rid = '', r_code = '', addr = 'etlApi/RequestNote/GetRequestNote') -> dict:
        '''
        数据系统 -> 数据任务 -> 申请单管理 -> （具体条目）详情
        可返回特定申请单的一些详细情况，包括录入性别，临床信息等

        Parameters:
            **rid**: str
                rid

            **r_code**: str
                申请单号

        Returns:
            if no result return raise ConnectionRefusedError
            **rid_detail_dict**: defaultdict(lambda: None)    # dict
        '''

        if not isinstance(rid, str):
            message = ''
            raise TypeError(message)

        if not isinstance(r_code, str):
            message = ''
            raise TypeError(message)
        # =============format check finish==============
        rid_detail_dict = defaultdict(lambda: None)

        payload = {'RID': rid,
                   'RCode': r_code,
                   }

        api_addr = self.main_page + addr
        r = requests.get(api_addr, params = payload, cookies = self.token_dict)
        if r.status_code != 200 or not json.loads(r.text)['success']:
            message = 'Fail to GetRequestNote at {addr}, get return code {code}\n\n{text} '.format(addr = api_addr,
                                                                                                        code = r.status_code,
                                                                                                        text = json.loads(r.text))
            raise ConnectionRefusedError(message)

        try:
            rid_detail_dict['rid'] = json.loads(r.text)['data']['RID']
            rid_detail_dict['r_code'] = json.loads(r.text)['data']['RCode']
            rid_detail_dict['BarCode'] = json.loads(r.text)['data']['BarCode']
            rid_detail_dict['GrpEncode'] = json.loads(r.text)['data']['GrpEncode']
            rid_detail_dict['bar_sn'] = json.loads(r.text)['data']['BarSn']

            rid_detail_dict['PatName'] = json.loads(r.text)['data']['PatientBase']['p_name']
            rid_detail_dict['sex_recorded'] = json.loads(r.text)['data']['PatientBase']['p_sex']
            rid_detail_dict['p_family_no'] = json.loads(r.text)['data']['PatientBase']['p_family_no']
            rid_detail_dict['family_relation'] = json.loads(r.text)['data']['FamilyTies']
            rid_detail_dict['p_test_goal'] = json.loads(r.text)['data']['RequestPatient']['p_test_goal']

            p_clinical_sign_str = json.loads(r.text)['data']['RequestPatient']['p_clinical_sign']
            if p_clinical_sign_str != '' and p_clinical_sign_str is not None:
                p_clinical_sign_str = p_clinical_sign_str.replace('\n', ' ')

            rid_detail_dict['p_clinical_sign'] = p_clinical_sign_str
            rid_detail_dict['sp_id'] = json.loads(r.text)['data']['Sample']['ExpSpecimenId']
            rid_detail_dict['urgent'] = json.loads(r.text)['data']['Sample']['UrgentStatus']
        except:
            pass

        return rid_detail_dict

    # 数据系统 -> 数据分析 -> 基础分析 -> 查询
    def _api_ExecLst(self, batch_no = None, bar_code = None, grp_encoded = None, p_family_no = None, PatName = None, status = None, startTime = '1996-01-01 00:00:00', endTime = '2099-01-01 00:00:00', addr = 'basefile/dnabusiness/apis/storage/service/v1beta1/ExecLst') -> list[dict]:
        '''
        数据系统 -> 数据分析 -> 基础分析 -> 查询

        Parameters:
            **batch_no**: str
                批次号 (batch)

            **bar_code**: str
                检测号 (serialNumber)

            **grp_encoded**: str
                项目号 (projectCoding)

            **p_family_no**: str
                家庭号 (familyNumber)

            **PatName**: str
                病人姓名 (nameOfPatient)

            **status**: str
                0, 所有；1，储存完成；2，任务就绪；3，待提交批次分析；4，分析中；5，分析成功；6，分析失败

            **startTime, endTime**: date
                任务创建时间范围

        Returns:
            **dict**:
                {aid:{}, aid:{} .......}
        '''
        if not isinstance(batch_no, str) and batch_no is not None:
            message = ''
            raise TypeError(message)

        if not isinstance(bar_code, str) and bar_code is not None:
            message = ''
            raise TypeError(message)

        if not isinstance(grp_encoded, str) and grp_encoded is not None:
            message = ''
            raise TypeError(message)

        if not isinstance(p_family_no, str) and p_family_no is not None:
            message = ''
            raise TypeError(message)

        if not isinstance(PatName, str) and PatName is not None:
            message = ''
            raise TypeError(message)

        if not isinstance(status, str) and status is not None:
            message = ''
            raise TypeError(message)
        # ===================================

        api_addr = self.payload_server + addr
        payload = {
            'pageSize': 500,
            'pageNum': 1,
            'sortOrder': 'asc',
            'sortName': '',
            'startTime': startTime,
            'endTime': endTime,
            'batch': batch_no,
            'familyNumber': p_family_no,
            'nameOfPatient': PatName,
            'serialNumber': bar_code,
            'projectCoding': grp_encoded,
            'status': status,
        }

        headers = {
                   'Content-Type': 'application/x-www-form-urlencoded;charset=UTF-8',
                   'Origin': 'http://www.dna.gz.cn',
                   'Referer': 'http://www.dna.gz.cn/',
                   'Pragma': 'no-cache',
                   }

        r = requests.post(api_addr, data = payload)
        if r.status_code != 200 or not json.loads(r.text)['message'] == 'The request is successful':
            message = 'Fail to ExecLst at {addr} with {payload}, get return code {code}\n\n{text} '.format(addr = api_addr,
                                                                                                           payload = payload,
                                                                                                           code = r.status_code,
                                                                                                           text = json.loads(r.text))
            raise ConnectionRefusedError(message)

        try:
            data_list = json.loads(r.text)['result']['content']  # data_list is a list[dict]
        except:
            message = 'Fail to ExecLst at {addr} with {payload}, get return code {code}\n\n{text} '.format(addr = api_addr,
                                                                                                           payload = payload,
                                                                                                           code = r.status_code,
                                                                                                           text = json.loads(r.text))
            raise ConnectionRefusedError(message)

        if len(data_list) == 0:
            message = '_api_ExecLst resurned empty results'
            print(message)

        aid_dict = {}
        for d in data_list:
            aid_dict[d['aid']] = d

        return aid_dict

    def _api_reRun(self, data_id: str, step: str, addr = 'basefile/dnabusiness/apis/storage/service/v1beta1/reRun') -> bool:
        '''
        数据系统 -> 数据分析 -> 基础分析 -> (条目)重新分析

        Parameters:
            **data_id**: str
                检测号

            **step**: str
                项目号

        Returns:
            **boolean**:
                True提交成功，False提交失败
        '''
        api_addr = self.payload_server + addr
        payload = {
            'dataId': data_id,
            'stepType': step
        }

        r = requests.post(api_addr, data = payload)
        if r.status_code != 200 or not json.loads(r.text)['message'] == 'The request is successful':
            message = 'Fail to reRun at {addr}, get return code {code}\n\n{text} '.format(addr = api_addr,
                                                                                            code = r.status_code,
                                                                                            text = json.loads(r.text))
            raise ConnectionRefusedError(message)

        return True


    def _api_LoadSampleLogData(self, bar_sn: str, addr = 'lab20Api/Sample/LoadSampleLogData') -> tuple[str, list[dict]]:
        '''
        实验室系统 -> 样本查询 -> 样本查询 -> 样本条目的日志

        用于查询特定样本的详细日志

        Parameters:
            **bar_sn**: str
                样本ID， 例如10175038

        Returns:
            **req_code**: str
                申请单编码， 例如‘10128656’

            **log_lst**: list
                日志列表, 每个元素为一个字典：

                {'desc': "DNA质检"  # could be None

                'date': "2022-03-11 10:55:40"

                'remark': "“DNA质检”的实验结果为“合格”。"

                'user_name': "廖水玉"}
        '''
        if not isinstance(bar_sn, str):
            message = ''
            raise TypeError(message)
        # format check finish

        api_addr = self.main_page + addr

        payload = {}
        payload['bar_sn'] = bar_sn

        r = requests.get(api_addr, params = payload, cookies = self.token_dict)
        if r.status_code != 200 or not json.loads(r.text)['success']:
            message = 'Fail to LoadSampleLogData at {addr} with {payload}, get return code {code}\n\n{text} '.format(addr = api_addr,
                                                                                                                     payload = payload,
                                                                                                                     code = r.status_code,
                                                                                                                     text = json.loads(r.text))
            raise ConnectionRefusedError(message)

        data_lst = json.loads(r.text)['data']
        req_code = ''
        log_lst = []
        for d in data_lst:
            # LogDes: "DNA质检"
            # log_bar_sn: "10175038"
            # log_date: "2022-03-11 10:55:40"
            # log_remark: "“DNA质检”的实验结果为“合格”。"
            # log_req_code: "10128656"
            # log_user_name: "廖水玉"
            try:
                req_code = d['log_req_code']

                temp_dict = {}
                temp_dict['desc'] = d['LogDes']
                temp_dict['date'] = d['log_date']
                temp_dict['remark'] = d['log_remark']
                temp_dict['user_name'] = d['log_user_name']

                log_lst.append(temp_dict)
            except:
                continue

        return req_code, log_lst

    def _api_GetDataRequest(self, addr = '/mgene/dataresposeNew/GetDataRequest'):
        '''
        订单系统 -> 申请单管理 -> 数据申请单_新版

        显示数据交付申请单

        Parameters:
            **无**

        Returns:
            **DataRequest_dict**: dict
                申请单字典,
                key是DRCode，例如b7345314-4a5e-4a8f-aaec-f466229b151a
                value是字典，例如：
                {'CreateDate': "2022-05-19 16:01"  # could be None
                'CreateUserName': "李俊英"
                'organization': "济南市妇幼保健院"
                'FinalDealUserName': "姚天然"
                'FinalDealDate':"2022-05-16 09:46"
                'base':9646162424  # 数据量（base）
                'sample': 10  #  样本量}
        '''
        OMS_SERVER = '/mgene/dataresposeNew/GetOmsRequest'  # 用于查询申请单细节

        payload = {'pageIndex': 1,
                   'pageSize': 10000,
                   'sortOrder': 'desc',
                   'posttype': 1, }
        search_addr = self.main_page + addr

        r = requests.get(search_addr, params = payload, cookies = self.token_dict)
        if r.status_code != 200:
            message = 'Attempt to get GetDataRequest at {addr}, get return code {re}'.format(addr = search_addr,
                                                                                  re = r.status_code)
            raise ConnectionRefusedError(message)

        if json.loads(r.text)['total'] == 0:
            return {}

        data_lst = json.loads(r.text)['rows']
        result_dict = defaultdict(lambda: {})
        for d in data_lst:
            '''
            d is a dictionary
            {'CreateUserName': '王欢',
            'FinalCheckUserName': None,
            'FinalDealUserName': '洪奋',
            'dep_name': '安徽',
            'DealCountTime': '1',
            'DRCode': '4d7b2f4a-0de4-4e03-96de-60c899458e70',
            'SortOrder': 1719,
            'StatusCode': '607',
            'RequestType': '',
            'isCustomType': '',
            'Status': '交付',
            'TypeString': '101',
            'BarList': '',
            'GrpEncodeList': '',
            'Reason': '科研需要，主任要求返回',
            'Remark': '',
            'Agreement': '',
            'ManHour': 30.0,
            'DelayReceiveDate': None,
            'RegularType': None,
            'ExpectedDate': '2022-06-28 00:00',
            'CreateDate': '2022-05-16 11:33',
            'CreateUser': '994',
            'FinalCheckUser': None,
            'FinalCheckDate': None,
            'FinalDealUser': '17',
            'FinalDealDate': '2022-05-17 08:58',
            'IsLock': 1,
            'DelFlag': '0',
            'Ext1': '安徽医科大学第一附属医院',
            'Ext2': '528',
            'Ext3': '1',
            'Ext4': '7968532890',
            'Ext5': ''}
            '''
            print('#', end = '')
            DRCode = d['DRCode']

            result_dict[DRCode]['CreateDate'] = d['CreateDate']
            result_dict[DRCode]['CreateUserName'] = d['CreateUserName']
            result_dict[DRCode]['organization'] = d['Ext1']
            result_dict[DRCode]['FinalDealUserName'] = d['FinalDealUserName']
            result_dict[DRCode]['FinalDealDate'] = d['FinalDealDate']
            try:
                result_dict[DRCode]['base'] = int(d['Ext4'])
            except Exception:
                result_dict[DRCode]['base'] = d['Ext4']

            # http://www.dna.gz.cn//mgene/dataresposeNew/GetOmsRequest?DRcode=9f7d932b-f4ca-4a5a-8238-6b3647e76459&pageIndex=1&pageSize=1000000
            oms_payload = {'pageIndex': 1,
                       'pageSize': 1000000,
                       'DRcode': DRCode,
                           }
            search_addr = self.main_page + OMS_SERVER
            r = requests.get(search_addr, params = oms_payload, cookies = self.token_dict)
            if r.status_code != 200:
                message = 'Attempt to get GetOmsRequest at {addr} with {payload}, get return code {re}'.format(addr = search_addr,
                                                                                                               payload = oms_payload,
                                                                                                               re = r.status_code)
                total_int = None
            else:
                try:
                    total_int = json.loads(r.text)['total']
                except Exception:
                    total_int = None

            result_dict[DRCode]['sample'] = total_int

        print()
        return result_dict

    # get the CNV plot from OSS files
    def _get_CNV_UPD_plot(self, bar_code: str, grp_encoded: str, folder = '.', prefix = ''):
        '''
        从OSS数据维护系统中下载CNV图和UPD图

        Parameters:
            **bar_code**: str
                检测号 (serialNumber)

            **grp_encoded**: str
                项目号 (projectCoding)

            **folder**: str
                图片存放路径

        Returns:
            **result_lst**: [boolean]
                True on success
        '''
        result_lst = []
        print('获取文件列表{}-{}...'.format(bar_code, grp_encoded), end = ' ')
        file_dict = self._api_SearchOssReportByPage(bar_code = bar_code, grp_encoded = grp_encoded, direction = 2)
        print(len(file_dict))
        for file_id, d in file_dict.items():
            '''
            file_id: 61d973cb0916877b4f8e3c2a
            d:
            {'file_id': '61d973cb0916877b4f8e3c2a',
             'file_name': '108102-9130-WES-UPD_chrall.png',
             'file_time': '2022-01-08 19:18:04',
             'file_url': 'http://172.20.0.32:9009/opengk/data/1641629175980/108102-9130-WES-UPD_chrall.png',
             'file_url_WAN': 'http://113.98.96.228:9005/mio/opengk/data/1641629175980/108102-9130-WES-UPD_chrall.png',
             'BarCode': '108102',
             'BatchNo': 'M2000-374',
             'Direction': 2,
             'GrpEncode': '9130',
             'GrpItem': '全外显-AmCare',
             'file_md5': '3bb4bb7f8bc047095ed24d1a88ecd506',
             'file_type': None,
             'r_code': '1000142239'}
            '''

            # CNVseqPLUS-Coverage_qc.txt
            # UPD_chrall.png
            # CNV_all_negative_CNV_plot.png
            if d['file_name'].endswith('_negative_CNV_plot.png') or d['file_name'].endswith('UPD_chrall.png'):
                try:
                    print('尝试下载 {}'.format(d['file_url_WAN']), end = '...')
                    r = self._api_download(folder, d['file_url_WAN'], prefix, inner = False)  # r is True if success
                    result_lst.append(r)
                    print('成功')
                except:
                    message = '失败'
                    print(message)
                    continue

        return result_lst


    # input a file name and get its path on remote server
    # batch_no, bar_code, grp_encode, p_name, file_path = _get_server_path(file_name = 'M2000-406-111738-8812-SZX-EXON-25_L01_1.fq.gz')
    def _get_server_path(self, file_name: str) -> str:
        '''
        # input a file name and get its path on remote server

        for example _get_server_path('M2000-352-102162-1002-LSL-EXON-41_L01_2.fq.gz')
        will return batch_no, bar_code, grp_encode, p_name, file_path

        核心： /mfs/osstore/LAB/HUADA/M2000-352/M2000-352-102162-1002-LSL-EXON-41_L01_2.fq.gz

        委外： /mfs/osstore/LAB/WES/W731/W731-105511-9100-LYF_L3_1.fq.gz

        重庆： /mfs/osstore/LAB/CHONGQING/A2000-157/A2000-157-107199CQ-2400-ZH-EXON-57_L01_1.fq.gz

        长沙： /mfs/osstore/LAB/CHANGSHA/C2000-023/C2000-023-108810XE-9130-YH-WES-124_L01_1.fq.gz

        郑州： /mfs/osstore/LAB/ZHENGZHOU/ZS2000-007/ZS2000-007-107480ZS-4100-PD21678JMF-EXON-99_L01_1.fq.gz

        北京协和： /mfs/osstore/LAB/BEIJING/X2000-001/X2000-001-109002BX-4110-BJXHYSY008-EXON-48_L01_1.fq.gz
        '''
        PREFIX = '/mfs/osstore/LAB/'

        batch_no = ''
        bar_code = ''
        grp_encode = ''
        p_name = ''
        direction_str = ''
        file_path = ''

        temp_lst = re.split(r'-|_', file_name)

        if len(temp_lst) not in [6, 9]:
            message = 'Can not parse file name "{}"'.format(file_name)
            raise ValueError(message)

        if temp_lst[-1] == '1.fq.gz' or temp_lst[-1] == 'R1.fq.gz':
            direction_str = '1'
        elif temp_lst[-1] == '2.fq.gz' or temp_lst[-1] == 'R2.fq.gz':
            direction_str = '2'
        else:
            message = 'Can not parse direction "{}"'.format(file_name)
            raise ValueError(message)

        if re.match(r'W\d+', file_name) is not None:  # 委外
            batch_no = temp_lst[0]
            bar_code = temp_lst[1]
            grp_encode = temp_lst[2]
            p_name = temp_lst[3]
            file_path = PREFIX + 'WES/' + batch_no + '/' + file_name
        else:  # 非委外
            batch_no = '-'.join(temp_lst[:2])
            bar_code = temp_lst[2]
            grp_encode = temp_lst[3]
            p_name = temp_lst[4]
            if file_name.startswith('M2000'):  # 核心
                file_path = PREFIX + 'HUADA/' + batch_no + '/' + file_name
            elif file_name.startswith('A2000'):  # 重庆
                file_path = PREFIX + 'CHONGQING/' + batch_no + '/' + file_name
            elif file_name.startswith('C2000'):  # 长沙
                file_path = PREFIX + 'CHANGSHA/' + batch_no + '/' + file_name
            elif file_name.startswith('ZS2000'):  # 郑州
                file_path = PREFIX + 'ZHENGZHOU/' + batch_no + '/' + file_name
            elif file_name.startswith('X2000'):  # 郑州
                file_path = PREFIX + 'BEIJING/' + batch_no + '/' + file_name
            else:
                message = 'Can not convert local path {} to remote server path {}/.......'.format(file_name, PREFIX)
                raise ValueError(message)

        return batch_no, bar_code, grp_encode, p_name, direction_str, file_path


    def _watch_analysis(self, bar_code: list[str], grp_encoded: list[str], time_out = 60) -> bool:
        '''
        监视特定检测号和项目号的分析，失败则重新启动分析
        time_out： 监视时间（分钟）

        '''
        STEP_SERVER = 'basefile/dnabusiness/apis/storage/service/v1beta1/stepList'  # 用于获取可重跑的步骤

        try:
            step_url = self.payload_server + STEP_SERVER
            # step_url = 'http://192.168.1.20:30755/dnabusiness/apis/storage/service/v1beta1/stepList'
            r = requests.get(step_url)
            step_lst = json.loads(r.text)['result']
        except:
            step_lst = []

        if step_lst != []:
            message = '成功获取可重跑的步骤 {}'.format(step_lst)
            print(message)

        time_float = time.time()

        barcode_grpcode_lst = list(zip(bar_code, grp_encoded))
        status_dict = {}  # 记录每个样本是否成功， False不成功， True成功
        for tu in barcode_grpcode_lst:
            status_dict[tu] = False

        while not all(status_dict.values()):
            for b, g in zip(bar_code, grp_encoded):
                time.sleep(0.5)
                if status_dict[(b, g)]:  # 如果已经分析成功，则直接跳过
                    continue

                self.reset()
                print('获取状态 {}-{}: '.format(b, g), end = '')
                try:
                    # r = self._api_ExecLst(bar_code = b, grp_encoded = g, addr = 'dnabusiness/apis/storage/service/v1beta1/ExecLst')  # 获取状态
                    r = self._api_ExecLst(bar_code = b, grp_encoded = g)  # 获取状态
                except:
                    continue

                if len(r) == 0:
                    message = '获取状态结果为空，跳过'
                    print(message)
                    continue

                aid = list(r.keys())[0]
                data_id = r[aid]['dataId']
                status = r[aid]['status']
                current_step = r[aid]['currentStep']
                if status == '分析失败':
                    step = re.findall(r'[A-Za-z]+', current_step)[0]
                    if step_lst != [] and step not in step_lst:  # step_lst 可重跑的步骤
                        message = '失败步骤 {} 不在可重跑步骤中 {}, 跳过'.format(step, step_lst)
                        print(message)
                        continue

                    try:
                        # _ = self._api_reRun(data_id = data_id, step = step, addr = 'dnabusiness/apis/storage/service/v1beta1/reRun')
                        _ = self._api_reRun(data_id = data_id, step = step)
                        print('失败步骤 {} 重跑'.format(step))
                    except:
                        message = '步骤 {} 重跑失败，跳过'.format(step)
                        print(message)
                        continue
                elif status == '分析成功':
                    status_dict[(b, g)] = True
                    print(status)
                else:
                    print('{}, 当前步骤 {}'.format(status, current_step))


            if time.time() - time_float > time_float * 60:
                message = '{} minutes time_out'.format(time_out)
                print(message)
                print(status_dict)
                break

        return True

    def reset(self):
        self.files_dict = defaultdict(lambda: defaultdict(lambda: None))
        self.rid_dict = defaultdict(lambda: defaultdict(lambda: None))
        self.qc_dict = defaultdict(lambda: None)
        return None
