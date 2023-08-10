# -*- coding: utf-8 -*-

from sao_etl_lib.sao_etl_json import name_to_json
from sao_etl_nice.inheritance.Inheritance_sao_etl import InheritanceSaoEtl
import calendar
import copy
from calendar import monthrange


class NiceVatClose(InheritanceSaoEtl):

    def __init__(self, sao_db_connection, schema_name, com_info, crypto_obj, default_date_version=None, base_da_standard_begin=None):
        super().__init__(sao_db_connection, schema_name, com_info, default_date_version, base_da_standard_begin)
        self._company_info = com_info
        self._schema_name = schema_name
        self._reporter = []
        self._crypto_obj = crypto_obj

    def sao_etl_start(self):
        return self.nice_vat_test()

    def nice_vat_test(self):
        params = {
            # "cno": self._company_info[0]["cno"],
            "ccode": self._schema_name,
            "no_biz": self._company_info[0]["no_biz"],
            "li_vat_close": self._company_info[0]["li_vat_close"],
            "li_tax_check": self._company_info[0]["li_tax_check"]
        }

        ls_info = [vat_close for vat_close in str(self._company_info[0]["li_vat_close"]).split(',') if vat_close]

        ### 1. 제출자정보 및 회사 정보 가져오기 ###
        self._sao_db_connection.cursor.execute(self.select_company_info(), params)
        com_info = name_to_json(self._sao_db_connection.cursor)
        self._crypto_obj.decrypt_data(data=com_info, cols={"ceoregno": "social", "bizregno": "social"})

        # # 회사 정보가 없으면 잘못된 회사
        # try:
        #     exists_com = com_info[0]['danggi_gisu']
        # except:
        #     return []

        # 세무대리인 DB접근 불가능으로 정보 깝데기만 가져오고 data_offer에서 정보 update
        self._reporter = []
        # # 수임처 회사의 ftx_trade[0]['cno']를 가지고 제출자 정보 가져오기
        # if str(com_info[0]['ty_report']) == '1':  # 세무대리
        #     self._sao_db_connection.cursor.execute(self.select_fta_reporter(), params)
        #     self._reporter = name_to_json(self._sao_db_connection.cursor)
        # else:  # 일반회사   #일반회사 이면 자기 자신의 DB에서 제출자 정보 가져오기
        #     self._sao_db_connection.cursor.execute(self.select_fta_reporter(), params)
        #     self._reporter = name_to_json(self._sao_db_connection.cursor)

        # 세무대리인 정보 임시 대응 소스
        if len(self._reporter) == 0:
            temp_dict = {
                "accnm": "",
                "accmngno": "",
                "acctelno": "",
                "accbizno": "",
                "acccompnm": "",
                "nm_userid": "",
                "tel_com1": "",
                "tel_com2": "",
                "tel_com3": "",
                "ty_bookkeep": "",
                "ty_fs_src": ""
            }
            self._reporter.append(temp_dict)

        # ## 2. CI.xml 및 CompanyInfo.xml 파일 만들기 ###
        ############################################################################
        ## com_info 와 reporter를 가지고 CI.xml 및 CompanyInfo.xml 파일을 제작하시면 됩니다 ##
        ############################################################################

        result = []
        for info in ls_info:
            temp_data = {}
            if str(info)[4:5] == '1':
                params['da_fndend'] = str(info)[0:4] + '03'
            elif str(info)[4:5] == '2':
                params['da_fndend'] = str(info)[0:4] + '06'
            elif str(info)[4:5] == '3':
                params['da_fndend'] = str(info)[0:4] + '09'
            elif str(info)[4:5] == '4':
                params['da_fndend'] = str(info)[0:4] + '12'

            params['old_view'] = 0
            if params['da_fndend'] >= com_info[0]['danggi_da_accbegin'][0:6]:
                params['old_view'] = 0
                diff_da_acc = int(str(info)[0:4]) - int(com_info[0]['danggi_da_accbegin'][0:4])
                params['gisu'] = int(com_info[0]['danggi_gisu']) + diff_da_acc
            else:
                params['old_view'] = 1
                diff_da_acc = int(com_info[0]['danggi_da_accbegin'][0:4]) - int(str(info)[0:4])
                params['gisu'] = int(com_info[0]['danggi_gisu']) - diff_da_acc

            ### 3. 최종 부가세 마감 정보 가져오기 ###
            self._sao_db_connection.cursor.execute(self.select_vat_close_g(), params)
            vat_info = name_to_json(self._sao_db_connection.cursor)
            if vat_info:
                params.update(vat_info[0])
            else:
                continue

            # 부가세 마감 string 조회
            self._sao_db_connection.cursor.execute(self.select_get_close_string(), params)
            vat_list = name_to_json(self._sao_db_connection.cursor)

            # 부가세 헤더 만들기
            close_head = self.make_vat_head(self._reporter[0], com_info[0], vat_info[0], vat_list)
            vat_list[0]['str_close'] = close_head

            # ## 4. 부가세 XML 구성 파일 만들기 - (1)의 제출자정보 및 회사 정보 활용 ###
            vat_xml_data = self.make_vat_xml_data(params)

            #####################################################
            ## vat_xml_data 을 가지로 부가세 xml 파일을 제작하시면 됩니다 ##
            #####################################################

            # ## 5. 합계표 전자분 마감 string 만들기 ###
            bill_close = self.make_billcont_str_close(params)
            # print(bill_close)

            # ## 6. 마감 스트링 한줄로 만들기 + 합계표 마감 스트링 붙이기 ###
            final_string = ''
            for str_close in vat_list:
                final_string += str_close['str_close']

            if bill_close['close_45'] is not None and len(bill_close['close_45']) > 0:
                final_string += bill_close['close_45']
            if bill_close['close_46'] is not None and len(bill_close['close_46']) > 0:
                final_string += bill_close['close_46']
            if bill_close['close_47'] is not None and len(bill_close['close_47']) > 0:
                final_string += bill_close['close_47']
            if bill_close['close_48'] is not None and len(bill_close['close_48']) > 0:
                final_string += bill_close['close_48']

            ################################################
            ## final_string 을 가지로 ers 파일을 제작하시면 됩니다 ##
            ################################################

            period_start = None
            period_end = None
            if str(info)[4:5] == '1':
                period_start = str(info)[0:4] + '01'
                period_end = str(info)[0:4] + '03'
            elif str(info)[4:5] == '2':
                period_start = str(info)[0:4] + '04'
                period_end = str(info)[0:4] + '06'
            elif str(info)[4:5] == '3':
                period_start = str(info)[0:4] + '07'
                period_end = str(info)[0:4] + '09'
            elif str(info)[4:5] == '4':
                period_start = str(info)[0:4] + '10'
                period_end = str(info)[0:4] + '12'

            temp_data["period_start"] = period_start
            temp_data["period_end"] = period_end

            temp_data["xml_data"] = vat_xml_data
            temp_data["ers_data"] = final_string

            result.append(temp_data)

            # print(temp_data)

        return result

    ## 개별 함수 영역 #########################################################################
    # 부가세 xml생성용 데이터 조회
    def make_vat_xml_data(self, params):
        vat_xml_data = ''

        # 부가세 신고서
        self._sao_db_connection.cursor.execute(self.select_vat(params['ty_simple']), params)
        vat_data = name_to_json(self._sao_db_connection.cursor)

        # 부가세 신고서 신보용
        self._sao_db_connection.cursor.execute(self.select_sinbo(params['ty_simple']), params)
        sinbo_data = name_to_json(self._sao_db_connection.cursor)

        bill_params = copy.deepcopy(params)
        # 1.정기, 2.조기, 3.폐업 -> 1.정기 / 3.수정, 4경정 -> 3.수정
        if int(bill_params['ty_rpt']) in (1, 2, 5):
            bill_params['ty_rpt'] = 1
        else:
            bill_params['ty_rpt'] = 3

        # 세금계산서 합계표 / 계산서 합계표
        # self._sao_db_connection.cursor.execute(self.select_detail(), bill_params)
        # detail_data = name_to_json(self._sao_db_connection.cursor)

        # 세금계산서 합계표 / 계산서 합계표 json
        self._sao_db_connection.cursor.execute(self.select_detail_js(), bill_params)
        detail_data_js = name_to_json(self._sao_db_connection.cursor)

        for data_js in detail_data_js:
            self._crypto_obj.dec_enc_data(data=data_js['js_detail_data'], cols={"v_biz_res_no": "social"})

        result_data = {
            'vat_data': vat_data,
            'sinbo_data': sinbo_data,
            # 'detail_data': detail_data,
            'detail_data_js': detail_data_js
        }

        return result_data

    # 합계표 전자분 데이터 조회하여 마감 스트링 제작
    def make_billcont_str_close(self, params):
        bill_params = copy.deepcopy(params)
        # 1.정기, 2.조기, 3.폐업 -> 1.정기 / 3.수정, 4경정 -> 3.수정
        if int(bill_params['ty_rpt']) in (1, 2, 5):
            bill_params['ty_rpt'] = 1
        else:
            bill_params['ty_rpt'] = 3

        if int(bill_params['ty_rpt']) == 3:
            if int(bill_params['no_count']) == 1:
                bill_params['ty_rpt_pre'] = 1
                bill_params['no_count_pre'] = 1
            elif int(bill_params['no_count']) > 1:
                bill_params['ty_rpt_pre'] = 2
                bill_params['no_count_pre'] = bill_params['no_count'] - 1

        # 세금계산서 레이아웃
        ls_vatclo = self.set_mta_vatclo('billcont_savf0117')
        # 세금계산서 매출 cd_form = 45
        bill_params['ty_buysale'] = 1  # 매출매입 구분 1.매출 2.매입
        self._sao_db_connection.cursor.execute(self.select_billcont_45_46_bfo_dang(int(bill_params['ty_rpt']), int(bill_params['old_view'])) + self.select_billcont_45_46(int(bill_params['ty_rpt'])), bill_params)
        data_45 = name_to_json(self._sao_db_connection.cursor)

        close_45 = ''
        for str_45 in data_45:
            close_45 += self.__get_close_string('vat', str(str_45['str_close']), record_json=ls_vatclo, special_char='§')
            close_45 += '♥'

        # 세금계산서 매입
        bill_params['ty_buysale'] = 2  # 매출매입 구분 1.매출 2.매입
        self._sao_db_connection.cursor.execute(self.select_billcont_45_46_bfo_dang(int(bill_params['ty_rpt']), int(bill_params['old_view'])) + self.select_billcont_45_46(int(bill_params['ty_rpt'])), bill_params)
        data_46 = name_to_json(self._sao_db_connection.cursor)

        close_46 = ''
        for str_46 in data_46:
            close_46 += self.__get_close_string('vat', str(str_46['str_close']), record_json=ls_vatclo, special_char='§')
            close_46 += '♥'

        # 계산서 레이아웃
        ls_vatclo = self.set_mta_vatclo('billcont_savf0103')
        #    계산서 매출
        bill_params['ty_buysale'] = 1  # 매출매입 구분 1.매출 2.매입
        self._sao_db_connection.cursor.execute(self.select_billcont_47_48_bfo_dang(int(bill_params['old_view'])) + self.select_billcont_47_48(), bill_params)
        data_47 = name_to_json(self._sao_db_connection.cursor)

        close_47 = ''
        for str_47 in data_47:
            close_47 += self.__get_close_string('vat', str(str_47['str_close']), record_json=ls_vatclo, special_char='§')
            close_47 += '♥'

        #    계산서 매입
        bill_params['ty_buysale'] = 2  # 매출매입 구분 1.매출 2.매입
        self._sao_db_connection.cursor.execute(self.select_billcont_47_48_bfo_dang(int(bill_params['old_view'])) + self.select_billcont_47_48(), bill_params)
        data_48 = name_to_json(self._sao_db_connection.cursor)

        close_48 = ''
        for str_48 in data_48:
            close_48 += self.__get_close_string('vat', str(str_48['str_close']), record_json=ls_vatclo, special_char='§')
            close_48 += '♥'

        result_data = {
            'close_45': close_45,
            'close_46': close_46,
            'close_47': close_47,
            'close_48': close_48
        }

        return result_data

    # 마감서식 지정 함수
    def set_mta_vatclo(self, type):

        list = []

        if type == 'billcont_savf0117':
            list.append({'no_num': '1', 'str_type': 'c', 'wm_len': '1'})  # 자료구분
            list.append({'no_num': '2', 'str_type': 'n', 'wm_len': '10'})  # 보고자등록번호
            list.append({'no_num': '3', 'str_type': 'n', 'wm_len': '4'})  # 일련번호
            list.append({'no_num': '4', 'str_type': 'n', 'wm_len': '10'})  # 거래자등록번호
            list.append({'no_num': '5', 'str_type': 'c', 'wm_len': '30'})  # 거래자상호
            list.append({'no_num': '6', 'str_type': 'c', 'wm_len': '17'})  # 거래자업태
            list.append({'no_num': '7', 'str_type': 'c', 'wm_len': '25'})  # 거래자종목
            list.append({'no_num': '8', 'str_type': 'n', 'wm_len': '7'})  # 세금계산서매수
            list.append({'no_num': '9', 'str_type': 'n', 'wm_len': '2'})  # 공란수
            list.append({'no_num': '10', 'str_type': 'n', 'wm_len': '14'})  # 공급가액
            list.append({'no_num': '11', 'str_type': 'n', 'wm_len': '13'})  # 세액
            list.append({'no_num': '12', 'str_type': 'n', 'wm_len': '1'})  # 신고자주류코드(도매)
            list.append({'no_num': '13', 'str_type': 'c', 'wm_len': '1'})  # 주류코드(소매)
            list.append({'no_num': '14', 'str_type': 'n', 'wm_len': '4'})  # 권번호
            list.append({'no_num': '15', 'str_type': 'n', 'wm_len': '3'})  # 제출서
            list.append({'no_num': '16', 'str_type': 'c', 'wm_len': '28'})  # 공란
        elif type == 'billcont_savf0103':
            list.append({'no_num': '1', 'str_type': 'c', 'wm_len': '1'})  # 레코드구분
            list.append({'no_num': '2', 'str_type': 'n', 'wm_len': '2'})  # 자료구분
            list.append({'no_num': '3', 'str_type': 'c', 'wm_len': '1'})  # 기구분
            list.append({'no_num': '4', 'str_type': 'c', 'wm_len': '1'})  # 신고구분
            list.append({'no_num': '5', 'str_type': 'c', 'wm_len': '3'})  # 세무서
            list.append({'no_num': '6', 'str_type': 'n', 'wm_len': '6'})  # 일련번호
            list.append({'no_num': '7', 'str_type': 'c', 'wm_len': '10'})  # 사업자등록번호
            list.append({'no_num': '8', 'str_type': 'c', 'wm_len': '10'})  # 매입처사업자등록번호
            list.append({'no_num': '9', 'str_type': 'c', 'wm_len': '40'})  # 매입처법인명(상호)
            list.append({'no_num': '10', 'str_type': 'n', 'wm_len': '5'})  # 계산서매수
            list.append({'no_num': '11', 'str_type': 'n', 'wm_len': '1'})  # 매입금액음수표시
            list.append({'no_num': '12', 'str_type': 'n', 'wm_len': '14'})  # 매입금액
            list.append({'no_num': '13', 'str_type': 'c', 'wm_len': '136'})  # 공란
        elif type == 'vat_head':
            list.append({'no_num': '1', 'str_type': 'c', 'wm_len': '2'})  # 자료구분
            list.append({'no_num': '2', 'str_type': 'c', 'wm_len': '7'})  # 서식코드
            list.append({'no_num': '3', 'str_type': 'c', 'wm_len': '13'})  # 납세자ID (사업자번호)
            list.append({'no_num': '4', 'str_type': 'c', 'wm_len': '2'})  # 세목코드
            list.append({'no_num': '5', 'str_type': 'c', 'wm_len': '2'})  # 신고구분코드
            list.append({'no_num': '6', 'str_type': 'c', 'wm_len': '2'})  # 신고구분상세코드
            list.append({'no_num': '7', 'str_type': 'c', 'wm_len': '6'})  # 과세기간_년기(월)
            list.append({'no_num': '8', 'str_type': 'c', 'wm_len': '3'})  # 신고서종류코드
            list.append({'no_num': '9', 'str_type': 'c', 'wm_len': '20'})  # 사용자ID
            list.append({'no_num': '10', 'str_type': 'c', 'wm_len': '13'})  # 납세자번호
            list.append({'no_num': '11', 'str_type': 'c', 'wm_len': '30'})  # 세무대리인성명
            list.append({'no_num': '12', 'str_type': 'c', 'wm_len': '4'})  # 세무대리인전화번호1(지역번호)
            list.append({'no_num': '13', 'str_type': 'c', 'wm_len': '5'})  # 세무대리인전화번호2(국번)
            list.append({'no_num': '14', 'str_type': 'c', 'wm_len': '5'})  # 세무대리인전화번호3(지역번호,국번을제외한번호)
            list.append({'no_num': '15', 'str_type': 'c', 'wm_len': '30'})  # 상호(법인명)
            list.append({'no_num': '16', 'str_type': 'c', 'wm_len': '30'})  # 성명(대표자명)
            list.append({'no_num': '17', 'str_type': 'c', 'wm_len': '70'})  # 사업장소재지
            list.append({'no_num': '18', 'str_type': 'c', 'wm_len': '14'})  # 사업장전화번호
            list.append({'no_num': '19', 'str_type': 'c', 'wm_len': '70'})  # 사업자주소
            list.append({'no_num': '20', 'str_type': 'c', 'wm_len': '14'})  # 사업자전화번호
            list.append({'no_num': '21', 'str_type': 'c', 'wm_len': '30'})  # 업태명
            list.append({'no_num': '22', 'str_type': 'c', 'wm_len': '50'})  # 종목명
            list.append({'no_num': '23', 'str_type': 'c', 'wm_len': '7'})  # 업종코드
            list.append({'no_num': '24', 'str_type': 'c', 'wm_len': '8'})  # 과세기간시작일자
            list.append({'no_num': '25', 'str_type': 'c', 'wm_len': '8'})  # 과세기간종료일자
            list.append({'no_num': '26', 'str_type': 'c', 'wm_len': '8'})  # 작성일자
            list.append({'no_num': '27', 'str_type': 'c', 'wm_len': '1'})  # 보정신고구분
            list.append({'no_num': '28', 'str_type': 'c', 'wm_len': '14'})  # 사업자휴대전화
            list.append({'no_num': '29', 'str_type': 'c', 'wm_len': '4'})  # 세무프로그램코드
            list.append({'no_num': '30', 'str_type': 'c', 'wm_len': '13'})  # 세무대리인사업자번호
            list.append({'no_num': '31', 'str_type': 'c', 'wm_len': '50'})  # 전자메일주소
            list.append({'no_num': '32', 'str_type': 'c', 'wm_len': '65'})  # 공란

        return list

    # 마감 스트링 제작해주는 공통 함수
    from collections import namedtuple
    def __get_close_string(self, module, data_string, record_json, special_char, data_list=[]):
        # 속도문제로 data_string 구성 => 분리단계를 삭제하기위해 data_list 를 직접받는경우 data_string 의 분리단계 생략함 20180620 오윤해
        if not data_list:
            data_list = data_string.split(special_char)

        for idx, data in enumerate(data_list):
            if type(data) != bytes:
                try:
                    data_list[idx] = "".join(data.replace(chr(160), ' ').splitlines()).encode(encoding='cp949')  # 공백제거 처리
                except UnicodeEncodeError as e:
                    data_list[idx] = b''
                    for text in data:
                        try:
                            data_list[idx] += "".join(text.replace(chr(160), ' ').splitlines()).encode(encoding='cp949')  # 공백제거 처리
                        except Exception as e:
                            data_list[idx] += b''
                except Exception as e:
                    raise e

        if len(data_list) != len(record_json):
            return ''

        # type_info = namedtuple('type_info', 'nm_type str number')
        # if module == 'vat':
        #     nm_length = 'wm_len'
        #     types = type_info('str_type', 'CHAR', 'NUMBER')
        # else:
        #     nm_length = 'num_len'
        #     types = type_info('dc_type', 'X', '9')

        return_string = ''

        zip_list = list(zip(data_list, record_json))
        for data, row in zip_list:
            if not row['wm_len']:
                continue

            if type(row['wm_len']) != str:
                row['wm_len'] = str(row['wm_len'])

            length_list = row['wm_len'].split(',')
            length = int(length_list[0])

            if row['str_type'].upper() == 'N':  # 숫자
                num = float(data.decode(encoding='cp949')) if data else 0

                # 마이너스 처리
                isminus = True if num < 0 else False
                if isminus:
                    num = abs(num)
                    length -= 1

                if len(length_list) > 1:  # 소수점 처리
                    point = float(length_list[1])
                    num = (pow(10, point) * num)

                tnum = str(round(num))
                if len(tnum) > length:
                    tnum = tnum[:length]

                if isminus:
                    return_string += '-{0}'.format(tnum.zfill(length))
                else:
                    return_string += tnum.zfill(length)
            else:
                if len(data) > length:
                    data = data[:length]

                try:
                    bspace = ' '.encode(encoding='cp949')
                    return_string += data.ljust(length, bspace).decode(encoding='cp949')
                except UnicodeDecodeError:
                    return_string += data[:-1].decode(encoding='cp949') + ' '

        return return_string

    # 마감 헤더 제작하는 함수
    def make_vat_head(self, reporter, com_info, vat_info, head_rs):

        # 마감 정보의 [0]번째 서식 (01)만 가져오기
        # ls_head = str(vat_list[0]['str_close']).split('♥')

        # 마감 head 만드는 로직
        com_da_start = com_info['da_start'].strip()  # 개업일자
        com_da_end = com_info['da_end'].strip()  # 폐업일자
        com_da_chage = com_info['str_2_1_1'].strip()  # 과세유형전환일
        vat_list = head_rs[0]['str_close'].split('♥')[:-1]
        magam_sDate = head_rs[0]['dm_fndbegin'] + '01'
        magam_eDate = vat_info['dm_fndend'][0:6] + str(
            monthrange(int(vat_info['dm_fndend'][0:4]), int(vat_info['dm_fndend'][4:6]))[1])

        # vat_info['ty_simple'] = 1 if vat_list[0][0:9] == '11I103200' else 2  # 일반/간이구분

        str_close = ''

        str_close = vat_list[0][0:2] + '§'  # 1.자료구분
        str_close += vat_list[0][2:9] + '§'  # 2.서식코드
        str_close += com_info['bizregno'] + '§'  # 3.납세자ID(회사사업자번호?)
        str_close += '41' + '§'  # 4.세목코드

        # 5. 신고구분코드
        if com_da_end and int(vat_info['dm_fndbegin']) <= int(com_da_end[:6]) and int(vat_info['dm_fndend']) >= int(
                com_da_end[:6]):
            str_close += '01§'  # 확정신고
        else:  # 폐업일자없으면
            if vat_info['ty_simple'] == 1:  # 일반
                str_close += '03§' if int(vat_info['dm_fndend'][4:6]) in [1, 2, 3, 7, 8, 9] else '01§'
            else:  # 간이 20190701
                if com_da_chage[0:4] == vat_info['dm_fndend'][0:4]:  # 과세유형전환자는 확정신고로 기재
                    str_close += '01§'
                else:  # 간이일경우 조회종료일이 6월이면 03 예정신고, 12월이면 01 확정신고
                    str_close += '03§' if int(vat_info['dm_fndend'][4:6]) < 7 else '01§'

        # 6. 신고구분상세코드
        if str(vat_info['ty_rpt']) == '1':  # 신고구분상세코드(01.정기, 02.기한후, 03.수정) 이건 ty_rpt로 대체
            str_close += '01§'
        elif str(vat_info['ty_rpt']) == '3':
            str_close += '02§'
        else:  # 기한후이면 마감 스트링에 기한후(과세표준)여부 판단
            if len(vat_list) >= 1:
                tmp = vat_list[1].encode(encoding='cp949')
                tmp_rs = (tmp[1039:1040] if tmp[0:9].decode(encoding='cp949') == '17I103200' else tmp[403:404]).decode(
                    encoding='cp949')  # 일반 1039, 간이 403

                str_close += '03§' if tmp_rs == 'Y' else '§'  # 기한후(과세표준)여부가 Y이면 03 아니면 공백

        # 7. 과세기간_년기(월)
        if str(vat_info['ty_simple']) == '1':  # 일반
            str_close += vat_info['dm_fndend'][0:4] + ('01§' if int(vat_info['dm_fndend'][4:6]) < 7 else '02§')
        else:  # 간이사업자
            str_close += vat_info['dm_fndend'][0:4] + '01§'

        # 8. 신고서종류코드
        if str(vat_info['ty_simple']) == '1':  # 일반
            if com_da_end and int(vat_info['dm_fndbegin']) <= int(com_da_end[:6]) and int(vat_info['dm_fndend']) >= int(
                    com_da_end[:6]):
                str_close += 'C07§'
            else:
                if int(vat_info['dm_fndend'][4:6]) in (4, 10):  # 부가가치세 확정(일반) 4,10월조기 신고서
                    str_close += 'C05§'
                elif int(vat_info['dm_fndend'][4:6]) in (5, 11):  # 부가가치세 확정(일반) 5,11월조기 신고서
                    str_close += 'C06§'
                elif int(vat_info['dm_fndend'][4:6]) in (6, 12):  # 부가가치세 확정(일반) 신고서
                    str_close += 'C07§'
                elif int(vat_info['dm_fndend'][4:6]) in (1, 7):  # 부가가치세 예정(일반) 1,7월조기 신고서
                    str_close += 'C15§'
                elif int(vat_info['dm_fndend'][4:6]) in (2, 8):  # 부가가치세 예정(일반) 2,8월조기 신고서
                    str_close += 'C16§'
                elif int(vat_info['dm_fndend'][4:6]) in (3, 9):  # 부가가치세 예정(일반) 신고서
                    str_close += 'C17§'
        else:  # 간이
            if com_da_end and int(vat_info['dm_fndbegin']) <= int(com_da_end[:6]) and int(vat_info['dm_fndend']) >= int(
                    com_da_end[:6]):  # 폐업회사면
                str_close += 'C03§'
            else:
                if str(vat_info['ty_simple']) == '1':  # 일반
                    str_close += 'C13§' if int(vat_info['dm_fndend'][4:6]) in [1, 2, 3, 7, 8, 9] else 'C03§'
                else:  # 간이 20190701
                    if com_da_chage[0:4] == vat_info['dm_fndend'][0:4]:  # 과세유형전환자는 확정신고로 기재
                        str_close += 'C03§'
                    else:  # 간이일경우 조회종료일이 6월이면 03 예정신고, 12월이면 01 확정신고
                        str_close += 'C13§' if int(vat_info['dm_fndend'][4:6]) < 7 else 'C03§'

        # 9. 사용자ID : 홈택스시스템에 등록된 사용자(개인 또는 세무대리인)의 ID, USERID 제출자정보
        # str_close += reporter['nm_userid'] + '§'  # id_hometax
        str_close += '|reporter_nm_userid|' + '§'  # id_hometax

        # 10. 납세자번호 : 주민등록번호(개인) 또는 법인등록번호(법인), 비영리법인은 공백(SPACE)
        str_close += (com_info['ceoregno'] if str(com_info['corprvgbn']) == '1' else com_info['corno']) + '§'

        str_close += '|reporter_accnm|' + '§'  # 11. 세무대리인성명
        str_close += '|reporter_tel_com1|' + '§'  # 12. 세무대리인전화번호 (지역번호)
        str_close += '|reporter_tel_com2|' + '§'  # 13. 세무대리인전화번호 (국번)
        str_close += '|reporter_tel_com3|' + '§'  # 14. 세무대리인전화번호 (나머지) (지역번호,국번을제외한번호)

        # if str(com_info['ty_report']) == '1':
        #     str_close += reporter['accnm'] + '§'  # 11. 세무대리인성명
        #     str_close += reporter['tel_com1'] + '§'  # 12. 세무대리인전화번호 (지역번호)
        #     str_close += reporter['tel_com2'] + '§'  # 13. 세무대리인전화번호 (국번)
        #     str_close += reporter['tel_com3'] + '§'  # 14. 세무대리인전화번호 (나머지) (지역번호,국번을제외한번호)
        # else:
        #     str_close += '§§§§'  # 다 공백

        # 15.상호(법인명), 16.성명(대표자명), 17.사업장소재지, 18.사업장전화번호
        str_close += com_info['companyname'] + '§'  # 상호(법인명)
        str_close += com_info['ceoname'] + '§'  # 성명(대표자명)
        str_close += com_info['address'] + '§'  # 사업장소재지
        str_close += com_info['tel_com'] + '§'  # 사업장전화번호

        # 19.사업자주소, 20.사업자전화번호
        if str(com_info['corprvgbn']) == '1':
            str_close += com_info['add_ceo'] + '§'  # 사업자주소
            str_close += com_info['tel_ceo'] + '§'  # 사업지전화번호 (개인)
        else:
            str_close += com_info['address'] + '§'  # 사업장소재지
            str_close += com_info['tel_com'] + '§'  # 사업장전화번호 (법인)

        # 21.업태명, 22.종목명, 23.업종코드 : 세무서에 사업자등록시 등록한 주업종코드를 기재합니다.
        str_close += com_info['bizcondnm'] + '§'
        str_close += com_info['bizitemnm'] + '§'
        str_close += com_info['bizcategory'] + '§' if com_info.get('bizcategory') is not None else '$'

        # 24. 과세기간시작일자 : ‘YYYYMMDD’의 형식이며 이중 MMDD는 다음과 같은 값을 갖는다.
        #      - 1 기 : 0101 부터 0630 까지 가능 (예정신고인 경우 0101 부터 0331 까지 가능)
        #      - 2 기 : 0701 부터 1231 까지 가능 (예정신고인 경우 0701 부터 0930 까지 가능)
        #     ※ 간이사업자의 경우 확정신고시 12개월분을 신고하므로 예정신고와 무관하게 과세년도+ 0101 로 기재합니다. (단, 년중 개업사업자는 개업일, 일반->간이 과세유형전환사업자는 과세유형전환일)
        if str(vat_info['ty_simple']) == '1':  # 일반
            # 개인사업자는 개업일로 -> 개업일만 체크
            if com_da_start and int(magam_sDate) <= int(com_da_start) and int(magam_eDate) >= int(com_da_start):
                tmp_rs = com_da_start
            else:
                tmp_rs = magam_sDate
        else:
            # 과세유형전환일이 조회범위안에 존재하면 과세유형전환일로
            if com_da_chage and len(com_da_chage) == 8 and int(magam_sDate) <= int(com_da_chage) and int(
                    magam_eDate) >= int(com_da_chage):
                tmp_rs = com_da_chage
            else:  # 개인사업자는 개업일로
                # if com_rs['yn_private'] and com_da_start and int(magam_sDate) <= int(com_da_start) and int(magam_eDate) >= int(com_da_start):
                if com_da_start and int(magam_sDate) <= int(com_da_start) and int(magam_eDate) >= int(com_da_start):
                    tmp_rs = com_da_start
                else:
                    tmp_rs = magam_sDate[0:4] + '0101'
        str_close += tmp_rs + '§'

        # 25.과세기간종료일자
        if com_da_end and int(vat_info['dm_fndbegin']) <= int(com_da_end[:6]) and int(vat_info['dm_fndend']) >= int(
                com_da_end[:6]):  # 폐업회사면
            str_close += com_da_end + '§'
        else:
            str_close += vat_info['dm_fndend'] + str(
                calendar.monthrange(int(vat_info['dm_fndend'][:4]), int(vat_info['dm_fndend'][4:6]))[-1]) + '§'

        # 26.작성일자, 27.보정신고구분, 28.사업자휴대전화, 29.세무프로그램코드, 30.세무대리님사업자번호
        self._sao_db_connection.cursor.execute(self.select_close_date_query(), vat_info)
        da_write = name_to_json(self._sao_db_connection.cursor)

        str_close += da_write[0]['str_save'] + '§' if len(da_write) > 0 else '' + '§'  # 26. 작성일자
        str_close += 'N§'  # 27. 보정신고구분
        str_close += com_info['cel_dtem'] + '§'  # 28. 사업자휴대전화 (신고담당자)
        str_close += '1001§'  # 29. 세무프로그램코드
        # str_close += (reporter['accbizno'] if com_info['ty_report'] == '1' else '') + '§'  # 30.세무대리님사업자번호
        str_close += ('|reporter_accbizno|' if com_info['ty_report'] == '1' else '') + '§'  # 30.세무대리님사업자번호

        # 31.전자메일주소, 32.공란
        str_close += ' ' + '§'  # 31.전자메일주소 - 사용안함
        str_close += ' '  # 32.공란

        # ls_vatclo = self.set_mta_vatclo('vat_head')

        # close_head = self.__get_close_string('vat', str_close, record_json=ls_vatclo, special_char='§')  # + '♥'

        close_head = str_close
        vat_list[0] = close_head

        fr_str = ''
        for str_close in vat_list:
            fr_str += str_close + '♥'

        return fr_str

    ## 쿼리 영역 #########################################################################

    def select_vat_close_g(self):
        query = """

    select
        vat.*, close_g.dm_fndbegin, close_g.dm_fndend, 
        close_g.no_count as no_count_g, 
        close_g.ty_rpt as ty_rpt_g
    from (
            select 
                dm_fndbegin, dm_fndend, max(no_count) as no_count, max(ty_rpt) as ty_rpt
            from fta_vatclose_g
            where dm_fndend = %(da_fndend)s
              and cd_form in ('01', '02')
            group by dm_fndbegin, dm_fndend

            union all 

            select 
                dm_fndbegin, dm_fndend, max(no_count) as no_count, max(ty_rpt) as ty_rpt
            from fta_vatclose_g_before
            where dm_fndend = %(da_fndend)s
              and cd_form in ('01', '02')
            group by dm_fndbegin, dm_fndend
         ) as close_g 
    inner join (
        select da_fndbegin, da_fndend, ty_rpt, max(no_count) as no_count, ty_simple, ty_month, prd_vat
        from fta_vatrpt
        where da_fndend = %(da_fndend)s || '00'
        group by da_fndbegin, da_fndend, ty_rpt, ty_simple, ty_month, prd_vat

        union all 

        select da_fndbegin, da_fndend, ty_rpt, max(no_count) as no_count, ty_simple, ty_month, prd_vat
        from fta_vatrpt_before
        where da_fndend = %(da_fndend)s || '00'
        group by da_fndbegin, da_fndend, ty_rpt, ty_simple, ty_month, prd_vat
    ) as vat on close_g.dm_fndbegin || '00' = vat.da_fndbegin 
        and close_g.dm_fndend || '00' = vat.da_fndend
        and close_g.ty_rpt = vat.ty_rpt 
        and close_g.no_count = (case when vat.no_count = 0 then 1 else vat.no_count end)

        """

        return query

    def select_fta_reporter(self):
        query = """
    select
      *, 
      case when length(accmngno::text) > 0 then '02' else '01' end as TY_BOOKKEEP,  --  장부기장구분(자체01, 세무사02)
      case when length(accmngno::text) > 0 then '03' else '01' end as TY_FS_SRC   -- 작성근거(회사01 99기타)   
    from 
    (
        select 
            coalesce(nm_krname, '') as AccNm,     --   세무사명   
            coalesce(no_manage1, '') || no_manage2 || no_manage3 as AccMngNo,   --  세무사관리번호   
            TEL_COM1 || (case when TEL_COM2 <> '' then '-' else '' end) || TEL_COM2 || (case when TEL_COM3 <> '' then '-' else '' end) || TEL_COM3 as AccTelNo,-- 세무사무소 전화번호	
            add_saaddr1 || add_saaddr2 as AccAddress,   --  세무사무소 주소 
            coalesce(no_biz, '') as AccBizNo,   -- 세무사무소 사업자번호     
            ''::text as AccCompNm,-- 세무사무소명
            coalesce(nm_userid, '') as nm_userid,
            coalesce(tel_com1, '') as tel_com1,
            coalesce(tel_com2, '') as tel_com2,
            coalesce(tel_com3, '') as tel_com3
        from fta_reporter
        where ty_report = 1
    ) as tt;
        """

        return query

    def select_company_info(self):
        query = """
    select

        com.ty_report,
        coalesce(com.tel_com1,'') || coalesce(com.tel_com2,'') || coalesce(com.tel_com3,'') as tel_com,
        coalesce(com.add_ceo1,'') || coalesce(com.add_ceo2,'') as add_ceo,
        coalesce(com.tel_ceo1,'') || coalesce(com.tel_ceo2,'') || coalesce(com.tel_ceo3,'') as tel_ceo,
        coalesce(com.cel_dtem1,'') || coalesce(com.cel_dtem2,'') || coalesce(com.cel_dtem3,'') as cel_dtem,
        coalesce(com.da_start,'') as da_start, --회사설립일
        coalesce(com.da_end,'') as da_end, --폐업일
        coalesce(com.str_2_1_1,'') as str_2_1_1, -- 과세유형전환일

        coalesce(com.nm_krcom, '') as CompanyName, --업체명
        com.prd_accounts as PRD_ACCOUNTS, --회계기수
        coalesce(com.DA_ACCBEGIN, '') as DA_ACCBEGIN, --회계시작
        coalesce(com.DA_ACCEND, '') as DA_ACCEND, --회계종료
        coalesce(com.no_biz, '') as BizRegNo, --사업자등록번호 또는 주민번호 
        coalesce(com.no_corpor, '') as CorNo, --법인번호
        coalesce(com.nm_ceo, '') as CeoName, --대표자명
        coalesce(com.no_ceosoc, '') as CeoRegNo, --대표자주민번호
        9 as JuPosition,  --주식회사 앞(1)/뒤(2)/기타(9) 구분
        99 as CompanyType, --기업형태 주식회사/유한회사 등
        coalesce(com.zip_com, '') as ZipCode, --우편번호

        coalesce(com.add_com1, '') || coalesce(com.add_com2, '') as Address,  -- 주소      
        '' as AddressDetail, --주소상세 (안쓰는 듯)
        rpad(com.tel_com1, 4, ' ') || rpad(com.tel_com2, 4, ' ') || rpad(com.tel_com3, 4, ' ') as TelNo,    --     전화 (지역번호-국번-번호, 각4자리 공백 padding) 
        rpad(com.fax_com1, 4, ' ') || rpad(com.fax_com2, 4, ' ') || rpad(com.fax_com3, 4, ' ') as FaxNo,    --   팩스번호(지역번호-국번-번호, 각4자리 공백 padding)    
        com.CD_BIZTYP as BizCategory,   --국세청 신고용 업종코드

        coalesce(com.nm_bizcond, '') as BizCondNm,  --  업태   
        coalesce(com.nm_item, '') as BizItemNm,   -- 종목   
        coalesce(com.da_build, '') as CorFCorPrvGbnoundDate,   -- 법인설립일
        com.yn_private as corprvgbn,   --   법인(1)/개인(2) 구분  

        coalesce(com.NM_DTEM, '') as DamNm,      -- 업체 담당자명     
        coalesce(com.EM_DTEM, '') as DamEmail,   -- 업체 담당자이메일     
        com.TEL_COM1 || (case when com.TEL_COM2 <> '' then '-' else '' end) || com.TEL_COM2 || (case when com.TEL_COM3 <> '' then '-' else '' end) || com.TEL_COM3 as DamTelNo,--	
        com.CEL_DTEM1 || (case when com.CEL_DTEM2 <> '' then '-' else '' end) || com.CEL_DTEM2 || (case when com.CEL_DTEM3 <> '' then '-' else '' end) || com.CEL_DTEM3 as DamHpNo,   -- 업체 담당자핸드폰번호
        danggi.gisu as danggi_gisu,
        danggi.da_accbegin as danggi_da_accbegin,
        danggi.ym_insa as danggi_ym_insa
    from ftb_com as com
    left join fts_migration_danggi as danggi on 1=1;
        """

        return query

    def select_get_close_string(self):
        query = """
    select
    cd_form, str_close, dm_fndend, dm_fndbegin, ty_rpt, no_count
    from 
    (
        select cd_form, str_close, dm_fndend, dm_fndbegin, ty_rpt, no_count
        from fta_vatclose_g
        where dm_fndend =  %(dm_fndend)s -- %(dm_fndend)s
          and ty_rpt = %(ty_rpt_g)s -- %(ty_rpt_g)s
          and no_count = %(no_count_g)s -- %(no_count_g)s

        union all 

        select cd_form, str_close, dm_fndend, dm_fndbegin, ty_rpt, no_count
        from fta_vatclose_g_before
        where dm_fndend =  %(dm_fndend)s -- %(dm_fndend)s
          and ty_rpt = %(ty_rpt_g)s -- %(ty_rpt_g)s
          and no_count = %(no_count_g)s -- %(no_count_g)s
    ) as tt
    order by cd_form

        """

        return query

    def select_vat(self, ty_simple):
        # ty_simple : 1 일반 / 2 간이
        if str(ty_simple) == '1':
            query = """
    select 
        max(case when no_div = 1 and no_cell = 10004 then str_save else '0' end) as V_SEL_TAX_TOT_AMT, --과세표준및매출세액금액(9) report_과세표준및매출세액금액_10004,
        max(case when no_div = 1 and no_cell = 10006 then str_save else '0' end) as V_SEL_TAX_TOT_TX,--과세표준및매출세액세액(9)report_과세표준및매출세액세액_10006,
        max(case when no_div = 1 and no_cell = 12004 then str_save else '0' end) as V_FIXASS_AMT, --고정자산매입금액(11+40)  report_고정자산매입금액_12004,
        max(case when no_div = 1 and no_cell = 12006 then str_save else '0' end) as V_FIXASS_TX, --고정자산매입세액(11+40) report_고정자산매입세액_12006,
        max(case when no_div = 1 and no_cell = 16004 then str_save else '0' end) as V_BUY_TAX_TOT_AMT, --매입차감계금액(17)report_매입차감계금액_16004,
        max(case when no_div = 1 and no_cell = 16006 then str_save else '0' end) as V_BUY_TAX_TOT_TX, --매입차감계세액(17)report_매입차감계세액_16006,
        max(case when no_div = 1 and no_cell = 25006 then str_save else '0' end) as V_ADD_TOT_TX, --가산세액계세액(24)report_가산세액계세액_25006,
        max(case when no_div = 1 and no_cell = 26006 then str_save else '0' end) as V_SUBADD_TX, --차가감납부할세액report_차감납부할세액_26006,

        max(case when no_div = 101 and no_cell = 7004 then str_save else '0' end) as V_TAX_STD_AMT, --과세표준명세합계(30)report_과세표준명세합계_7004,
        max(case when no_div = 102 and no_cell = 5004 then str_save else '0' end) as V_FREE_TAX_TOT_AMT, --면세사업 수입금액합계report_면세사업수입금액합계_5004,

        max(case when no_div = 1 and no_cell = 27006 then str_save else '0' end) as V_TOT_TX --총괄납부사업자납부할세액report_총괄납부사업자납부할세액_27006

    from fta_vatrpt
    where da_fndbegin = %(da_fndbegin)s and da_fndend = %(da_fndend)s and prd_vat = %(prd_vat)s and ty_month = %(ty_month)s and ty_rpt = %(ty_rpt)s and ty_simple = %(ty_simple)s and no_count = %(no_count)s 
      having (select count(1) from fta_vatrpt where da_fndbegin = %(da_fndbegin)s and da_fndend = %(da_fndend)s and prd_vat = %(prd_vat)s and ty_month = %(ty_month)s and ty_rpt = %(ty_rpt)s and ty_simple = %(ty_simple)s and no_count = %(no_count)s ) > 0 

    union all

    select 

        max(case when no_div = 1 and no_cell = 10004 then str_save else '0' end) as V_SEL_TAX_TOT_AMT, --과세표준및매출세액금액(9) report_과세표준및매출세액금액_10004,
        max(case when no_div = 1 and no_cell = 10006 then str_save else '0' end) as V_SEL_TAX_TOT_TX,--과세표준및매출세액세액(9)report_과세표준및매출세액세액_10006,
        max(case when no_div = 1 and no_cell = 12004 then str_save else '0' end) as V_FIXASS_AMT, --고정자산매입금액(11+40)  report_고정자산매입금액_12004,
        max(case when no_div = 1 and no_cell = 12006 then str_save else '0' end) as V_FIXASS_TX, --고정자산매입세액(11+40) report_고정자산매입세액_12006,
        max(case when no_div = 1 and no_cell = 16004 then str_save else '0' end) as V_BUY_TAX_TOT_AMT, --매입차감계금액(17)report_매입차감계금액_16004,
        max(case when no_div = 1 and no_cell = 16006 then str_save else '0' end) as V_BUY_TAX_TOT_TX, --매입차감계세액(17)report_매입차감계세액_16006,
        max(case when no_div = 1 and no_cell = 25006 then str_save else '0' end) as V_ADD_TOT_TX, --가산세액계세액(24)report_가산세액계세액_25006,
        max(case when no_div = 1 and no_cell = 26006 then str_save else '0' end) as V_SUBADD_TX, --차가감납부할세액report_차감납부할세액_26006,0

        max(case when no_div = 101 and no_cell = 7004 then str_save else '0' end) as V_TAX_STD_AMT, --과세표준명세합계(30)report_과세표준명세합계_7004,
        max(case when no_div = 102 and no_cell = 5004 then str_save else '0' end) as V_FREE_TAX_TOT_AMT, --면세사업 수입금액합계report_면세사업수입금액합계_5004,

        max(case when no_div = 1 and no_cell = 27006 then str_save else '0' end) as V_TOT_TX --총괄납부사업자납부할세액report_총괄납부사업자납부할세액_27006

    from fta_vatrpt_before
    where da_fndbegin = %(da_fndbegin)s and da_fndend = %(da_fndend)s and prd_vat = %(prd_vat)s and ty_month = %(ty_month)s and ty_rpt = %(ty_rpt)s and ty_simple = %(ty_simple)s and no_count = %(no_count)s 
      having (select count(1) from fta_vatrpt_before where da_fndbegin = %(da_fndbegin)s and da_fndend = %(da_fndend)s and prd_vat = %(prd_vat)s and ty_month = %(ty_month)s and ty_rpt = %(ty_rpt)s and ty_simple = %(ty_simple)s and no_count = %(no_count)s ) > 0 
    ;
            """
        else:
            query = """
    select 

        max(case when no_div = 1 and no_cell = 9004 then str_save else '0' end) as V_SEL_TAX_TOT_AMT, --과세표준및매출세액금액(9)report_과세표준및매출세액금액_9004,
        max(case when no_div = 1 and no_cell = 9007 then str_save else '0' end) as V_SEL_TAX_TOT_TX,--과세표준및매출세액세액(9)report_과세표준및매출세액세액_9007,
        --max(case when no_div = 1 and no_cell =  then str_save else '' end) as report_고정자산매입금액,
        --max(case when no_div = 1 and no_cell = 12006 then str_save else '' end) as report_고정자산매입세액,
        --max(case when no_div = 1 and no_cell = 16004 then str_save else '' end) as report_매입차감계금액,
        --max(case when no_div = 1 and no_cell = 16006 then str_save else '' end) as report_매입차감계세액,
        0 as V_FIXASS_AMT, --고정자산매입금액(11+40) report_고정자산매입금액,
        0 as V_FIXASS_TX, --고정자산매입세액(11+40) report_고정자산매입세액,
        0 as V_BUY_TAX_TOT_AMT, --매입차감계금액(17)report_매입차감계금액,
        0 as V_BUY_TAX_TOT_TX, --매입차감계세액(17)report_매입차감계세액,

        max(case when no_div = 1 and no_cell = 18007 then str_save else '0' end) as V_ADD_TOT_TX, --가산세액계세액(24)report_가산세액계세액_18007,
        max(case when no_div = 1 and no_cell = 19007 then str_save else '0' end) as V_SUBADD_TX, --차가감납부할세액report_차감납부할세액_19007,

        max(case when no_div = 101 and no_cell = 6004 then str_save else '0' end) as V_TAX_STD_AMT, --과세표준명세합계(30)report_과세표준명세합계_6004,
        max(case when no_div = 102 and no_cell = 5004 then str_save else '0' end) as V_FREE_TAX_TOT_AMT, --면세사업 수입금액합계report_면세사업수입금액합계_5004,

        max(case when no_div = 1 and no_cell = 20007 then str_save else '0' end) as V_TOT_TX --총괄납부사업자납부할세액report_총괄납부사업자납부할세액_20007

    from fta_vatrpt
    where da_fndbegin = %(da_fndbegin)s and da_fndend = %(da_fndend)s and prd_vat = %(prd_vat)s and ty_month = %(ty_month)s and ty_rpt = %(ty_rpt)s and ty_simple = %(ty_simple)s and no_count = %(no_count)s 
      having (select count(1) from fta_vatrpt where da_fndbegin = %(da_fndbegin)s and da_fndend = %(da_fndend)s and prd_vat = %(prd_vat)s and ty_month = %(ty_month)s and ty_rpt = %(ty_rpt)s and ty_simple = %(ty_simple)s and no_count = %(no_count)s ) > 0 

    union ALL 

    select 

        max(case when no_div = 1 and no_cell = 9004 then str_save else '0' end) as V_SEL_TAX_TOT_AMT, --과세표준및매출세액금액(9)report_과세표준및매출세액금액_9004,
        max(case when no_div = 1 and no_cell = 9007 then str_save else '0' end) as V_SEL_TAX_TOT_TX,--과세표준및매출세액세액(9)report_과세표준및매출세액세액_9007,
        --max(case when no_div = 1 and no_cell =  then str_save else '' end) as report_고정자산매입금액,
        --max(case when no_div = 1 and no_cell = 12006 then str_save else '' end) as report_고정자산매입세액,
        --max(case when no_div = 1 and no_cell = 16004 then str_save else '' end) as report_매입차감계금액,
        --max(case when no_div = 1 and no_cell = 16006 then str_save else '' end) as report_매입차감계세액,
        0 as V_FIXASS_AMT, --고정자산매입금액(11+40) report_고정자산매입금액,
        0 as V_FIXASS_TX, --고정자산매입세액(11+40) report_고정자산매입세액,
        0 as V_BUY_TAX_TOT_AMT, --매입차감계금액(17)report_매입차감계금액,
        0 as V_BUY_TAX_TOT_TX, --매입차감계세액(17)report_매입차감계세액,

        max(case when no_div = 1 and no_cell = 18007 then str_save else '0' end) as V_ADD_TOT_TX, --가산세액계세액(24)report_가산세액계세액_18007,
        max(case when no_div = 1 and no_cell = 19007 then str_save else '0' end) as V_SUBADD_TX, --차가감납부할세액report_차감납부할세액_19007,

        max(case when no_div = 101 and no_cell = 6004 then str_save else '0' end) as V_TAX_STD_AMT, --과세표준명세합계(30)report_과세표준명세합계_6004,
        max(case when no_div = 102 and no_cell = 5004 then str_save else '0' end) as V_FREE_TAX_TOT_AMT, --면세사업 수입금액합계report_면세사업수입금액합계_5004,

        max(case when no_div = 1 and no_cell = 20007 then str_save else '0' end) as V_TOT_TX --총괄납부사업자납부할세액report_총괄납부사업자납부할세액_20007

    from fta_vatrpt_before
    where da_fndbegin = %(da_fndbegin)s and da_fndend = %(da_fndend)s and prd_vat = %(prd_vat)s and ty_month = %(ty_month)s and ty_rpt = %(ty_rpt)s and ty_simple = %(ty_simple)s and no_count = %(no_count)s 
      having (select count(1) from fta_vatrpt_before where da_fndbegin = %(da_fndbegin)s and da_fndend = %(da_fndend)s and prd_vat = %(prd_vat)s and ty_month = %(ty_month)s and ty_rpt = %(ty_rpt)s and ty_simple = %(ty_simple)s and no_count = %(no_count)s ) > 0 



                    """
        return query

    def select_sinbo(self, ty_simple):
        if str(ty_simple) == '1':
            query = """
    select 
        max(case when no_div = 1 and no_cell = 3004 then str_save else '' end) as T08, --08. 과세/세금계산서교부분/금액 sinbo_세금계산서교부분금액_3004,
        max(case when no_div = 1 and no_cell = 3006 then str_save else '' end) as T09, --09. 과세/세금계산서교부분/세액 sinbo_세금계산서교부분세액_3006,
        max(case when no_div = 1 and no_cell = 5004 then str_save else '' end) as T10, --10. 과세/기타/금액  sinbo_과세기타금액_5004,
        max(case when no_div = 1 and no_cell = 5006 then str_save else '' end) as T11, --11. 과세/기타/세액  sinbo_과세기타세액_5006,
        max(case when no_div = 1 and no_cell = 6004 then str_save else '' end) as T12, --12. 영세/세금계산서교부분/금액 sinbo_영세세금계산서교부분금액_6004,
        max(case when no_div = 1 and no_cell = 7006 then str_save else '' end) as T13, --13. 영세/기타/금액   sinbo_영세세금계산서교부분세액_7006,
        max(case when no_div = 1 and no_cell = 8004 then str_save else '' end) as T14, --14. 예정신고누락분/금액(매출)sinbo_예정신고누락분금액매출_8004,
        max(case when no_div = 1 and no_cell = 8006 then str_save else '' end) as T15, --15. 예정신고누락분/세액(매출)sinbo_예정신고누락분세액매출_8006,
        max(case when no_div = 1 and no_cell = 9006 then str_save else '' end) as T16, --16. 대손세액가감/세액sinbo_대손세액가감세액_9006,
        max(case when no_div = 1 and no_cell = 10004 then str_save else '' end) as T17, --17. 합계/금액(매출)sinbo_합계금액매출_10004,
        max(case when no_div = 1 and no_cell = 10006 then str_save else '' end) as T18, --18. 합계/세액(매출) sinbo_합계세액매출_10006,

        max(case when no_div = 1 and no_cell = 11004 then str_save else '' end) as T19, --19. 세금계산서수취분/일반매입/금액  sinbo_세금계산서수취분일반매입금액_11004,
        max(case when no_div = 1 and no_cell = 11006 then str_save else '' end) as T20, --20. 세금계산서수취분/일반매입/세액sinbo_세금계산서수취분일반매입세액_11006,
        max(case when no_div = 1 and no_cell = 12004 then str_save else '' end) as T21, --21. 세금계산서수취분/고정자산매입/금액 sinbo_세금계산서수취분고정자산매입금액_12004,
        max(case when no_div = 1 and no_cell = 12006 then str_save else '' end) as T22, --22. 세금계산서수취분/고정자산매입/세액sinbo_세금계산서수취분고정자산매입세액_12006,
        max(case when no_div = 1 and no_cell = 13004 then str_save else '' end) as T70, --70. 예정신고누락분/금액(매입)  sinbo_예정신고누락분금액매입_13004,
        max(case when no_div = 1 and no_cell = 13006 then str_save else '' end) as T71, --71. 예정신고누락분/세액(매입)sinbo_예정신고누락분세액매입_13006,
        max(case when no_div = 1 and no_cell = 15004 then str_save else '' end) as T23, --23. 기타공제매입세액/금액   sinbo_기타공제매입세액금액_15004,
        max(case when no_div = 1 and no_cell = 15006 then str_save else '' end) as T24, --24. 기타공제매입세액/세액     sinbo_기타공제매입세액세액_15006,
        max(case when no_div = 1 and no_cell = 16004 then str_save else '' end) as T25, --25. 합계/금액(매입) sinbo_합계금액매입_16004,
        max(case when no_div = 1 and no_cell = 16006 then str_save else '' end) as T26, --26. 합계/세액(매입) sinbo_합계세액매입_16006,
        max(case when no_div = 1 and no_cell = 17004 then str_save else '' end) as T27, --27. 공제받지못할매입세액/금액  sinbo_공제받지못할매입세액금액_17004,
        max(case when no_div = 1 and no_cell = 17006 then str_save else '' end) as T28, --28. 공제받지못할매입세액/세액  sinbo_공제받지못할매입세액세액_17006,
        max(case when no_div = 1 and no_cell = 18004 then str_save else '' end) as T29, --29. 차감계/금액  sinbo_차감계금액_18004,
        max(case when no_div = 1 and no_cell = 18006 then str_save else '' end) as T30, --30. 차감계/세액  sinbo_차감계세액_18006,

        max(case when no_div = 1 and no_cell = 19006 then str_save else '' end) as T31, --31. 납부(환급)세액  sinbo_납부환급세액_19006,
        max(case when no_div = 1 and no_cell = 20006 then str_save else '' end) as T34, --34. 기타공제경감세액/세액 sinbo_기타공제경감세액_20006,
        max(case when no_div = 1 and no_cell = 21004 then str_save else '' end) as T32, --32. 신용카드매출전표발행공제/금액 sinbo_신용카드매출전표발생공제금액_21004,
        max(case when no_div = 1 and no_cell = 21006 then str_save else '' end) as T33, --33. 신용카드매출전표발행공제/세액 sinbo_신용카드매출전표발생공제세액_21006,
        max(case when no_div = 1 and no_cell = 2006  then str_save else '' end) as T35, --35. 경감공제세액합계/세액 sinbo_경감공제세액합계_2006,
        max(case when no_div = 1 and no_cell = 23006 then str_save else '' end) as T36, --36. 예정신고미환급세액/세액 sinbo_예정신고미환급세액_23006,
        max(case when no_div = 1 and no_cell = 24006 then str_save else '' end) as T37, --37. 예정고지세액/세액  sinbo_예정고지세액_24006,
        max(case when no_div = 1 and no_cell = 25006 then str_save else '' end) as T38, --38. 가산세액계/세액  sinbo_가산세액계_25006,
        max(case when no_div = 1 and no_cell = 26006 then str_save else '' end) as T39, --39. 차가감납부할세액/세액 sinbo_차가감납부할세액_26006,
        max(case when no_div = 1 and no_cell = 27006 then str_save else '' end) as T76, --76.총괄납부사업자납부할세액(환급받을세액)/세액 sinbo_총괄납부사업자납부할세액_27006,

        max(case when no_div = 101 and no_cell = 6004 then str_save else '' end) as T40, --40. 과세표준명세합계  sinbo_과세표준명세합계_6004,

        max(case when no_div = 6 and no_cell = 2005 then str_save else '' end) as T41, --41. 매출/과세/세금계산서/금액 sinbo_예정신고누락분매출과세세금계산서금액_2005,
        max(case when no_div = 6 and no_cell = 2007 then str_save else '' end) as T42, --42. 매출/과세/세금계산서/세액 sinbo_예정신고누락분매출과세세금계산서세액_2007,
        max(case when no_div = 6 and no_cell = 4005 then str_save else '' end) as T43, --43. 매출/영세율/세금계산서/금액 sinbo_예정신고누락분매출영세세금계산서금액_4005,
        max(case when no_div = 11 and no_cell = 7005 then str_save else '' end) as T72, --72. 매입/세금계산서/금액sinbo_예정신고누락분매입세금계산서금액_7005,
        max(case when no_div = 11 and no_cell = 7007 then str_save else '' end) as T73, --73. 매입/세금계산서/세액 sinbo_예정신고누락분매입세금계산서세액_7007,
        max(case when no_div = 11 and no_cell = 8005 then str_save else '' end) as T74, --74. 매입/기타매입세액/금액 sinbo_예정신고누락분매입세액금액_8005,
        max(case when no_div = 11 and no_cell = 8007 then str_save else '' end) as T75, --74. 매입/기타매입세액/세액 sinbo_예정신고누락분매입세액세액_8007,
        max(case when no_div = 102 and no_cell = 5004 then str_save else '' end) as T44, --44. 면세사업수입금액/합계/금액  sinbo_기타면세사업수입금액합계_5004,
        max(case when no_div = 103 and no_cell = 1002 then str_save else '' end) as T45, --45. 계산서교부및수취내역/계산서교부/금액 sinbo_기타계산서교부및수취내역계산서교부금액_1002,
        max(case when no_div = 103 and no_cell = 2002 then str_save else '' end) as T46 --46. 계산서교부및수취내역/계산서수취/금액 sinbo_기타계산서교부및수취내역계산서수취금액_2002


    from fta_vatrpt
    where da_fndbegin = %(da_fndbegin)s and da_fndend = %(da_fndend)s and prd_vat = %(prd_vat)s and ty_month = %(ty_month)s and ty_rpt = %(ty_rpt)s and ty_simple = %(ty_simple)s and no_count = %(no_count)s 
      having (select count(1) from fta_vatrpt where da_fndbegin = %(da_fndbegin)s and da_fndend = %(da_fndend)s and prd_vat = %(prd_vat)s and ty_month = %(ty_month)s and ty_rpt = %(ty_rpt)s and ty_simple = %(ty_simple)s and no_count = %(no_count)s ) > 0 

    union all

    select 
        max(case when no_div = 1 and no_cell = 3004 then str_save else '' end) as T08, --08. 과세/세금계산서교부분/금액 sinbo_세금계산서교부분금액_3004,
        max(case when no_div = 1 and no_cell = 3006 then str_save else '' end) as T09, --09. 과세/세금계산서교부분/세액 sinbo_세금계산서교부분세액_3006,
        max(case when no_div = 1 and no_cell = 5004 then str_save else '' end) as T10, --10. 과세/기타/금액  sinbo_과세기타금액_5004,
        max(case when no_div = 1 and no_cell = 5006 then str_save else '' end) as T11, --11. 과세/기타/세액  sinbo_과세기타세액_5006,
        max(case when no_div = 1 and no_cell = 6004 then str_save else '' end) as T12, --12. 영세/세금계산서교부분/금액 sinbo_영세세금계산서교부분금액_6004,
        max(case when no_div = 1 and no_cell = 7006 then str_save else '' end) as T13, --13. 영세/기타/금액   sinbo_영세세금계산서교부분세액_7006,
        max(case when no_div = 1 and no_cell = 8004 then str_save else '' end) as T14, --14. 예정신고누락분/금액(매출)sinbo_예정신고누락분금액매출_8004,
        max(case when no_div = 1 and no_cell = 8006 then str_save else '' end) as T15, --15. 예정신고누락분/세액(매출)sinbo_예정신고누락분세액매출_8006,
        max(case when no_div = 1 and no_cell = 9006 then str_save else '' end) as T16, --16. 대손세액가감/세액sinbo_대손세액가감세액_9006,
        max(case when no_div = 1 and no_cell = 10004 then str_save else '' end) as T17, --17. 합계/금액(매출)sinbo_합계금액매출_10004,
        max(case when no_div = 1 and no_cell = 10006 then str_save else '' end) as T18, --18. 합계/세액(매출) sinbo_합계세액매출_10006,

        max(case when no_div = 1 and no_cell = 11004 then str_save else '' end) as T19, --19. 세금계산서수취분/일반매입/금액  sinbo_세금계산서수취분일반매입금액_11004,
        max(case when no_div = 1 and no_cell = 11006 then str_save else '' end) as T20, --20. 세금계산서수취분/일반매입/세액sinbo_세금계산서수취분일반매입세액_11006,
        max(case when no_div = 1 and no_cell = 12004 then str_save else '' end) as T21, --21. 세금계산서수취분/고정자산매입/금액 sinbo_세금계산서수취분고정자산매입금액_12004,
        max(case when no_div = 1 and no_cell = 12006 then str_save else '' end) as T22, --22. 세금계산서수취분/고정자산매입/세액sinbo_세금계산서수취분고정자산매입세액_12006,
        max(case when no_div = 1 and no_cell = 13004 then str_save else '' end) as T70, --70. 예정신고누락분/금액(매입)  sinbo_예정신고누락분금액매입_13004,
        max(case when no_div = 1 and no_cell = 13006 then str_save else '' end) as T71, --71. 예정신고누락분/세액(매입)sinbo_예정신고누락분세액매입_13006,
        max(case when no_div = 1 and no_cell = 15004 then str_save else '' end) as T23, --23. 기타공제매입세액/금액   sinbo_기타공제매입세액금액_15004,
        max(case when no_div = 1 and no_cell = 15006 then str_save else '' end) as T24, --24. 기타공제매입세액/세액     sinbo_기타공제매입세액세액_15006,
        max(case when no_div = 1 and no_cell = 16004 then str_save else '' end) as T25, --25. 합계/금액(매입) sinbo_합계금액매입_16004,
        max(case when no_div = 1 and no_cell = 16006 then str_save else '' end) as T26, --26. 합계/세액(매입) sinbo_합계세액매입_16006,
        max(case when no_div = 1 and no_cell = 17004 then str_save else '' end) as T27, --27. 공제받지못할매입세액/금액  sinbo_공제받지못할매입세액금액_17004,
        max(case when no_div = 1 and no_cell = 17006 then str_save else '' end) as T28, --28. 공제받지못할매입세액/세액  sinbo_공제받지못할매입세액세액_17006,
        max(case when no_div = 1 and no_cell = 18004 then str_save else '' end) as T29, --29. 차감계/금액  sinbo_차감계금액_18004,
        max(case when no_div = 1 and no_cell = 18006 then str_save else '' end) as T30, --30. 차감계/세액  sinbo_차감계세액_18006,

        max(case when no_div = 1 and no_cell = 19006 then str_save else '' end) as T31, --31. 납부(환급)세액  sinbo_납부환급세액_19006,
        max(case when no_div = 1 and no_cell = 20006 then str_save else '' end) as T34, --34. 기타공제경감세액/세액 sinbo_기타공제경감세액_20006,
        max(case when no_div = 1 and no_cell = 21004 then str_save else '' end) as T32, --32. 신용카드매출전표발행공제/금액 sinbo_신용카드매출전표발생공제금액_21004,
        max(case when no_div = 1 and no_cell = 21006 then str_save else '' end) as T33, --33. 신용카드매출전표발행공제/세액 sinbo_신용카드매출전표발생공제세액_21006,
        max(case when no_div = 1 and no_cell = 2006  then str_save else '' end) as T35, --35. 경감공제세액합계/세액 sinbo_경감공제세액합계_2006,
        max(case when no_div = 1 and no_cell = 23006 then str_save else '' end) as T36, --36. 예정신고미환급세액/세액 sinbo_예정신고미환급세액_23006,
        max(case when no_div = 1 and no_cell = 24006 then str_save else '' end) as T37, --37. 예정고지세액/세액  sinbo_예정고지세액_24006,
        max(case when no_div = 1 and no_cell = 25006 then str_save else '' end) as T38, --38. 가산세액계/세액  sinbo_가산세액계_25006,
        max(case when no_div = 1 and no_cell = 26006 then str_save else '' end) as T39, --39. 차가감납부할세액/세액 sinbo_차가감납부할세액_26006,
        max(case when no_div = 1 and no_cell = 27006 then str_save else '' end) as T76, --76.총괄납부사업자납부할세액(환급받을세액)/세액 sinbo_총괄납부사업자납부할세액_27006,

        max(case when no_div = 101 and no_cell = 6004 then str_save else '' end) as T40, --40. 과세표준명세합계  sinbo_과세표준명세합계_6004,

        max(case when no_div = 6 and no_cell = 2005 then str_save else '' end) as T41, --41. 매출/과세/세금계산서/금액 sinbo_예정신고누락분매출과세세금계산서금액_2005,
        max(case when no_div = 6 and no_cell = 2007 then str_save else '' end) as T42, --42. 매출/과세/세금계산서/세액 sinbo_예정신고누락분매출과세세금계산서세액_2007,
        max(case when no_div = 6 and no_cell = 4005 then str_save else '' end) as T43, --43. 매출/영세율/세금계산서/금액 sinbo_예정신고누락분매출영세세금계산서금액_4005,
        max(case when no_div = 11 and no_cell = 7005 then str_save else '' end) as T72, --72. 매입/세금계산서/금액sinbo_예정신고누락분매입세금계산서금액_7005,
        max(case when no_div = 11 and no_cell = 7007 then str_save else '' end) as T73, --73. 매입/세금계산서/세액 sinbo_예정신고누락분매입세금계산서세액_7007,
        max(case when no_div = 11 and no_cell = 8005 then str_save else '' end) as T74, --74. 매입/기타매입세액/금액 sinbo_예정신고누락분매입세액금액_8005,
        max(case when no_div = 11 and no_cell = 8007 then str_save else '' end) as T75, --74. 매입/기타매입세액/세액 sinbo_예정신고누락분매입세액세액_8007,
        max(case when no_div = 102 and no_cell = 5004 then str_save else '' end) as T44, --44. 면세사업수입금액/합계/금액  sinbo_기타면세사업수입금액합계_5004,
        max(case when no_div = 103 and no_cell = 1002 then str_save else '' end) as T45, --45. 계산서교부및수취내역/계산서교부/금액 sinbo_기타계산서교부및수취내역계산서교부금액_1002,
        max(case when no_div = 103 and no_cell = 2002 then str_save else '' end) as T46 --46. 계산서교부및수취내역/계산서수취/금액 sinbo_기타계산서교부및수취내역계산서수취금액_2002
    from fta_vatrpt_before
    where da_fndbegin = %(da_fndbegin)s and da_fndend = %(da_fndend)s and prd_vat = %(prd_vat)s and ty_month = %(ty_month)s and ty_rpt = %(ty_rpt)s and ty_simple = %(ty_simple)s and no_count = %(no_count)s 
      having (select count(1) from fta_vatrpt_before where da_fndbegin = %(da_fndbegin)s and da_fndend = %(da_fndend)s and prd_vat = %(prd_vat)s and ty_month = %(ty_month)s and ty_rpt = %(ty_rpt)s and ty_simple = %(ty_simple)s and no_count = %(no_count)s ) > 0 

    ;
            """
        else:
            query = """

    select 
        max(case when no_div = 1 and no_cell = 4004 then str_save else '' end) as T34, --34. 과세분/소매업/금액sinbo_과세분소매업금액,
        max(case when no_div = 1 and no_cell = 4007 then str_save else '' end) as T35, --35. 과세분/소매업/세액          sinbo_과세분소매업세액,
        max(case when no_div = 1 and no_cell = 3004 then str_save else '' end) as T08, --08. 과세분/제조업,전기가스및수도사업/금액sinbo_과세분제조업금액,
        max(case when no_div = 1 and no_cell = 3007 then str_save else '' end) as T09, --09. 과세분/제조업,전기가스및수도사업/세액          sinbo_과세분제조업세액,
        max(case when no_div = 1 and no_cell = 6004 then str_save else '' end) as T10, --10. 과세분/건설업,부동산임대업,농수임어업,기타,서비스업,음식점업,숙박업/금액 sinbo_과세분농수임어업금액,
        max(case when no_div = 1 and no_cell = 6007 then str_save else '' end) as T11, --11.과세분/건설업,부동산임대업,농수임어업,기타,서비스업,음식점업,숙박업/세액  sinbo_과세분농수임어업세액,
        max(case when no_div = 1 and no_cell = 5004 then str_save else '' end) as T12, --12. 과세분/운수창고및통신업/금액                      sinbo_과세분운수창고금액,
        max(case when no_div = 1 and no_cell = 5007 then str_save else '' end) as T13, --13. 과세분/운수창고및통신업/세액                      sinbo_과세분운수창고세액,
        max(case when no_div = 1 and no_cell = 7004 then str_save else '' end) as T14, --14. 영세율적용분/금액sinbo_영세율적용분금액,
        max(case when no_div = 1 and no_cell = 8004 then str_save else '' end) as T15, --15. 재고납부/세액sinbo_재고납부세액,
        max(case when no_div = 1 and no_cell = 9004 then str_save else '' end) as T16, --16. 합계/금액(매출)             sinbo_합계금액매출,
        max(case when no_div = 1 and no_cell = 9007 then str_save else '' end) as T17, --17. 합계/세액(매출)             sinbo_합계세액매출,
        max(case when no_div = 1 and no_cell = 10004 then str_save else '' end) as T18, --18. 매입세금계산서세액공제/금액 sinbo_매입세금계산서세액공제금액,
        max(case when no_div = 1 and no_cell = 10007 then str_save else '' end) as T19, --19. 매입세금계산서세액공제/세액 sinbo_매입세금계산서세액공제세액,
        max(case when no_div = 1 and no_cell = 11004 then str_save else '' end) as T20, --20. 의제매입세액공제/금액     sinbo_의제매입세액공제금액,
        max(case when no_div = 1 and no_cell = 11007 then str_save else '' end) as T21, --21. 의제매입세액공제/세액     sinbo_의제매입세액공제세액,
        max(case when no_div = 1 and no_cell = 13007 then str_save else '' end) as T22, --22. 전자신고세액공제/세액     sinbo_전자신고세액공제세액,
        0 as T23, --23. 성실신고사업자세액/세액   sinbo_성실신고사업자세액,
        max(case when no_div = 1 and no_cell = 15004 then str_save else '' end) as T24, --24. 신용카드세액공제/금액     sinbo_신용카드세액공제금액,
        max(case when no_div = 1 and no_cell = 15007 then str_save else '' end) as T25, --25. 신용카드세액공제/세액     sinbo_신용카드세액공제세액,
        0 as T26, --26. 기타/금액(공제세액)sinbo_기타금액공제세액,
        max(case when no_div = 1 and no_cell = 16007 then str_save else '' end) as T27, --27. 기타/세액(공제세액)      sinbo_기타세액공제세액,
        max(case when no_div = 1 and no_cell = 17004 then str_save else '' end) as T28, --28. 합계/금액(공제세액)      sinbo_합계금액공제세액,
        max(case when no_div = 1 and no_cell = 17007 then str_save else '' end) as T29, --29. 합계/세액(공제세액)      sinbo_합계세액공제세액,
        max(case when no_div = 1 and no_cell = 18007 then str_save else '' end) as T30, --30. 가산세액계/세액         sinbo_가산세액계세액,

        max(case when no_div = 1 and no_cell = 19007 then str_save else '' end)  as T31, --31. 차감납부할세액(환급받을세액)/세액 sinbo_차감납부할세액,
        max(case when no_div = 101 and no_cell = 6004 then str_save else '' end) as T32, --32. 합계/금액(과세표준)         sinbo_합계금액과세표준,
        max(case when no_div = 102 and no_cell = 5004 then str_save else '' end) as T33 --33. 합계/금액(면세수입금액)        sinbo_합계금액면세수입금액

    from fta_vatrpt
    where da_fndbegin = %(da_fndbegin)s and da_fndend = %(da_fndend)s and prd_vat = %(prd_vat)s and ty_month = %(ty_month)s and ty_rpt = %(ty_rpt)s and ty_simple = %(ty_simple)s and no_count = %(no_count)s 
      having (select count(1) from fta_vatrpt where da_fndbegin = %(da_fndbegin)s and da_fndend = %(da_fndend)s and prd_vat = %(prd_vat)s and ty_month = %(ty_month)s and ty_rpt = %(ty_rpt)s and ty_simple = %(ty_simple)s and no_count = %(no_count)s ) > 0 

    union all

    select 
        max(case when no_div = 1 and no_cell = 4004 then str_save else '' end) as T34, --34. 과세분/소매업/금액sinbo_과세분소매업금액,
        max(case when no_div = 1 and no_cell = 4007 then str_save else '' end) as T35, --35. 과세분/소매업/세액          sinbo_과세분소매업세액,
        max(case when no_div = 1 and no_cell = 3004 then str_save else '' end) as T08, --08. 과세분/제조업,전기가스및수도사업/금액sinbo_과세분제조업금액,
        max(case when no_div = 1 and no_cell = 3007 then str_save else '' end) as T09, --09. 과세분/제조업,전기가스및수도사업/세액          sinbo_과세분제조업세액,
        max(case when no_div = 1 and no_cell = 6004 then str_save else '' end) as T10, --10. 과세분/건설업,부동산임대업,농수임어업,기타,서비스업,음식점업,숙박업/금액 sinbo_과세분농수임어업금액,
        max(case when no_div = 1 and no_cell = 6007 then str_save else '' end) as T11, --11.과세분/건설업,부동산임대업,농수임어업,기타,서비스업,음식점업,숙박업/세액  sinbo_과세분농수임어업세액,
        max(case when no_div = 1 and no_cell = 5004 then str_save else '' end) as T12, --12. 과세분/운수창고및통신업/금액                      sinbo_과세분운수창고금액,
        max(case when no_div = 1 and no_cell = 5007 then str_save else '' end) as T13, --13. 과세분/운수창고및통신업/세액                      sinbo_과세분운수창고세액,
        max(case when no_div = 1 and no_cell = 7004 then str_save else '' end) as T14, --14. 영세율적용분/금액sinbo_영세율적용분금액,
        max(case when no_div = 1 and no_cell = 8004 then str_save else '' end) as T15, --15. 재고납부/세액sinbo_재고납부세액,
        max(case when no_div = 1 and no_cell = 9004 then str_save else '' end) as T16, --16. 합계/금액(매출)             sinbo_합계금액매출,
        max(case when no_div = 1 and no_cell = 9007 then str_save else '' end) as T17, --17. 합계/세액(매출)             sinbo_합계세액매출,
        max(case when no_div = 1 and no_cell = 10004 then str_save else '' end) as T18, --18. 매입세금계산서세액공제/금액 sinbo_매입세금계산서세액공제금액,
        max(case when no_div = 1 and no_cell = 10007 then str_save else '' end) as T19, --19. 매입세금계산서세액공제/세액 sinbo_매입세금계산서세액공제세액,
        max(case when no_div = 1 and no_cell = 11004 then str_save else '' end) as T20, --20. 의제매입세액공제/금액     sinbo_의제매입세액공제금액,
        max(case when no_div = 1 and no_cell = 11007 then str_save else '' end) as T21, --21. 의제매입세액공제/세액     sinbo_의제매입세액공제세액,
        max(case when no_div = 1 and no_cell = 13007 then str_save else '' end) as T22, --22. 전자신고세액공제/세액     sinbo_전자신고세액공제세액,
        0 as T23, --23. 성실신고사업자세액/세액   sinbo_성실신고사업자세액,
        max(case when no_div = 1 and no_cell = 15004 then str_save else '' end) as T24, --24. 신용카드세액공제/금액     sinbo_신용카드세액공제금액,
        max(case when no_div = 1 and no_cell = 15007 then str_save else '' end) as T25, --25. 신용카드세액공제/세액     sinbo_신용카드세액공제세액,
        0 as T26, --26. 기타/금액(공제세액)sinbo_기타금액공제세액,
        max(case when no_div = 1 and no_cell = 16007 then str_save else '' end) as T27, --27. 기타/세액(공제세액)      sinbo_기타세액공제세액,
        max(case when no_div = 1 and no_cell = 17004 then str_save else '' end) as T28, --28. 합계/금액(공제세액)      sinbo_합계금액공제세액,
        max(case when no_div = 1 and no_cell = 17007 then str_save else '' end) as T29, --29. 합계/세액(공제세액)      sinbo_합계세액공제세액,
        max(case when no_div = 1 and no_cell = 18007 then str_save else '' end) as T30, --30. 가산세액계/세액         sinbo_가산세액계세액,

        max(case when no_div = 1 and no_cell = 19007 then str_save else '' end)  as T31, --31. 차감납부할세액(환급받을세액)/세액 sinbo_차감납부할세액,
        max(case when no_div = 101 and no_cell = 6004 then str_save else '' end) as T32, --32. 합계/금액(과세표준)         sinbo_합계금액과세표준,
        max(case when no_div = 102 and no_cell = 5004 then str_save else '' end) as T33 --33. 합계/금액(면세수입금액)        sinbo_합계금액면세수입금액

    from fta_vatrpt_before
    where da_fndbegin = %(da_fndbegin)s and da_fndend = %(da_fndend)s and prd_vat = %(prd_vat)s and ty_month = %(ty_month)s and ty_rpt = %(ty_rpt)s and ty_simple = %(ty_simple)s and no_count = %(no_count)s 
      having (select count(1) from fta_vatrpt_before where da_fndbegin = %(da_fndbegin)s and da_fndend = %(da_fndend)s and prd_vat = %(prd_vat)s and ty_month = %(ty_month)s and ty_rpt = %(ty_rpt)s and ty_simple = %(ty_simple)s and no_count = %(no_count)s ) > 0 

                    """

        return query

    def select_detail(self):
        # ty_buysale = 매출매입종류
        # 1 : 세금계산서
        # 3 : 계산서

        query = """
    select
            ty_buysale as V_SB_GB -- 1 매입세금, 2 매입계산, 3 매출세금, 3 매출계산
            , num as V_SEQ_NO--일련번호
            , ty_biz as V_BIZRES_GB -- 1 2 사업자번호 구분
            , no_biz as V_BIZ_RES_NO --사업자번호
            , nm_trade as V_COMP_NM --업체명
            , cnt_sumsh as V_ISSUE_QTY --매수
            , mn_supply as V_AMT --공급가액
            , mn_vat as V_TAX --세액
            , '' as V_ETC --비고
    from
    (
        |inner_select_query|
    ) as all_data
    order by ty_buysale, num;

        """

        # 정기신고, 수정신고
        # 정기신고와 수정신고 조건이 모두 걸리지 않는 경우 query_tax_bill 쿼리 설정이 안되어서 에러 발생
        # query_tax_bill 쿼리 조건을 확인하여 수정 필요
        # 세금계산서  ty_buysale=1:매출세금 / ty_buysale=2:매입세금
        query_tax_bill = """
            select (case when |ty_buysale| = 2 then 1 else 3 end) as ty_buysale -- 1 매입세금, 2 매입계산, 3 매출세금, 4 매출계산
                 , row_number() over(order by ord_key, no_biz) as num --일련번호
                 , ty_biz -- 1 2 사업자번호 구분
                 , no_biz --사업자번호
                 , nm_trade --업체명
                 , cnt_sumsh --매수
                 , mn_supply --공급가액
                 , mn_vat --세액
            from
                (
                    select ord_key --일련번호
                         , ty_biz -- 1 2 사업자번호 구분
                         , max(coalesce(no_biz,'')) as no_biz --사업자번호
                         , nm_trade --업체명
                         , coalesce(sum(cnt_sumsh), 0) as cnt_sumsh --매수
                         , coalesce(sum(mn_supply), 0) as mn_supply --공급가액
                         , coalesce(sum(mn_vat), 0) as mn_vat --세액
                    from (
                             select 0 as ord_key
                                  , a.cd_trade
                                  , a.nm_trade
                                  , a.no_trade as no_biz
                                  , coalesce(sum(a.cnt_sumsh), 0) as cnt_sumsh
                                  , coalesce(sum(a.mn_supply), 0) as mn_supply
                                  , coalesce(sum(a.mn_vat), 0) as mn_vat
                                  , b.ty_biz
                             from fta_billcont as a
                                      left join ftb_trade as b on a.cd_trade = b.cd_trade
                             where a.dm_fndend = %(dm_fndend)s
                               and a.ty_rpt = %(ty_rpt)s    -- 신고구분
                               and a.ty_use = 1
                               and coalesce(a.no_count,1) = case when %(ty_rpt)s  = 3 then %(no_count_g)s else coalesce(no_count,1) end -- 수정신고일경우수정차수조건추가
                               and a.ty_buysale = |ty_buysale| -- 매입매출구분 1.매출 2.매입
                               and a.ty_elctax = 1
                               and (b.ty_biz = 0 and coalesce(a.yn_beforebiz, 0) = 0)
                             group by a.cd_trade, a.nm_trade, a.no_trade, a.nm_ceo
                                    , a.str_mainbiz, a.yn_unitrpt, a.ty_elctax, b.ty_biz

                             union all

                             select 1 as ord_key
                                  , max(a.cd_trade)
                                  , ''::varchar(30) as nm_trade
                                  , max(a.no_trade) as no_biz
                                  , coalesce(sum(a.cnt_sumsh), 0) as cnt_sumsh
                                  , sum(a.mn_supply) as mn_supply
                                  , sum(a.mn_vat) as mn_vat
                                  , 1 as ty_biz
                             from fta_billcont as a
                                      left join ftb_trade as b on a.cd_trade = b.cd_trade
                             where a.dm_fndend = %(dm_fndend)s
                               and a.ty_rpt = %(ty_rpt)s    -- 신고구분
                               and a.ty_use = 1
                               and coalesce(a.no_count,1) = case when %(ty_rpt)s = 3 then %(no_count_g)s else coalesce(no_count,1) end -- 수정신고일경우수정차수조건추가
                               and a.ty_buysale = |ty_buysale| -- 매입매출구분 1.매출 2.매입
                               and a.ty_elctax = 1
                               and (b.ty_biz is null or b.ty_biz in (1,2) or a.yn_beforebiz = 1)
                               --          group by a.nm_trade

                             union all

                             select 0 as ord_key
                                  , a.cd_trade
                                  , a.nm_trade
                                  , a.no_trade as no_biz
                                  , coalesce(sum(a.cnt_sumsh), 0) as cnt_sumsh
                                  , coalesce(sum(a.mn_supply), 0) as mn_supply
                                  , coalesce(sum(a.mn_vat), 0) as mn_vat
                                  , b.ty_biz
                             from fta_billcont_before as a
                                      left join ftb_trade_before as b on a.cd_trade = b.cd_trade and b.gisu = %(gisu)s and a.gisu = b.gisu
                             where a.dm_fndend = %(dm_fndend)s
                               and a.ty_rpt = %(ty_rpt)s    -- 신고구분
                               and a.ty_use = 1
                               and coalesce(a.no_count,1) = case when %(ty_rpt)s = 3 then %(no_count_g)s else coalesce(no_count,1) end -- 수정신고일경우수정차수조건추가
                               and a.ty_buysale = |ty_buysale| -- 매입매출구분 1.매출 2.매입
                               and a.gisu = %(gisu)s
                               and a.ty_elctax = 1
                               and (b.ty_biz = 0 and coalesce(a.yn_beforebiz, 0) = 0)
                             group by a.cd_trade, a.nm_trade, a.no_trade, a.nm_ceo
                                    , a.str_mainbiz, a.yn_unitrpt, a.ty_elctax, b.ty_biz

                             union all

                             select 1 as ord_key
                                  , max(a.cd_trade)
                                  , ''::varchar(30) as nm_trade
                                  , max(a.no_trade) as no_biz
                                  , coalesce(sum(a.cnt_sumsh), 0) as cnt_sumsh
                                  , sum(a.mn_supply) as mn_supply
                                  , sum(a.mn_vat) as mn_vat
                                  , 1 as ty_biz
                             from fta_billcont_before as a
                                      left join ftb_trade_before as b on a.cd_trade = b.cd_trade and b.gisu = %(gisu)s and a.gisu = b.gisu
                             where a.dm_fndend = %(dm_fndend)s
                               and a.ty_rpt = %(ty_rpt)s    -- 신고구분
                               and a.ty_use = 1
                               and coalesce(a.no_count,1) = case when %(ty_rpt)s = 3 then %(no_count_g)s else coalesce(no_count,1) end -- 수정신고일경우수정차수조건추가
                               and a.ty_buysale = |ty_buysale| -- 매입매출구분 1.매출 2.매입
                               and a.gisu = %(gisu)s
                               and a.ty_elctax = 1
                               and (b.ty_biz is null or b.ty_biz in (1,2) or a.yn_beforebiz = 1)
                         ) main
                    group by main.ord_key, main.cd_trade, main.nm_trade
                           , main.ty_biz
                    order by main.ord_key
                ) as tt
        """
        # 계산서  ty_buysale=1:매출계산서 / ty_buysale=2:매입계산서
        query_bill = """
            select (case when |ty_buysale| = 2 then 2 else 4 end) as ty_buysale -- 1 매입세금, 2 매입계산, 3 매출세금, 4 매출계산
                 , row_number() over(order by ord_key, no_biz) as num --일련번호
                 , ty_biz -- 1 2 사업자번호 구분
                 , no_biz --사업자번호
                 , nm_trade --업체명
                 , cnt_sumsh --매수
                 , mn_supply --공급가액
                 , mn_vat --세액
            from
                (
                    select ord_key                                  --일련번호
                         , ty_biz                                   -- 1 2 사업자번호 구분
                         , max(coalesce(no_biz, ''))   as no_biz    --사업자번호
                         , nm_trade                                 --업체명
                         , coalesce(sum(cnt_sumsh), 0) as cnt_sumsh --매수
                         , coalesce(sum(mn_supply), 0) as mn_supply --공급가액
                         , coalesce(sum(mn_vat), 0)    as mn_vat    --세액
                    from (
                             select 0                             as ord_key
                                  , a.cd_trade
                                  , a.nm_trade
                                  , a.no_trade                    as no_biz
                                  , coalesce(sum(a.cnt_sumsh), 0) as cnt_sumsh
                                  , coalesce(sum(a.mn_supply), 0) as mn_supply
                                  , coalesce(sum(a.mn_vat), 0)    as mn_vat
                                  , b.ty_biz
                             from fta_billcont as a
                                      left join ftb_trade as b on a.cd_trade = b.cd_trade
                             where a.dm_fndend = %(dm_fndend)s
                               and a.ty_rpt = 1
                               and a.ty_use = 2
                               and a.no_count = coalesce(no_count, 1)
                               and a.ty_buysale = |ty_buysale|
                               and a.ty_elctax = 1
                               and (b.ty_biz = 0 and coalesce(a.yn_beforebiz, 0) = 0)
                             group by a.ty_buysale, a.ty_elctax, a.cd_trade, a.nm_trade, a.no_trade, a.nm_ceo
                                    , a.str_mainbiz, a.yn_unitrpt, a.ty_elctax, b.ty_biz

                             union all

                             select 1                             as ord_key
                                  , max(a.cd_trade)
                                  , ''::varchar(30)               as nm_trade
                                  , max(a.no_trade)               as no_biz
                                  , coalesce(sum(a.cnt_sumsh), 0) as cnt_sumsh
                                  , sum(a.mn_supply)              as mn_supply
                                  , sum(a.mn_vat)                 as mn_vat
                                  , 1                             as ty_biz
                             from fta_billcont as a
                                      left join ftb_trade as b on a.cd_trade = b.cd_trade
                             where a.dm_fndend = %(dm_fndend)s
                               and a.ty_rpt = 1
                               and a.ty_use = 2
                               and a.no_count = coalesce(no_count, 1)
                               and a.ty_buysale = |ty_buysale|
                               and a.ty_elctax = 1
                               and (a.cd_trade is null or b.ty_biz in (1, 2) or a.yn_beforebiz = 1)
                             group by a.ty_buysale, a.ty_elctax -- , a.cd_trade, a.no_trade, a.nm_trade

                             union all

                             select 0                             as ord_key
                                  , a.cd_trade
                                  , a.nm_trade
                                  , a.no_trade                    as no_biz
                                  , coalesce(sum(a.cnt_sumsh), 0) as cnt_sumsh
                                  , coalesce(sum(a.mn_supply), 0) as mn_supply
                                  , coalesce(sum(a.mn_vat), 0)    as mn_vat
                                  , b.ty_biz
                             from fta_billcont_before as a
                                      left join ftb_trade_before as b on a.cd_trade = b.cd_trade and b.gisu = %(gisu)s and a.gisu = b.gisu
                             where a.dm_fndend = %(dm_fndend)s
                               and a.ty_rpt = 1
                               and a.ty_use = 2
                               and a.no_count = coalesce(no_count, 1)
                               and a.ty_buysale = |ty_buysale|
                               and a.ty_elctax = 1
                               and (b.ty_biz = 0 and coalesce(a.yn_beforebiz, 0) = 0)
                               and a.gisu = %(gisu)s
                             group by a.ty_buysale, a.ty_elctax, a.cd_trade, a.nm_trade, a.no_trade, a.nm_ceo
                                    , a.str_mainbiz, a.yn_unitrpt, a.ty_elctax, b.ty_biz

                             union all

                             select 1                             as ord_key
                                  , max(a.cd_trade)
                                  , ''::varchar(30)               as nm_trade
                                  , max(a.no_trade)               as no_biz
                                  , coalesce(sum(a.cnt_sumsh), 0) as cnt_sumsh
                                  , sum(a.mn_supply)              as mn_supply
                                  , sum(a.mn_vat)                 as mn_vat
                                  , 1                             as ty_biz
                             from fta_billcont_before as a
                                      left join ftb_trade_before as b on a.cd_trade = b.cd_trade and b.gisu = %(gisu)s and a.gisu = b.gisu
                             where a.dm_fndend = %(dm_fndend)s
                               and a.ty_rpt = 1
                               and a.ty_use = 2
                               and a.no_count = coalesce(no_count, 1)
                               and a.ty_buysale = |ty_buysale|
                               and a.ty_elctax = 1
                               and (a.cd_trade is null or b.ty_biz in (1, 2) or a.yn_beforebiz = 1)
                               and a.gisu = %(gisu)s
                             group by a.ty_buysale, a.ty_elctax -- , a.cd_trade, a.no_trade, a.nm_trade
                         ) main
                    group by main.ord_key, main.cd_trade, main.nm_trade, main.no_biz
                           , main.ty_biz
                    order by main.ord_key
                ) as tt
        """

        inner_select_query = """/* 매입 세금계산서 */ """ + query_tax_bill.replace('|ty_buysale|', '2')  # 1. 매입 세금 계산서

        inner_select_query += """ union all /* 매입계산서 */ """ + query_bill.replace('|ty_buysale|', '2')  # 1. 매입 계산서

        inner_select_query += """ union all /* 매출 세금계산서 */ """ + query_tax_bill.replace('|ty_buysale|', '1')  # 1. 매출 세금 계산서

        inner_select_query += """ union all /* 매출계산서 */ """ + query_bill.replace('|ty_buysale|', '1')  # 1. 매출 계산서

        return query.replace('|inner_select_query|', inner_select_query)

    def select_detail_js(self):
        # ty_buysale = 매출매입종류
        # 1 : 세금계산서
        # 3 : 계산서

        query = """
        select (select array_to_json(array_agg(js_row))
        from (
            select
                    ty_buysale as V_SB_GB -- 1 매입세금, 2 매입계산, 3 매출세금, 3 매출계산
                    , num as V_SEQ_NO--일련번호
                    , ty_biz as V_BIZRES_GB -- 1 2 사업자번호 구분
                    , no_biz as V_BIZ_RES_NO --사업자번호
                    , nm_trade as V_COMP_NM --업체명
                    , cnt_sumsh as V_ISSUE_QTY --매수
                    , mn_supply as V_AMT --공급가액
                    , mn_vat as V_TAX --세액
                    , '' as V_ETC --비고
            from
            (
                |inner_select_query|
            ) as all_data
            order by ty_buysale, num
        ) as js_row) as js_detail_data;
        """

        # 정기신고, 수정신고
        # 정기신고와 수정신고 조건이 모두 걸리지 않는 경우 query_tax_bill 쿼리 설정이 안되어서 에러 발생
        # query_tax_bill 쿼리 조건을 확인하여 수정 필요
        # 세금계산서  ty_buysale=1:매출세금 / ty_buysale=2:매입세금
        query_tax_bill = """
            select (case when |ty_buysale| = 2 then 1 else 3 end) as ty_buysale -- 1 매입세금, 2 매입계산, 3 매출세금, 4 매출계산
                 , row_number() over(order by ord_key, no_biz) as num --일련번호
                 , ty_biz -- 1 2 사업자번호 구분
                 , no_biz --사업자번호
                 , nm_trade --업체명
                 , cnt_sumsh --매수
                 , mn_supply --공급가액
                 , mn_vat --세액
            from
                (
                    select ord_key --일련번호
                         , ty_biz -- 1 2 사업자번호 구분
                         , max(coalesce(no_biz,'')) as no_biz --사업자번호
                         , nm_trade --업체명
                         , coalesce(sum(cnt_sumsh), 0) as cnt_sumsh --매수
                         , coalesce(sum(mn_supply), 0) as mn_supply --공급가액
                         , coalesce(sum(mn_vat), 0) as mn_vat --세액
                    from (
                             select 0 as ord_key
                                  , a.cd_trade
                                  , a.nm_trade
                                  , a.no_trade as no_biz
                                  , coalesce(sum(a.cnt_sumsh), 0) as cnt_sumsh
                                  , coalesce(sum(a.mn_supply), 0) as mn_supply
                                  , coalesce(sum(a.mn_vat), 0) as mn_vat
                                  , b.ty_biz
                             from fta_billcont as a
                                      left join ftb_trade as b on a.cd_trade = b.cd_trade
                             where a.dm_fndend = %(dm_fndend)s
                               and a.ty_rpt = %(ty_rpt)s    -- 신고구분
                               and a.ty_use = 1
                               and coalesce(a.no_count,1) = case when %(ty_rpt)s  = 3 then %(no_count_g)s else coalesce(no_count,1) end -- 수정신고일경우수정차수조건추가
                               and a.ty_buysale = |ty_buysale| -- 매입매출구분 1.매출 2.매입
                               and a.ty_elctax = 1
                               and (b.ty_biz = 0 and coalesce(a.yn_beforebiz, 0) = 0)
                             group by a.cd_trade, a.nm_trade, a.no_trade, a.nm_ceo
                                    , a.str_mainbiz, a.yn_unitrpt, a.ty_elctax, b.ty_biz

                             union all

                             select 1 as ord_key
                                  , max(a.cd_trade)
                                  , ''::varchar(30) as nm_trade
                                  , max(a.no_trade) as no_biz
                                  , coalesce(sum(a.cnt_sumsh), 0) as cnt_sumsh
                                  , sum(a.mn_supply) as mn_supply
                                  , sum(a.mn_vat) as mn_vat
                                  , 1 as ty_biz
                             from fta_billcont as a
                                      left join ftb_trade as b on a.cd_trade = b.cd_trade
                             where a.dm_fndend = %(dm_fndend)s
                               and a.ty_rpt = %(ty_rpt)s    -- 신고구분
                               and a.ty_use = 1
                               and coalesce(a.no_count,1) = case when %(ty_rpt)s = 3 then %(no_count_g)s else coalesce(no_count,1) end -- 수정신고일경우수정차수조건추가
                               and a.ty_buysale = |ty_buysale| -- 매입매출구분 1.매출 2.매입
                               and a.ty_elctax = 1
                               and (b.ty_biz is null or b.ty_biz in (1,2) or a.yn_beforebiz = 1)
                               --          group by a.nm_trade

                             union all

                             select 0 as ord_key
                                  , a.cd_trade
                                  , a.nm_trade
                                  , a.no_trade as no_biz
                                  , coalesce(sum(a.cnt_sumsh), 0) as cnt_sumsh
                                  , coalesce(sum(a.mn_supply), 0) as mn_supply
                                  , coalesce(sum(a.mn_vat), 0) as mn_vat
                                  , b.ty_biz
                             from fta_billcont_before as a
                                      left join ftb_trade_before as b on a.cd_trade = b.cd_trade and b.gisu = %(gisu)s and a.gisu = b.gisu
                             where a.dm_fndend = %(dm_fndend)s
                               and a.ty_rpt = %(ty_rpt)s    -- 신고구분
                               and a.ty_use = 1
                               and coalesce(a.no_count,1) = case when %(ty_rpt)s = 3 then %(no_count_g)s else coalesce(no_count,1) end -- 수정신고일경우수정차수조건추가
                               and a.ty_buysale = |ty_buysale| -- 매입매출구분 1.매출 2.매입
                               and a.gisu = %(gisu)s
                               and a.ty_elctax = 1
                               and (b.ty_biz = 0 and coalesce(a.yn_beforebiz, 0) = 0)
                             group by a.cd_trade, a.nm_trade, a.no_trade, a.nm_ceo
                                    , a.str_mainbiz, a.yn_unitrpt, a.ty_elctax, b.ty_biz

                             union all

                             select 1 as ord_key
                                  , max(a.cd_trade)
                                  , ''::varchar(30) as nm_trade
                                  , max(a.no_trade) as no_biz
                                  , coalesce(sum(a.cnt_sumsh), 0) as cnt_sumsh
                                  , sum(a.mn_supply) as mn_supply
                                  , sum(a.mn_vat) as mn_vat
                                  , 1 as ty_biz
                             from fta_billcont_before as a
                                      left join ftb_trade_before as b on a.cd_trade = b.cd_trade and b.gisu = %(gisu)s and a.gisu = b.gisu
                             where a.dm_fndend = %(dm_fndend)s
                               and a.ty_rpt = %(ty_rpt)s    -- 신고구분
                               and a.ty_use = 1
                               and coalesce(a.no_count,1) = case when %(ty_rpt)s = 3 then %(no_count_g)s else coalesce(no_count,1) end -- 수정신고일경우수정차수조건추가
                               and a.ty_buysale = |ty_buysale| -- 매입매출구분 1.매출 2.매입
                               and a.gisu = %(gisu)s
                               and a.ty_elctax = 1
                               and (b.ty_biz is null or b.ty_biz in (1,2) or a.yn_beforebiz = 1)
                         ) main
                    group by main.ord_key, main.cd_trade, main.nm_trade
                           , main.ty_biz
                    order by main.ord_key
                ) as tt
        """
        # 계산서  ty_buysale=1:매출계산서 / ty_buysale=2:매입계산서
        query_bill = """
            select (case when |ty_buysale| = 2 then 2 else 4 end) as ty_buysale -- 1 매입세금, 2 매입계산, 3 매출세금, 4 매출계산
                 , row_number() over(order by ord_key, no_biz) as num --일련번호
                 , ty_biz -- 1 2 사업자번호 구분
                 , no_biz --사업자번호
                 , nm_trade --업체명
                 , cnt_sumsh --매수
                 , mn_supply --공급가액
                 , mn_vat --세액
            from
                (
                    select ord_key                                  --일련번호
                         , ty_biz                                   -- 1 2 사업자번호 구분
                         , max(coalesce(no_biz, ''))   as no_biz    --사업자번호
                         , nm_trade                                 --업체명
                         , coalesce(sum(cnt_sumsh), 0) as cnt_sumsh --매수
                         , coalesce(sum(mn_supply), 0) as mn_supply --공급가액
                         , coalesce(sum(mn_vat), 0)    as mn_vat    --세액
                    from (
                             select 0                             as ord_key
                                  , a.cd_trade
                                  , a.nm_trade
                                  , a.no_trade                    as no_biz
                                  , coalesce(sum(a.cnt_sumsh), 0) as cnt_sumsh
                                  , coalesce(sum(a.mn_supply), 0) as mn_supply
                                  , coalesce(sum(a.mn_vat), 0)    as mn_vat
                                  , b.ty_biz
                             from fta_billcont as a
                                      left join ftb_trade as b on a.cd_trade = b.cd_trade
                             where a.dm_fndend = %(dm_fndend)s
                               and a.ty_rpt = 1
                               and a.ty_use = 2
                               and a.no_count = coalesce(no_count, 1)
                               and a.ty_buysale = |ty_buysale|
                               and a.ty_elctax = 1
                               and (b.ty_biz = 0 and coalesce(a.yn_beforebiz, 0) = 0)
                             group by a.ty_buysale, a.ty_elctax, a.cd_trade, a.nm_trade, a.no_trade, a.nm_ceo
                                    , a.str_mainbiz, a.yn_unitrpt, a.ty_elctax, b.ty_biz

                             union all

                             select 1                             as ord_key
                                  , max(a.cd_trade)
                                  , ''::varchar(30)               as nm_trade
                                  , max(a.no_trade)               as no_biz
                                  , coalesce(sum(a.cnt_sumsh), 0) as cnt_sumsh
                                  , sum(a.mn_supply)              as mn_supply
                                  , sum(a.mn_vat)                 as mn_vat
                                  , 1                             as ty_biz
                             from fta_billcont as a
                                      left join ftb_trade as b on a.cd_trade = b.cd_trade
                             where a.dm_fndend = %(dm_fndend)s
                               and a.ty_rpt = 1
                               and a.ty_use = 2
                               and a.no_count = coalesce(no_count, 1)
                               and a.ty_buysale = |ty_buysale|
                               and a.ty_elctax = 1
                               and (a.cd_trade is null or b.ty_biz in (1, 2) or a.yn_beforebiz = 1)
                             group by a.ty_buysale, a.ty_elctax -- , a.cd_trade, a.no_trade, a.nm_trade

                             union all

                             select 0                             as ord_key
                                  , a.cd_trade
                                  , a.nm_trade
                                  , a.no_trade                    as no_biz
                                  , coalesce(sum(a.cnt_sumsh), 0) as cnt_sumsh
                                  , coalesce(sum(a.mn_supply), 0) as mn_supply
                                  , coalesce(sum(a.mn_vat), 0)    as mn_vat
                                  , b.ty_biz
                             from fta_billcont_before as a
                                      left join ftb_trade_before as b on a.cd_trade = b.cd_trade and b.gisu = %(gisu)s and a.gisu = b.gisu
                             where a.dm_fndend = %(dm_fndend)s
                               and a.ty_rpt = 1
                               and a.ty_use = 2
                               and a.no_count = coalesce(no_count, 1)
                               and a.ty_buysale = |ty_buysale|
                               and a.ty_elctax = 1
                               and (b.ty_biz = 0 and coalesce(a.yn_beforebiz, 0) = 0)
                               and a.gisu = %(gisu)s
                             group by a.ty_buysale, a.ty_elctax, a.cd_trade, a.nm_trade, a.no_trade, a.nm_ceo
                                    , a.str_mainbiz, a.yn_unitrpt, a.ty_elctax, b.ty_biz

                             union all

                             select 1                             as ord_key
                                  , max(a.cd_trade)
                                  , ''::varchar(30)               as nm_trade
                                  , max(a.no_trade)               as no_biz
                                  , coalesce(sum(a.cnt_sumsh), 0) as cnt_sumsh
                                  , sum(a.mn_supply)              as mn_supply
                                  , sum(a.mn_vat)                 as mn_vat
                                  , 1                             as ty_biz
                             from fta_billcont_before as a
                                      left join ftb_trade_before as b on a.cd_trade = b.cd_trade and b.gisu = %(gisu)s and a.gisu = b.gisu
                             where a.dm_fndend = %(dm_fndend)s
                               and a.ty_rpt = 1
                               and a.ty_use = 2
                               and a.no_count = coalesce(no_count, 1)
                               and a.ty_buysale = |ty_buysale|
                               and a.ty_elctax = 1
                               and (a.cd_trade is null or b.ty_biz in (1, 2) or a.yn_beforebiz = 1)
                               and a.gisu = %(gisu)s
                             group by a.ty_buysale, a.ty_elctax -- , a.cd_trade, a.no_trade, a.nm_trade
                         ) main
                    group by main.ord_key, main.cd_trade, main.nm_trade, main.no_biz
                           , main.ty_biz
                    order by main.ord_key
                ) as tt
        """

        inner_select_query = """/* 매입 세금계산서 */ """ + query_tax_bill.replace('|ty_buysale|', '2')  # 1. 매입 세금 계산서

        inner_select_query += """ union all /* 매입계산서 */ """ + query_bill.replace('|ty_buysale|', '2')  # 1. 매입 계산서

        inner_select_query += """ union all /* 매출 세금계산서 */ """ + query_tax_bill.replace('|ty_buysale|', '1')  # 1. 매출 세금 계산서

        inner_select_query += """ union all /* 매출계산서 */ """ + query_bill.replace('|ty_buysale|', '1')  # 1. 매출 계산서

        return query.replace('|inner_select_query|', inner_select_query)

    # 세금계산서 합계표 (매출/매입) 과거 / 당기 구분
    def select_billcont_45_46_bfo_dang(self, ty_rpt, old_view):
        # 당기
        if int(old_view) == 0:
            # 정기
            if int(ty_rpt) == 1:
                query = """
                    --favf0117_mr_acct_data
                    drop table if exists temp_usp_savf0117_mr_acct_data;
                    create temp table temp_usp_savf0117_mr_acct_data on commit drop as
                    select a.cd_trade, cast(a.nm_trade as varchar(40)) as nm_trade,
                           case when coalesce(a.cd_trade,'') <> '' then b.ty_biz
                                else case when length(a.no_trade) = 10 then 0 else 1 end end as ty_biz,
                           cast(a.mn_supply as numeric(17)) as mn_mnam, cast(a.mn_vat as numeric(17)) as mn_vat,
                           a.no_trade as no_biz,
                           a.str_mainbiz,
                           a.cnt_sumsh as cnt,
                           1 as ty_elctax,
                           coalesce(a.yn_beforebiz, 0) as yn_beforebiz --필드 추가 등록전매입구분(0.해당없음(null), 1.해당됨)
                    from fta_billcont a
                             left join ftb_trade b on a.cd_trade = b.cd_trade
                    where a.ty_use = 1 -- 사용구분 1.세금계산서합계표 2.계산서합계표
                      and a.dm_fndend = %(dm_fndend)s
                      and a.ty_buysale = %(ty_buysale)s  --매입/매출구분
                      and a.ty_rpt = %(ty_rpt)s    --신고구분
                      and case when %(ty_buysale)s = '1' then a.ty_item in (11,12,25) else a.ty_item in (51,52,54,55) end
                      and coalesce(a.no_count,1) = case when %(ty_rpt)s = 3 then %(no_count)s else coalesce(no_count,1) end -- 수정신고일경우수정차수조건추가
                      and coalesce(ty_elctax,0) = 1 -- 전자분
                      and length(a.no_trade) = 10 -- 주민번호로 데이터 입력 후 거래처등록에서 사업자로 바뀐 케이스 처리 
                    order by b.ty_biz, a.cd_trade, a.no_trade;
                """
            # 수정
            else:
                query = """
                    --favf0117_mr_acct_data
                    drop table if exists temp_usp_savf0117_mr_acct_data;
                    create temp table temp_usp_savf0117_mr_acct_data on commit drop as
                    select a.cd_trade, cast(a.nm_trade as varchar(40)) as nm_trade
                         , case when coalesce(a.cd_trade,'') <> '' then b.ty_biz
                                else case when length(a.no_trade) = 10 then 0 else 1 end end as ty_biz
                         , cast(a.mn_supply as numeric(17)) as mn_mnam, cast(a.mn_vat as numeric(17)) as mn_vat
                         , a.no_trade as no_biz
                         , a.str_mainbiz
                         , a.cnt_sumsh as cnt
                         , 1 as ty_elctax
                         , coalesce(a.yn_beforebiz, 0) as yn_beforebiz --필드 추가 등록전매입구분(0.해당없음(null), 1.해당됨)
                    from fta_billcont a
                             left join ftb_trade b on a.cd_trade = b.cd_trade
                    where a.ty_use = 1
                      and a.dm_fndend = %(dm_fndend)s
                      and a.ty_buysale = %(ty_buysale)s  --매입/매출구분
                      and a.ty_rpt = %(ty_rpt)s    --신고구분
                      and case when %(ty_buysale)s = '1' then a.ty_item in (11,12,25) else a.ty_item in (51,52,54,55) end
                      and coalesce(a.no_count,1) = case when %(ty_rpt)s  = 3 then %(no_count)s else coalesce(no_count,1) end -- 수정신고일경우수정차수조건추가
                      and coalesce(ty_elctax,0) = 1
                      and length(a.no_trade) = 10 -- 주민번호로 데이터 입력 후 거래처등록에서 사업자로 바뀐 케이스 처리
                    order by b.ty_biz, a.cd_trade, a.no_trade;


                    --favf0117_mr_acct_data --정기 신고 데이터 조회
                    drop table if exists temp_usp_savf0117_mr_acct_data_pre;
                    create temp table temp_usp_savf0117_mr_acct_data_pre on commit drop as
                    select a.cd_trade, cast(a.nm_trade as varchar(40)) as nm_trade
                         , case when coalesce(a.cd_trade,'') <> '' then b.ty_biz
                                else case when length(a.no_trade) = 10 then 0 else 1 end end as ty_biz
                         , cast(a.mn_supply as numeric(17)) as mn_mnam, cast(a.mn_vat as numeric(17)) as mn_vat
                         , a.no_trade as no_biz
                         , a.str_mainbiz
                         , a.cnt_sumsh as cnt
                         , 1 as ty_elctax
                         , coalesce(a.yn_beforebiz, 0) as yn_beforebiz --필드 추가 등록전매입구분(0.해당없음(null), 1.해당됨)
                    from fta_billcont a
                             left join ftb_trade b on a.cd_trade = b.cd_trade
                    where a.ty_use = 1
                      and a.dm_fndend = %(dm_fndend)s
                      and a.ty_buysale = %(ty_buysale)s  --매입/매출구분
                      and a.ty_rpt = %(ty_rpt_pre)s    --신고구분
                      and case when %(ty_buysale)s = '1' then a.ty_item in (11,12,25) else a.ty_item in (51,52,54,55) end
                      and coalesce(a.no_count,1) = case when %(ty_rpt_pre)s  = 3 then %(no_count_pre)s else coalesce(no_count,1) end -- 수정신고일경우수정차수조건추가
                      and coalesce(ty_elctax,0) = 1
                      and length(a.no_trade) = 10 -- 주민번호로 데이터 입력 후 거래처등록에서 사업자로 바뀐 케이스 처리
                    order by b.ty_biz, a.cd_trade, a.no_trade;
                """
        # 과거
        else:
            # 정기
            if int(ty_rpt) == 1:
                query = """
                    --favf0117_mr_acct_data
                    drop table if exists temp_usp_savf0117_mr_acct_data;
                    create temp table temp_usp_savf0117_mr_acct_data on commit drop as
                    select a.cd_trade, cast(a.nm_trade as varchar(40)) as nm_trade,
                           case when coalesce(a.cd_trade,'') <> '' then b.ty_biz
                                else case when length(a.no_trade) = 10 then 0 else 1 end end as ty_biz,
                           cast(a.mn_supply as numeric(17)) as mn_mnam, cast(a.mn_vat as numeric(17)) as mn_vat,
                           a.no_trade as no_biz,
                           a.str_mainbiz,
                           a.cnt_sumsh as cnt,
                           1 as ty_elctax,
                           coalesce(a.yn_beforebiz, 0) as yn_beforebiz --필드 추가 등록전매입구분(0.해당없음(null), 1.해당됨)
                    from FTA_BILLCONT_BEFORE a
                             left join FTB_TRADE_BEFORE b on a.cd_trade = b.cd_trade and b.gisu = %(gisu)s
                    where a.ty_use = 1 -- 사용구분 1.세금계산서합계표 2.계산서합계표
                      and a.dm_fndend = %(dm_fndend)s
                      and a.ty_buysale = %(ty_buysale)s  --매입/매출구분
                      and a.ty_rpt = %(ty_rpt)s    --신고구분
                      and case when %(ty_buysale)s = '1' then a.ty_item in (11,12,25) else a.ty_item in (51,52,54,55) end
                      and a.gisu = %(gisu)s
                      and coalesce(a.no_count,1) = case when %(ty_rpt)s = 3 then %(no_count)s else coalesce(no_count,1) end -- 수정신고일경우수정차수조건추가
                      and coalesce(ty_elctax,0) = 1 -- 전자분
                      and length(a.no_trade) = 10 -- 주민번호로 데이터 입력 후 거래처등록에서 사업자로 바뀐 케이스 처리 
                    order by b.ty_biz, a.cd_trade, a.no_trade;
                """
            # 수정
            else:
                query = """
                    --favf0117_mr_acct_data
                    drop table if exists temp_usp_savf0117_mr_acct_data;
                    create temp table temp_usp_savf0117_mr_acct_data on commit drop as
                    select a.cd_trade, cast(a.nm_trade as varchar(40)) as nm_trade
                         , case when coalesce(a.cd_trade,'') <> '' then b.ty_biz
                                else case when length(a.no_trade) = 10 then 0 else 1 end end as ty_biz
                         , cast(a.mn_supply as numeric(17)) as mn_mnam, cast(a.mn_vat as numeric(17)) as mn_vat
                         , a.no_trade as no_biz
                         , a.str_mainbiz
                         , a.cnt_sumsh as cnt
                         , 1 as ty_elctax
                         , coalesce(a.yn_beforebiz, 0) as yn_beforebiz --필드 추가 등록전매입구분(0.해당없음(null), 1.해당됨)
                    from FTA_BILLCONT_BEFORE a
                             left join FTB_TRADE_BEFORE b on a.cd_trade = b.cd_trade and b.gisu = %(gisu)s
                    where a.ty_use = 1
                      and a.dm_fndend = %(dm_fndend)s
                      and a.ty_buysale = %(ty_buysale)s  --매입/매출구분
                      and a.ty_rpt = %(ty_rpt)s    --신고구분
                      and case when %(ty_buysale)s = '1' then a.ty_item in (11,12,25) else a.ty_item in (51,52,54,55) end
                      and a.gisu = %(gisu)s
                      and coalesce(a.no_count,1) = case when %(ty_rpt)s  = 3 then %(no_count)s else coalesce(no_count,1) end -- 수정신고일경우수정차수조건추가
                      and coalesce(ty_elctax,0) = 1
                      and length(a.no_trade) = 10 -- 주민번호로 데이터 입력 후 거래처등록에서 사업자로 바뀐 케이스 처리
                    order by b.ty_biz, a.cd_trade, a.no_trade;


                    --favf0117_mr_acct_data --정기 신고 데이터 조회
                    drop table if exists temp_usp_savf0117_mr_acct_data_pre;
                    create temp table temp_usp_savf0117_mr_acct_data_pre on commit drop as
                    select a.cd_trade, cast(a.nm_trade as varchar(40)) as nm_trade
                         , case when coalesce(a.cd_trade,'') <> '' then b.ty_biz
                                else case when length(a.no_trade) = 10 then 0 else 1 end end as ty_biz
                         , cast(a.mn_supply as numeric(17)) as mn_mnam, cast(a.mn_vat as numeric(17)) as mn_vat
                         , a.no_trade as no_biz
                         , a.str_mainbiz
                         , a.cnt_sumsh as cnt
                         , 1 as ty_elctax
                         , coalesce(a.yn_beforebiz, 0) as yn_beforebiz --필드 추가 등록전매입구분(0.해당없음(null), 1.해당됨)
                    from FTA_BILLCONT_BEFORE a
                             left join FTB_TRADE_BEFORE b on a.cd_trade = b.cd_trade and b.gisu = %(gisu)s
                    where a.ty_use = 1
                      and a.dm_fndend = %(dm_fndend)s
                      and a.ty_buysale = %(ty_buysale)s  --매입/매출구분
                      and a.ty_rpt = %(ty_rpt_pre)s    --신고구분
                      and case when %(ty_buysale)s = '1' then a.ty_item in (11,12,25) else a.ty_item in (51,52,54,55) end
                      and a.gisu = %(gisu)s
                      and coalesce(a.no_count,1) = case when %(ty_rpt_pre)s  = 3 then %(no_count_pre)s else coalesce(no_count,1) end -- 수정신고일경우수정차수조건추가
                      and coalesce(ty_elctax,0) = 1
                      and length(a.no_trade) = 10 -- 주민번호로 데이터 입력 후 거래처등록에서 사업자로 바뀐 케이스 처리
                    order by b.ty_biz, a.cd_trade, a.no_trade;
                """
        return query

    # 세금계산서 합계표 (매출/매입)
    def select_billcont_45_46(self, ty_rpt):
        # 정기
        if int(ty_rpt) == 1:
            query = """
    -- 세금계산서 매출 마감 스트링 만들기  45번 46번(정기)

    drop table if exists temp_usp_savf0117_mr_acct_sum;
    create temp table temp_usp_savf0117_mr_acct_sum on commit drop as
    select case when t.yn_beforebiz = 1 or t.ty_biz in (1, 2) then '주민등록기재분' else t.no_biz end as no_biz
         , t.cd_trade_temp as cd_trade, t.ty_biz as ty_biz, t.str_mainbiz as str_mainbiz, t.nm_trade as nm_trade
         , t.cnt as cnt, t.mn_mnam as mn_mnam, t.mn_vat as mn_vat
         , cast(0 as numeric(17,0)) as cnt_pre, cast(0 as numeric(17,0)) as mn_mnam_pre, cast(0 as numeric(17,0)) as mn_vat_pre   --수정전
    from
        (
            select min(coalesce(no_biz,''))                                                as no_biz
                 , case when yn_beforebiz = 1 or ty_biz in (1, 2) then '' else coalesce(cd_trade,'')      end  as cd_trade_temp
                 , case when yn_beforebiz = 1 or ty_biz in (1, 2) then '' else min(coalesce(nm_trade,'')) end  as nm_trade
                 , case when yn_beforebiz = 1 or ty_biz in (1, 2) then 1 else ty_biz end as ty_biz
                 , min(coalesce(str_mainbiz,''))   as str_mainbiz
                 , 0 as cnt_pre, 0 as mn_mnam_pre, 0 as mn_vat_pre
                 , sum(coalesce(cnt,0)) as cnt, sum(coalesce(mn_mnam,0)) as mn_mnam, sum(coalesce(mn_vat,0)) as mn_vat
                 , ty_elctax as  ty_elctax
                 , yn_beforebiz --등록전매입
            from temp_usp_savf0117_mr_acct_data
            group by case when yn_beforebiz = 1 or ty_biz in (1, 2) then '' else coalesce(cd_trade,'') end
                   , case when yn_beforebiz = 1 or ty_biz in (1, 2) then 1 else ty_biz end
                   , ty_elctax, yn_beforebiz, str_mainbiz, ty_biz
        ) t
    order by  t.ty_biz, no_biz, t.cd_trade_temp, t.ty_elctax;


    create temp table if not exists tmp_multi_key on commit drop as
    select row_number() over() - 1 AS key, alpha
    from regexp_split_to_table('}JKLMNOPQR', '') as alpha;


    select 
    	case when %(ty_buysale)s = 1 then '1' else '2' end as ty_datastr,
    	case when %(ty_buysale)s = 1 then '1' else '2' end as cd_formstr,
    	'201409' as da_useend,
    	 170 as wm_max,                 

        ( (case when %(ty_buysale)s = 1 then 'W' else 'Y' end)
           || '§' || '0'           --보고자등록번호
           || '§' || row_number() over(order by no_biz)
           || '§' || coalesce(no_biz,'0')           --거래자등록번호
           || '§' || coalesce(replace(replace(nm_trade,'・','?'), '#', ''),'0')          --거래자상호
           || '§' || ' '               --업태
           || '§' || ' '               --종목
           || '§' || coalesce(cnt,0) --매수
           || '§' || 0                   --공란수
           -- || '§' || (case when coalesce(mn_mnam,0) < 0 then abs(floor(mn_mnam/10)) || multi_key.alpha else cast(mn_mnam as string) end)         --공급가액
           -- || '§' || (case when coalesce(mn_vat,0) < 0 then abs(floor(mn_vat/10)) || multi_key.alpha else cast(mn_vat as string) end)         --세액
           || '§' || coalesce(mn_mnam, 0)     --공급가액
           || '§' || coalesce(mn_vat, 0)      --세액    
           || '§' || case when %(dm_fndend)s >= '201301'
               then (case coalesce(substr(str_mainbiz,length(str_mainbiz), 1), '') --when 'ⓞ' then 0  --거래처등록에 주류코드 없이 수록할 경우도 매입매출따라가도록
                                                when ''   then '0'
                                                else '1' end)
               else substr( '0' || (select coalesce(cd_jr, 0)::varchar from ftb_com), length((select coalesce(cd_jr, 0)::varchar from ftb_com)) + 1, 1)
            end              --주류여부[0,1]_20130624
           || '§' || ' '          -- 주류코드 201706
           || '§' || (case when %(ty_buysale)s = 1 then '7501' else '8501' end)                          --권번호
           || '§' || strtonumeric(((select cd_taxoffcom from ftb_com)))           --제출서
           || '§' || ' '  --공란
        ) as str_close            
    from (
        select ty_biz, no_biz, cd_trade, nm_trade, cnt - cnt_pre as cnt, mn_mnam - mn_mnam_pre as mn_mnam, mn_vat - mn_vat_pre as mn_vat, str_mainbiz, '1' as gb_data, 1 as ord
        from temp_usp_savf0117_mr_acct_sum
        where coalesce(cnt - cnt_pre, 0) <> 0 or coalesce(mn_mnam - mn_mnam_pre, 0) <> 0 or coalesce(mn_vat - mn_vat_pre, 0) <> 0

        union all

        select ty_biz, no_biz, cd_trade, nm_trade, cnt_pre as cnta, mn_mnam_pre as mn_mnama, mn_vat_pre as mn_vata, str_mainbiz, 'a', 0
        from temp_usp_savf0117_mr_acct_sum
        where coalesce(cnt_pre, 0) <> 0 or coalesce(mn_mnam_pre, 0) <> 0 or coalesce(mn_vat_pre, 0) <> 0
    ) a
    where ty_biz = 0 and no_biz <> '주민등록기재분'                 --사업자등록분만, 주민기재분제외
    order by ty_biz, no_biz, cd_trade, ord;
            """
        # 수정
        else:
            query = """ 
    -- 세금계산서 매출 마감 스트링 만들기 45번 46번 (수정)

    drop table if exists temp_usp_savf0117_mr_acct_sum;
    create temp table temp_usp_savf0117_mr_acct_sum on commit drop as
    select case when t.yn_beforebiz = 1 or t.ty_biz in (1, 2) then '주민등록기재분' else t.no_biz end as no_biz
         , t.cd_trade                                               as cd_trade
         , t.ty_biz                                                 as ty_biz
         , t.str_mainbiz                                            as str_mainbiz
         , t.nm_trade                                               as nm_trade
         , t.cnt                                                    as cnt
         , t.mn_mnam                                                as mn_mnam
         , t.mn_vat                                                 as mn_vat
         , cnt_pre                                                  as cnt_pre
         , mn_mnam_pre                                              as mn_mnam_pre
         , mn_vat_pre                                               as mn_vat_pre--수정전
    from
        (
            select case when cur.no_biz is null or cur.no_biz = '' then pre.no_biz else cur.no_biz end as no_biz
                 , m.ty_biz
                 , m.cd_trade
                 , case when cur.nm_trade is null or cur.nm_trade = '' then pre.nm_trade else cur.nm_trade end as nm_trade
                 , case when cur.str_mainbiz is null or cur.str_mainbiz = '' then pre.str_mainbiz else cur.str_mainbiz end as str_mainbiz
                 , coalesce(pre.cnt,0) as cnt_pre, coalesce(pre.mn_mnam,0) as mn_mnam_pre, coalesce(pre.mn_vat,0) as mn_vat_pre
                 , coalesce(cur.cnt,0) as cnt, coalesce(cur.mn_mnam,0) as mn_mnam, coalesce(cur.mn_vat,0) as mn_vat
                 , m.ty_elctax
                 , cur.yn_beforebiz
            from (
                     select case when ty_biz in (1, 2) then '' else coalesce(cd_trade,'') end  as cd_trade
                          , case when ty_biz in (1, 2) then 1 else ty_biz end as ty_biz, ty_elctax
                     from temp_usp_savf0117_mr_acct_data
                     union
                     select case when ty_biz in (1, 2) then '' else coalesce(cd_trade,'') end  as cd_trade
                          , case when ty_biz in (1, 2) then 1 else ty_biz end as ty_biz, ty_elctax
                     from temp_usp_savf0117_mr_acct_data_pre
                 ) m
                     left outer join (
                select min(coalesce(no_biz,'')) as no_biz
                     , case when yn_beforebiz = 1 or ty_biz in (1, 2) then 1 else ty_biz end as ty_biz
                     , case when yn_beforebiz = 1 or ty_biz in (1, 2) then '' else coalesce(cd_trade,'')      end  as cd_trade
                     , case when yn_beforebiz = 1 or ty_biz in (1, 2) then '' else min(coalesce(nm_trade,'')) end  as nm_trade
                     , min(coalesce(str_mainbiz,''))    as str_mainbiz
                     , sum(coalesce(cnt,0)) as cnt, sum(coalesce(mn_mnam,0))  as mn_mnam, sum(coalesce(mn_vat,0)) as mn_vat
                     , ty_elctax
                     , yn_beforebiz --등록전매입
                from temp_usp_savf0117_mr_acct_data_pre
                group by case when yn_beforebiz = 1 or ty_biz in (1, 2) then '' else coalesce(cd_trade,'') end
                       , case when yn_beforebiz = 1 or ty_biz in (1, 2) then 1 else ty_biz end
                       , ty_elctax, yn_beforebiz, str_mainbiz, ty_biz
            ) pre on m.cd_trade = pre.cd_trade and m.ty_biz = pre.ty_biz and m.ty_elctax = pre.ty_elctax
                     left outer join (
                select min(coalesce(no_biz,'')) as no_biz
                     , case when ty_biz in (1, 2) then 1 else ty_biz end as ty_biz
                     , case when ty_biz in (1, 2) then '' else coalesce(cd_trade,'')      end  as cd_trade
                     , case when ty_biz in (1, 2) then '' else min(coalesce(nm_trade,'')) end  as nm_trade
                     , min(coalesce(str_mainbiz,''))    as str_mainbiz
                     , sum(coalesce(cnt,0)) as cnt, sum(coalesce(mn_mnam,0))  as mn_mnam, sum(coalesce(mn_vat,0)) as mn_vat
                     , ty_elctax
                     , yn_beforebiz --등록전매입
                from temp_usp_savf0117_mr_acct_data
                group by case when ty_biz in (1, 2) then '' else coalesce(cd_trade,'') end
                       , case when ty_biz in (1, 2) then 1 else ty_biz end
                       , ty_elctax, str_mainbiz, yn_beforebiz, ty_biz
            ) cur on m.cd_trade = cur.cd_trade and m.ty_biz = cur.ty_biz and m.ty_elctax = cur.ty_elctax and coalesce(pre.str_mainbiz,'') = coalesce(cur.str_mainbiz,'')
        ) t
    order by  t.ty_biz, no_biz, t.cd_trade, t.ty_elctax;


    create temp table if not exists tmp_multi_key on commit drop as 
    select row_number() over() - 1 AS key, alpha 
    from regexp_split_to_table('}JKLMNOPQR', '') as alpha;


    select
        '1' as ty_datastr,
    	'1' as cd_formstr,
    	'201409' as da_useend,
    	 170 as wm_max,       

       ( case when ord = 0 then (case when %(ty_buysale)s = 1 then 'X' else 'Z' end)          --자료구분
                 else (case when %(ty_buysale)s = 1 then 'W' else 'Y' end) end --'1'
           || '§' || '0'           --보고자등록번호
           || '§' || row_number() over(order by no_biz)
           || '§' || coalesce(no_biz,'0')           --거래자등록번호
           || '§' || coalesce(replace(replace(replace(nm_trade, chr(13) || chr(10), ''),'・','?'), '#', ''),'0')          --거래자상호
           || '§' || ' '               --업태
           || '§' || ' '               --종목
           || '§' || coalesce(cnt,0) --매수
           || '§' || 0                   --공란수
           -- || '§' || (case when coalesce(mn_mnam,0) < 0 then abs(floor(mn_mnam/10)) || multi_key.alpha else cast(mn_mnam as string) end)         --공급가액
           -- || '§' || (case when coalesce(mn_vat,0) < 0 then abs(floor(mn_vat/10)) || multi_key.alpha else cast(mn_vat as string) end)         --세액
           || '§' || coalesce(mn_mnam, 0)     --공급가액
           || '§' || coalesce(mn_vat, 0)      --세액    
           || '§' || case when %(dm_fndend)s >= '201301'
               then (case coalesce(substr(str_mainbiz,length(str_mainbiz), 1), '') --when 'ⓞ' then 0  --거래처등록에 주류코드 없이 수록할 경우도 매입매출따라가도록
                                                when ''   then '0'
                                                else '1' end)
               else substr( '0' || (select coalesce(cd_jr, 0)::varchar from ftb_com), length((select coalesce(cd_jr, 0)::varchar from ftb_com)) + 1, 1)
            end              --주류여부[0,1]_20130624
           || '§' || ' '          -- 주류코드 201706
           || '§' || (case when %(ty_buysale)s = 1 then '7501' else '8501' end)                          --권번호
           || '§' || strtonumeric((select coalesce(cd_taxoffcom, '') from ftb_com))           --제출서
           || '§' || ' '   --공란
         ) as str_close 
    from (
            select ty_biz, no_biz, cd_trade, nm_trade, cnt as cnt, mn_mnam as mn_mnam, mn_vat as mn_vat, str_mainbiz, '1' as gb_data, 0 as ord
            from temp_usp_savf0117_mr_acct_sum
            where coalesce(cnt - cnt_pre, 0) <> 0 or coalesce(mn_mnam - mn_mnam_pre, 0) <> 0 or coalesce(mn_vat - mn_vat_pre, 0) <> 0

            union all

            select ty_biz, no_biz, cd_trade, nm_trade, cnt_pre as cnta, mn_mnam_pre as mn_mnama, mn_vat_pre as mn_vata, str_mainbiz, 'a', 1
            from temp_usp_savf0117_mr_acct_sum
            where coalesce(cnt_pre, 0) <> 0 or coalesce(mn_mnam_pre, 0) <> 0 or coalesce(mn_vat_pre, 0) <> 0
        ) a
    where ty_biz = 0 and no_biz <> '주민등록기재분'                 --사업자등록분만, 주민기재분제외
    order by ty_biz, no_biz, cd_trade, ord;
            """

        return query

    # 세금계산서 합계표 (매출/매입) 과거 / 당기 구분
    def select_billcont_47_48_bfo_dang(self, old_view):
        # 당기
        if int(old_view) == 0:
            query = """
                --favf0117_mr_acct_data
                drop table if exists temp_usp_favf0117_mr_acct_data;
                create temp table temp_usp_favf0117_mr_acct_data on commit drop as
                select a.cd_trade, cast(a.nm_trade as varchar(40)) as nm_trade,
                       case when coalesce(a.cd_trade,'') <> '' then b.ty_biz
                            else case when length(a.no_trade) = 10 then 0 else 1 end end as ty_biz,
                       cast(a.mn_supply as numeric(17)) as mn_mnam, cast(a.mn_vat as numeric(17)) as mn_vat,
                       a.no_trade as no_biz,
                       a.cnt_sumsh as cnt,
                       1 as ty_elctax,
                       coalesce(a.yn_beforebiz, 0) as yn_beforebiz --필드 추가 등록전매입구분(0.해당없음(null), 1.해당됨)
                from fta_billcont a
                         left join ftb_trade b on a.cd_trade = b.cd_trade
                where a.ty_use = 2
                  and a.dm_fndend = %(dm_fndend)s
                  and a.ty_buysale = %(ty_buysale)s  --매입/매출구분
                  and a.ty_rpt = 1    --신고구분
                  and case when %(ty_buysale)s = '1' then a.ty_item = 13 else a.ty_item = 53 end
                  and coalesce(a.no_count,1) = 1 -- 수정신고일경우수정차수조건추가
                  and coalesce(ty_elctax,0) = 1
                  and length(a.no_trade) = 10 -- 주민번호로 데이터 입력 후 거래처등록에서 사업자로 바뀐 케이스 처리
                order by b.ty_biz, a.cd_trade, a.no_trade;
            """
        # 과거
        else:
            query = """
                --favf0117_mr_acct_data
                drop table if exists temp_usp_favf0117_mr_acct_data;
                create temp table temp_usp_favf0117_mr_acct_data on commit drop as
                select a.cd_trade, cast(a.nm_trade as varchar(40)) as nm_trade,
                       case when coalesce(a.cd_trade,'') <> '' then b.ty_biz
                            else case when length(a.no_trade) = 10 then 0 else 1 end end as ty_biz,
                       cast(a.mn_supply as numeric(17)) as mn_mnam, cast(a.mn_vat as numeric(17)) as mn_vat,
                       a.no_trade as no_biz,
                       a.cnt_sumsh as cnt,
                       1 as ty_elctax,
                       coalesce(a.yn_beforebiz, 0) as yn_beforebiz --필드 추가 등록전매입구분(0.해당없음(null), 1.해당됨)
                from FTA_BILLCONT_BEFORE a
                         left join FTB_TRADE_BEFORE b on a.cd_trade = b.cd_trade and b.gisu = %(gisu)s
                where a.ty_use = 2
                  and a.dm_fndend = %(dm_fndend)s
                  and a.ty_buysale = %(ty_buysale)s  --매입/매출구분
                  and a.ty_rpt = 1    --신고구분
                  and case when %(ty_buysale)s = '1' then a.ty_item = 13 else a.ty_item = 53 end
                  and a.gisu = %(gisu)s
                  and coalesce(a.no_count,1) = 1 -- 수정신고일경우수정차수조건추가
                  and coalesce(ty_elctax,0) = 1
                  and length(a.no_trade) = 10 -- 주민번호로 데이터 입력 후 거래처등록에서 사업자로 바뀐 케이스 처리
                order by b.ty_biz, a.cd_trade, a.no_trade;
            """
        return query

    # 계산서 합계표 (매출/매출) - 무조건 정기만 존재
    def select_billcont_47_48(self):
        query = """

    drop table if exists temp_usp_favf0117_mr_acct_sum;
    create temp table temp_usp_favf0117_mr_acct_sum on commit drop as
    select case when t.ty_biz in (1, 2) then '주민등록기재분' else t.no_biz end as no_biz,
           t.cd_trade_temp as cd_trade,
           t.ty_biz as ty_biz,
           t.nm_trade as nm_trade,
           t.cnt as cnt,
           t.mn_mnam as mn_mnam,
           t.mn_vat as mn_vat,
           cast(0 as numeric(17,0)) as cnt_pre,
           cast(0 as numeric(17,0)) as mn_mnam_pre,
           cast(0 as numeric(17,0)) as mn_vat_pre   --수정전
    from
        (
            select min(coalesce(no_biz,''))                                                as no_biz
                 , case when ty_biz in (1, 2) then '' else coalesce(cd_trade,'')      end  as cd_trade_temp
                 , case when ty_biz in (1, 2) then '' else min(coalesce(nm_trade,'')) end  as nm_trade
                 , case when ty_biz in (1, 2) then 1 else ty_biz end as ty_biz
                 , 0 as cnt_pre, 0 as mn_mnam_pre, 0 as mn_vat_pre
                 , sum(coalesce(cnt,0)) as cnt, sum(coalesce(mn_mnam,0)) as mn_mnam, sum(coalesce(mn_vat,0)) as mn_vat
                 , ty_elctax as  ty_elctax
                 , yn_beforebiz --등록전매입
            from temp_usp_favf0117_mr_acct_data
            group by case when ty_biz in (1, 2) then '' else coalesce(cd_trade,'') end
                   , case when ty_biz in (1, 2) then 1 else ty_biz end
                   , ty_elctax, yn_beforebiz, ty_biz
        ) t
    order by  t.ty_biz, no_biz, t.cd_trade_temp, t.ty_elctax;

    --------------------------------------------------------

    --매출처별거래명세레코드여러건
    select '17' as ty_datastr,
           'D' as cd_formstr,
           '201409' as da_useend,
           230 as wm_max,                 --자료구분
           case when %(ty_rpt)s = 1
                    then case when %(ty_buysale)s = 1 then 'S' else 'U' end
                else case when %(ty_buysale)s = 1 then 'T' else 'V' end
               end
               || '§' || case when %(ty_buysale)s = 1 then '17' else '18' end
               || '§' || %(prd_vat)s --1 기
               || '§' || %(ty_month)s -- 예확정구분 1.예정 2.확정
               || '§' || coalesce((select coalesce(cd_taxoffcom, '') from ftb_com),'0')  --3 세무서코드
               || '§' || row_number() over(order by no_biz) --6 일련번호
               || '§' || coalesce((select no_biz from ftb_com),'0') --10 사업자등록번호
               || '§' || coalesce(no_biz,'0')    --10 매출처사업자등록번호
               || '§' || coalesce(replace(replace(replace(nm_trade, chr(13) || chr(10), ''),'・','?'),'',''),'0')   --40 매출처법인명
               || '§' || coalesce(cnt,0)    --5 계산서매수
               || '§' || case when mn_mnam < 0 then '1' else  '0' end    --1 매출금액음수표시
               || '§' || case when mn_mnam < 0
                                  then coalesce(abs(mn_mnam),0)
                              else coalesce(mn_mnam,0) end   --14 매출금액
               || '§' || ' ' as str_close
    from temp_usp_favf0117_mr_acct_sum
    where ty_biz = 0 and no_biz <> '주민등록기재분'
            """

        return query

    def select_close_date_query(self):  # 작성일자
        query = """
            select COALESCE(str_save, '') as str_save from fta_vatrpt_before
            where da_fndend = %(da_fndend)s
            and ty_simple   = %(ty_simple)s
            and no_div      = 100
            and ty_save     = 2
            and no_cell     = 3005
            AND TY_RPT = case when %(ty_rpt)s::int = 2 then 1 else %(ty_rpt)s::int end

            union all

            select COALESCE(str_save, '') as str_save  from fta_vatrpt
            where da_fndend = %(da_fndend)s
            and ty_simple   = %(ty_simple)s
            and no_div      = 100
            and ty_save     = 2
            and no_cell     = 3005
            AND TY_RPT = case when %(ty_rpt)s::int = 2 then 1 else %(ty_rpt)s::int end;
        """
        return query