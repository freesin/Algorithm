# -*- coding: utf-8 -*-

from sao_etl_lib.sao_etl_json import name_to_json
from sao_etl_nice.inheritance.Inheritance_sao_etl import InheritanceSaoEtl
import calendar
from calendar import monthrange

class NiceVatClose(InheritanceSaoEtl):

    def __init__(self, sao_db_connection, schema_name, default_date_version, base_da_standard_begin):
        super().__init__(sao_db_connection, schema_name, default_date_version, base_da_standard_begin)

    def sao_etl_start(self):
        self.nice_vat_test()

    def nice_vat_test(self):
        query_string = """select * from ftb_com;"""
        self._sao_db_connection.cursor.execute(query_string)


        ls_info = []
        info = {}
        info['quarter'] = '20201'
        ls_info.append(info)

        params={'cno':'test'}

        ### 1. 제출자정보 및 회사 정보 가져오기 ###
        self._sao_db_connection.cursor.execute(self.select_company_info(self), params)
        com_info = name_to_json(self._sao_db_connection.cursor)

        # 수임처 회사의 ftx_trade[0]['cno']를 가지고 제출자 정보 가져오기
        if str(com_info[0]['ty_report']) == '1':  # 세무대리
            self._sao_db_connection.cursor.execute(self.select_fta_reporter(self), params)
            reperter = name_to_json(self._sao_db_connection.cursor)
        else:  # 일반회사   #일반회사 이면 자기 자신의 DB에서 제출자 정보 가져오기
            self._sao_db_connection.cursor.execute(self.select_fta_reporter(self), params)
            reperter = name_to_json(self._sao_db_connection.cursor)

        # ## 2. CI.xml 및 CompanyInfo.xml 파일 만들기 ###
        ############################################################################
        ## com_info 와 reperter를 가지고 CI.xml 및 CompanyInfo.xml 파일을 제작하시면 됩니다 ##
        ############################################################################


        for info in ls_info:
            if str(info['quarter'])[4:5] == '1':
                params['da_fndend'] = str(info['quarter'])[0:4] + '03'
            elif str(info['quarter'])[4:5] == '2':
                params['da_fndend'] = str(info['quarter'])[0:4] + '06'
            elif str(info['quarter'])[4:5] == '3':
                params['da_fndend'] = str(info['quarter'])[0:4] + '09'
            elif str(info['quarter'])[4:5] == '4':
                params['da_fndend'] = str(info['quarter'])[0:4] + '12'

            ### 3. 최종 부가세 마감 정보 가져오기 ###
            self._sao_db_connection.cursor.execute(self.select_vat_close_g(self), params)
            vat_info = name_to_json(self._sao_db_connection.cursor)
            params.update(vat_info[0])

            # 부가세 마감 string 조회
            self._sao_db_connection.cursor.execute(self.select_get_close_string(self), params)
            vat_list = name_to_json(self._sao_db_connection.cursor)

            # 부가세 헤더 만들기
            close_head = self.make_vat_head(self.reperter[0], com_info[0], vat_info[0], vat_list,  self._sao_db_connection.cursor)
            vat_list[0]['str_close'] = close_head

            # ## 4. 부가세 XML 구성 파일 만들기 - (1)의 제출자정보 및 회사 정보 활용 ###
            vat_xml_data = self.make_vat_xml_data(self, params)

            #####################################################
            ## vat_xml_data 을 가지로 부가세 xml 파일을 제작하시면 됩니다 ##
            #####################################################

            # ## 5. 합계표 전자분 마감 string 만들기 ###
            bill_close = self.make_billcont_str_close(self, params)


            # ## 6. 마감 스트링 한줄로 만들기 + 합계표 마감 스트링 붙이기 ###
            final_string = ''
            for str_close in vat_list:
                final_string += str_close['str_close']

            if bill_close['close_45'] is not None and len(bill_close['close_45']) > 0:
                final_string += bill_close['close_45']
            elif bill_close['close_46'] is not None and len(bill_close['close_46']) > 0:
                final_string += bill_close['close_46']
            elif bill_close['close_47'] is not None and len(bill_close['close_47']) > 0:
                final_string += bill_close['close_47']
            elif bill_close['close_48'] is not None and len(bill_close['close_48']) > 0:
                final_string += bill_close['close_48']


            ################################################
            ## final_string 을 가지로 ers 파일을 제작하시면 됩니다 ##
            ################################################

        return final_string



    ## 개별 함수 영역 #########################################################################
    # 부가세 xml생성용 데이터 조회
    def make_vat_xml_data(self, params):
        vat_xml_data = ''

        # 부가세 신고서
        self._sao_db_connection.cursor.execute(self.select_vat(self, params['ty_simple']), params)
        vat_data = name_to_json(self._sao_db_connection.cursor)

        # 부가세 신고서 신보용
        self._sao_db_connection.cursor.execute(self.select_sinbo(self, params['ty_simple']), params)
        sinbo_data = name_to_json(self._sao_db_connection.cursor)

        # 세금계산서 합계표 / 계산서 합계표
        self._sao_db_connection.cursor.execute(self.select_detail(self, params['ty_rpt']), params)
        detail_data = name_to_json(self._sao_db_connection.cursor)

        result_data = {
            'vat_data':vat_data,
            'sinbo_data':sinbo_data,
            'detail_data':detail_data
        }

        return result_data

    # 합계표 전자분 데이터 조회하여 마감 스트링 제작
    def make_billcont_str_close(self, params):
        ls_vatclo = self.set_mta_vatclo(self, 'billcont')

        if params['ty_rpt'] == 3:
            if params['count'] == 1:
                params['ty_rpt_pre'] = 1
                params['count_pre'] = 1
            elif params['count'] > 1:
                params['ty_rpt_pre'] = 2
                params['count_pre'] = params['count'] - 1

        # 세금계산서 매출 cd_form = 45
        params['ty_buysale'] = 1  # 매출매입 구분 1.매출 2.매입
        self._sao_db_connection.cursor.execute(self.select_billcont_45_46(self, params['ty_rpt']), params)
        data_45 = name_to_json(self._sao_db_connection.cursor)

        close_45 = ''
        for str_45 in data_45:
            close_45 += self.__get_close_string(self, 'vat', str(str_45['str_close']), record_json=ls_vatclo,
                                           special_char='§')
            close_45 += '♥'

        # 세금계산서 매입
        params['ty_buysale'] = 2  # 매출매입 구분 1.매출 2.매입
        self._sao_db_connection.cursor.execute(self.select_billcont_45_46(self, params['ty_rpt']), params)
        data_46 = name_to_json(self._sao_db_connection.cursor)

        close_46 = ''
        for str_46 in data_46:
            close_46 += self.__get_close_string(self, 'vat', str(str_46['str_close']), record_json=ls_vatclo, special_char='§')
            close_46 += '♥'

        #    계산서 매출
        params['ty_buysale'] = 1  # 매출매입 구분 1.매출 2.매입
        self._sao_db_connection.cursor.execute(self.select_billcont_47_48(self), params)
        data_47 = name_to_json(self._sao_db_connection.cursor)

        close_47 = ''
        for str_47 in data_47:
            close_47 += self.__get_close_string(self, 'vat', str(str_47['str_close']), record_json=ls_vatclo, special_char='§')
            close_47 += '♥'

        #    계산서 매입
        params['ty_buysale'] = 2  # 매출매입 구분 1.매출 2.매입
        self._sao_db_connection.cursor.execute(self.select_billcont_47_48(self), params)
        data_48 = name_to_json(self._sao_db_connection.cursor)

        close_48 = ''
        for str_48 in data_48:
            close_48 += self.__get_close_string(self, 'vat', str(str_48['str_close']), record_json=ls_vatclo, special_char='§')
            close_48 += '♥'

        result_data = {
            'close_45': close_45,
            'close_45': close_46,
            'close_45': close_47,
            'close_45': close_48
        }

        return result_data

    # 마감서식 지정 함수
    def set_mta_vatclo(self, type):

        list = []

        if type == 'billcont':
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
                data_list[idx] = "".join(data.replace(chr(160), ' ').splitlines()).encode(encoding='cp949')  # 공백제거 처리

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
    def make_vat_head(self, reperter, com_info, vat_info, head_rs, cursor):

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
            if com_da_end and int(vat_info['dm_fndbegin']) <= int(com_da_end[:6]) and int(vat_info['da_end']) >= int(
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
        str_close += reperter['nm_userid'] + '§'  # id_hometax

        # 10. 납세자번호 : 주민등록번호(개인) 또는 법인등록번호(법인), 비영리법인은 공백(SPACE)
        str_close += (com_info['ceoregno'] if str(com_info['corprvgbn']) == '1' else com_info['corno']) + '§'

        if str(com_info['ty_report']) == '1':
            str_close += reperter['accnm'] + '§'  # 11. 세무대리인성명
            str_close += reperter['tel_com1'] + '§'  # 12. 세무대리인전화번호 (지역번호)
            str_close += reperter['tel_com2'] + '§'  # 13. 세무대리인전화번호 (국번)
            str_close += reperter['tel_com3'] + '§'  # 14. 세무대리인전화번호 (나머지) (지역번호,국번을제외한번호)
        else:
            str_close += '§§§§'  # 다 공백

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
        str_close += com_info['bizcategory'] + '§'

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
        self._sao_db_connection.cursor.execute(self.select_close_date_query(self), vat_info)
        da_write = name_to_json(self._sao_db_connection.cursor)

        str_close += da_write[0]['str_save'] + '§'  # 26. 작성일자
        str_close += 'N§'  # 27. 보정신고구분
        str_close += com_info['cel_dtem'] + '§'  # 28. 사업자휴대전화 (신고담당자)
        str_close += '1001§'  # 29. 세무프로그램코드
        str_close += (reperter['accbizno'] if com_info['ty_report'] == '1' else '') + '§'  # 30.세무대리님사업자번호

        # 31.전자메일주소, 32.공란
        str_close += ' ' + '§'  # 31.전자메일주소 - 사용안함
        str_close += ' '  # 32.공란

        ls_vatclo = self.set_mta_vatclo(self, 'vat_head')

        close_head = self.__get_close_string(self, 'vat', str_close, record_json=ls_vatclo, special_char='§')  # + '♥'

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
    left join (
        select da_fndbegin, da_fndend, ty_rpt, no_count, ty_simple, ty_month, prd_vat
        from fta_vatrpt
        where da_fndend = %(da_fndend)s || '00'
        group by da_fndbegin, da_fndend, ty_rpt, no_count, ty_simple, ty_month, prd_vat

        union all 

        select da_fndbegin, da_fndend, ty_rpt, no_count, ty_simple, ty_month, prd_vat
        from fta_vatrpt_before
        where da_fndend = %(da_fndend)s || '00'
        group by da_fndbegin, da_fndend, ty_rpt, no_count, ty_simple, ty_month, prd_vat
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
            coalesce(tel_com3, '') as tel_com3,
        from fta_reporter
    ) as tt;
        """

        return query

    def select_company_info(self):
        query = """
    select

        ty_report,
        coalesce(tel_com1,'') || coalesce(tel_com2,'') || coalesce(tel_com3,'') as tel_com,
        coalesce(add_ceo1,'') || coalesce(add_ceo2,'') as add_ceo,
        coalesce(tel_ceo1,'') || coalesce(tel_ceo2,'') || coalesce(tel_ceo3,'') as tel_ceo,
        coalesce(cel_dtem1,'') || coalesce(cel_dtem2,'') || coalesce(cel_dtem3,'') as cel_dtem,
        coalesce(da_start,'') as da_start, --회사설립일
        coalesce(da_end,'') as da_end, --폐업일
        coalesce(str_2_1_1,'') as str_2_1_1, -- 과세유형전환일

        coalesce(nm_krcom, '') as CompanyName, --업체명
        prd_accounts as PRD_ACCOUNTS, --회계기수
        coalesce(DA_ACCBEGIN, '') as DA_ACCBEGIN, --회계시작
        coalesce(DA_ACCEND, '') as DA_ACCEND, --회계종료
        coalesce(no_biz, '') as BizRegNo, --사업자등록번호 또는 주민번호 
        coalesce(no_corpor, '') as CorNo, --법인번호
        coalesce(nm_ceo, '') as CeoName, --대표자명
        coalesce(no_ceosoc, '') as CeoRegNo, --대표자주민번호
        9 as JuPosition,  --주식회사 앞(1)/뒤(2)/기타(9) 구분
        99 as CompanyType, --기업형태 주식회사/유한회사 등
        coalesce(zip_com, '') as ZipCode, --우편번호

        coalesce(add_com1, '') || coalesce(add_com2, '') as Address,  -- 주소      
        '' as AddressDetail, --주소상세 (안쓰는 듯)
        rpad(tel_com1, 4, ' ') || rpad(tel_com2, 4, ' ') || rpad(tel_com3, 4, ' ') as TelNo,    --     전화 (지역번호-국번-번호, 각4자리 공백 padding) 
        rpad(fax_com1, 4, ' ') || rpad(fax_com2, 4, ' ') || rpad(fax_com3, 4, ' ') as FaxNo,    --   팩스번호(지역번호-국번-번호, 각4자리 공백 padding)    
        CD_BIZTYP as BizCategory,   --국세청 신고용 업종코드

        coalesce(nm_bizcond, '') as BizCondNm,  --  업태   
        coalesce(nm_item, '') as BizItemNm,   -- 종목   
        coalesce(da_build, '') as CorFCorPrvGbnoundDate,   -- 법인설립일
        yn_private as corprvgbn,   --   법인(1)/개인(2) 구분  

        coalesce(NM_DTEM, '') as DamNm,      -- 업체 담당자명     
        coalesce(EM_DTEM, '') as DamEmail,   -- 업체 담당자이메일     
        TEL_COM1 || (case when TEL_COM2 <> '' then '-' else '' end) || TEL_COM2 || (case when TEL_COM3 <> '' then '-' else '' end) || TEL_COM3 as DamTelNo,--	
        CEL_DTEM1 || (case when CEL_DTEM2 <> '' then '-' else '' end) || CEL_DTEM2 || (case when CEL_DTEM3 <> '' then '-' else '' end) || CEL_DTEM3 as DamHpNo   -- 업체 담당자핸드폰번호 	  
    from ftb_com;
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
        max(case when no_div = 1 and no_cell = 10004 then str_save else '' end) as V_SEL_TAX_TOT_AMT, --과세표준및매출세액금액(9) report_과세표준및매출세액금액_10004,
        max(case when no_div = 1 and no_cell = 10006 then str_save else '' end) as V_SEL_TAX_TOT_TX,--과세표준및매출세액세액(9)report_과세표준및매출세액세액_10006,
        max(case when no_div = 1 and no_cell = 12004 then str_save else '' end) as V_FIXASS_AMT, --고정자산매입금액(11+40)  report_고정자산매입금액_12004,
        max(case when no_div = 1 and no_cell = 12006 then str_save else '' end) as V_FIXASS_TX, --고정자산매입세액(11+40) report_고정자산매입세액_12006,
        max(case when no_div = 1 and no_cell = 16004 then str_save else '' end) as V_BUY_TAX_TOT_AMT, --매입차감계금액(17)report_매입차감계금액_16004,
        max(case when no_div = 1 and no_cell = 16006 then str_save else '' end) as V_BUY_TAX_TOT_TX, --매입차감계세액(17)report_매입차감계세액_16006,
        max(case when no_div = 1 and no_cell = 25006 then str_save else '' end) as V_ADD_TOT_TX, --가산세액계세액(24)report_가산세액계세액_25006,
        max(case when no_div = 1 and no_cell = 26006 then str_save else '' end) as V_SUBADD_TX, --차가감납부할세액report_차감납부할세액_26006,

        max(case when no_div = 101 and no_cell = 7004 then str_save else '' end) as V_TAX_STD_AMT, --과세표준명세합계(30)report_과세표준명세합계_7004,
        max(case when no_div = 102 and no_cell = 5004 then str_save else '' end) as V_FREE_TAX_TOT_AMT, --면세사업 수입금액합계report_면세사업수입금액합계_5004,

        max(case when no_div = 1 and no_cell = 27006 then str_save else '' end) as V_TOT_TX --총괄납부사업자납부할세액report_총괄납부사업자납부할세액_27006

    from fta_vatrpt
    where da_fndbegin = %(da_fndbegin)s and da_fndend = %(da_fndend)s and prd_vat = %(prd_vat)s and ty_month = %(ty_month)s and ty_rpt = %(ty_rpt)s and ty_simple = %(ty_simple)s and no_count = %(no_count)s 
      having (select count(1) from fta_vatrpt where da_fndbegin = %(da_fndbegin)s and da_fndend = %(da_fndend)s and prd_vat = %(prd_vat)s and ty_month = %(ty_month)s and ty_rpt = %(ty_rpt)s and ty_simple = %(ty_simple)s and no_count = %(no_count)s ) > 0 

    union all

    select 

        max(case when no_div = 1 and no_cell = 10004 then str_save else '' end) as V_SEL_TAX_TOT_AMT, --과세표준및매출세액금액(9) report_과세표준및매출세액금액_10004,
        max(case when no_div = 1 and no_cell = 10006 then str_save else '' end) as V_SEL_TAX_TOT_TX,--과세표준및매출세액세액(9)report_과세표준및매출세액세액_10006,
        max(case when no_div = 1 and no_cell = 12004 then str_save else '' end) as V_FIXASS_AMT, --고정자산매입금액(11+40)  report_고정자산매입금액_12004,
        max(case when no_div = 1 and no_cell = 12006 then str_save else '' end) as V_FIXASS_TX, --고정자산매입세액(11+40) report_고정자산매입세액_12006,
        max(case when no_div = 1 and no_cell = 16004 then str_save else '' end) as V_BUY_TAX_TOT_AMT, --매입차감계금액(17)report_매입차감계금액_16004,
        max(case when no_div = 1 and no_cell = 16006 then str_save else '' end) as V_BUY_TAX_TOT_TX, --매입차감계세액(17)report_매입차감계세액_16006,
        max(case when no_div = 1 and no_cell = 25006 then str_save else '' end) as V_ADD_TOT_TX, --가산세액계세액(24)report_가산세액계세액_25006,
        max(case when no_div = 1 and no_cell = 26006 then str_save else '' end) as V_SUBADD_TX, --차가감납부할세액report_차감납부할세액_26006,

        max(case when no_div = 101 and no_cell = 7004 then str_save else '' end) as V_TAX_STD_AMT, --과세표준명세합계(30)report_과세표준명세합계_7004,
        max(case when no_div = 102 and no_cell = 5004 then str_save else '' end) as V_FREE_TAX_TOT_AMT, --면세사업 수입금액합계report_면세사업수입금액합계_5004,

        max(case when no_div = 1 and no_cell = 27006 then str_save else '' end) as V_TOT_TX --총괄납부사업자납부할세액report_총괄납부사업자납부할세액_27006

    from fta_vatrpt_before
    where da_fndbegin = %(da_fndbegin)s and da_fndend = %(da_fndend)s and prd_vat = %(prd_vat)s and ty_month = %(ty_month)s and ty_rpt = %(ty_rpt)s and ty_simple = %(ty_simple)s and no_count = %(no_count)s 
      having (select count(1) from fta_vatrpt_before where da_fndbegin = %(da_fndbegin)s and da_fndend = %(da_fndend)s and prd_vat = %(prd_vat)s and ty_month = %(ty_month)s and ty_rpt = %(ty_rpt)s and ty_simple = %(ty_simple)s and no_count = %(no_count)s ) > 0 
    ;
            """
        else:
            query = """
    select 

        max(case when no_div = 1 and no_cell = 9004 then str_save else '' end) as V_SEL_TAX_TOT_AMT, --과세표준및매출세액금액(9)report_과세표준및매출세액금액_9004,
        max(case when no_div = 1 and no_cell = 9007 then str_save else '' end) as V_SEL_TAX_TOT_TX,--과세표준및매출세액세액(9)report_과세표준및매출세액세액_9007,
        --max(case when no_div = 1 and no_cell =  then str_save else '' end) as report_고정자산매입금액,
        --max(case when no_div = 1 and no_cell = 12006 then str_save else '' end) as report_고정자산매입세액,
        --max(case when no_div = 1 and no_cell = 16004 then str_save else '' end) as report_매입차감계금액,
        --max(case when no_div = 1 and no_cell = 16006 then str_save else '' end) as report_매입차감계세액,
        0 as V_FIXASS_AMT, --고정자산매입금액(11+40) report_고정자산매입금액,
        0 as V_FIXASS_TX, --고정자산매입세액(11+40) report_고정자산매입세액,
        0 as V_BUY_TAX_TOT_AMT, --매입차감계금액(17)report_매입차감계금액,
        0 as V_BUY_TAX_TOT_TX, --매입차감계세액(17)report_매입차감계세액,

        max(case when no_div = 1 and no_cell = 18007 then str_save else '' end) as V_ADD_TOT_TX, --가산세액계세액(24)report_가산세액계세액_18007,
        max(case when no_div = 1 and no_cell = 19007 then str_save else '' end) as V_SUBADD_TX, --차가감납부할세액report_차감납부할세액_19007,

        max(case when no_div = 101 and no_cell = 6004 then str_save else '' end) as V_TAX_STD_AMT, --과세표준명세합계(30)report_과세표준명세합계_6004,
        max(case when no_div = 102 and no_cell = 5004 then str_save else '' end) as V_FREE_TAX_TOT_AMT, --면세사업 수입금액합계report_면세사업수입금액합계_5004,

        max(case when no_div = 1 and no_cell = 20007 then str_save else '' end) as V_TOT_TX --총괄납부사업자납부할세액report_총괄납부사업자납부할세액_20007

    from fta_vatrpt
    where da_fndbegin = %(da_fndbegin)s and da_fndend = %(da_fndend)s and prd_vat = %(prd_vat)s and ty_month = %(ty_month)s and ty_rpt = %(ty_rpt)s and ty_simple = %(ty_simple)s and no_count = %(no_count)s 
      having (select count(1) from fta_vatrpt where da_fndbegin = %(da_fndbegin)s and da_fndend = %(da_fndend)s and prd_vat = %(prd_vat)s and ty_month = %(ty_month)s and ty_rpt = %(ty_rpt)s and ty_simple = %(ty_simple)s and no_count = %(no_count)s ) > 0 

    union ALL 

    select 

        max(case when no_div = 1 and no_cell = 9004 then str_save else '' end) as V_SEL_TAX_TOT_AMT, --과세표준및매출세액금액(9)report_과세표준및매출세액금액_9004,
        max(case when no_div = 1 and no_cell = 9007 then str_save else '' end) as V_SEL_TAX_TOT_TX,--과세표준및매출세액세액(9)report_과세표준및매출세액세액_9007,
        --max(case when no_div = 1 and no_cell =  then str_save else '' end) as report_고정자산매입금액,
        --max(case when no_div = 1 and no_cell = 12006 then str_save else '' end) as report_고정자산매입세액,
        --max(case when no_div = 1 and no_cell = 16004 then str_save else '' end) as report_매입차감계금액,
        --max(case when no_div = 1 and no_cell = 16006 then str_save else '' end) as report_매입차감계세액,
        0 as V_FIXASS_AMT, --고정자산매입금액(11+40) report_고정자산매입금액,
        0 as V_FIXASS_TX, --고정자산매입세액(11+40) report_고정자산매입세액,
        0 as V_BUY_TAX_TOT_AMT, --매입차감계금액(17)report_매입차감계금액,
        0 as V_BUY_TAX_TOT_TX, --매입차감계세액(17)report_매입차감계세액,

        max(case when no_div = 1 and no_cell = 18007 then str_save else '' end) as V_ADD_TOT_TX, --가산세액계세액(24)report_가산세액계세액_18007,
        max(case when no_div = 1 and no_cell = 19007 then str_save else '' end) as V_SUBADD_TX, --차가감납부할세액report_차감납부할세액_19007,

        max(case when no_div = 101 and no_cell = 6004 then str_save else '' end) as V_TAX_STD_AMT, --과세표준명세합계(30)report_과세표준명세합계_6004,
        max(case when no_div = 102 and no_cell = 5004 then str_save else '' end) as V_FREE_TAX_TOT_AMT, --면세사업 수입금액합계report_면세사업수입금액합계_5004,

        max(case when no_div = 1 and no_cell = 20007 then str_save else '' end) as V_TOT_TX --총괄납부사업자납부할세액report_총괄납부사업자납부할세액_20007

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

    def select_detail(self, ty_rpt):
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
            , cnt as V_ISSUE_QTY --매수
            , mn_supply as V_AMT --공급가액
            , mn_vat as V_TAX --세액
            , '' as V_ETC --비고
    from
    (
        |inner_select_query|
    ) as all_data
    order by ty_buysale, num;

        """

        # 정기신고
        if str(ty_rpt) == '1':
            # 세금계산서  ty_buysale=1:매출세금 / ty_buysale=2:매입세금
            query_tax_bill = """
    select 
        (case when |ty_buysale| = 2 then 1 else 3 end) as ty_buysale -- 1 매입세금, 2 매입계산, 3 매출세금, 4 매출계산
        , row_number() over(order by ord_key, no_biz) as num --일련번호
        , ty_biz -- 1 2 사업자번호 구분
        , no_biz --사업자번호
        , nm_trade --업체명
        , cnt --매수
        , mn_supply --공급가액
        , mn_vat --세액
    from 
    (
        select 
             ord_key
            , ty_biz -- 1 2 사업자번호 구분
            , max(coalesce(no_biz,'')) as no_biz --사업자번호
            , nm_trade --업체명
            , coalesce(sum(cnt), 0) as cnt --매수
            , coalesce(sum(mn_supply), 0) as mn_supply --공급가액
            , coalesce(sum(mn_vat), 0) as mn_vat --세액
            --, no_trade
            --, cd_trade
            --,nm_ceo
            --,str_mainbiz
            --,yn_unitrpt
            --,ty_elctax
        from (
            select 0 as ord_key
                , fbc.cd_trade
                , fbc.nm_trade
                --, fbc.nm_trade as temp_nm_trade
                --, fbc.no_trade
                , fbc.no_trade as no_biz
                --, coalesce(sum(fbc.cnt_sumsh), 0) as cnt_sumsh
                , coalesce(sum(fbc.cnt_sumsh), 0) as cnt
                , coalesce(sum(fbc.mn_supply), 0) as mn_supply
                , coalesce(sum(fbc.mn_vat), 0) as mn_vat
                --, fbc.nm_ceo
                --, fbc.str_mainbiz
                --, fbc.yn_unitrpt
                , fbc.ty_elctax
                , ft.ty_biz
            from FTA_BILLCONT as fbc left join
                FTB_TRADE as ft on fbc.cd_trade = ft.cd_trade
            where fbc.dm_fndend = %(dm_fndend)s
            and fbc.ty_rpt = %(ty_rpt)s
            and fbc.ty_use = 1
            and fbc.no_count = %(no_count_g)s --차수
            and fbc.ty_buysale = |ty_buysale| --매입매출구분 1.매출 2.매입
            and fbc.prd_vat = %(prd_vat)s --부가세기수
            and fbc.ty_month = %(ty_month)s --예정확정 구분 1.예정 2.확정
            and fbc.ty_elctax = 0
            and (ft.ty_biz = 0 and coalesce(fbc.yn_beforebiz, 0) = 0) 
            group by fbc.cd_trade, fbc.nm_trade, fbc.no_trade, fbc.nm_ceo
            , fbc.str_mainbiz, fbc.yn_unitrpt, fbc.ty_elctax, ft.ty_biz

                union all

            select 0 as ord_key
                , fbc.cd_trade
                , fbc.nm_trade
                --, fbc.nm_trade as temp_nm_trade
                --, fbc.no_trade
                , fbc.no_trade as no_biz
                --, coalesce(sum(fbc.cnt_sumsh), 0) as cnt_sumsh
                , coalesce(sum(fbc.cnt_sumsh), 0) as cnt
                , coalesce(sum(fbc.mn_supply), 0) as mn_supply
                , coalesce(sum(fbc.mn_vat), 0) as mn_vat
                --, fbc.nm_ceo
                --, fbc.str_mainbiz
                --, fbc.yn_unitrpt
                , fbc.ty_elctax
                , ft.ty_biz
            from FTA_BILLCONT as fbc left join
                FTB_TRADE as ft on fbc.cd_trade = ft.cd_trade
            where fbc.dm_fndend = %(dm_fndend)s
            and fbc.ty_rpt = %(ty_rpt)s
            and fbc.ty_use = 1 --사용구분 1.세금계산서합계표 2.계산서합계표
            and fbc.no_count = %(no_count_g)s --차수
            and fbc.ty_buysale = |ty_buysale| --매입매출구분 1.매출 2.매입
            and fbc.prd_vat = %(prd_vat)s --부가세기수
            and fbc.ty_month = %(ty_month)s --예정확정 구분 1.예정 2.확정
            and fbc.ty_elctax = 1
            and (ft.ty_biz = 0 and coalesce(fbc.yn_beforebiz, 0) = 0)
            group by fbc.no_trade, fbc.nm_trade, fbc.cd_trade, fbc.nm_ceo
                    , fbc.str_mainbiz, fbc.yn_unitrpt, fbc.ty_elctax, ft.ty_biz

            union all

            select 1 as ord_key
                , fbc.cd_trade
                , ''::varchar(30) as nm_trade
                --, fbc.nm_trade as temp_nm_trade
                --, '주민등록기재분'::varchar as no_trade
                , max(fbc.no_trade) as no_biz
                --, coalesce(sum(fbc.cnt_sumsh), 0) as cnt_sumsh
                , coalesce(sum(case when fbc.sq_acttax1 is null and fbc.key_addslip > 0 then coalesce(fbc.cnt_trade, 0) end), 0) +
                  count(distinct(
                      case when not(fbc.sq_acttax1 is null and fbc.key_addslip > 0) then fbc.cd_trade
                        end)) as cnt
                , sum(fbc.mn_supply) as mn_supply
                , sum(fbc.mn_vat) as mn_vat
                --, ''::varchar(20) as nm_ceo
                --, ''::varchar(5) as str_mainbiz
                --, 0 as yn_unitrpt
                , 0 as ty_elctax
                , 1 as ty_biz
            from FTA_BILLCONT as fbc left join
                FTB_TRADE as ft on fbc.cd_trade = ft.cd_trade
            where fbc.dm_fndend = %(dm_fndend)s
            and fbc.ty_rpt = %(ty_rpt)s
            and fbc.ty_use = 1 --사용구분 1.세금계산서합계표 2.계산서합계표
            and fbc.no_count = %(no_count_g)s --차수
            and fbc.ty_buysale = |ty_buysale| --매입매출구분 1.매출 2.매입
            and fbc.prd_vat = %(prd_vat)s --부가세기수
            and fbc.ty_month = %(ty_month)s --예정확정 구분 1.예정 2.확정
            and fbc.ty_elctax = 0
            and (case when (coalesce(trim(fbc.cd_trade),'') <> '') then (ft.ty_biz in (1,2) or fbc.yn_beforebiz = 1) else 1=1 end)
            group by fbc.cd_trade, fbc.nm_trade

            union all

            select 1 as ord_key
                , fbc.cd_trade
                , ''::varchar(30) as nm_trade
                --, fbc.nm_trade as temp_nm_trade
                --, '주민등록기재분'::varchar as no_trade
                , max(fbc.no_trade) as no_biz
                --, coalesce(sum(fbc.cnt_sumsh), 0) as cnt_sumsh
                , coalesce(sum(case when fbc.sq_acttax1 is null and fbc.key_addslip > 0 then coalesce(fbc.cnt_trade, 0) end), 0) +
                  count(distinct(
                      case when not(fbc.sq_acttax1 is null and fbc.key_addslip > 0) then fbc.cd_trade end)) as cnt
                , sum(fbc.mn_supply) as mn_supply
                , sum(fbc.mn_vat) as mn_vat
                --, ''::varchar(20) as nm_ceo
                --, ''::varchar(5) as str_mainbiz
                --, 0 as yn_unitrpt
                , 1 as ty_elctax
                , 1 as ty_biz
            from FTA_BILLCONT as fbc left join
                FTB_TRADE as ft on fbc.cd_trade = ft.cd_trade
            where fbc.dm_fndend = %(dm_fndend)s
            and fbc.ty_rpt = %(ty_rpt)s --신고구분 1.정기 2.조기 3.수정 4.경정 5.폐업
            and fbc.ty_use = 1 --사용구분 1.세금계산서합계표 2.계산서합계표
            and fbc.no_count = %(no_count_g)s --차수
            and fbc.ty_buysale = |ty_buysale| --매입매출구분 1.매출 2.매입
            and fbc.prd_vat = %(prd_vat)s --부가세기수
            and fbc.ty_month = %(ty_month)s --예정확정 구분 1.예정 2.확정
            and fbc.ty_elctax = 1 --전자세금 구분 1.전자 2.전자외
            and (case when (coalesce(trim(fbc.cd_trade),'') <> '') then (ft.ty_biz in (1,2) or fbc.yn_beforebiz = 1) else 1=1 end)
            group by fbc.cd_trade, fbc.nm_trade

            union all

            select 0 as ord_key
                , fbc.cd_trade
                , fbc.nm_trade
                --, fbc.nm_trade as temp_nm_trade
                --, fbc.no_trade
                , fbc.no_trade as no_biz
                --, coalesce(sum(fbc.cnt_sumsh), 0) as cnt_sumsh
                , coalesce(sum(fbc.cnt_sumsh), 0) as cnt
                , coalesce(sum(fbc.mn_supply), 0) as mn_supply
                , coalesce(sum(fbc.mn_vat), 0) as mn_vat
                --, fbc.nm_ceo
                --, fbc.str_mainbiz
                --, fbc.yn_unitrpt
                , fbc.ty_elctax
                , ft.ty_biz
            from FTA_BILLCONT_BEFORE as fbc left join
                FTB_TRADE_BEFORE as ft on fbc.cd_trade = ft.cd_trade and ft.gisu = %(gisu)s
            where fbc.dm_fndend = %(dm_fndend)s
            and fbc.ty_rpt = %(ty_rpt)s
            and fbc.ty_use = 1
            and fbc.no_count = %(no_count_g)s --차수
            and fbc.ty_buysale = |ty_buysale| --매입매출구분 1.매출 2.매입
            and fbc.prd_vat = %(prd_vat)s --부가세기수
            and fbc.ty_month = %(ty_month)s --예정확정 구분 1.예정 2.확정
            and fbc.ty_elctax = 0
            and (ft.ty_biz = 0 and coalesce(fbc.yn_beforebiz, 0) = 0) 
            and fbc.gisu = %(gisu)s
            group by fbc.cd_trade, fbc.nm_trade, fbc.no_trade, fbc.nm_ceo
            , fbc.str_mainbiz, fbc.yn_unitrpt, fbc.ty_elctax, ft.ty_biz

                union all

            select 0 as ord_key
                , fbc.cd_trade
                , fbc.nm_trade
                --, fbc.nm_trade as temp_nm_trade
                --, fbc.no_trade
                , fbc.no_trade as no_biz
                --, coalesce(sum(fbc.cnt_sumsh), 0) as cnt_sumsh
                , coalesce(sum(fbc.cnt_sumsh), 0) as cnt
                , coalesce(sum(fbc.mn_supply), 0) as mn_supply
                , coalesce(sum(fbc.mn_vat), 0) as mn_vat
                --, fbc.nm_ceo
                --, fbc.str_mainbiz
                --, fbc.yn_unitrpt
                , fbc.ty_elctax
                , ft.ty_biz
            from FTA_BILLCONT_BEFORE as fbc left join
                FTB_TRADE_BEFORE as ft on fbc.cd_trade = ft.cd_trade and ft.gisu = %(gisu)s
            where fbc.dm_fndend = %(dm_fndend)s
            and fbc.ty_rpt = %(ty_rpt)s
            and fbc.ty_use = 1 --사용구분 1.세금계산서합계표 2.계산서합계표
            and fbc.no_count = %(no_count_g)s --차수
            and fbc.ty_buysale = |ty_buysale| --매입매출구분 1.매출 2.매입
            and fbc.prd_vat = %(prd_vat)s --부가세기수
            and fbc.ty_month = %(ty_month)s --예정확정 구분 1.예정 2.확정
            and fbc.ty_elctax = 1
            and (ft.ty_biz = 0 and coalesce(fbc.yn_beforebiz, 0) = 0)
            and fbc.gisu = %(gisu)s
            group by fbc.no_trade, fbc.nm_trade, fbc.cd_trade, fbc.nm_ceo
                    , fbc.str_mainbiz, fbc.yn_unitrpt, fbc.ty_elctax, ft.ty_biz

            union all

            select 1 as ord_key
                , fbc.cd_trade
                , ''::varchar(30) as nm_trade
                --, fbc.nm_trade as temp_nm_trade
                --, '주민등록기재분'::varchar as no_trade
                , max(fbc.no_trade) as no_biz
                --, coalesce(sum(fbc.cnt_sumsh), 0) as cnt_sumsh
                , coalesce(sum(case when fbc.sq_acttax1 is null and fbc.key_addslip > 0 then coalesce(fbc.cnt_trade, 0) end), 0) +
                  count(distinct(
                      case when not(fbc.sq_acttax1 is null and fbc.key_addslip > 0) then fbc.cd_trade
                        end)) as cnt
                , sum(fbc.mn_supply) as mn_supply
                , sum(fbc.mn_vat) as mn_vat
                --, ''::varchar(20) as nm_ceo
                --, ''::varchar(5) as str_mainbiz
                --, 0 as yn_unitrpt
                , 0 as ty_elctax
                , 1 as ty_biz
            from FTA_BILLCONT_BEFORE as fbc left join
                FTB_TRADE_BEFORE as ft on fbc.cd_trade = ft.cd_trade and ft.gisu = %(gisu)s
            where fbc.dm_fndend = %(dm_fndend)s
            and fbc.ty_rpt = %(ty_rpt)s
            and fbc.ty_use = 1 --사용구분 1.세금계산서합계표 2.계산서합계표
            and fbc.no_count = %(no_count_g)s --차수
            and fbc.ty_buysale = |ty_buysale| --매입매출구분 1.매출 2.매입
            and fbc.prd_vat = %(prd_vat)s --부가세기수
            and fbc.ty_month = %(ty_month)s --예정확정 구분 1.예정 2.확정
            and fbc.ty_elctax = 0
            and (case when (coalesce(trim(fbc.cd_trade),'') <> '') then (ft.ty_biz in (1,2) or fbc.yn_beforebiz = 1) else 1=1 end)
            and fbc.gisu = %(gisu)s
            group by fbc.cd_trade, fbc.nm_trade

            union all

            select 1 as ord_key
                , fbc.cd_trade
                , ''::varchar(30) as nm_trade
                --, fbc.nm_trade as temp_nm_trade
                --, '주민등록기재분'::varchar as no_trade
                , max(fbc.no_trade) as no_biz
                --, coalesce(sum(fbc.cnt_sumsh), 0) as cnt_sumsh
                , coalesce(sum(case when fbc.sq_acttax1 is null and fbc.key_addslip > 0 then coalesce(fbc.cnt_trade, 0) end), 0) +
                  count(distinct(
                      case when not(fbc.sq_acttax1 is null and fbc.key_addslip > 0) then fbc.cd_trade end)) as cnt
                , sum(fbc.mn_supply) as mn_supply
                , sum(fbc.mn_vat) as mn_vat
                --, ''::varchar(20) as nm_ceo
                --, ''::varchar(5) as str_mainbiz
                --, 0 as yn_unitrpt
                , 1 as ty_elctax
                , 1 as ty_biz
            from FTA_BILLCONT_BEFORE as fbc left join
                FTB_TRADE_BEFORE as ft on fbc.cd_trade = ft.cd_trade and ft.gisu = %(gisu)s
            where fbc.dm_fndend = %(dm_fndend)s
            and fbc.ty_rpt = %(ty_rpt)s --신고구분 1.정기 2.조기 3.수정 4.경정 5.폐업
            and fbc.ty_use = 1 --사용구분 1.세금계산서합계표 2.계산서합계표
            and fbc.no_count = %(no_count_g)s --차수
            and fbc.ty_buysale = |ty_buysale| --매입매출구분 1.매출 2.매입
            and fbc.prd_vat = %(prd_vat)s --부가세기수
            and fbc.ty_month = %(ty_month)s --예정확정 구분 1.예정 2.확정
            and fbc.ty_elctax = 1 --전자세금 구분 1.전자 2.전자외
            and (case when (coalesce(trim(fbc.cd_trade),'') <> '') then (ft.ty_biz in (1,2) or fbc.yn_beforebiz = 1) else 1=1 end)
            and fbc.gisu = %(gisu)s
            group by fbc.cd_trade, fbc.nm_trade

        ) main
        group by main.ord_key, main.cd_trade, main.nm_trade
        --, main.no_trade, main.nm_ceo, main.str_mainbiz, main.yn_unitrpt
        , main.ty_elctax, main.ty_biz


    ) as tt	


                                """

        # 수정신고
        elif str(ty_rpt) == '3':
            # 세금계산서  ty_buysale=1:매출세금 / ty_buysale=2:매입세금
            query_tax_bill = """
    select 
        (case when |ty_buysale| = 2 then 1 else 3 end) as ty_buysale -- 1 매입세금, 2 매입계산, 3 매출세금, 3 매출계산
        , row_number() over(order by ord_key, no_trade) as num --일련번호
        , ty_biz
        , max(coalesce(no_biz,''))
        , nm_trade --업체명
        , coalesce(sum(cnt_sumsh2), 0) as cnt --매수
        , coalesce(sum(mn_supply2), 0) as mn_supply --공급가액
        , coalesce(sum(mn_vat2), 0) as mn_vat --세액
    from 
    (
        select ord_key, no_trade, max(no_biz) as no_biz, max(nm_trade)::varchar(30) as nm_trade
                , max(cd_trade)::varchar(100) as cd_trade,

               sum(cnt_sumsh2)::integer as cnt_sumsh2,
               sum(mn_supply2)::numeric(14,0) as mn_supply2,
               sum(mn_vat2)::numeric(13,0) as mn_vat2,

               --max(nm_ceo2)::varchar(20) as nm_ceo2,
               --max(str_mainbiz2)::varchar(5) as str_mainbiz2,
               --max(yn_unitrpt2)::smallint as yn_unitrpt2,

               ty_elctax,
               ty_biz--,

               --case when ty_elctax = 0 and (coalesce(sum(cnt_sumsh2), 0) or coalesce(sum(mn_supply2), 0) or coalesce(sum(mn_vat2), 0)) then 1
                --	else 0 end as checkdiff

          from (
                    select 0 as ord_key
                        , fbc.cd_trade
                        , max(fbc.nm_trade) as nm_trade
                        , max(fbc.no_trade) as no_trade
                        , max(fbc.no_trade) as no_biz

                        , coalesce(sum(cnt_sumsh), 0) as cnt_sumsh2
                        , coalesce(sum(mn_supply), 0) as mn_supply2
                        , coalesce(sum(mn_vat), 0) as mn_vat2
                        --, max(nm_ceo) as nm_ceo2
                        --, max(str_mainbiz) as str_mainbiz2
                        --, coalesce(max(yn_unitrpt), 0) as yn_unitrpt2
                        , fbc.ty_elctax
                        , ft.ty_biz

                    from FTA_BILLCONT as fbc left join
                        FTB_TRADE as ft on fbc.cd_trade = ft.cd_trade
                    where dm_fndend = %(dm_fndend)s
                    and fbc.ty_rpt = %(ty_rpt)s
                    and fbc.ty_use = 1
                    and fbc.no_count = %(no_count_g)s
                    and fbc.ty_buysale = |ty_buysale|
                    and fbc.prd_vat = %(prd_vat)s
                    and fbc.ty_month = %(ty_month)s
                    and (ft.ty_biz = 0 and coalesce(fbc.yn_beforebiz, 0) = 0)
                    group by fbc.cd_trade, ft.ty_biz, fbc.ty_elctax

                    union all

                    select 1 as ord_key
                        , '' cd_trade
                        , '' as nm_trade
                        , '주민등록기재분'::varchar(100) as no_trade
                        , coalesce(max(fbc.no_trade), '') as no_biz

                        , coalesce(sum(fbc.cnt_sumsh), 0) as cnt_sumsh2
                        , coalesce(sum(fbc.mn_supply), 0) as mn_supply2
                        , coalesce(sum(fbc.mn_vat), 0) as mn_vat2
                        --, ''::varchar(20) as nm_ceo2
                        --, ''::varchar(5) as str_mainbiz2
                        --, 0 as yn_unitrpt2
                        , fbc.ty_elctax
                        , 1 ty_biz
                    from FTA_BILLCONT as fbc left join
                        FTB_TRADE as ft on fbc.cd_trade = ft.cd_trade
                    where fbc.dm_fndend = %(dm_fndend)s
                    and fbc.ty_rpt = %(ty_rpt)s
                    and fbc.ty_use = 1
                    and fbc.no_count = %(no_count_g)s
                    and fbc.ty_buysale = |ty_buysale|
                    and fbc.prd_vat = %(prd_vat)s
                    and fbc.ty_month = %(ty_month)s
                    and (case when (coalesce(trim(fbc.cd_trade),'') <> '') then (ft.ty_biz in (1,2) or fbc.yn_beforebiz = 1) else 1=1 end)
                    group by fbc.ty_elctax

                    union all

                    select 0 as ord_key
                        , fbc.cd_trade
                        , max(fbc.nm_trade) as nm_trade
                        , max(fbc.no_trade) as no_trade
                        , max(fbc.no_trade) as no_biz

                        , coalesce(sum(cnt_sumsh), 0) as cnt_sumsh2
                        , coalesce(sum(mn_supply), 0) as mn_supply2
                        , coalesce(sum(mn_vat), 0) as mn_vat2
                        --, max(nm_ceo) as nm_ceo2
                        --, max(str_mainbiz) as str_mainbiz2
                        --, coalesce(max(yn_unitrpt), 0) as yn_unitrpt2
                        , fbc.ty_elctax
                        , ft.ty_biz

                    from FTA_BILLCONT_BEFORE as fbc left join
                        FTB_TRADE_BEFORE as ft on fbc.cd_trade = ft.cd_trade and ft.gisu = %(gisu)s
                    where dm_fndend = %(dm_fndend)s
                    and fbc.ty_rpt = %(ty_rpt)s
                    and fbc.ty_use = 1
                    and fbc.no_count = %(no_count_g)s
                    and fbc.ty_buysale = |ty_buysale|
                    and fbc.prd_vat = %(prd_vat)s
                    and fbc.ty_month = %(ty_month)s
                    and (ft.ty_biz = 0 and coalesce(fbc.yn_beforebiz, 0) = 0)
                    and fbc.gisu = %(gisu)s
                    group by fbc.cd_trade, ft.ty_biz, fbc.ty_elctax

                    union all

                    select 1 as ord_key
                        , '' cd_trade
                        , '' as nm_trade
                        , '주민등록기재분'::varchar(100) as no_trade
                        , coalesce(max(fbc.no_trade), '') as no_biz

                        , coalesce(sum(fbc.cnt_sumsh), 0) as cnt_sumsh2
                        , coalesce(sum(fbc.mn_supply), 0) as mn_supply2
                        , coalesce(sum(fbc.mn_vat), 0) as mn_vat2
                        --, ''::varchar(20) as nm_ceo2
                        --, ''::varchar(5) as str_mainbiz2
                        --, 0 as yn_unitrpt2
                        , fbc.ty_elctax
                        , 1 ty_biz
                    from FTA_BILLCONT_BEFORE as fbc left join
                        FTB_TRADE_BEFORE as ft on fbc.cd_trade = ft.cd_trade and ft.gisu = %(gisu)s
                    where fbc.dm_fndend = %(dm_fndend)s
                    and fbc.ty_rpt = %(ty_rpt)s
                    and fbc.ty_use = 1
                    and fbc.no_count = %(no_count_g)s
                    and fbc.ty_buysale = |ty_buysale|
                    and fbc.prd_vat = %(prd_vat)s
                    and fbc.ty_month = %(ty_month)s
                    and (case when (coalesce(trim(fbc.cd_trade),'') <> '') then (ft.ty_biz in (1,2) or fbc.yn_beforebiz = 1) else 1=1 end)
                    and fbc.gisu = %(gisu)s
                    group by fbc.ty_elctax
            ) sub
            group by sub.ord_key, sub.cd_trade, sub.no_trade, sub.nm_trade, sub.ty_biz, sub.ty_elctax

    ) as tt
    group by ord_key, cd_trade, no_trade, nm_trade, ty_biz, ty_elctax



            """

        # 계산서  ty_buysale=1:매출계산서 / ty_buysale=2:매입계산서
        query_bill = """
                select 
        (case when |ty_buysale| = 2 then 2 else 4 end) as ty_buysale -- 1 매입세금, 2 매입계산, 3 매출세금, 4 매출계산
        , row_number() over(order by ord_key, ty_elctax, no_trade) as num --일련번호
        , ty_biz -- 1 2 사업자번호 구분
        ,  no_biz --사업자번호
        , nm_trade --업체명
        ,  cnt --매수
        ,  mn_supply --공급가액
        ,  mn_vat --세액
    from 
    (
        select ty_buysale
            , ord_key
            , coalesce(cd_trade,'') as cd_trade
            , nm_trade
            , no_trade
            , coalesce(no_biz,'') as no_biz
            --, coalesce(sum(cnt_sumsh), 0) as cnt_sumsh
            , coalesce(sum(cnt), 0) as cnt
            , coalesce(sum(mn_supply), 0) as mn_supply
            , coalesce(sum(mn_vat), 0) as mn_vat
            --, nm_ceo
            --, str_mainbiz
            --, yn_unitrpt
            , ty_elctax
            , ty_biz	
        from (
                select fbc.ty_buysale
                    , 0 as ord_key
                    , fbc.cd_trade
                    , fbc.nm_trade
                    --, fbc.nm_trade as temp_nm_trade
                    , fbc.no_trade
                    , fbc.no_trade as no_biz
                    --, coalesce(sum(fbc.cnt_sumsh), 0) as cnt_sumsh
                    , coalesce(sum(fbc.cnt_sumsh), 0) as cnt
                    , coalesce(sum(fbc.mn_supply), 0) as mn_supply
                    , coalesce(sum(fbc.mn_vat), 0) as mn_vat
                    --, fbc.nm_ceo
                    --, fbc.str_mainbiz
                    --, fbc.yn_unitrpt
                    , fbc.ty_elctax
                    , ft.ty_biz
                from FTA_BILLCONT as fbc left join
                    FTB_TRADE as ft on fbc.cd_trade = ft.cd_trade
                where fbc.dm_fndend = %(dm_fndend)s
                and fbc.ty_rpt = 1
                and fbc.ty_use = 2
                and fbc.no_count = 1
                and fbc.ty_buysale = |ty_buysale|
                and fbc.prd_vat = %(prd_vat)s
                and fbc.ty_month = %(ty_month)s
                and fbc.ty_elctax in (0, 1)
                and (ft.ty_biz = 0 and coalesce(fbc.yn_beforebiz, 0) = 0)
                group by fbc.ty_buysale, fbc.ty_elctax, fbc.cd_trade, fbc.nm_trade, fbc.no_trade, fbc.nm_ceo
                        , fbc.str_mainbiz, fbc.yn_unitrpt, fbc.ty_elctax, ft.ty_biz

                union all

                select fbc.ty_buysale
                    , 1 as ord_key
                    , fbc.cd_trade
                    , count(fbc.no_trade)::char(4) as nm_trade
                    --, fbc.nm_trade as temp_nm_trade
                    , '주민등록기재분'::varchar as no_trade
                    , fbc.no_trade as no_biz
                    --, coalesce(sum(fbc.cnt_sumsh), 0) as cnt_sumsh
                    , coalesce(sum(case when fbc.sq_acttax1 is null and fbc.key_addslip > 0 then coalesce(fbc.cnt_trade, 0) end), 0) +
                      count(distinct(
                          case when not(fbc.sq_acttax1 is null and fbc.key_addslip > 0) then fbc.cd_trade end)) as cnt
                    , coalesce(sum(fbc.mn_supply), 0) as mn_supply
                    , coalesce(sum(fbc.mn_vat), 0) as mn_vat
                    --, ''::varchar(20) as nm_ceo
                    --, ''::varchar(5) as str_mainbiz
                    --, 0 as yn_unitrpt
                    , ty_elctax
                    , 1 as ty_biz
                from FTA_BILLCONT as fbc left join
                    FTB_TRADE as ft on fbc.cd_trade = ft.cd_trade
                where fbc.dm_fndend = %(dm_fndend)s
                and fbc.ty_rpt = 1
                and fbc.ty_use = 2
                and fbc.no_count = 1
                and fbc.ty_buysale = |ty_buysale|
                and fbc.prd_vat = %(prd_vat)s
                and fbc.ty_month = %(ty_month)s
                and fbc.ty_elctax in (0, 1)
                and (fbc.cd_trade is null or ft.ty_biz in (1,2) or fbc.yn_beforebiz = 1)
                group by fbc.ty_buysale, fbc.ty_elctax, fbc.cd_trade, fbc.no_trade, fbc.nm_trade

                union all

            ---
                select fbc.ty_buysale
                    , 0 as ord_key
                    , fbc.cd_trade
                    , fbc.nm_trade
                    --, fbc.nm_trade as temp_nm_trade
                    , fbc.no_trade
                    , fbc.no_trade as no_biz
                    --, coalesce(sum(fbc.cnt_sumsh), 0) as cnt_sumsh
                    , coalesce(sum(fbc.cnt_sumsh), 0) as cnt
                    , coalesce(sum(fbc.mn_supply), 0) as mn_supply
                    , coalesce(sum(fbc.mn_vat), 0) as mn_vat
                    --, fbc.nm_ceo
                    --, fbc.str_mainbiz
                    --, fbc.yn_unitrpt
                    , fbc.ty_elctax
                    , ft.ty_biz
                from FTA_BILLCONT_BEFORE as fbc left join
                    FTB_TRADE_BEFORE as ft on fbc.cd_trade = ft.cd_trade and ft.gisu = %(gisu)s
                where fbc.dm_fndend = %(dm_fndend)s
                and fbc.ty_rpt = 1
                and fbc.ty_use = 2
                and fbc.no_count = 1
                and fbc.ty_buysale = |ty_buysale|
                and fbc.prd_vat = %(prd_vat)s
                and fbc.ty_month = %(ty_month)s
                and fbc.ty_elctax in (0, 1)
                and (ft.ty_biz = 0 and coalesce(fbc.yn_beforebiz, 0) = 0)
                and fbc.gisu = %(gisu)s
                group by fbc.ty_buysale, fbc.ty_elctax, fbc.cd_trade, fbc.nm_trade, fbc.no_trade, fbc.nm_ceo
                        , fbc.str_mainbiz, fbc.yn_unitrpt, fbc.ty_elctax, ft.ty_biz

                union all

                select fbc.ty_buysale
                    , 1 as ord_key
                    , fbc.cd_trade
                    , count(fbc.no_trade)::char(4) as nm_trade
                    --, fbc.nm_trade as temp_nm_trade
                    , '주민등록기재분'::varchar as no_trade
                    , fbc.no_trade as no_biz
                    --, coalesce(sum(fbc.cnt_sumsh), 0) as cnt_sumsh
                    , coalesce(sum(case when fbc.sq_acttax1 is null and fbc.key_addslip > 0 then coalesce(fbc.cnt_trade, 0) end), 0) +
                      count(distinct(
                          case when not(fbc.sq_acttax1 is null and fbc.key_addslip > 0) then fbc.cd_trade end)) as cnt
                    , coalesce(sum(fbc.mn_supply), 0) as mn_supply
                    , coalesce(sum(fbc.mn_vat), 0) as mn_vat
                    --, ''::varchar(20) as nm_ceo
                    --, ''::varchar(5) as str_mainbiz
                    --, 0 as yn_unitrpt
                    , ty_elctax
                    , 1 as ty_biz
                from FTA_BILLCONT_BEFORE as fbc left join
                    FTB_TRADE_BEFORE as ft on fbc.cd_trade = ft.cd_trade and ft.gisu = %(gisu)s
                where fbc.dm_fndend = %(dm_fndend)s
                and fbc.ty_rpt = 1
                and fbc.ty_use = 2
                and fbc.no_count = 1
                and fbc.ty_buysale = |ty_buysale|
                and fbc.prd_vat = %(prd_vat)s
                and fbc.ty_month = %(ty_month)s
                and fbc.ty_elctax in (0, 1)
                and (fbc.cd_trade is null or ft.ty_biz in (1,2) or fbc.yn_beforebiz = 1)
                and fbc.gisu = %(gisu)s
                group by fbc.ty_buysale, fbc.ty_elctax, fbc.cd_trade, fbc.no_trade, fbc.nm_trade

        ) main
        group by main.ty_buysale, main.ord_key, main.cd_trade, main.nm_trade, main.no_trade, main.no_biz
        --, main.nm_ceo
        --	, main.str_mainbiz, main.yn_unitrpt
        , main.ty_elctax, main.ty_biz
    ) as tt



                """

        inner_select_query = """/* 매입 세금계산서 */ """ + query_tax_bill.replace('|ty_buysale|', '2')  # 1. 매입 세금 계산서

        inner_select_query += """ union all /* 매입계산서 */ """ + query_bill.replace('|ty_buysale|', '2')  # 1. 매입 계산서

        inner_select_query += """ union all /* 매출 세금계산서 */ """ + query_tax_bill.replace('|ty_buysale|',
                                                                                        '1')  # 1. 매출 세금 계산서

        inner_select_query += """ union all /* 매출계산서 */ """ + query_bill.replace('|ty_buysale|', '1')  # 1. 매출 계산서

        return query.replace('|inner_select_query|', inner_select_query)

    # 세금계산서 합계표 (매출/매입)
    def select_billcont_45_46(self, ty_rpt):
        # 정기
        if str(ty_rpt) == '1':
            query = """
    -- 세금계산서 매출 마감 스트링 만들기  45번 (정기)
    /*
    create temp table temp_savf0117_param_table on commit drop as
    select --%(dm_fndend)s::varchar  as dm_fndend   -- '{0}'
     , --%(dm_fndbegin)s::varchar  as dm_fndbegin  --'{1}':
     --, %(ty_rpt)s  as ty_rpt  -- {2} 정기신고 1 / 수정신고 3
     --, %(no_count)s  as no_count  --{3}

     --, ''::varchar   as cd_taxoffcom  --
     --, 1   as ty_elctax  --1.전자 2.전자외
     --, '45'::varchar as cd_form  --

     --, ''::varchar   as no_biz  --
     --, %(ty_buysale)s as gb_inout  -- --매입/매출구분 ty_buysale
     --, 0    as cd_jr  --주류코드 / 1.종합주류~7.제조업직매장(유흥음식…)
    ;
    */

    --favf0117_mr_acct_data
    create temp table temp_usp_savf0117_mr_acct_data on commit drop as
    select a.cd_trade
     , cast(a.nm_trade as varchar(40)) as nm_trade
     , case when coalesce(a.cd_trade,'') <> '' then b.ty_biz
            else case when length(a.no_trade) = 10 then 0 else 1 end end as ty_biz
     , cast(a.mn_supply as numeric(17)) as mn_mnam
     , cast(a.mn_vat as numeric(17)) as mn_vat
     , a.no_trade as no_biz
     , a.str_mainbiz
     , a.cnt_sumsh as cnt
     , 1 as ty_elctax
     , coalesce(a.yn_beforebiz, 0) as yn_beforebiz --필드 추가 등록전매입구분(0.해당없음(null), 1.해당됨)
    from FTA_BILLCONT a
    left join FTB_TRADE b on a.cd_trade = b.cd_trade --and b.gisu = 19
    where a.ty_use = 1 -- 사용구분 1.세금계산서합계표 2.계산서합계표
    and a.dm_fndend = %(dm_fndend)s
    and a.ty_buysale = %(ty_buysale)s  --매입/매출구분
    and a.ty_rpt = %(ty_rpt)s    --신고구분
    --and a.ty_item in (11,12,25)
    and coalesce(a.no_count,1) = case when %(ty_rpt)s = 3 then %(no_count)s else coalesce(no_count,1) end -- 수정신고일경우수정차수조건추가
    and coalesce(ty_elctax,0) = 1
    order by b.ty_biz, a.cd_trade, a.no_trade;




    create temp table temp_usp_savf0117_mr_acct_sum on commit drop as
    select case when t.yn_beforebiz = 1 or t.ty_biz in (1, 2) then '주민등록기재분' else t.no_biz end as no_biz
     , t.cd_trade_temp as cd_trade
     , t.ty_biz as ty_biz
     , t.str_mainbiz as str_mainbiz
     , t.nm_trade as nm_trade
     , t.cnt as cnt
     , t.mn_mnam as mn_mnam
     , t.mn_vat as mn_vat
     , cast(0 as numeric(17,0)) as cnt_pre
     , cast(0 as numeric(17,0)) as mn_mnam_pre
     , cast(0 as numeric(17,0)) as mn_vat_pre   --수정전

    from
    (
       select min(coalesce(no_biz,'')) as no_biz
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
    order by  t.ty_biz, no_biz, t.ty_elctax;


    create temp table tmp_multi_key on commit drop as 
    select row_number() over() - 1 AS key, alpha 
    from regexp_split_to_table('}JKLMNOPQR', '') as alpha;


    select 
    	'1' as ty_datastr,
    	'1' as cd_formstr,
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
           || '§' || (case when coalesce(mn_mnam,0) < 0 then (select abs(trunc(mn_mnam/10, 0))::text || alpha from tmp_multi_key where abs(mod(mn_mnam, 10)) = key) else mn_mnam::text end)         --공급가액
           || '§' || (case when coalesce(mn_vat,0) < 0 then (select abs(trunc(mn_vat/10, 0))::text || alpha from tmp_multi_key where abs(mod(mn_vat, 10)) = key) else mn_vat::text end)         --세액
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
    where ty_biz = 0                 --사업자등록분만, 주민기재분제외
    order by ty_biz, no_biz, cd_trade, ord;


    /*
    select string_agg(mk_string,'') as str_close
    from (
    select erstring('VAT','1','1','201409',170,                 --자료구분
        (case when %(ty_buysale)s = 1 then 'W' else 'Y' end)
       || '§' || '0'           --보고자등록번호
       || '§' || row_number() over(order by no_biz)
       || '§' || coalesce(no_biz,'0')           --거래자등록번호
       || '§' || coalesce(replace(replace(nm_trade,'・','?'), '#', ''),'0')          --거래자상호
       || '§' || ' '               --업태
       || '§' || ' '               --종목
       || '§' || coalesce(cnt,0) --매수
       || '§' || 0                   --공란수
       || '§' || (case when coalesce(mn_mnam,0) < 0 then (select abs(trunc(mn_mnam/10, 0))::text || alpha from tmp_multi_key where abs(mod(mn_mnam, 10)) = key) else mn_mnam::text end)         --공급가액
       || '§' || (case when coalesce(mn_vat,0) < 0 then (select abs(trunc(mn_vat/10, 0))::text || alpha from tmp_multi_key where abs(mod(mn_vat, 10)) = key) else mn_vat::text end)         --세액
       || '§' || case when %(dm_fndend)s >= '201301'
           then (case coalesce(substr(str_mainbiz,length(str_mainbiz), 1), '') --when 'ⓞ' then 0  --거래처등록에 주류코드 없이 수록할 경우도 매입매출따라가도록
                                            when ''   then '0'
                                            else '1' end)
           else substr( '0' || (select coalesce(cd_jr, 0)::varchar from ftb_com), length((select coalesce(cd_jr, 0)::varchar from ftb_com)) + 1, 1)
        end              --주류여부[0,1]_20130624
       || '§' || ' '          -- 주류코드 201706
       || '§' || (case when %(ty_buysale)s = 1 then '7501' else '8501' end)                          --권번호
       || '§' || strtonumeric(((select cd_taxoffcom from ftb_com)))           --제출서
       || '§' || ' ')              --공란
       || '♥' as mk_string
    from (
        select ty_biz, no_biz, cd_trade, nm_trade, cnt - cnt_pre as cnt, mn_mnam - mn_mnam_pre as mn_mnam, mn_vat - mn_vat_pre as mn_vat, str_mainbiz, '1' as gb_data, 1 as ord
        from temp_usp_savf0117_mr_acct_sum
        where coalesce(cnt - cnt_pre, 0) <> 0 or coalesce(mn_mnam - mn_mnam_pre, 0) <> 0 or coalesce(mn_vat - mn_vat_pre, 0) <> 0

        union all

        select ty_biz, no_biz, cd_trade, nm_trade, cnt_pre as cnta, mn_mnam_pre as mn_mnama, mn_vat_pre as mn_vata, str_mainbiz, 'a', 0
        from temp_usp_savf0117_mr_acct_sum
        where coalesce(cnt_pre, 0) <> 0 or coalesce(mn_mnam_pre, 0) <> 0 or coalesce(mn_vat_pre, 0) <> 0
    ) a
    where ty_biz = 0                 --사업자등록분만, 주민기재분제외
    order by ty_biz, no_biz, cd_trade, ord
    ) main
    */

            """
        # 수정
        else:
            query = """ 
    -- 세금계산서 매출 마감 스트링 만들기 45번 (수정)
    /*
    create temp table temp_savf0117_param_table on commit drop as
    select --'202003'::varchar  as dm_fndend
     --, '202001'::varchar  as dm_fndbegin
    -- , 3  as ty_rpt
    -- , 1  as no_count
     --, ''::varchar   as cd_taxoffcom
     --, 1    as ty_elctax
     --, '45'::varchar as cd_form
    -- , 1     as gb_inout --매입/매출구분 .ty_buysale
    -- , 1     as ty_use -- 사용구분 1.세금계산서합계표 2.계산서합계표
     --, 0    as cd_jr

     --, 0    as ty_rpt_pre
     --, 0    as no_count_pre
     --, 3  as ty_rpt_org -- ty_rpt
     --, 1  as no_count_org --no_count
    ;
    */

    --favf0117_mr_acct_data
    create temp table temp_usp_savf0117_mr_acct_data on commit drop as
    select a.cd_trade, cast(a.nm_trade as varchar(40)) as nm_trade
     , case when coalesce(a.cd_trade,'') <> '' then b.ty_biz
            else case when length(a.no_trade) = 10 then 0 else 1 end end as ty_biz
     , cast(a.mn_supply as numeric(17)) as mn_mnam, cast(a.mn_vat as numeric(17)) as mn_vat
     , a.no_trade as no_biz
     , a.str_mainbiz
     , a.cnt_sumsh as cnt
     , ty_elctax
     , coalesce(a.yn_beforebiz, 0) as yn_beforebiz --필드 추가 등록전매입구분(0.해당없음(null), 1.해당됨)
    from FTA_BILLCONT a
    left join FTB_TRADE b on a.cd_trade = b.cd_trade-- and b.gisu = 19
    where a.ty_use = 1
    and a.dm_fndend = %(dm_fndend)s
    and a.ty_buysale = %(ty_buysale)s  --매입/매출구분
    and a.ty_rpt = %(ty_rpt)s    --신고구분
    --and a.ty_item in (11,12,25)
    and coalesce(a.no_count,1) = case when %(ty_rpt)s = 3 then %(no_count)s else coalesce(no_count,1) end -- 수정신고일경우수정차수조건추가
    and coalesce(ty_elctax,0) = 1
    order by b.ty_biz, a.cd_trade, a.no_trade;

    /*
    update temp_savf0117_param_table
    set ty_rpt_pre   = 1
     , no_count_pre = null
    where ty_rpt = 3 and no_count = 1;

    update temp_savf0117_param_table
    set ty_rpt_pre   = ty_rpt
     , no_count_pre = no_count - 1
    where ty_rpt = 3 and no_count <> 1;

    --값 교체뒤 favf0117_mr_acct_data 호출
    update temp_savf0117_param_table
    set ty_rpt           = ty_rpt_pre
     , no_count         = no_count_pre
    where ty_rpt = 3;
    */

    --favf0117_mr_acct_data --정기 신고 데이터 조회
    create temp table temp_usp_savf0117_mr_acct_data_pre on commit drop as
    select a.cd_trade, cast(a.nm_trade as varchar(40)) as nm_trade
     , case when coalesce(a.cd_trade,'') <> '' then b.ty_biz
            else case when length(a.no_trade) = 10 then 0 else 1 end end as ty_biz
     , cast(a.mn_supply as numeric(17)) as mn_mnam, cast(a.mn_vat as numeric(17)) as mn_vat
     , a.no_trade as no_biz
     , a.str_mainbiz
     , a.cnt_sumsh as cnt
     , a.ty_elctax
     , coalesce(a.yn_beforebiz, 0) as yn_beforebiz --필드 추가 등록전매입구분(0.해당없음(null), 1.해당됨)
    from FTA_BILLCONT a
    left join FTB_TRADE b on a.cd_trade = b.cd_trade-- and b.gisu = 19
    where a.ty_use = 1
    and a.dm_fndend = %(dm_fndend)s
    and a.ty_buysale = %(ty_buysale)s  --매입/매출구분
    and a.ty_rpt = %(ty_rpt_pre)s    --신고구분  1
    --and a.ty_item in (11,12,25)
    and coalesce(a.no_count,1) = case when %(ty_rpt_pre)s = 3 then %(no_count_pre)s else coalesce(no_count,1) end -- 수정신고일경우수정차수조건추가
    and coalesce(ty_elctax,0) = 1
    order by b.ty_biz, a.cd_trade, a.no_trade;




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


    /*
    --값 교체뒤  변수값 다시 원복 시켜줌
    update temp_savf0117_param_table
    set ty_rpt           = ty_rpt_org
     , no_count         = no_count_org
    where ty_rpt_org = 3;
    */


    create temp table tmp_multi_key on commit drop as 
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
           || '§' || coalesce(replace(replace(nm_trade,'・','?'), '#', ''),'0')          --거래자상호
           || '§' || ' '               --업태
           || '§' || ' '               --종목
           || '§' || coalesce(cnt,0) --매수
           || '§' || 0                   --공란수
           || '§' || (case when coalesce(mn_mnam,0) < 0 then (select abs(trunc(mn_mnam/10, 0))::text || alpha from tmp_multi_key where abs(mod(mn_mnam, 10)) = key) else mn_mnam::text end)         --공급가액
           || '§' || (case when coalesce(mn_vat,0) < 0 then (select abs(trunc(mn_vat/10, 0))::text || alpha from tmp_multi_key where abs(mod(mn_vat, 10)) = key) else mn_vat::text end)         --세액
           || '§' || case when %(dm_fndend)s >= '201301'
               then (case coalesce(substr(str_mainbiz,length(str_mainbiz), 1), '') --when 'ⓞ' then 0  --거래처등록에 주류코드 없이 수록할 경우도 매입매출따라가도록
                                                when ''   then '0'
                                                else '1' end)
               else substr( '0' || (select coalesce(cd_jr, 0)::varchar from ftb_com), length((select coalesce(cd_jr, 0)::varchar from ftb_com)) + 1, 1)
            end              --주류여부[0,1]_20130624
           || '§' || ' '          -- 주류코드 201706
           || '§' || (case when %(ty_buysale)s = 1 then '7501' else '8501' end)                          --권번호
           || '§' || strtonumeric((select coalesce(cd_taxoffcomm '') from ftb_com))           --제출서
           || '§' || ' '   --공란
         ) as str_colse 
    from (
            select ty_biz, no_biz, cd_trade, nm_trade, cnt as cnt, mn_mnam as mn_mnam, mn_vat as mn_vat, str_mainbiz, '1' as gb_data, 0 as ord
            from temp_usp_savf0117_mr_acct_sum
            where coalesce(cnt - cnt_pre, 0) <> 0 or coalesce(mn_mnam - mn_mnam_pre, 0) <> 0 or coalesce(mn_vat - mn_vat_pre, 0) <> 0

            union all

            select ty_biz, no_biz, cd_trade, nm_trade, cnt_pre as cnta, mn_mnam_pre as mn_mnama, mn_vat_pre as mn_vata, str_mainbiz, 'a', 1
            from temp_usp_savf0117_mr_acct_sum
            where coalesce(cnt_pre, 0) <> 0 or coalesce(mn_mnam_pre, 0) <> 0 or coalesce(mn_vat_pre, 0) <> 0
        ) a
    where ty_biz = 0                 --사업자등록분만, 주민기재분제외
    order by ty_biz, no_biz, cd_trade, ord;



    /*
    -- 매출자료data record
    select string_agg(mk_string,'') as str_close
    from (
    select erstring('VAT','1','1','201409',170,                 --자료구분
        case when ord = 0 then (case when %(ty_buysale)s = 1 then 'X' else 'Z' end) 
             else (case when %(ty_buysale)s = 1 then 'W' else 'Y' end) end --'1'
       || '§' || '0'           --보고자등록번호
       || '§' || row_number() over(order by no_biz)
       || '§' || coalesce(no_biz,'0')           --거래자등록번호
       || '§' || coalesce(replace(replace(nm_trade,'・','?'), '#', ''),'0')          --거래자상호
       || '§' || ' '               --업태
       || '§' || ' '               --종목
       || '§' || coalesce(cnt,0) --매수
       || '§' || 0                   --공란수
       || '§' || (case when coalesce(mn_mnam,0) < 0 then (select abs(trunc(mn_mnam/10, 0))::text || alpha from tmp_multi_key where abs(mod(mn_mnam, 10)) = key) else mn_mnam::text end)         --공급가액
       || '§' || (case when coalesce(mn_vat,0) < 0 then (select abs(trunc(mn_vat/10, 0))::text || alpha from tmp_multi_key where abs(mod(mn_vat, 10)) = key) else mn_vat::text end)         --세액
       || '§' || case when %(dm_fndend)s >= '201301'
           then (case coalesce(substr(str_mainbiz,length(str_mainbiz), 1), '') --when 'ⓞ' then 0  --거래처등록에 주류코드 없이 수록할 경우도 매입매출따라가도록
                                            when ''   then '0'
                                            else '1' end)
           else substr( '0' || (select coalesce(cd_jr, 0)::varchar from ftb_com), length((select coalesce(cd_jr, 0)::varchar from ftb_com)) + 1, 1)
        end              --주류여부[0,1]_20130624
       || '§' || ' '          -- 주류코드 201706
       || '§' || (case when %(ty_buysale)s = 1 then '7501' else '8501' end)                          --권번호
       || '§' || strtonumeric((select coalesce(cd_taxoffcomm '') from ftb_com))           --제출서
       || '§' || ' ' )             --공란
       || '♥' as mk_string
    from (
            select ty_biz, no_biz, cd_trade, nm_trade, cnt as cnt, mn_mnam as mn_mnam, mn_vat as mn_vat, str_mainbiz, '1' as gb_data, 0 as ord
            from temp_usp_savf0117_mr_acct_sum
            where coalesce(cnt - cnt_pre, 0) <> 0 or coalesce(mn_mnam - mn_mnam_pre, 0) <> 0 or coalesce(mn_vat - mn_vat_pre, 0) <> 0

            union all

            select ty_biz, no_biz, cd_trade, nm_trade, cnt_pre as cnta, mn_mnam_pre as mn_mnama, mn_vat_pre as mn_vata, str_mainbiz, 'a', 1
            from temp_usp_savf0117_mr_acct_sum
            where coalesce(cnt_pre, 0) <> 0 or coalesce(mn_mnam_pre, 0) <> 0 or coalesce(mn_vat_pre, 0) <> 0
        ) a
    where ty_biz = 0                 --사업자등록분만, 주민기재분제외
    order by ty_biz, no_biz, cd_trade, ord
    ) main
    */
            """

        return query

    # 계산서 합계표 (매출/매출) - 무조건 정기만 존재
    def select_billcont_47_48(self):
        query = """
    /*
    create temp table temp_ftb_com on commit drop as
    select coalesce(cd_taxoffcom, '') as cd_taxoffcom
    	, coalesce(no_biz,'')        as no_biz
    	, coalesce(da_start,'')      as da_start
    	, coalesce(da_end,'')        as da_end
    	, coalesce(nm_krcom,'')      as nm_krcom
    	, coalesce(nm_ceo,'')        as nm_ceo
    	, coalesce(cd_lawcom,'')     as cd_lawcom
    	, replace(coalesce(add_com1,''), '', '')  as add_com1
    	, replace(coalesce(add_com2,''), '', '') as add_com2
    from ftb_com
    where cd_com = 'biz202010200000130'::varchar;



    create temp table temp_param_table on commit drop as
    select '202003'::varchar	as	dm_fndend
    	--, '202001'::varchar	as	dm_fndbegin
    	--, 1	as	ty_rpt -- 1.정기 / 2.수정
    	--, 1	as	no_count
    	--, '47'::varchar	as	cd_form
    	--, 0    as ty_elctax

    	--, cd_taxoffcom as	cd_taxoffcom
    	--, no_biz as	no_biz

    	--, 2	    as	prd_vat --%(prd_vat)s --부가세기수
    	--, 1	    as	ty_month --%(ty_month)s --ty_month
    	--, 1	    as	ty_simple --%(ty_simple)s --일반간이구분
    	--, 1	    as	gb_inout -- ty_buysale  -- --매입/매출구분 ty_buysale
    	--, 2	    as	ty_use  -- 사용구분 1.세금계산서합계표 2.계산서합계표

    from temp_ftb_com;
    */



    --favf0117_mr_acct_data
    create temp table temp_usp_favf0117_mr_acct_data on commit drop as
    select a.cd_trade, cast(a.nm_trade as varchar(40)) as nm_trade 
    	 , case when coalesce(a.cd_trade,'') <> '' then b.ty_biz 
    			else case when length(a.no_trade) = 10 then 0 else 1 end end as ty_biz 
    	 , cast(a.mn_supply as numeric(17)) as mn_mnam, cast(a.mn_vat as numeric(17)) as mn_vat 
    	 , a.no_trade as no_biz
    	 , a.cnt_sumsh as cnt 
    	 , case ty_elctax when 1 then 1 else 0 end as ty_elctax
    	 , coalesce(a.yn_beforebiz, 0) as yn_beforebiz --필드 추가 등록전매입구분(0.해당없음(null), 1.해당됨)
    	 , a.jeonjasend15_yn -- 전자세금계산서 15일이내 경과 구분 (0 또는 null :전자, 그외 :전자외)
    from fta_billcont a 
    left join ftb_trade b on a.cd_trade = b.cd_trade 
    where a.ty_use = 2
    	and a.dm_fndend = %(dm_fndend)s
    	and a.ty_buysale = %(ty_buysale)s  --매입/매출구분 
    	and a.ty_rpt = 1    --신고구분 
    	--and a.ty_item = 13
    	and coalesce(a.no_count,1) = 1 -- 수정신고일경우수정차수조건추가 
    	and coalesce(ty_elctax,0) = 1
    order by b.ty_biz, a.cd_trade, a.no_trade;



    create temp table temp_usp_favf0117_mr_acct_sum on commit drop as
    select case when t.ty_biz in (1, 2) then '주민등록기재분' else t.no_biz end as no_biz
    	 , t.ty_biz as ty_biz, t.nm_trade as nm_trade
    	 , t.cnt as cnt, t.mn_mnam as mn_mnam
    from
    (
    	   select min(coalesce(no_biz,'')) as no_biz
    			, case when ty_biz in (1, 2) then '' else coalesce(cd_trade,'')      end  as cd_trade_temp
    			, case when ty_biz in (1, 2) then '' else min(coalesce(nm_trade,'')) end  as nm_trade
    			, case when ty_biz in (1, 2) then 1 else ty_biz end as ty_biz
    			, sum(coalesce(cnt,0)) as cnt, sum(coalesce(mn_mnam,0)) as mn_mnam
    			, ty_elctax as  ty_elctax
    			, yn_beforebiz --등록전매입
    	   from temp_usp_favf0117_mr_acct_data
       group by case when ty_biz in (1, 2) then '' else coalesce(cd_trade,'') end
    		  , case when ty_biz in (1, 2) then 1 else ty_biz end
    		  , ty_elctax, yn_beforebiz, ty_biz
    ) t left join ftb_trade b on t.cd_trade_temp = b.cd_trade
    order by  t.ty_biz, no_biz, t.cd_trade_temp, t.ty_elctax;

    --------------------------------------------------------

    --매출처별거래명세레코드여러건
    select '17' as ty_datastr,
           'D' as cd_formstr,
           '201409' as da_useend,
            230 as wm_max,                 --자료구분
            case when %(ty_buysale)s = 1 then 'S' else 'U' end
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
     where ty_biz = 0


    /*
    --매출처별거래명세레코드여러건
    select string_agg(form_d_sell,'') as str_close
    from(
    	select erstring('VAT','17','D','201409',230,                 --자료구분
    		case when %(ty_buysale)s = 1 then 'S' else 'U' end
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
    		|| '§' || ' ') --136 공란
    		|| '♥'  as form_d_sell
    	  from temp_usp_favf0117_mr_acct_sum
    	 where ty_biz = 0) main              --사업자등록분만, 주민기재분제외
    */
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