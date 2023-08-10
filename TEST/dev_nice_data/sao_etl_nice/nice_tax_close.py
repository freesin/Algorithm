# -*- coding: utf-8 -*-

from sao_etl_lib.sao_etl_json import name_to_json
from sao_etl_nice.inheritance.Inheritance_sao_etl import InheritanceSaoEtl
import calendar
import copy
from calendar import monthrange


class NiceFinance(InheritanceSaoEtl):

    def __init__(self, sao_db_connection, schema_name, com_info, crypto_obj, default_date_version=None, base_da_standard_begin=None):
        super().__init__(sao_db_connection, schema_name, com_info, default_date_version, base_da_standard_begin)
        self._company_info = com_info
        self._schema_name = schema_name
        self._reporter = []
        self._crypto_obj = crypto_obj

    def sao_etl_start(self):
        return self.nice_finance_test()

    def nice_finance_test(self):
        params = {
            # "cno": self._company_info[0]["cno"],
            "ccode": self._schema_name,
            "no_biz": self._company_info[0]["no_biz"],
            "li_vat_close": self._company_info[0]["li_vat_close"],
            "li_tax_check": self._company_info[0]["li_tax_check"]
        }

        ls_info = [li_tax for li_tax in str(self._company_info[0]["li_tax_check"]).split(',') if li_tax]
        ### 1. 제출자정보 및 회사 정보 가져오기 ###
        self._sao_db_connection.cursor.execute(self.select_company_info(), params)
        com_info = name_to_json(self._sao_db_connection.cursor)
        self._crypto_obj.decrypt_data(data=com_info, cols={"ceoregno": "social", "bizregno": "social"})

        # # 회사 정보가 없으면 잘못된 회사
        # try:
        #     exists_com = com_info[0]['danggi_gisu']
        # except:
        #     return []

        result = []
        # 재무제표 데이터 저장 로직
        for tax_info in ls_info:
            params['dt_from'] = str(tax_info) + '0101'
            params['dt_to'] = str(tax_info) + '1231'

            params['old_view'] = 0
            if str(tax_info) >= com_info[0]['danggi_da_accbegin'][0:4]:
                params['old_view'] = 0
                diff_da_acc = int(str(tax_info)[0:4]) - int(com_info[0]['danggi_da_accbegin'][0:4])
                params['gisu'] = int(com_info[0]['danggi_gisu']) + diff_da_acc
            else:
                params['old_view'] = 1
                diff_da_acc = int(com_info[0]['danggi_da_accbegin'][0:4]) - int(str(tax_info)[0:4])
                params['gisu'] = int(com_info[0]['danggi_gisu']) - diff_da_acc

            temp_data = {}
            # self._sao_db_connection.cursor.execute(self.select_financesend_query(), params)
            # tax_data = name_to_json(self._sao_db_connection.cursor)

            self._sao_db_connection.cursor.execute(self.select_financesend_query_json(), params)
            tax_data_json = name_to_json(self._sao_db_connection.cursor)

            for tax_data_js in tax_data_json:
                self._crypto_obj.dec_enc_data(data=tax_data_js['js_detail_data'], cols={"str_2": "social"})

            if str(com_info[0]['corprvgbn']) == '0':
                self._sao_db_connection.cursor.execute(self.select_cor_hometax_data(), params)
            else:
                self._sao_db_connection.cursor.execute(self.select_rec_hometax_data(tax_info), params)
            tax_ers_data = name_to_json(self._sao_db_connection.cursor)

            temp_data['gisu'] = params['gisu']
            # temp_data['tax_data'] = tax_data
            temp_data['tax_data_json'] = tax_data_json
            temp_data['tax_ers_data'] = tax_ers_data

            result.append(temp_data)

        return result


    ## 쿼리 영역 #########################################################################
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
        com.yn_private as corprvgbn,   --   법인(0)/개인(1) 구분  

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

    # fta_financesend 조회 로직
    def select_financesend_query(self):
        query = """
            drop table if exists max_data;
            create temp table max_data on commit drop as
            select str_5, str_4, dt_insert, row_number() over (partition by str_4 order by dt_insert desc) as row_id
            from fta_financesend
            where dt_from = %(dt_from)s
              and dt_to = %(dt_to)s
              and gisu = %(gisu)s
              and str_5 in ('1', '3')
            group by str_5, str_4, dt_insert

            union all

            select str_5, str_4, dt_insert, row_number() over (partition by str_4 order by dt_insert desc) as row_id
            from fta_financesend
            where dt_from = %(dt_from)s
              and dt_to = %(dt_to)s
              and gisu = %(gisu)s
              and str_5 = '2'
            group by str_5, str_4, dt_insert
            order by row_id, str_5, str_4;


            select gisu,    -- 기수
                   dt_from, -- 조회시작월
                   dt_to,   -- 조회종료월
                   page,    -- 서식별일련키
                   grp,     -- 소분류순위
                   code,    -- 일련키
                   mn_1,    -- 당기_왼쪽
                   mn_2,    -- 당기_오른쪽
                   mn_3,    -- 전기_왼쪽
                   mn_4,    -- 전기_오른쪽
                   mn_5,    -- 전전기_왼쪽
                   mn_6,    -- 전전기_오른쪽
                   str_2,   -- 사업자등록번호
                   str_3,   -- 상호명
                   str_4,   -- 재무제표구분
                   str_5,   -- 저장방법
                   str_6,   -- 대표자소득구분
                   str_7,   -- 계정과목코드
                   str_8,   -- 계정과목관계코드
                   str_9,   -- 계정과목명
                   str_10,  -- 과목명
                   str_11,  -- 계정과목 성격
                   str_12,  -- 분류코드
                   str_13,  -- 구분
                   str_14,  -- 차감계정여부
                   str_15,  -- 음수여부
                   str_16,  -- 생성날짜
                   str_17,  -- 계정과목코드
                   id_insert,   -- 작성아이디
                   to_char(dt_insert, 'YYYY-MM-DD HH24:MI:SS') as dt_insert, -- 작성일시
                   id_modify,   -- 수정자아이디
                   to_char(dt_modify, 'YYYY-MM-DD HH24:MI:SS') as dt_modify  -- 수정일시
            from fta_financesend as main_tb
            where dt_from = %(dt_from)s
              and dt_to = %(dt_to)s
              and gisu = %(gisu)s
              and (
                    -- 1번은 무조건 화면에 보여줘야함
                    (str_5 = '1'
                        and dt_insert = (select max(dt_insert) as dt_insert from max_data where str_5 = '1' and str_4 = main_tb.str_4))
                    or
                    -- 2에서 최근값 가져옴
                    (str_5 = '2'
                        and dt_insert = (select dt_insert from max_data where str_4 = main_tb.str_4 and row_id = 1 and str_5 in ('2')))
                    or
                    -- 1,3에서 최근값 가져옴
                    (str_5 = (select str_5 from max_data where str_4 = main_tb.str_4 and row_id = 1 and str_5 in ('1', '3'))
                        and dt_insert = (select dt_insert from max_data where str_4 = main_tb.str_4 and row_id = 1 and str_5 in ('1', '3'))
                        and str_5 in ('1','3'))
                )
            order by gisu, str_5, str_4, page;
        """
        return query

    # fta_financesend 조회 로직
    def select_financesend_query_json(self):
        query = """
            drop table if exists max_data;
            create temp table max_data on commit drop as
            select str_5, str_4, dt_insert, row_number() over (partition by str_4 order by dt_insert desc) as row_id
            from fta_financesend
            where dt_from = %(dt_from)s
              and dt_to = %(dt_to)s
              and gisu = %(gisu)s
              and str_5 in ('1', '3')
            group by str_5, str_4, dt_insert

            union all

            select str_5, str_4, dt_insert, row_number() over (partition by str_4 order by dt_insert desc) as row_id
            from fta_financesend
            where dt_from = %(dt_from)s
              and dt_to = %(dt_to)s
              and gisu = %(gisu)s
              and str_5 = '2'
            group by str_5, str_4, dt_insert
            order by row_id, str_5, str_4;


            select gisu,    -- 기수
                   dt_from, -- 조회시작월
                   dt_to,   -- 조회종료월
                   str_4,   -- 재무제표구분
                   str_5,   -- 저장방법
                   str_16,  -- 생성날짜
                   id_insert,   -- 작성아이디
                   to_char(dt_insert, 'YYYY-MM-DD HH24:MI:SS') as dt_insert, -- 작성일시
                   id_modify,   -- 수정자아이디
                   to_char(dt_modify, 'YYYY-MM-DD HH24:MI:SS') as dt_modify,  -- 수정일시
                   (select array_to_json(array_agg(js_row))
                                       from (
                                                select *
                                                from fta_financesend as json_tb
                                                where json_tb.gisu = main_tb.gisu
                                                  and json_tb.dt_from = main_tb.dt_from
                                                  and json_tb.dt_to = main_tb.dt_to
                                                  and json_tb.str_4 = main_tb.str_4
                                                  and json_tb.str_5 = main_tb.str_5
                                                  and json_tb.dt_insert = main_tb.dt_insert
                                            ) as js_row) as js_detail_data
            from fta_financesend as main_tb
            where dt_from = %(dt_from)s
              and dt_to = %(dt_to)s
              and gisu = %(gisu)s
              and (
                    -- 1번은 무조건 화면에 보여줘야함
                    (str_5 = '1'
                        and dt_insert = (select max(dt_insert) as dt_insert from max_data where str_5 = '1' and str_4 = main_tb.str_4))
                    or
                    -- 2에서 최근값 가져옴
                    (str_5 = '2'
                        and dt_insert = (select dt_insert from max_data where str_4 = main_tb.str_4 and row_id = 1 and str_5 in ('2')))
                    or
                    -- 1,3에서 최근값 가져옴
                    (str_5 = (select str_5 from max_data where str_4 = main_tb.str_4 and row_id = 1 and str_5 in ('1', '3'))
                        and dt_insert = (select dt_insert from max_data where str_4 = main_tb.str_4 and row_id = 1 and str_5 in ('1', '3'))
                        and str_5 in ('1','3'))
                )
            group by gisu, dt_from, dt_to, str_4, str_5, str_16, id_insert, dt_insert, id_modify, dt_modify
            order by str_5, str_4;
        """
        return query

    # 마감스트링 조회 로직
    def select_cor_hometax_data(self):
        query = """
            -- 법인세
            select distinct home.prd_accounts as gisu, cor.dm_fndbegin, cor.dm_fndend,
                            regexp_replace(hometax_data, E'[\\n\\r]+', '♥', 'g') as hometax_data,
                            home.key_close as key_close, home.cd_com
            from ftc_hometax_data home
                     inner join ftc_corclose cor
                                on cor.cd_com = home.cd_com and cor.gubun_com = home.gubun_com
                                    and cor.prd_accounts = home.prd_accounts and cor.key_close = home.key_close
                                    and cor.yn_close = 1 and cor.gubun_com = 1
            where home.prd_accounts = %(gisu)s;
        """
        return query

    # 마감스트링 조회 로직
    def select_rec_hometax_data(self, tax_info):
        query = """
            -- 종소세
            select distinct home.prd_accounts as gisu, rec.dm_fndbegin, rec.dm_fndend,
                                        regexp_replace(hometax_data, E'[\\n\\r]+', '♥', 'g') as hometax_data,
                                        home.key_close as key_close, home.cd_com
            from ftr_hometax_data as home
                     inner join ftr_recclose as rec
                                on rec.cd_com = home.cd_com and rec.gubun_com = home.gubun_com
                                    and rec.prd_accounts = home.prd_accounts and rec.key_close = home.key_close
                                    and rec.yn_close = 1 and rec.gubun_com = 1
            WHERE  SUBSTR(home.dm_fndend,1,4) = '{tax_info}'
              -- 2020 11 12 추가
              AND home.key_close is not null; -- key_close가 null 인 데이터는 스마트에이 데이터
        """.format(tax_info=tax_info)
        return query