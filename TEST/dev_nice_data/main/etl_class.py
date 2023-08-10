# -*- coding: utf-8 -*-

import base64
import datetime
import hashlib
import json
import multiprocessing
import sys
import time
import traceback
import decimal

from calendar import monthrange

import requests

from dev_nice_data.settings import DATABASES
from sao_etl_common.psycogp2_connection import Psycopg2Connection
from sao_etl_common.pymysql_connection import PyMySqlConnection
from sao_etl_lib.sao_etl_crypt import CryptHelper
from sao_etl_lib.sao_etl_json import name_to_json
from sao_etl_nice.nice_tax_close import NiceFinance
from sao_etl_nice.nice_vat_close import NiceVatClose

SEMAPHORE = multiprocessing.Semaphore(6)


class SaoEtlThread(multiprocessing.Process):
    def __init__(self, host, sdb_name, schema_name, default_date_version, cno, dec_key=None):
        multiprocessing.Process.__init__(self)
        self._host = host
        self._vm_host_name = host
        self._sdb_name = sdb_name
        self._schema_name = schema_name
        self.sdb_name = sdb_name
        self.schema_name = schema_name
        self._default_date_version = default_date_version
        self._base_nice_check_date = self.__setting_initial_date_info(base_job_proc_date=datetime.datetime.now().strftime("%Y%m%d"))
        self._cno = cno

        self._sao_db_connection = Psycopg2Connection()
        self._sao_meta_connection = PyMySqlConnection()
        self._inner_api_url = "http://dev.innerapi.wehago.com"
        self._dec_key = dec_key
        self._enc_key = "7a11b8b3f1e48b771808bd437a29181b"

    def run(self):
        query_params = {
            "nm_vm_host": self._vm_host_name,
            "da_work": self._default_date_version
        }
        print("| >> VM Start [host : {nm_vm_host} / {sdb_name} / {schema_name}] start".format(nm_vm_host=query_params["nm_vm_host"], sdb_name=self._sdb_name, schema_name=self._schema_name))
        try:
            SEMAPHORE.acquire()
        except Exception as exception:
            print(exception)

        if self._schema_name.startswith("biz") and len(self._schema_name) == 18:
            self._sao_db_connection.host = self._host
            self._sao_db_connection.port = 5432
            self._sao_db_connection.user = 'smarta_user'
            self._sao_db_connection.password = 'qwer1234'
            self._sao_db_connection.database = self._sdb_name

            self._sao_meta_connection.user = DATABASES["default"]["USER"]
            self._sao_meta_connection.password = DATABASES["default"]["PASSWORD"]
            self._sao_meta_connection.host = DATABASES["default"]["HOST"]
            self._sao_meta_connection.port = int(DATABASES["default"]["PORT"])
            self._sao_meta_connection.database = DATABASES["default"]["NAME"]
            if self._sao_db_connection.connect():
                self._sao_db_connection.cursor.execute("set search_path to {schema_name};".format(schema_name=self._schema_name))

                try:
                    query_string = """select t1.*,
                       replace(coalesce(t1.no_biz,''),'-','') as com_no_biz,
                       case when t1.yn_private = 1 then replace(coalesce(t1.no_ceosoc,''),'-','')
                                else replace(coalesce(t1.no_corpor,''),'-','') end as com_no_corpor,
                       t2.gisu as mig_prd_accounts,
                       t2.da_accbegin as mig_da_accbegin
                from {schema_name}.ftb_com as t1
                left join {schema_name}.fts_migration_danggi as t2 on t1.cd_com = t2.cd_com
                where t1.cd_com = '{schema_name}';""".format(schema_name=self._schema_name)
                    self._sao_db_connection.cursor.execute(query_string)
                    result_ftb_com = name_to_json(self._sao_db_connection.cursor)

                    # 암호화 복호화
                    ch = CryptHelper(dec_key=self._dec_key, enc_key=self._enc_key)
                    for ftb_com_data in result_ftb_com:
                        ch.dec_enc_data(data=ftb_com_data, cols={"no_ceosoc": "social"})

                    if len(result_ftb_com) == 1 and result_ftb_com[0].get("cd_com"):
                        result_cnt_emp = self.__select_cnt_emp(sao_db_connection=self._sao_db_connection,
                                                               schema_name=self._schema_name)

                        result_company_close_info = {}
                        result_company_close_info["ccode"] = self._schema_name
                        result_company_close_info["dd_last_job_history"] = self._default_date_version
                        result_company_close_info["li_vat_close"] = self.__select_vat_close_check(sao_db_connection=self._sao_db_connection,
                                                                                                  schema_name=self._schema_name,
                                                                                                  query_params=self._base_nice_check_date["base_vat_close_check_year"])

                        # 기준 부가세 요청 기간의 데이터 입력 시간 조회
                        if result_company_close_info["li_vat_close"]:
                            result_company_close_info["base_dt_insert"] = self.__select_vat_close_last_insert_date(sao_db_connection=self._sao_db_connection,
                                                                                                                   schema_name=self._schema_name,
                                                                                                                   da_end_list=result_company_close_info["li_vat_close"])
                        else:
                            result_company_close_info["base_dt_insert"] = None

                        result_company_close_info["li_tax_check"] = self.__select_tax_check(sao_db_connection=self._sao_db_connection,
                                                                                            schema_name=self._schema_name,
                                                                                            query_params=self._base_nice_check_date["base_tax_check_date"])

                        time.sleep(0.1)
                        query_string = """select '{ccode}' as ccode, '{da_work}' as da_work, * from fta_reporter""".format(ccode=self._schema_name,
                                                                                                                           da_work=self._default_date_version)
                        self._sao_db_connection.cursor.execute(query_string)
                        result_reporter_data = name_to_json(self._sao_db_connection.cursor)  # cursor 동작 순서 때문에 위치 변경

                        query_string = """
                        -- 신고마지막날 + 1일 가져오기
                            select to_char(dateadd('day', +1, min(dt_cdr)), 'yyyyMMdd') as dt_cdr
                            from sdb_mono.mtk_esero_calendar
                            where dt_cdr >= concat(substr('{0}',1,6), '25')
                              and yn_holiday = '0';
                        """.format(self._default_date_version)
                        self._sao_db_connection.cursor.execute(query_string)
                        da_singo = name_to_json(self._sao_db_connection.cursor)  # cursor 동작 순서 때문에 위치 변경

                        for reporter_data in result_reporter_data:
                            ch.dec_enc_data(data=reporter_data, cols={"no_social": "social"})

                        time.sleep(0.1)
                        result_ftb_com[0]["li_vat_close"] = result_company_close_info["li_vat_close"] if result_company_close_info["li_vat_close"] is not None else ''
                        result_ftb_com[0]["li_tax_check"] = result_company_close_info["li_tax_check"] if result_company_close_info["li_tax_check"] is not None else ''

                        # 부가세 로직
                        nice_vat_obj = NiceVatClose(sao_db_connection=self._sao_db_connection,
                                                    schema_name=self._schema_name,
                                                    com_info=result_ftb_com,
                                                    crypto_obj=ch,
                                                    default_date_version=self._default_date_version)
                        result_nice_data = nice_vat_obj.sao_etl_start()

                        # 재무제표로직
                        nice_tax_obj = NiceFinance(sao_db_connection=self._sao_db_connection,
                                                   schema_name=self._schema_name,
                                                   com_info=result_ftb_com,
                                                   crypto_obj=ch,
                                                   default_date_version=self._default_date_version)
                        result_nice_data_tax = nice_tax_obj.sao_etl_start()

                        try:
                            print("| >>>> save data start")
                            self._sao_meta_connection.connect()
                            self.__company_info_save(sao_meta_connection=self._sao_meta_connection,
                                                     default_date_version=self._default_date_version,
                                                     schema_name=self._schema_name,
                                                     result_ftb_com=result_ftb_com,
                                                     result_cnt_emp=result_cnt_emp)

                            print("| >>>> success company_info_save")

                            self.__save_company_close_info(sao_meta_connection=self._sao_meta_connection,
                                                           query_params=result_company_close_info)

                            print("| >>>> success save_company_close_info")

                            self.__save_nice_fta_reporter(sao_meta_connection=self._sao_meta_connection,
                                                          result_reporter_data=result_reporter_data)

                            print("| >>>> success save_nice_fta_reporter")

                            self.__save_nice_vat_data(sao_meta_connection=self._sao_meta_connection,
                                                      nice_vat_data=result_nice_data,
                                                      com_info=result_ftb_com)

                            print("| >>>> success save_nice_vat_data")
                            self.__save_nice_finance_data(sao_meta_connection=self._sao_meta_connection,
                                                          nice_finance_data=result_nice_data_tax,
                                                          com_info=result_ftb_com)
                            print("| >>>> success save_nice_finance_data")

                            self.__insert_ccode_map(sao_meta_connection=self._sao_meta_connection)
                            print("| >>>> insert ccode map")
                            print("| >>>> save data end")
                        except Exception as exception:
                            err_message = '\n'.join(traceback.format_exception(*(sys.exc_info())))
                            print(err_message)
                            self._sao_db_connection.close()
                        self._sao_meta_connection.close()
                    self._sao_db_connection.close()
                except Exception as exception:

                    err_message = '\n'.join(traceback.format_exception(*(sys.exc_info())))
                    print(err_message)
                    self._sao_db_connection.close()
                    # self._sao_meta_connection.close()
        print("| >> VM End [host : {nm_vm_host} / {sdb_name} / {schema_name}] end".format(nm_vm_host=query_params["nm_vm_host"], sdb_name=self._sdb_name, schema_name=self._schema_name))
        time.sleep(0.5)
        SEMAPHORE.release()

    def __company_info_save(self, sao_meta_connection, default_date_version, schema_name, result_ftb_com, result_cnt_emp):
        temp_params = {
            "schema_name": schema_name
        }

        temp_params.update(
            {
                "ccode": result_ftb_com[0]["cd_com"],
                "cd_com": result_ftb_com[0]["cd_com"],
                "nm_krcom": result_ftb_com[0]["nm_krcom"],
                "yn_private": result_ftb_com[0]["yn_private"],
                "prd_accounts": result_ftb_com[0]["prd_accounts"],
                "da_accbegin": result_ftb_com[0]["da_accbegin"],
                "da_accend": result_ftb_com[0]["da_accend"],
                "no_biz": result_ftb_com[0]["no_biz"],
                "no_corpor": result_ftb_com[0]["no_corpor"],
                "ty_addtax": result_ftb_com[0]["ty_addtax"],
                "nm_ceo": result_ftb_com[0]["nm_ceo"],
                "no_ceosoc": result_ftb_com[0]["no_ceosoc"],
                "yn_forceo": result_ftb_com[0]["yn_forceo"],
                "yn_resident": result_ftb_com[0]["yn_resident"],
                "ty_pceo": result_ftb_com[0]["ty_pceo"],
                "zip_com": result_ftb_com[0]["zip_com"],
                "add_com1": result_ftb_com[0]["add_com1"],
                "add_com2": result_ftb_com[0]["add_com2"],
                "tel_com1": result_ftb_com[0]["tel_com1"],
                "tel_com2": result_ftb_com[0]["tel_com2"],
                "tel_com3": result_ftb_com[0]["tel_com3"],
                "fax_com1": result_ftb_com[0]["fax_com1"],
                "fax_com2": result_ftb_com[0]["fax_com2"],
                "fax_com3": result_ftb_com[0]["fax_com3"],
                "cd_taxoffcom": result_ftb_com[0]["cd_taxoffcom"],
                "cd_taxoffceo": result_ftb_com[0]["cd_taxoffceo"],
                "cd_lawcom": result_ftb_com[0]["cd_lawcom"],
                "cd_biztyp": result_ftb_com[0]["cd_biztyp"],
                "nm_bizcond": result_ftb_com[0]["nm_bizcond"],
                "nm_item": result_ftb_com[0]["nm_item"],
                "da_build": result_ftb_com[0]["da_build"],
                "da_start": result_ftb_com[0]["da_start"],
                "da_end": result_ftb_com[0]["da_end"],
                "yn_jijum": result_ftb_com[0]["yn_jijum"],
                "cnt_emp": result_cnt_emp[0]["cnt_emp"],
                "yn_join": result_ftb_com[0]["yn_join"],
                "ty_report": result_ftb_com[0]["ty_report"],
                "mig_prd_accounts": result_ftb_com[0]["mig_prd_accounts"],
                "mig_da_accbegin": result_ftb_com[0]["mig_da_accbegin"],
                "dd_last_job_history": default_date_version,
                "dd_last_update": None
            }
        )

        # sao_company_info 테이블 업데이트
        query_string = """INSERT INTO sao_company_info
    SET ccode = %(ccode)s,
        nm_krcom = %(nm_krcom)s,
        yn_private = %(yn_private)s,
        prd_accounts = %(prd_accounts)s,
        da_accbegin = %(da_accbegin)s,
        da_accend = %(da_accend)s,
        no_biz = %(no_biz)s,
        no_corpor = %(no_corpor)s,
        ty_addtax = %(ty_addtax)s,
        nm_ceo = %(nm_ceo)s,
        no_ceosoc = %(no_ceosoc)s,
        yn_forceo = %(yn_forceo)s,
        yn_resident = %(yn_resident)s,
        ty_pceo = %(ty_pceo)s,
        zip_com = %(zip_com)s,
        add_com1 = %(add_com1)s,
        add_com2 = %(add_com2)s,
        tel_com1 = %(tel_com1)s,
        tel_com2 = %(tel_com2)s,
        tel_com3 = %(tel_com3)s,
        fax_com1 = %(fax_com1)s,
        fax_com2 = %(fax_com2)s,
        fax_com3 = %(fax_com3)s,
        cd_taxoffcom = %(cd_taxoffcom)s,
        cd_taxoffceo = %(cd_taxoffceo)s,
        cd_lawcom = %(cd_lawcom)s,
        cd_biztyp = %(cd_biztyp)s,
        nm_bizcond = %(nm_bizcond)s,
        nm_item = %(nm_item)s,
        da_build = %(da_build)s,
        da_start = %(da_start)s,
        da_end = %(da_end)s,
        yn_jijum = %(yn_jijum)s,
        cnt_emp = %(cnt_emp)s,
        yn_join = %(yn_join)s,
        ty_report = %(ty_report)s,
        mig_prd_accounts = %(mig_prd_accounts)s,
        mig_da_accbegin = %(mig_da_accbegin)s,
        dd_last_job_history = %(dd_last_job_history)s,
        dd_last_update = now()
    ON DUPLICATE KEY UPDATE
        nm_krcom = %(nm_krcom)s,
        yn_private = %(yn_private)s,
        prd_accounts = %(prd_accounts)s,
        da_accbegin = %(da_accbegin)s,
        da_accend = %(da_accend)s,
        no_biz = %(no_biz)s,
        no_corpor = %(no_corpor)s,
        ty_addtax = %(ty_addtax)s,
        nm_ceo = %(nm_ceo)s,
        no_ceosoc = %(no_ceosoc)s,
        yn_forceo = %(yn_forceo)s,
        yn_resident = %(yn_resident)s,
        ty_pceo = %(ty_pceo)s,
        zip_com = %(zip_com)s,
        add_com1 = %(add_com1)s,
        add_com2 = %(add_com2)s,
        tel_com1 = %(tel_com1)s,
        tel_com2 = %(tel_com2)s,
        tel_com3 = %(tel_com3)s,
        fax_com1 = %(fax_com1)s,
        fax_com2 = %(fax_com2)s,
        fax_com3 = %(fax_com3)s,
        cd_taxoffcom = %(cd_taxoffcom)s,
        cd_taxoffceo = %(cd_taxoffceo)s,
        cd_lawcom = %(cd_lawcom)s,
        cd_biztyp = %(cd_biztyp)s,
        nm_bizcond = %(nm_bizcond)s,
        nm_item = %(nm_item)s,
        da_build = %(da_build)s,
        da_start = %(da_start)s,
        da_end = %(da_end)s,
        yn_jijum = %(yn_jijum)s,
        cnt_emp = %(cnt_emp)s,
        yn_join = %(yn_join)s,
        ty_report = %(ty_report)s,
        mig_prd_accounts = %(mig_prd_accounts)s,
        mig_da_accbegin = %(mig_da_accbegin)s,
        dd_last_job_history = %(dd_last_job_history)s,
        dd_last_update = now()"""
        sao_meta_connection.cursor.execute(query_string, temp_params)
        sao_meta_connection.commit()

    def __select_cnt_emp(self, sao_db_connection, schema_name):
        query_string = "select count(*) as cnt_emp from {schema_name}.ftw_emp where da_retire is not null;".format(schema_name=schema_name)
        sao_db_connection.cursor.execute(query_string)
        return name_to_json(sao_db_connection.cursor)

    # sao_company_close_info 테이블 업데이트
    def __save_company_close_info(self, sao_meta_connection, query_params):
        query_string = """INSERT INTO sao_company_close_info
    SET ccode = %(ccode)s,
        li_vat_close = %(li_vat_close)s,
        li_tax_check = %(li_tax_check)s,
        dd_last_job_history = %(dd_last_job_history)s,
        dd_vat_dt_insert = timestamp(%(base_dt_insert)s),
        dd_last_update = now()
    ON DUPLICATE KEY UPDATE
        li_vat_close = %(li_vat_close)s,
        li_tax_check = %(li_tax_check)s,
        dd_last_job_history = %(dd_last_job_history)s,
        dd_vat_dt_insert = timestamp(%(base_dt_insert)s),
        dd_last_update = now()"""
        sao_meta_connection.cursor.execute(query_string, query_params)
        sao_meta_connection.commit()

    def __select_vat_close_check(self, sao_db_connection, schema_name, query_params):
        query_string = """----  부가세 마감 여부 확인 쿼리 -- 
    drop table if exists tmp_da;
    create temp table tmp_da on commit drop
    as
    select %(da_year_1)s || '03' as da_end
      union all 
      select %(da_year_1)s || '06' as da_end
      union all 
      select %(da_year_1)s || '09' as da_end
      union all 
      select %(da_year_1)s || '12' as da_end

      union all

      select %(da_year_2)s || '03' as da_end
      union all 
      select %(da_year_2)s || '06' as da_end
      union all 
      select %(da_year_2)s || '09' as da_end
      union all 
      select %(da_year_2)s || '12' as da_end

      union all

      select %(da_year_3)s || '03' as da_end
      union all 
      select %(da_year_3)s || '06' as da_end
      union all 
      select %(da_year_3)s || '09' as da_end
      union all 
      select %(da_year_3)s || '12' as da_end

      union all

      select %(da_year_4)s || '03' as da_end
      union all 
      select %(da_year_4)s || '06' as da_end
      union all 
      select %(da_year_4)s || '09' as da_end
      union all 
      select %(da_year_4)s || '12' as da_end;


    select 
      array_to_string(array_agg(
      case when substr(da_end, 5,2) = '03' then substr(da_end, 1,4) || '1'
           when substr(da_end, 5,2) = '06' then substr(da_end, 1,4) || '2'
           when substr(da_end, 5,2) = '09' then substr(da_end, 1,4) || '3'
           when substr(da_end, 5,2) = '12' then substr(da_end, 1,4) || '4' end
      ), ',') as da_end 
    from 
    (
      select row_number() over(order by da_end desc) as num, da.da_end, (case when vat.cd_form is null then 0 else 1 end) as is_magam
      from tmp_da as da
      left join (
          select cd_form, dm_fndend 
          from {schema_name}.fta_vatclose_g where cd_form in ('01', '02')
          group by cd_form, dm_fndend

          union all

          select cd_form, dm_fndend 
          from {schema_name}.fta_vatclose_g_before where cd_form in ('01', '02')
          group by cd_form, dm_fndend

       ) as vat on vat.dm_fndend = da.da_end
      where (case when vat.cd_form is null then 0 else 1 end) = 1
      order by da.da_end desc
    ) as aa
    where is_magam = 1 and num <= 12;""".format(schema_name=schema_name)
        sao_db_connection.cursor.execute(query_string, query_params)
        result_data = name_to_json(sao_db_connection.cursor)
        return result_data[0]["da_end"] if len(result_data) > 0 else None

    def __select_vat_close_last_insert_date(self, sao_db_connection, schema_name, da_end_list):
        last_da_end = da_end_list.split(',')[0]
        if last_da_end and len(last_da_end) == 5:
            dm_fndend = ''
            if last_da_end[4:5] == '1':
                dm_fndend = last_da_end[0:4] + '03'
            elif last_da_end[4:5] == '2':
                dm_fndend = last_da_end[0:4] + '06'
            elif last_da_end[4:5] == '3':
                dm_fndend = last_da_end[0:4] + '09'
            elif last_da_end[4:5] == '4':
                dm_fndend = last_da_end[0:4] + '12'

            query_string = """
                    -- 정보제공 기준 부가세기간의 dt_insert 조회
                    select cd_form, dm_fndend, to_char(max(dt_insert), 'YYYY-MM-DD HH24:MI:SS') as dt_insert
                    from {schema_name}.fta_vatclose_g
                    where cd_form in ('01', '02')
                      and dm_fndend = '{dm_fndend}'
                    group by cd_form, dm_fndend

                    union all

                    select cd_form, dm_fndend, to_char(max(dt_insert), 'YYYY-MM-DD HH24:MI:SS') as dt_insert
                    from {schema_name}.fta_vatclose_g_before
                    where cd_form in ('01', '02')
                      and dm_fndend = '{dm_fndend}'
                    group by cd_form, dm_fndend
                """.format(schema_name=schema_name, dm_fndend=dm_fndend)
            sao_db_connection.cursor.execute(query_string)
            result_data = name_to_json(sao_db_connection.cursor)
            return result_data[0]["dt_insert"] if len(result_data) > 0 else None
        else:
            return None

    def __select_tax_check(self, sao_db_connection, schema_name, query_params):
        query_string = """
    drop table if exists tmp_da;
    create temp table tmp_da on commit drop
    as
    select %(da_start_1)s as da_start,  %(da_end_1)s as da_end
    union all
    select %(da_start_2)s as da_start,  %(da_end_2)s as da_end
    union all
    select %(da_start_3)s as da_start,  %(da_end_3)s as da_end ;

    select array_to_string(array_agg(substr(da_end, 1, 4)), ',') as da_end
    from (
             select substr(da_end, 1, 4)                       as da_end,
                    max((case when cnt > 0 then 1 else 0 end)) as is_data
             from (
                      -- 법인세과세표준 및 세액신고서
                      select da.da_end, count(1) as cnt
                      from {schema_name}.ftc_hometax_data as hometax_data
                               inner join {schema_name}.ftc_corclose as cor_close_data
                                          on cor_close_data.cd_com = hometax_data.cd_com and cor_close_data.gubun_com = hometax_data.gubun_com
                                              and cor_close_data.prd_accounts = hometax_data.prd_accounts and cor_close_data.key_close = hometax_data.key_close
                               left join tmp_da as da on cor_close_data.dm_fndend between substr(da.da_start, 1, 6) and substr(da.da_end, 1, 6)
                      where cor_close_data.gubun_com = 1
                        and cor_close_data.yn_close = 1
                        and (cor_close_data.execode IN('FCTP0101', 'FCFA0101', 'FCNE0101', 'FCTP0107')
                          or cor_close_data.execode IN('SCTP0101', 'SCFA0101', 'SCNE0101', 'SCTP0107'))
                      group by da.da_end

                      union all

                      -- 종합소득세신고서
                      select da.da_end, count(1) as cnt
                      from {schema_name}.ftr_hometax_data as hometax_data
                               inner join {schema_name}.ftr_recclose as rec_close_data
                                          on rec_close_data.cd_com = hometax_data.cd_com and rec_close_data.gubun_com = hometax_data.gubun_com
                                              and rec_close_data.prd_accounts = hometax_data.prd_accounts and rec_close_data.key_close = hometax_data.key_close
                                              and rec_close_data.yn_close = 1 and rec_close_data.gubun_com = 1
                               left join tmp_da as da on SUBSTR(hometax_data.dm_fndend,1,4) = substr(da.da_end, 1, 4)
                      where rec_close_data.execode not like 'SRDF%%' and rec_close_data.execode not like 'FRDF%%' --면세 제외
                        and rec_close_data.execode not like 'SRLP%%' and rec_close_data.execode not like 'FRLP%%' --지방세 제외
                        and (rec_close_data.execode IN('FRTP0102')
                          or rec_close_data.execode IN('SRTP0102'))
                      group by da.da_end
                  ) as aa
             group by substr(da_end, 1, 4)
             order by substr(da_end, 1, 4) desc
         ) as tt
    where is_data = 1;""".format(schema_name=schema_name)
        sao_db_connection.cursor.execute(query_string, query_params)
        result_data = name_to_json(sao_db_connection.cursor)
        return result_data[0]["da_end"] if len(result_data) > 0 else None

    def __save_nice_vat_data(self, sao_meta_connection, nice_vat_data, com_info):
        for data in nice_vat_data:
            period_start = data["period_start"]
            period_end = data["period_end"]

            vat_report = data["xml_data"]["vat_data"][0]
            vat_sinbo = data["xml_data"]["sinbo_data"][0]
            # vat_detail_list = data["xml_data"]["detail_data"]
            vat_detail_list_js = data["xml_data"]["detail_data_js"]
            vat_ers_string = data["ers_data"]

            temp_query_params = {
                "ccode": self._schema_name,
                "da_work": self._default_date_version,
                "period_start": period_start,
                "period_end": period_end,
                "v_biz_no": com_info[0]["no_biz"],
                "v_bub_no": com_info[0]["no_corpor"],
                "v_period_st": period_start,
                "v_period_ed": period_end,
                "v_acc_bizno": "",
                "v_acc_manno": "",
                "v_acc_name": "",
                "v_acc_addr": "",
                "v_acc_telno": "",
                "v_rtp_gb": "1"
            }

            query_string = "select * from nice_vat_report_v2_{0} where ccode = %(ccode)s and da_work = %(da_work)s and period_start = %(period_start)s and period_end = %(period_end)s;".format(temp_query_params['da_work'])
            sao_meta_connection.cursor.execute(query_string, temp_query_params)
            result_query = name_to_json(sao_meta_connection.cursor)
            temp_query_params.update(vat_report)

            if vat_report and len(result_query) == 0:
                query_string = """insert into nice_vat_report_v2_{0}(ccode,
 da_work,
 period_start,
 period_end,
 v_biz_no,
 v_bub_no,
 v_period_st,
 v_period_ed,
 v_acc_bizno,
 v_acc_manno,
 v_acc_name,
 v_acc_addr,
 v_acc_telno,
 v_rtp_gb,
 v_sel_tax_tot_amt,
 v_sel_tax_tot_tx,
 v_fixass_amt,
 v_fixass_tx,
 v_buy_tax_tot_amt,
 v_buy_tax_tot_tx,
 v_add_tot_tx,
 v_subadd_tx,
 v_tax_std_amt,
 v_free_tax_tot_amt,
 v_tot_tx)
values (
 %(ccode)s,
 %(da_work)s,
 %(period_start)s,
 %(period_end)s,
 %(v_biz_no)s,
 %(v_bub_no)s,
 %(v_period_st)s,
 %(v_period_ed)s,
 %(v_acc_bizno)s,
 %(v_acc_manno)s,
 %(v_acc_name)s,
 %(v_acc_addr)s,
 %(v_acc_telno)s,
 %(v_rtp_gb)s,
 %(v_sel_tax_tot_amt)s,
 %(v_sel_tax_tot_tx)s,
 %(v_fixass_amt)s,
 %(v_fixass_tx)s,
 %(v_buy_tax_tot_amt)s,
 %(v_buy_tax_tot_tx)s,
 %(v_add_tot_tx)s,
 %(v_subadd_tx)s,
 %(v_tax_std_amt)s,
 %(v_free_tax_tot_amt)s,
 %(v_tot_tx)s
        );""".format(temp_query_params['da_work'])
                sao_meta_connection.cursor.execute(query_string, temp_query_params)
                sao_meta_connection.commit()
            else:
                query_string = """update nice_vat_report_v2_{0} set
 v_biz_no = %(v_biz_no)s,
 v_bub_no = %(v_bub_no)s,
 v_period_st = %(v_period_st)s,
 v_period_ed = %(v_period_ed)s,
 v_acc_bizno = %(v_acc_bizno)s,
 v_acc_manno = %(v_acc_manno)s,
 v_acc_name = %(v_acc_name)s,
 v_acc_addr = %(v_acc_addr)s,
 v_acc_telno = %(v_acc_telno)s,
 v_rtp_gb = %(v_rtp_gb)s,
 v_sel_tax_tot_amt = %(v_sel_tax_tot_amt)s,
 v_sel_tax_tot_tx = %(v_sel_tax_tot_tx)s,
 v_fixass_amt = %(v_fixass_amt)s,
 v_fixass_tx = %(v_fixass_tx)s,
 v_buy_tax_tot_amt = %(v_buy_tax_tot_amt)s,
 v_buy_tax_tot_tx = %(v_buy_tax_tot_tx)s,
 v_add_tot_tx = %(v_add_tot_tx)s,
 v_subadd_tx = %(v_subadd_tx)s,
 v_tax_std_amt = %(v_tax_std_amt)s,
 v_free_tax_tot_amt = %(v_free_tax_tot_amt)s,
 v_tot_tx = %(v_tot_tx)s
where ccode = %(ccode)s and da_work = %(da_work)s and period_start = %(period_start)s and period_end = %(period_end)s;""".format(temp_query_params['da_work'])
                sao_meta_connection.cursor.execute(query_string, temp_query_params)
                sao_meta_connection.commit()

            query_string = "select * from nice_vat_reportsinbo_v2_{0} where ccode = %(ccode)s and da_work = %(da_work)s and period_start = %(period_start)s and period_end = %(period_end)s".format(temp_query_params['da_work'])
            sao_meta_connection.cursor.execute(query_string, temp_query_params)
            result_query = name_to_json(sao_meta_connection.cursor)

            # 간이과세자 추가?로 인해 기본값 셋팅
            temp_query_params["t36"] = None
            temp_query_params["t37"] = None
            temp_query_params["t38"] = None
            temp_query_params["t39"] = None
            temp_query_params["t40"] = None
            temp_query_params["t41"] = None
            temp_query_params["t42"] = None
            temp_query_params["t43"] = None
            temp_query_params["t44"] = None
            temp_query_params["t45"] = None
            temp_query_params["t46"] = None
            temp_query_params["t49"] = None
            temp_query_params["t70"] = None
            temp_query_params["t71"] = None
            temp_query_params["t72"] = None
            temp_query_params["t73"] = None
            temp_query_params["t74"] = None
            temp_query_params["t75"] = None
            temp_query_params["t76"] = None

            # vat_reportsinbo update
            temp_query_params.update(vat_sinbo)

            temp_query_params["t00"] = com_info[0]["ty_addtax"]
            temp_query_params["t01"] = com_info[0]["no_corpor"]
            temp_query_params["t02"] = com_info[0]["no_biz"]
            temp_query_params["t03"] = None
            temp_query_params["t04"] = None
            temp_query_params["t05"] = period_start
            temp_query_params["t06"] = period_end
            temp_query_params["t07"] = None
            temp_query_params["t49"] = None
            temp_query_params["t50"] = None
            temp_query_params["t51"] = None

            if vat_sinbo and len(result_query) == 0:
                query_string = """insert into nice_vat_reportsinbo_v2_{0}(ccode,
 da_work,
 period_start,
 period_end,
 t00,
 t01,
 t02,
 t03,
 t04,
 t06,
 t07,
 t08,
 t09,
 t10,
 t11,
 t12,
 t13,
 t14,
 t15,
 t16,
 t17,
 t18,
 t19,
 t20,
 t21,
 t22,
 t23,
 t24,
 t25,
 t26,
 t27,
 t28,
 t29,
 t30,
 t31,
 t32,
 t33,
 t34,
 t35,
 t36,
 t37,
 t38,
 t39,
 t40,
 t41,
 t42,
 t43,
 t44,
 t45,
 t46,
 t49,
 t50,
 t51,
 t70,
 t71,
 t72,
 t73,
 t74,
 t75,
 t76) 
values (
 %(ccode)s,
 %(da_work)s,
 %(period_start)s,
 %(period_end)s,
 %(t00)s,
 %(t01)s,
 %(t02)s,
 %(t03)s,
 %(t04)s,
 %(t06)s,
 %(t07)s,
 %(t08)s,
 %(t09)s,
 %(t10)s,
 %(t11)s,
 %(t12)s,
 %(t13)s,
 %(t14)s,
 %(t15)s,
 %(t16)s,
 %(t17)s,
 %(t18)s,
 %(t19)s,
 %(t20)s,
 %(t21)s,
 %(t22)s,
 %(t23)s,
 %(t24)s,
 %(t25)s,
 %(t26)s,
 %(t27)s,
 %(t28)s,
 %(t29)s,
 %(t30)s,
 %(t31)s,
 %(t32)s,
 %(t33)s,
 %(t34)s,
 %(t35)s,
 %(t36)s,
 %(t37)s,
 %(t38)s,
 %(t39)s,
 %(t40)s,
 %(t41)s,
 %(t42)s,
 %(t43)s,
 %(t44)s,
 %(t45)s,
 %(t46)s,
 %(t49)s,
 %(t50)s,
 %(t51)s,
 %(t70)s,
 %(t71)s,
 %(t72)s,
 %(t73)s,
 %(t74)s,
 %(t75)s,
 %(t76)s
)""".format(temp_query_params['da_work'])
                sao_meta_connection.cursor.execute(query_string, temp_query_params)
                sao_meta_connection.commit()
            else:
                query_string = """update nice_vat_reportsinbo_v2_{0} set
 t00 = %(t00)s,
 t01 = %(t01)s,
 t02 = %(t02)s,
 t03 = %(t03)s,
 t04 = %(t04)s,
 t06 = %(t06)s,
 t07 = %(t07)s,
 t08 = %(t08)s,
 t09 = %(t09)s,
 t10 = %(t10)s,
 t11 = %(t11)s,
 t12 = %(t12)s,
 t13 = %(t13)s,
 t14 = %(t14)s,
 t15 = %(t15)s,
 t16 = %(t16)s,
 t17 = %(t17)s,
 t18 = %(t18)s,
 t19 = %(t19)s,
 t20 = %(t20)s,
 t21 = %(t21)s,
 t22 = %(t22)s,
 t23 = %(t23)s,
 t24 = %(t24)s,
 t25 = %(t25)s,
 t26 = %(t26)s,
 t27 = %(t27)s,
 t28 = %(t28)s,
 t29 = %(t29)s,
 t30 = %(t30)s,
 t31 = %(t31)s,
 t32 = %(t32)s,
 t33 = %(t33)s,
 t34 = %(t34)s,
 t35 = %(t35)s,
 t36 = %(t36)s,
 t37 = %(t37)s,
 t38 = %(t38)s,
 t39 = %(t39)s,
 t40 = %(t40)s,
 t41 = %(t41)s,
 t42 = %(t42)s,
 t43 = %(t43)s,
 t44 = %(t44)s,
 t45 = %(t45)s,
 t46 = %(t46)s,
 t49 = %(t49)s,
 t50 = %(t50)s,
 t51 = %(t51)s,
 t70 = %(t70)s,
 t71 = %(t71)s,
 t72 = %(t72)s,
 t73 = %(t73)s,
 t74 = %(t74)s,
 t75 = %(t75)s,
 t76 = %(t76)s
where ccode = %(ccode)s and da_work = %(da_work)s and period_start = %(period_start)s and period_end = %(period_end)s;""".format(temp_query_params['da_work'])
                sao_meta_connection.cursor.execute(query_string, temp_query_params)
                sao_meta_connection.commit()

            # query_string = """delete from nice_vat_summarydetail_v2 where ccode = %(ccode)s and da_work = %(da_work)s and period_start = %(period_start)s and period_end = %(period_end)s;"""
            query_string_js = """delete from nice_vat_summarydetail_js_{0} where ccode = %(ccode)s and da_work = %(da_work)s and period_start = %(period_start)s and period_end = %(period_end)s;""".format(temp_query_params['da_work'])
            # sao_meta_connection.cursor.execute(query_string, temp_query_params)
            sao_meta_connection.cursor.execute(query_string_js, temp_query_params)
            sao_meta_connection.commit()
            time.sleep(0.1)

            #             for vat_detail_data in vat_detail_list:
            #                 temp_query_params.update(vat_detail_data)
            #                 query_string = """insert into nice_vat_summarydetail_v2(ccode,
            # da_work,
            # period_start,
            # period_end,
            # v_sb_gb,
            # v_seq_no,
            # v_bizres_gb,
            # v_biz_res_no,
            # v_comp_nm,
            # v_issue_qty,
            # v_amt,
            # v_tax)
            # values(
            # %(ccode)s,
            # %(da_work)s,
            # %(period_start)s,
            # %(period_end)s,
            # %(v_sb_gb)s,
            # %(v_seq_no)s,
            # %(v_bizres_gb)s,
            # %(v_biz_res_no)s,
            # %(v_comp_nm)s,
            # %(v_issue_qty)s,
            # %(v_amt)s,
            # %(v_tax)s
            # )"""
            #                 sao_meta_connection.cursor.execute(query_string, temp_query_params)

            for vat_detail_data in vat_detail_list_js:
                js_detail_data = json.dumps(vat_detail_data["js_detail_data"], cls=DecimalEncoder) if vat_detail_data["js_detail_data"] is not None else ''
                temp_query_params['js_detail_data'] = js_detail_data
                query_string = """
                    insert into nice_vat_summarydetail_js_{0}(ccode, da_work, period_start, period_end, js_detail_data)
                    values (%(ccode)s, %(da_work)s, %(period_start)s, %(period_end)s, %(js_detail_data)s);
                """.format(temp_query_params['da_work'])
                sao_meta_connection.cursor.execute(query_string, temp_query_params)

                # if vat_detail_list or vat_detail_list_js:
                # if vat_detail_list_js:
                sao_meta_connection.commit()

            # query_string = "select * from nice_vat_ers_idx where ccode = %(ccode)s and da_work = %(da_work)s and period_start = %(period_start)s and period_end = %(period_end)s"
            # sao_meta_connection.cursor.execute(query_string, temp_query_params)
            # result_query = name_to_json(sao_meta_connection.cursor)
            # 마감 스트링 길이가 길어 일정 구분자 기준 30000개 단위로 저장
            max_split = 30000
            split_count = vat_ers_string.count('♥')

            # 건수 넘어가는거만 분할저장 진행
            rs_ers_data_list = []
            if split_count > max_split:
                ers_data_list = vat_ers_string.split('♥')[:-1]

                ers_data_list_len = len(ers_data_list)
                max_loop_cnt_r = ers_data_list_len % max_split
                max_loop_cnt_d = ers_data_list_len // max_split

                max_loop_cnt = max_loop_cnt_d if max_loop_cnt_r == 0 else max_loop_cnt_d + 1

                for idx in range(max_loop_cnt):
                    if idx + 1 == max_loop_cnt:
                        rs_ers_data_list.append('♥'.join(ers_data_list[max_split * idx:]) + '♥')
                    else:
                        rs_ers_data_list.append('♥'.join(ers_data_list[max_split * idx:max_split * (idx + 1)]) + '♥')
            else:
                rs_ers_data_list.append(vat_ers_string)

            # 삭제 후 진행 (index가 달라 질 수 있음)
            query_string = """delete from nice_vat_ers_idx_{0} where  ccode = %(ccode)s and da_work = %(da_work)s and period_start = %(period_start)s and period_end = %(period_end)s""".format(temp_query_params['da_work'])
            sao_meta_connection.cursor.execute(query_string, temp_query_params)
            sao_meta_connection.commit()

            ers_index = 0
            for ers_data in rs_ers_data_list:
                ers_index += 1
                temp_query_params["ers_index"] = ers_index
                temp_query_params["ers_string"] = ers_data

                query_string = """insert into nice_vat_ers_idx_{0}(ccode,
 da_work,
 period_start,
 period_end,
 ers_string,
 ers_index)
values(
 %(ccode)s,
 %(da_work)s,
 %(period_start)s,
 %(period_end)s,
 %(ers_string)s,
 %(ers_index)s
 )""".format(temp_query_params['da_work'])
                sao_meta_connection.cursor.execute(query_string, temp_query_params)
                sao_meta_connection.commit()

            # 파일생성용 데이터
            report_file_data = self.__save_vat_report_file_data(company_info=com_info[0], vat_report_data=temp_query_params)
            # 파일생성용 데이터
            reportsinbo_file_data = self.__save_vat_reportsinbo_file_data(company_info=com_info[0], vat_reportsinbo_data=temp_query_params)
            # 파일생성용 데이터
            summarydetail_file_data = self.__save_vat_summarydetail_file_data(company_info=com_info[0], vat_summarydetail_data=temp_query_params)

    # 재무제표 적재 로직
    def __save_nice_finance_data(self, sao_meta_connection, nice_finance_data, com_info):
        for data in nice_finance_data:

            # finance_data_list = data["tax_data"]
            finance_data_json_list = data["tax_data_json"]
            finance_ers_data_list = data["tax_ers_data"]

            temp_query_params = {
                "ccode": self._schema_name,
                "da_work": self._default_date_version,
                "gisu": data["gisu"]
            }

            # 법인세 데이터 적재
            # 삭제 후 진행
            # del_query_string = "delete from nice_financesend where ccode = %(ccode)s and da_work = %(da_work)s and gisu = %(gisu)s;"
            del_query_string_js = "delete from nice_financesend_js_{0} where ccode = %(ccode)s and da_work = %(da_work)s and gisu = %(gisu)s;".format(temp_query_params['da_work'])
            # sao_meta_connection.cursor.execute(del_query_string, temp_query_params)
            sao_meta_connection.cursor.execute(del_query_string_js, temp_query_params)
            sao_meta_connection.commit()
            time.sleep(0.1)

            # for finance_data in finance_data_list:
            #     finance_params = finance_data
            #     finance_params.update(temp_query_params)
            #     query_string = """
            #         insert into nice_financesend(ccode, da_work, gisu, dt_from, dt_to, page, grp, code,
            #                                      mn_1, mn_2, mn_3, mn_4, mn_5, mn_6,
            #                                      str_2, str_3, str_4, str_5, str_6, str_7, str_8, str_9,
            #                                      str_10, str_11, str_12, str_13, str_14, str_15, str_16,
            #                                      str_17, id_insert, dt_insert, id_modify, dt_modify)
            #         values (%(ccode)s, %(da_work)s, %(gisu)s, %(dt_from)s, %(dt_to)s, %(page)s, %(grp)s, %(code)s,
            #                 %(mn_1)s, %(mn_2)s, %(mn_3)s, %(mn_4)s, %(mn_5)s, %(mn_6)s,
            #                 %(str_2)s, %(str_3)s, %(str_4)s, %(str_5)s, %(str_6)s, %(str_7)s, %(str_8)s, %(str_9)s,
            #                 %(str_10)s, %(str_11)s, %(str_12)s, %(str_13)s, %(str_14)s, %(str_15)s, %(str_16)s,
            #                 %(str_17)s, %(id_insert)s, %(dt_insert)s, %(id_modify)s, %(dt_modify)s);
            #     """
            #     sao_meta_connection.cursor.execute(query_string, finance_params)

            # print("finance_data_json_list")
            # print(finance_data_json_list)
            for finance_data_json in finance_data_json_list:
                finance_js_params = finance_data_json
                finance_js_params.update(temp_query_params)

                finance_js_params["js_detail_data"] = json.dumps(finance_js_params["js_detail_data"], cls=DecimalEncoder) if finance_js_params["js_detail_data"] is not None else ''
                query_string = """
                    insert into nice_financesend_js_{0}(ccode, da_work, gisu, dt_from, dt_to, str_4, str_5,
                                                    js_detail_data, str_16, id_insert, dt_insert, id_modify, dt_modify)
                    values (%(ccode)s, %(da_work)s, %(gisu)s, %(dt_from)s, %(dt_to)s, %(str_4)s, %(str_5)s,
                            %(js_detail_data)s, %(str_16)s, %(id_insert)s, %(dt_insert)s, %(id_modify)s, %(dt_modify)s);
                """.format(temp_query_params['da_work'])
                sao_meta_connection.cursor.execute(query_string, finance_js_params)
                # if finance_data_list or finance_data_json_list:
                # if finance_data_json_list:
                sao_meta_connection.commit()

            # ers data 적재
            for finance_ers_data in finance_ers_data_list:
                ers_data = finance_ers_data['hometax_data']
                # 마감 스트링 길이가 길어 일정 구분자 기준 30000개 단위로 저장
                max_split = 30000
                split_count = ers_data.count('♥')

                # 건수 넘어가는거만 분할저장 진행
                rs_ers_data_list = []
                if split_count > max_split:
                    ers_data_list = ers_data.split('♥')[:-1]

                    ers_data_list_len = len(ers_data_list)
                    max_loop_cnt_r = ers_data_list_len % max_split
                    max_loop_cnt_d = ers_data_list_len // max_split

                    max_loop_cnt = max_loop_cnt_d if max_loop_cnt_r == 0 else max_loop_cnt_d + 1

                    for idx in range(max_loop_cnt):
                        if idx + 1 == max_loop_cnt:
                            rs_ers_data_list.append('♥'.join(ers_data_list[max_split * idx:]) + '♥')
                        else:
                            rs_ers_data_list.append('♥'.join(ers_data_list[max_split * idx:max_split * (idx + 1)]) + '♥')
                else:
                    rs_ers_data_list.append(ers_data)

                ers_index = 0
                temp_query_params["dt_from_ers"] = finance_ers_data["dm_fndbegin"]
                temp_query_params["dt_to_ers"] = finance_ers_data["dm_fndend"]
                temp_query_params["gisu"] = finance_ers_data["gisu"]

                # 삭제 후 진행
                del_query_string = "delete from nice_tax_ers_idx_{0} where ccode = %(ccode)s and da_work = %(da_work)s and gisu = %(gisu)s and dt_from = %(dt_from_ers)s and dt_to = %(dt_to_ers)s;".format(temp_query_params['da_work'])
                sao_meta_connection.cursor.execute(del_query_string, temp_query_params)
                sao_meta_connection.commit()
                time.sleep(0.1)

                for ers_data in rs_ers_data_list:
                    ers_index += 1
                    temp_query_params["ers_index"] = ers_index
                    temp_query_params["ers_string"] = ers_data

                    query_string = """
                        insert into nice_tax_ers_idx_{0}(ccode, da_work, gisu, dt_from, dt_to, ers_index, ers_string)
                        values(%(ccode)s, %(da_work)s, %(gisu)s, %(dt_from_ers)s, %(dt_to_ers)s, %(ers_index)s, %(ers_string)s);
                    """.format(temp_query_params['da_work'])
                    sao_meta_connection.cursor.execute(query_string, temp_query_params)
                    # if finance_ers_data_list:
                    sao_meta_connection.commit()

    def __save_nice_fta_reporter(self, sao_meta_connection, result_reporter_data):
        for reporter_data in result_reporter_data:
            query_string = """INSERT INTO sao_fta_reporter_v2_{0}
        SET ccode = %(ccode)s,
            da_work = %(da_work)s,
            key_reporter = %(key_reporter)s,
            ty_report = %(ty_report)s,
            nm_krname = %(nm_krname)s,
            nm_userid = %(nm_userid)s,
            nm_trade = %(nm_trade)s,
            no_manage1 = %(no_manage1)s,
            no_manage2 = %(no_manage2)s,
            no_manage3 = %(no_manage3)s,
            ty_mediation = %(ty_mediation)s,
            no_mediation = %(no_mediation)s,
            no_biz = %(no_biz)s,
            no_social = %(no_social)s,
            zip_com = %(zip_com)s,
            add_saaddr1 = %(add_saaddr1)s,
            add_saaddr2 = %(add_saaddr2)s,
            tel_com1 = %(tel_com1)s,
            tel_com2 = %(tel_com2)s,
            tel_com3 = %(tel_com3)s,
            key_taxoffice = %(key_taxoffice)s,
            cd_taxoffcom = %(cd_taxoffcom)s,
            ty_intro = %(ty_intro)s,
            da_report = %(da_report)s,
            key_laware = %(key_laware)s,
            cd_lawcom = %(cd_lawcom)s,
            ty_disk = %(ty_disk)s,
            qt_disk = %(qt_disk)s,
            da_accbegin = %(da_accbegin)s,
            da_accend = %(da_accend)s,
            nm_linecolor = %(nm_linecolor)s,
            em_taxoffice = %(em_taxoffice)s,
            nm_chargedept = %(nm_chargedept)s,
            nm_charger = %(nm_charger)s,
            tel1_charger = %(tel1_charger)s,
            tel2_charger = %(tel2_charger)s,
            tel3_charger = %(tel3_charger)s,
            ty_united = %(ty_united)s,
            file_pw = %(file_pw)s,
            cd_lawcom2 = %(cd_lawcom2)s,
            str_3_99_99 = %(str_3_99_99)s
        ON DUPLICATE KEY UPDATE
            ty_report = %(ty_report)s,
            nm_krname = %(nm_krname)s,
            nm_userid = %(nm_userid)s,
            nm_trade = %(nm_trade)s,
            no_manage1 = %(no_manage1)s,
            no_manage2 = %(no_manage2)s,
            no_manage3 = %(no_manage3)s,
            ty_mediation = %(ty_mediation)s,
            no_mediation = %(no_mediation)s,
            no_biz = %(no_biz)s,
            no_social = %(no_social)s,
            zip_com = %(zip_com)s,
            add_saaddr1 = %(add_saaddr1)s,
            add_saaddr2 = %(add_saaddr2)s,
            tel_com1 = %(tel_com1)s,
            tel_com2 = %(tel_com2)s,
            tel_com3 = %(tel_com3)s,
            key_taxoffice = %(key_taxoffice)s,
            cd_taxoffcom = %(cd_taxoffcom)s,
            ty_intro = %(ty_intro)s,
            da_report = %(da_report)s,
            key_laware = %(key_laware)s,
            cd_lawcom = %(cd_lawcom)s,
            ty_disk = %(ty_disk)s,
            qt_disk = %(qt_disk)s,
            da_accbegin = %(da_accbegin)s,
            da_accend = %(da_accend)s,
            nm_linecolor = %(nm_linecolor)s,
            em_taxoffice = %(em_taxoffice)s,
            nm_chargedept = %(nm_chargedept)s,
            nm_charger = %(nm_charger)s,
            tel1_charger = %(tel1_charger)s,
            tel2_charger = %(tel2_charger)s,
            tel3_charger = %(tel3_charger)s,
            ty_united = %(ty_united)s,
            file_pw = %(file_pw)s,
            cd_lawcom2 = %(cd_lawcom2)s,
            str_3_99_99 = %(str_3_99_99)s""".format(reporter_data['da_work'])
            sao_meta_connection.cursor.execute(query_string, reporter_data)
            sao_meta_connection.commit()

    def __insert_ccode_map(self, sao_meta_connection):
        seq_vm = {
            "172.16.115.103": 886,
            "172.16.115.104": 887,
            "172.16.115.105": 885,
            "172.16.115.106": 888,
            "172.16.115.107": 881,
            "172.16.115.108": 882,
            "172.16.115.109": 883,
            "172.16.115.110": 889,
            "172.16.115.111": 884,
            "172.16.115.112": 890,
            "172.16.114.69": 891,
        }

        temp_query_params = {
            "cno": self._cno,
            "ccode": self._schema_name,
            "seq_vm": seq_vm[self._host]
        }

        query_string = """
        INSERT INTO DATAOFFER.sao_cno_ccode_map (cno, ccode, seq_vm) VALUES (%(cno)s, %(ccode)s, %(seq_vm)s)
        ON DUPLICATE KEY UPDATE ccode = %(ccode)s, seq_vm = %(seq_vm)s;
        """
        sao_meta_connection.cursor.execute(query_string, temp_query_params)
        sao_meta_connection.commit()

    def __save_vat_report_file_data(self, company_info, vat_report_data):
        subitem = {
            "Code": "VatReport",
            "KorName": "부가세신고서",
            "BizNo": str(company_info["com_no_biz"]),
            "BubNo": str(company_info["com_no_corpor"]),
            "Period": "%s%s" % (vat_report_data["period_start"], vat_report_data["period_end"]),
            "items": []
        }

        item = {}
        item["Code"] = "Report"
        item["V_BIZ_NO"] = str(vat_report_data["v_biz_no"]) if vat_report_data["v_biz_no"] is not None and str(vat_report_data["v_biz_no"]) != "None" else ''
        item["V_BUB_NO"] = str(vat_report_data["v_bub_no"]) if vat_report_data["v_bub_no"] is not None and str(vat_report_data["v_bub_no"]) != "None" else ''
        item["V_PERIOD_ST"] = str(vat_report_data["v_period_st"]) if vat_report_data["v_period_st"] is not None and str(vat_report_data["v_period_st"]) != "None" else ''
        item["V_PERIOD_ED"] = str(vat_report_data["v_period_ed"]) if vat_report_data["v_period_ed"] is not None and str(vat_report_data["v_period_ed"]) != "None" else ''
        # item["V_ACC_BIZNO"] = str(reporter_info["accbizno"]) if reporter_info["accbizno"] is not None and str(reporter_info["accbizno"]) != "None" else ''
        # item["V_ACC_MANNO"] = str(reporter_info["accmngno"]) if reporter_info["accmngno"] is not None and str(reporter_info["accmngno"]) != "None" else ''
        # item["V_ACC_NAME"] = str(reporter_info["accnm"]) if reporter_info["accnm"] is not None and str(reporter_info["accnm"]) != "None" else ''
        # item["V_ACC_ADDR"] = str(reporter_info["accaddress"]) if reporter_info["accaddress"] is not None and str(reporter_info["accaddress"]) != "None" else ''
        # item["V_ACC_TELNO"] = str(reporter_info["acctelno"]) if reporter_info["acctelno"] is not None and str(reporter_info["acctelno"]) != "None" else ''
        item["V_RTP_GB"] = str(vat_report_data["v_rtp_gb"]) if vat_report_data["v_rtp_gb"] is not None and str(vat_report_data["v_rtp_gb"]) != "None" else ''
        item["V_SEL_TAX_TOT_AMT"] = str(vat_report_data["v_sel_tax_tot_amt"]) if vat_report_data["v_sel_tax_tot_amt"] is not None and str(vat_report_data["v_sel_tax_tot_amt"]) != "None" else ''
        item["V_SEL_TAX_TOT_TX"] = str(vat_report_data["v_sel_tax_tot_tx"]) if vat_report_data["v_sel_tax_tot_tx"] is not None and str(vat_report_data["v_sel_tax_tot_tx"]) != "None" else ''
        item["V_FIXASS_AMT"] = str(vat_report_data["v_fixass_amt"]) if vat_report_data["v_fixass_amt"] is not None and str(vat_report_data["v_fixass_amt"]) != "None" else ''
        item["V_FIXASS_TX"] = str(vat_report_data["v_fixass_tx"]) if vat_report_data["v_fixass_tx"] is not None and str(vat_report_data["v_fixass_tx"]) != "None" else ''
        item["V_BUY_TAX_TOT_AMT"] = str(vat_report_data["v_buy_tax_tot_amt"]) if vat_report_data["v_buy_tax_tot_amt"] is not None and str(vat_report_data["v_buy_tax_tot_amt"]) != "None" else ''
        item["V_BUY_TAX_TOT_TX"] = str(vat_report_data["v_buy_tax_tot_tx"]) if vat_report_data["v_buy_tax_tot_tx"] is not None and str(vat_report_data["v_buy_tax_tot_tx"]) != "None" else ''
        item["V_ADD_TOT_TX"] = str(vat_report_data["v_add_tot_tx"]) if vat_report_data["v_add_tot_tx"] is not None and str(vat_report_data["v_add_tot_tx"]) != "None" else ''
        item["V_SUBADD_TX"] = str(vat_report_data["v_subadd_tx"]) if vat_report_data["v_subadd_tx"] is not None and str(vat_report_data["v_subadd_tx"]) != "None" else ''
        item["V_TAX_STD_AMT"] = str(vat_report_data["v_tax_std_amt"]) if vat_report_data["v_tax_std_amt"] is not None and str(vat_report_data["v_tax_std_amt"]) != "None" else ''
        item["V_FREE_TAX_TOT_AMT"] = str(vat_report_data["v_free_tax_tot_amt"]) if vat_report_data["v_free_tax_tot_amt"] is not None and str(vat_report_data["v_free_tax_tot_amt"]) != "None" else ''
        item["V_TOT_TX"] = str(vat_report_data["v_tot_tx"]) if vat_report_data["v_tot_tx"] is not None and str(vat_report_data["v_tot_tx"]) != "None" else ''

        subitem["items"].append(item.copy())

        return subitem

    def __save_vat_reportsinbo_file_data(self, company_info, vat_reportsinbo_data):
        period_start = vat_reportsinbo_data["period_start"]
        period_end = vat_reportsinbo_data["period_end"]

        file_period_start = "%s%s" % (period_start, '01')
        file_period_end = "%s%s" % (period_end, str(monthrange(int(period_end[:4]), int(period_end[-2:]))[1]))

        subitem = {
            "Code": "VatReportSinbo",
            "KorName": "부가세신고서(신보용)",
            "BizNo": str(company_info["com_no_biz"]),
            "BubNo": str(company_info["com_no_corpor"]),
            "Period": "%s%s" % (period_start, period_end),
            "items": []
        }

        item = {}
        item["Code"] = "Report"
        temp_t01 = ''
        if company_info["ty_addtax"] is not None:
            if company_info["ty_addtax"] == 0:
                temp_t01 = '1'
            elif company_info["ty_addtax"] == 1:
                temp_t01 = '2'
        item["T00"] = temp_t01
        item["T01"] = str(company_info["com_no_corpor"]) if company_info["com_no_corpor"] is not None and str(company_info["com_no_corpor"]) != "None" else ''
        item["T02"] = str(company_info["com_no_biz"]) if company_info["com_no_biz"] is not None and str(company_info["com_no_biz"]) != "None" else ''
        # item["T03"] = str(reporter_info["accbizno"]) if reporter_info["accbizno"] is not None and str(reporter_info["accbizno"]) != "None" else ''
        # item["T04"] = str(reporter_info["accmngno"]) if reporter_info["accmngno"] is not None and str(reporter_info["accmngno"]) != "None" else ''
        item["T06"] = file_period_start
        item["T07"] = file_period_end
        item["T08"] = str(vat_reportsinbo_data["t08"]) if vat_reportsinbo_data["t08"] is not None and str(vat_reportsinbo_data["t08"]) != "None" else ''
        item["T09"] = str(vat_reportsinbo_data["t09"]) if vat_reportsinbo_data["t09"] is not None and str(vat_reportsinbo_data["t09"]) != "None" else ''
        item["T10"] = str(vat_reportsinbo_data["t10"]) if vat_reportsinbo_data["t10"] is not None and str(vat_reportsinbo_data["t10"]) != "None" else ''
        item["T11"] = str(vat_reportsinbo_data["t11"]) if vat_reportsinbo_data["t11"] is not None and str(vat_reportsinbo_data["t11"]) != "None" else ''
        item["T12"] = str(vat_reportsinbo_data["t12"]) if vat_reportsinbo_data["t12"] is not None and str(vat_reportsinbo_data["t12"]) != "None" else ''
        item["T13"] = str(vat_reportsinbo_data["t13"]) if vat_reportsinbo_data["t13"] is not None and str(vat_reportsinbo_data["t13"]) != "None" else ''
        item["T14"] = str(vat_reportsinbo_data["t14"]) if vat_reportsinbo_data["t14"] is not None and str(vat_reportsinbo_data["t14"]) != "None" else ''
        item["T15"] = str(vat_reportsinbo_data["t15"]) if vat_reportsinbo_data["t15"] is not None and str(vat_reportsinbo_data["t15"]) != "None" else ''
        item["T16"] = str(vat_reportsinbo_data["t16"]) if vat_reportsinbo_data["t16"] is not None and str(vat_reportsinbo_data["t16"]) != "None" else ''
        item["T17"] = str(vat_reportsinbo_data["t17"]) if vat_reportsinbo_data["t17"] is not None and str(vat_reportsinbo_data["t17"]) != "None" else ''
        item["T18"] = str(vat_reportsinbo_data["t18"]) if vat_reportsinbo_data["t18"] is not None and str(vat_reportsinbo_data["t18"]) != "None" else ''
        item["T19"] = str(vat_reportsinbo_data["t19"]) if vat_reportsinbo_data["t19"] is not None and str(vat_reportsinbo_data["t19"]) != "None" else ''
        item["T20"] = str(vat_reportsinbo_data["t20"]) if vat_reportsinbo_data["t20"] is not None and str(vat_reportsinbo_data["t20"]) != "None" else ''
        item["T21"] = str(vat_reportsinbo_data["t21"]) if vat_reportsinbo_data["t21"] is not None and str(vat_reportsinbo_data["t21"]) != "None" else ''
        item["T22"] = str(vat_reportsinbo_data["t22"]) if vat_reportsinbo_data["t22"] is not None and str(vat_reportsinbo_data["t22"]) != "None" else ''
        item["T23"] = str(vat_reportsinbo_data["t23"]) if vat_reportsinbo_data["t23"] is not None and str(vat_reportsinbo_data["t23"]) != "None" else ''
        item["T24"] = str(vat_reportsinbo_data["t24"]) if vat_reportsinbo_data["t24"] is not None and str(vat_reportsinbo_data["t24"]) != "None" else ''
        item["T25"] = str(vat_reportsinbo_data["t25"]) if vat_reportsinbo_data["t25"] is not None and str(vat_reportsinbo_data["t25"]) != "None" else ''
        item["T26"] = str(vat_reportsinbo_data["t26"]) if vat_reportsinbo_data["t26"] is not None and str(vat_reportsinbo_data["t26"]) != "None" else ''
        item["T27"] = str(vat_reportsinbo_data["t27"]) if vat_reportsinbo_data["t27"] is not None and str(vat_reportsinbo_data["t27"]) != "None" else ''
        item["T28"] = str(vat_reportsinbo_data["t28"]) if vat_reportsinbo_data["t28"] is not None and str(vat_reportsinbo_data["t28"]) != "None" else ''
        item["T29"] = str(vat_reportsinbo_data["t29"]) if vat_reportsinbo_data["t29"] is not None and str(vat_reportsinbo_data["t29"]) != "None" else ''
        item["T30"] = str(vat_reportsinbo_data["t30"]) if vat_reportsinbo_data["t30"] is not None and str(vat_reportsinbo_data["t30"]) != "None" else ''
        item["T31"] = str(vat_reportsinbo_data["t31"]) if vat_reportsinbo_data["t31"] is not None and str(vat_reportsinbo_data["t31"]) != "None" else ''
        item["T32"] = str(vat_reportsinbo_data["t32"]) if vat_reportsinbo_data["t32"] is not None and str(vat_reportsinbo_data["t32"]) != "None" else ''
        item["T33"] = str(vat_reportsinbo_data["t33"]) if vat_reportsinbo_data["t33"] is not None and str(vat_reportsinbo_data["t33"]) != "None" else ''
        item["T34"] = str(vat_reportsinbo_data["t34"]) if vat_reportsinbo_data["t34"] is not None and str(vat_reportsinbo_data["t34"]) != "None" else ''
        item["T35"] = str(vat_reportsinbo_data["t35"]) if vat_reportsinbo_data["t35"] is not None and str(vat_reportsinbo_data["t35"]) != "None" else ''
        item["T36"] = str(vat_reportsinbo_data["t36"]) if vat_reportsinbo_data["t36"] is not None and str(vat_reportsinbo_data["t36"]) != "None" else ''
        item["T37"] = str(vat_reportsinbo_data["t37"]) if vat_reportsinbo_data["t37"] is not None and str(vat_reportsinbo_data["t37"]) != "None" else ''
        item["T38"] = str(vat_reportsinbo_data["t38"]) if vat_reportsinbo_data["t38"] is not None and str(vat_reportsinbo_data["t38"]) != "None" else ''
        item["T39"] = str(vat_reportsinbo_data["t39"]) if vat_reportsinbo_data["t39"] is not None and str(vat_reportsinbo_data["t39"]) != "None" else ''
        item["T40"] = str(vat_reportsinbo_data["t40"]) if vat_reportsinbo_data["t40"] is not None and str(vat_reportsinbo_data["t40"]) != "None" else ''
        item["T41"] = str(vat_reportsinbo_data["t41"]) if vat_reportsinbo_data["t41"] is not None and str(vat_reportsinbo_data["t41"]) != "None" else ''
        item["T42"] = str(vat_reportsinbo_data["t42"]) if vat_reportsinbo_data["t42"] is not None and str(vat_reportsinbo_data["t42"]) != "None" else ''
        item["T43"] = str(vat_reportsinbo_data["t43"]) if vat_reportsinbo_data["t43"] is not None and str(vat_reportsinbo_data["t43"]) != "None" else ''
        item["T44"] = str(vat_reportsinbo_data["t44"]) if vat_reportsinbo_data["t44"] is not None and str(vat_reportsinbo_data["t44"]) != "None" else ''
        item["T45"] = str(vat_reportsinbo_data["t45"]) if vat_reportsinbo_data["t45"] is not None and str(vat_reportsinbo_data["t45"]) != "None" else ''
        item["T46"] = str(vat_reportsinbo_data["t46"]) if vat_reportsinbo_data["t46"] is not None and str(vat_reportsinbo_data["t46"]) != "None" else ''
        item["T70"] = str(vat_reportsinbo_data["t70"]) if vat_reportsinbo_data["t70"] is not None and str(vat_reportsinbo_data["t70"]) != "None" else ''
        item["T71"] = str(vat_reportsinbo_data["t71"]) if vat_reportsinbo_data["t71"] is not None and str(vat_reportsinbo_data["t71"]) != "None" else ''
        item["T72"] = str(vat_reportsinbo_data["t72"]) if vat_reportsinbo_data["t72"] is not None and str(vat_reportsinbo_data["t72"]) != "None" else ''
        item["T73"] = str(vat_reportsinbo_data["t73"]) if vat_reportsinbo_data["t73"] is not None and str(vat_reportsinbo_data["t73"]) != "None" else ''
        item["T74"] = str(vat_reportsinbo_data["t74"]) if vat_reportsinbo_data["t74"] is not None and str(vat_reportsinbo_data["t74"]) != "None" else ''
        item["T75"] = str(vat_reportsinbo_data["t75"]) if vat_reportsinbo_data["t75"] is not None and str(vat_reportsinbo_data["t75"]) != "None" else ''
        item["T76"] = str(vat_reportsinbo_data["t76"]) if vat_reportsinbo_data["t76"] is not None and str(vat_reportsinbo_data["t76"]) != "None" else ''
        # item["T49"] = str(reporter_info["accnm"]) if reporter_info["accnm"] is not None and str(reporter_info["accnm"]) != "None" else ''
        # item["T50"] = str(reporter_info["acctelno"]) if reporter_info["acctelno"] is not None and str(reporter_info["acctelno"]) != "None" else ''
        # item["T51"] = str(reporter_info["accaddress"]) if reporter_info["accaddress"] is not None and str(reporter_info["accaddress"]) != "None" else ''

        subitem["items"].append(item.copy())

        return subitem

    def __save_vat_summarydetail_file_data(self, company_info, vat_summarydetail_data):
        vat_summarydetail_dict = {}

        period_start = vat_summarydetail_data["period_start"]
        period_end = vat_summarydetail_data["period_end"]
        # div_date = vat_summarydetail_data["div_date"]

        vat_summarydetail_list = []
        try:
            json_data = json.loads(str(vat_summarydetail_data['js_detail_data']).replace('\'', '"'))
        except:
            json_data = json.loads(vat_summarydetail_data['js_detail_data'])
        vat_summarydetail_list.extend(json_data)

        # 코드순으로 재정렬
        result_vat_summarydetail = sorted(vat_summarydetail_list, key=lambda k: (int(k['v_sb_gb']), int(k['v_seq_no'])))
        subitem = {
            "Code": "VatSummaryDetail",
            "KorName": "부가세 거래처별 합계표",
            "BizNo": str(company_info["com_no_biz"]),
            "BubNo": str(company_info["com_no_corpor"]),
            "Period": "%s%s" % (period_start, period_end),
            "items": []
        }
        for idx, vat_summarydetail_json_data in enumerate(result_vat_summarydetail):
            item = {}
            item["Code"] = "Detail"
            item["V_SB_GB"] = str(vat_summarydetail_json_data["v_sb_gb"]) if vat_summarydetail_json_data["v_sb_gb"] is not None and str(vat_summarydetail_json_data["v_sb_gb"]) != "None" else ''
            item["V_SEQ_NO"] = str(vat_summarydetail_json_data["v_seq_no"]) if vat_summarydetail_json_data["v_seq_no"] is not None and str(vat_summarydetail_json_data["v_seq_no"]) != "None" else ''
            item["V_BIZRES_GB"] = '1' if str(vat_summarydetail_json_data["v_bizres_gb"]) == '0' else '2' if vat_summarydetail_json_data["v_bizres_gb"] is not None and str(vat_summarydetail_json_data["v_bizres_gb"]) != "None" else ''
            item["V_BIZ_RES_NO"] = str(vat_summarydetail_json_data["v_biz_res_no"]) if vat_summarydetail_json_data["v_biz_res_no"] is not None and str(vat_summarydetail_json_data["v_biz_res_no"]) != "None" else ''
            item["V_COMP_NM"] = str(vat_summarydetail_json_data["v_comp_nm"]) if vat_summarydetail_json_data["v_comp_nm"] is not None and str(vat_summarydetail_json_data["v_comp_nm"]) != "None" else ''
            item["V_ISSUE_QTY"] = str(vat_summarydetail_json_data["v_issue_qty"]) if vat_summarydetail_json_data["v_issue_qty"] is not None and str(vat_summarydetail_json_data["v_issue_qty"]) != "None" else ''
            item["V_AMT"] = str(vat_summarydetail_json_data["v_amt"]) if vat_summarydetail_json_data["v_amt"] is not None and str(vat_summarydetail_json_data["v_amt"]) != "None" else ''
            item["V_TAX"] = str(vat_summarydetail_json_data["v_tax"]) if vat_summarydetail_json_data["v_tax"] is not None and str(vat_summarydetail_json_data["v_tax"]) != "None" else ''
            item["V_ETC"] = ""

            subitem["items"].append(item.copy())

        return subitem

    def __setting_initial_date_info(self, base_job_proc_date):
        base_tax_check_date = {}
        base_vat_close_check_year = {}
        base_tax_check_date["da_start_1"] = (datetime.datetime(int(base_job_proc_date[0:4]), int(base_job_proc_date[4:6]),
                                                               int(base_job_proc_date[6:8])) + datetime.timedelta(days=-365)).strftime("%Y") + "0101"
        base_tax_check_date["da_end_1"] = (datetime.datetime(int(base_job_proc_date[0:4]), int(base_job_proc_date[4:6]),
                                                             int(base_job_proc_date[6:8])) + datetime.timedelta(days=-365)).strftime("%Y") + "1231"
        base_tax_check_date["da_start_2"] = (datetime.datetime(int(base_job_proc_date[0:4]), int(base_job_proc_date[4:6]),
                                                               int(base_job_proc_date[6:8])) + datetime.timedelta(days=-(365 * 2))).strftime("%Y") + "0101"
        base_tax_check_date["da_end_2"] = (datetime.datetime(int(base_job_proc_date[0:4]), int(base_job_proc_date[4:6]),
                                                             int(base_job_proc_date[6:8])) + datetime.timedelta(days=-(365 * 2))).strftime("%Y") + "1231"
        base_tax_check_date["da_start_3"] = (datetime.datetime(int(base_job_proc_date[0:4]), int(base_job_proc_date[4:6]),
                                                               int(base_job_proc_date[6:8])) + datetime.timedelta(days=-(365 * 3))).strftime("%Y") + "0101"
        base_tax_check_date["da_end_3"] = (datetime.datetime(int(base_job_proc_date[0:4]), int(base_job_proc_date[4:6]),
                                                             int(base_job_proc_date[6:8])) + datetime.timedelta(days=-(365 * 3))).strftime("%Y") + "1231"

        base_vat_close_check_year["da_year_1"] = base_job_proc_date[0:4]
        base_vat_close_check_year["da_year_2"] = (datetime.datetime(int(base_job_proc_date[0:4]), int(base_job_proc_date[4:6]),
                                                                    int(base_job_proc_date[6:8])) + datetime.timedelta(days=-365)).strftime("%Y")
        base_vat_close_check_year["da_year_3"] = (datetime.datetime(int(base_job_proc_date[0:4]), int(base_job_proc_date[4:6]),
                                                                    int(base_job_proc_date[6:8])) + datetime.timedelta(days=-(365 * 2))).strftime("%Y")
        base_vat_close_check_year["da_year_4"] = (datetime.datetime(int(base_job_proc_date[0:4]), int(base_job_proc_date[4:6]),
                                                                    int(base_job_proc_date[6:8])) + datetime.timedelta(days=-(365 * 3))).strftime("%Y")
        return_dice = {
            "base_tax_check_date": base_tax_check_date,
            "base_vat_close_check_year": base_vat_close_check_year
        }

        return return_dice


class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, decimal.Decimal):
            return float(o)
        elif isinstance(o, datetime.date):
            return o.strftime('%Y-%m-%d %H:%M:%S')
        return super(DecimalEncoder, self).default(o)


def make_new_table(data_offer_connection, da_work):
    # if data_offer_connection.connect():
    query_string = """
            create table if not exists nice_financesend_js_{0}
            (
             seq_financesend_js bigint auto_increment not null primary key,
             ccode varchar(20) not null,
             da_work varchar(8) not null,
             gisu int not null,
             dt_from varchar(8) not null,
             dt_to varchar(8) not null,
             str_4 varchar(2) not null,
             str_5 varchar(2) not null,
             js_detail_data longtext collate utf8mb4_bin null,
             str_16 varchar(8) not null,
             id_insert varchar(100) null,
             dt_insert timestamp null,
             id_modify varchar(100) null,
             dt_modify timestamp null
            );

            create unique index if not exists nf_js_uindex_{0}
                on nice_financesend_js_{0} (ccode, da_work, gisu, dt_from, dt_to, str_4, str_5, str_16);

            create index if not exists nf_js_index_{0}
                on nice_financesend_js_{0} (ccode, da_work, gisu, str_5);
            """

    query_string += """
            create table if not exists nice_tax_ers_idx_{0}
            (
             seq_tax_ers bigint auto_increment not null primary key,
             ccode varchar(20) not null,
             da_work varchar(8) not null,
             gisu int not null,
             dt_from varchar(20) not null,
             dt_to varchar(8) not null,
             ers_string longtext collate utf8mb4_bin null,
             ers_index bigint default 1 not null
            );

            create unique index if not exists ntei_uindex_{0}
                on nice_tax_ers_idx_{0} (ccode, da_work, gisu, dt_from, dt_to, ers_index);
            """

    query_string += """
            create table if not exists nice_vat_ers_idx_{0}
            (
             seq_vat_ers bigint auto_increment not null primary key,
             ccode varchar(20) not null,
             da_work varchar(8) not null,
             period_start varchar(6) not null,
             period_end varchar(6) not null,
             ers_index bigint default 1 not null,
             ers_string longtext collate utf8mb4_bin null
            );

            create unique index if not exists nvei_uindex_{0}
                on nice_vat_ers_idx_{0} (ccode, da_work, period_start, period_end, ers_index);
            """

    query_string += """
            create table if not exists nice_vat_report_v2_{0}
            (
             seq_vat_report bigint auto_increment not null primary key,
             ccode varchar(20) not null,
             da_work varchar(8) not null,
             period_start varchar(6) not null,
             period_end varchar(6) not null,
             v_biz_no varchar(10) null,
             v_bub_no varchar(13) null,
             v_period_st varchar(6) null,
             v_period_ed varchar(6) null,
             v_acc_bizno varchar(10) null,
             v_acc_manno varchar(30) null,
             v_acc_name varchar(30) null,
             v_acc_addr varchar(100) null,
             v_acc_telno varchar(30) null,
             v_rtp_gb varchar(10) null,
             v_sel_tax_tot_amt decimal(17) null,
             v_sel_tax_tot_tx decimal(17) null,
             v_fixass_amt decimal(17) null,
             v_fixass_tx decimal(17) null,
             v_buy_tax_tot_amt decimal(17) null,
             v_buy_tax_tot_tx decimal(17) null,
             v_add_tot_tx decimal(17) null,
             v_subadd_tx decimal(17) null,
             v_tax_std_amt decimal(17) null,
             v_free_tax_tot_amt decimal(17) null,
             v_tot_tx decimal(17) null
            );

            create unique index if not exists nvr_uindex_{0}
                on nice_vat_report_v2_{0} (ccode, da_work, period_start, period_end);
            """

    query_string += """
            create table if not exists nice_vat_reportsinbo_v2_{0}
            (
             seq_vat_reportsinbo bigint auto_increment not null primary key,
             ccode varchar(20) not null,
             da_work varchar(8) not null,
             period_start varchar(6) not null,
             period_end varchar(6) not null,
             t00 varchar(1) null,
             t01 varchar(13) null,
             t02 varchar(10) null,
             t03 varchar(10) null,
             t04 varchar(8) null,
             t06 varchar(8) null,
             t07 varchar(8) null,
             t08 varchar(30) null,
             t09 varchar(30) null,
             t10 varchar(30) null,
             t11 varchar(30) null,
             t12 varchar(30) null,
             t13 varchar(30) null,
             t14 varchar(30) null,
             t15 varchar(30) null,
             t16 varchar(30) null,
             t17 varchar(30) null,
             t18 varchar(30) null,
             t19 varchar(30) null,
             t20 varchar(30) null,
             t21 varchar(30) null,
             t22 varchar(30) null,
             t23 varchar(30) null,
             t24 varchar(30) null,
             t25 varchar(30) null,
             t26 varchar(30) null,
             t27 varchar(30) null,
             t28 varchar(30) null,
             t29 varchar(30) null,
             t30 varchar(30) null,
             t31 varchar(30) null,
             t32 varchar(30) null,
             t33 varchar(30) null,
             t34 varchar(30) null,
             t35 varchar(30) null,
             t36 varchar(30) null,
             t37 varchar(30) null,
             t38 varchar(30) null,
             t39 varchar(30) null,
             t40 varchar(30) null,
             t41 varchar(30) null,
             t42 varchar(30) null,
             t43 varchar(30) null,
             t44 varchar(30) null,
             t45 varchar(30) null,
             t46 varchar(30) null,
             t49 varchar(50) null,
             t50 varchar(30) null,
             t51 varchar(150) null,
             t70 varchar(30) null,
             t71 varchar(30) null,
             t72 varchar(30) null,
             t73 varchar(30) null,
             t74 varchar(30) null,
             t75 varchar(30) null,
             t76 varchar(30) null
            );

            create unique index if not exists nvrs_uindex_{0}
                on nice_vat_reportsinbo_v2_{0} (ccode, da_work, period_start, period_end);
            """

    query_string += """
            create table if not exists nice_vat_summarydetail_js_{0}
            (
             seq_vat_summarydetail bigint auto_increment not null primary key,
             ccode varchar(20) not null,
             da_work varchar(8) not null,
             period_start varchar(6) not null,
             period_end varchar(6) not null,
             js_detail_data longtext collate utf8mb4_bin null
            );

            create unique index if not exists nvsj_uindex_{0}
                on nice_vat_summarydetail_js_{0} (ccode, da_work, period_start, period_end);
            """

    query_string += """
            create table if not exists sao_fta_reporter_v2_{0}
            (
             seq_reporter bigint auto_increment not null primary key,
             ccode varchar(20) not null,
             da_work varchar(8) not null,
             key_reporter bigint not null,
             ty_report int default 2 not null,
             nm_krname varchar(12) null,
             nm_userid varchar(20) null,
             nm_trade varchar(80) null,
             no_manage1 char null,
             no_manage2 char(4) null,
             no_manage3 char null,
             ty_mediation smallint null,
             no_mediation decimal(4) null,
             no_biz char(10) null,
             no_social varchar(100) null,
             zip_com char(6) null,
             add_saaddr1 varchar(80) null,
             add_saaddr2 varchar(40) null,
             tel_com1 varchar(4) null,
             tel_com2 varchar(4) null,
             tel_com3 varchar(4) null,
             key_taxoffice int null,
             cd_taxoffcom char(3) null,
             ty_intro smallint null,
             da_report char(8) null,
             key_laware int null,
             cd_lawcom char(12) null,
             ty_disk smallint null,
             qt_disk decimal(4, 2) null,
             da_accbegin char(8) null,
             da_accend char(8) null,
             nm_linecolor smallint null,
             em_taxoffice varchar(255) null,
             nm_chargedept varchar(30) null,
             nm_charger varchar(20) null,
             tel1_charger varchar(4) null,
             tel2_charger varchar(4) null,
             tel3_charger varchar(4) null,
             ty_united smallint default 2 not null,
             file_pw varchar(100) null,
             cd_lawcom2 varchar(5) null,
             str_3_99_99 varchar(20) null
            );

            create unique index if not exists sfr_uindex_{0}
                on sao_fta_reporter_v2_{0} (ccode, da_work, key_reporter);
            """

    qry = query_string.format(da_work)

    data_offer_connection.cursor.execute(qry)
    data_offer_connection.commit()
    # data_offer_connection.close()


def get_tms_randomstring(params):
    api_url = "http://dev.innerapi.wehago.com/WEHAGO2ndTMS/services/duzon"
    headers = {
        'content-type': "application/json",
        'service': "TenantService",
        'method': "getSAORandomStringMultiple",
        'clientid': "kyungmin"
    }
    res = requests.request("POST", api_url, headers=headers, data=json.dumps(params))
    if res.status_code == 200:
        return res.text
        # req_dict = json.loads(res.text)
        # if req_dict and req_dict.get('resultCode', '') == "0000":
        #     return req_dict.get('resultList')
        # else:
        #     raise Exception(req_dict.get('serverMsg'))
    else:
        raise Exception(str(res.status_code))


def make_enckey(data, ccodemap):
    retdata = {}
    for d in data:
        for k, v in d.items():
            if v != "null":
                if ccodemap.get(k, "") != "":
                    rs = str(v)
                    if len(rs.strip()) == 32:
                        merge_string = "SAO#" + str(k).strip() + "#" + rs.strip()
                        sha_string = base64.b64encode(hashlib.sha256(merge_string.encode('utf-8')).digest()).decode(
                            'utf-8')

                        md5_obj = hashlib.md5()
                        md5_obj.update(sha_string.encode('utf-8'))
                        retdata[ccodemap[k]] = md5_obj.hexdigest()
                    else:
                        merge_string = "SAO#" + rs.strip()[33:] + "#" + rs.strip()[:32]
                        sha_string = base64.b64encode(hashlib.sha256(merge_string.encode('utf-8')).digest()).decode(
                            'utf-8')

                        md5_obj = hashlib.md5()
                        md5_obj.update(sha_string.encode('utf-8'))
                        retdata[ccodemap[k]] = md5_obj.hexdigest()
    return retdata.copy()


def run(company_info_list, default_date_version):
    # 암호화키
    cno_list = {}
    for i in company_info_list:
        cno_list[str(i['cno'])] = i['schema_name']
    params = {}
    params["company_no_list"] = "|".join(cno_list.keys())
    # params["company_no_list"] = '41682|41099|27730|42551|27731|27732|44322|42184|27733|27728|42190|27729'

    random_data = get_tms_randomstring(params)
    req_dict = json.loads(random_data)
    if req_dict and req_dict.get('resultCode', '') == "0000":
        random_data = req_dict.get('resultList')
        enckey = make_enckey(random_data, cno_list)

    # da_work 체크
    data_offer_db_connection = PyMySqlConnection()
    data_offer_db_connection.user = DATABASES["default"]["USER"]
    data_offer_db_connection.password = DATABASES["default"]["PASSWORD"]
    data_offer_db_connection.host = DATABASES["default"]["HOST"]
    data_offer_db_connection.port = int(DATABASES["default"]["PORT"])
    data_offer_db_connection.database = DATABASES["default"]["NAME"]

    if data_offer_db_connection.connect():
        query_string = """select * from hdfs_create_table_job_history where da_work = '{da_work}'""".format(da_work=default_date_version)
        data_offer_db_connection.cursor.execute(query_string)
        temp_query_result = name_to_json(data_offer_db_connection.cursor)
        print("-----------------------------------------------------------")
        print(">> hdfs_create_table_job_history cnt : {0}".format(str(len(temp_query_result))))
        if len(temp_query_result) == 0:
            query_string = """insert into hdfs_create_table_job_history(da_work, work_status) values ('{da_work}', '1');""".format(da_work=default_date_version)
            data_offer_db_connection.cursor.execute(query_string)
            data_offer_db_connection.commit()

            make_new_table(data_offer_db_connection, da_work=default_date_version)
        print(">>>> success make_new_table")
        data_offer_db_connection.close()
        time.sleep(0.3)
        print(">> db close")
        print("-----------------------------------------------------------")

    # 실행부
    thread_schema_list = []
    for company_info in company_info_list:
        thread_schema_list.append(SaoEtlThread(company_info["host"], company_info["sdb_name"], company_info["schema_name"], default_date_version, company_info["cno"], enckey[company_info["schema_name"]]))

    for thread_obj in thread_schema_list:
        thread_obj.start()
    for thread_obj in thread_schema_list:
        thread_obj.join()

    if data_offer_db_connection.connect():
        print("-----------------------------------------------------------")
        query_string = """update hdfs_create_table_job_history set work_status = 5 where da_work = '{da_work}'""".format(da_work=default_date_version)
        data_offer_db_connection.cursor.execute(query_string)
        data_offer_db_connection.commit()
        data_offer_db_connection.close()
        print(">> success update hdfs")
        print(">> db close")
        print("-----------------------------------------------------------")
