#!/usr/bin/env python

from main.etl_class import *

if __name__ == "__main__":
    """
    세무대리 - 수임처 세트로 넣어줘야함.
    """
    default_date_version = '20230726'  # da_work
    company_info_list = [
        # # dev 세무대리
        #{"cno": "54119", "host": "172.16.115.106", "sdb_name": "sdb001", "schema_name": "biz202206160000176"},
        #{"cno": "21800", "host": "172.16.115.111", "sdb_name": "sdb002", "schema_name": "biz202001200000437"},
        # {"cno": "35680", "host": "172.16.115.103", "sdb_name": "sdb001", "schema_name": "biz202011050000154"},
         {"cno": "35547", "host": "172.16.114.69", "sdb_name": "sdb002", "schema_name": "biz202011020000311"},
        #
        # # dev test
        # {"cno": "41557", "host": "172.16.115.105", "sdb_name": "sdb002", "schema_name": "biz202103260000392"},
        # {"cno": "41682", "host": "172.16.115.103", "sdb_name": "sdb001", "schema_name": "biz202103310000314"},
        # {"cno": "41687", "host": "172.16.115.112", "sdb_name": "sdb002", "schema_name": "biz202103310000361"},
        # {"cno": "41693", "host": "172.16.114.69", "sdb_name": "sdb001", "schema_name": "biz202103310000421"},
        # {"cno": "42038", "host": "172.16.115.106", "sdb_name": "sdb002", "schema_name": "biz202104080000542"},
        # {"cno": "42039", "host": "172.16.115.109", "sdb_name": "sdb003", "schema_name": "biz202104080000553"},
        # {"cno": "42057", "host": "172.16.115.111", "sdb_name": "sdb003", "schema_name": "biz202104090000045"},
        #
        # {"cno": "40374", "host": "172.16.114.69", "sdb_name": "sdb008", "schema_name": "biz202102250000314"},
        # {"cno": "39389", "host": "172.16.115.103", "sdb_name": "sdb002", "schema_name": "biz202102080001197"},
        # {"cno": "39991", "host": "172.16.115.105", "sdb_name": "sdb002", "schema_name": "biz202102180000121"},
        # {"cno": "42188", "host": "172.16.115.106", "sdb_name": "sdb001", "schema_name": "biz202104130000587"},
        # {"cno": "42301", "host": "172.16.115.105", "sdb_name": "sdb003", "schema_name": "biz202104150000097"},
        # {"cno": "47196", "host": "172.16.115.112", "sdb_name": "sdb001", "schema_name": "biz202111100000028"},
        #
        # # 개인
        # {"cno": "42751", "host": "172.16.115.110", "sdb_name": "sdb002", "schema_name": "biz202104290000909"},
        #
        # # 나이스 영상촬영용
        # {"cno": "40339", "host": "172.16.115.111", "sdb_name": "sdb001", "schema_name": "biz202102240000734"},
        # {"cno": "43703", "host": "172.16.115.104", "sdb_name": "sdb003", "schema_name": "biz202105260000909"},
        # {"cno": "43697", "host": "172.16.115.103", "sdb_name": "sdb001", "schema_name": "biz202105260000844"},
        # {"cno": "43700", "host": "172.16.115.109", "sdb_name": "sdb001", "schema_name": "biz202105260000874"},
        # {"cno": "43769", "host": "172.16.114.69", "sdb_name": "sdb010", "schema_name": "biz202105260001605"},
        #
        #
        # {"cno": "40439", "host": "172.16.115.112", "sdb_name": "sdb001", "schema_name": "biz202102260000408"},
        # {"cno": "43028", "host": "172.16.115.103", "sdb_name": "sdb001", "schema_name": "biz202105100000363"},
        # {"cno": "40520", "host": "172.16.115.104", "sdb_name": "sdb002", "schema_name": "biz202103020000801"},

    ]

    run(company_info_list, default_date_version)
