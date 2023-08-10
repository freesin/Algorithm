import pandas as pd

a = {"dept_main": [
    {
        "cd_dept": "00000001",
        "cd_pdept": '',
        "organization_no": 3248940,
        "nm_dept": "기술지원본부",
        "yn_disabled": 0,
        "rmk_bigo": '',
        "sq_order": ''
    },
    {
        "cd_dept": "00000031",
        "cd_pdept": '',
        "organization_no": 3248993,
        "nm_dept": "미등록부서",
        "yn_disabled": 0,
        "rmk_bigo": '',
        "sq_order": ''
    },
    {
        "cd_dept": "00000032",
        "cd_pdept": '',
        "organization_no": '',
        "nm_dept": "SF ENG",
        "yn_disabled": 0,
        "rmk_bigo": '',
        "sq_order": ''
    }
],
    "dept_wehago": [
        {
            "organization_no": 3248902,
            "organization_level": 0,
            "insert_timestamp": '',
            "full_path": '',
            "updated_timestamp": '',
            "organization_name": "(주)일양엔지니어링",
            "organization_order": 0,
            "sao_organization_code": '',
            "is_new": 0,
            "is_deleted": "F",
            "organization_parent_no": -1,
            "company_no": 2515773,
            "organization_state": ''
        },
        {
            "organization_no": 3248940,
            "organization_level": 1,
            "insert_timestamp": '',
            "full_path": '',
            "updated_timestamp": '',
            "organization_name": "기술지원본부",
            "organization_order": 1,
            "sao_organization_code": "00000001",
            "is_new": 0,
            "is_deleted": "F",
            "organization_parent_no": 3248902,
            "company_no": 2515773,
            "organization_state": ''

        }
    ]}

df = pd.DataFrame(a.get('dept_wehago'))
print(df)
df.to_excel('ftb_dept_wehago(2259504).xlsx', index=False)

# 3294828 / 2259504 / 2989310 / 2800741

