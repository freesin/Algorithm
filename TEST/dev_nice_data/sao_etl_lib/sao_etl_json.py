import collections


def json_result(status=None, data=None, message=None):
    """
    response format
    :param status: status code
    :param data: response data
    :param message: response message
    :return: JSON
    """
    result = {
        'code': status,
        'message': message,
        'data': data
    }

    return result


def name_to_json(cursor):
    """
    cursor.fetchall() 함수로 받아온 쿼리 결과를 json 형식으로 만들어 반환해주는 함수입니다.
    :param cursor: SQL 연결 변수
    :return: JSON 쿼리 결과 LIST
    """
    row = [dict((cursor.description[i][0], value)
                for i, value in enumerate(row)) for row in cursor.fetchall()]
    return row


def name_to_json_col_dic(cursor, col_01, col_02):
    """
    cursor.fetchall() 함수로 받아온 쿼리 결과를 json 형식으로 만들어 반환해주는 함수입니다.
    음수금액 처리를 위한 금액이 음수인지 양수인지 판단하여 yn_positive 컬럼 추가 되었습니다.
    :param cursor: SQL 연결 변수
            col_01: 해당 컬럼의 데이터를 키값으로 가지고 같은 값을 가지는 데이터들 끼리 묶어줍니다.
            col_02: col_01값 안에 col_02를 키로 가지고 있는 데이터들을 묶어 줍니다.
    :return: JSON 쿼리 결과 JSON
    """
    array = []
    dic = collections.OrderedDict()     # 순서 보장
    dic2 = collections.OrderedDict()    # 순서 보장

    fetchall = cursor.fetchall()
    yn_positive = 1     # 금액이 음수인지 양수인지 판별 (1: 양수만 있음, 2: 음수 존재함)
    for i, row in enumerate(fetchall):
        row_dic = dict((cursor.description[i][0], value) for i, value in enumerate(row))
        array.append(row_dic)
        if row_dic['mn_bungae'] < 0:
            yn_positive = 0
        if i == 0:
            dic2[row_dic[col_02]] = array.copy()
            dic[row_dic[col_01]] = {'data': dic2.copy(), 'yn_positive': yn_positive}

        if i > 0:
            temp_row = dict((cursor.description[i][0], value) for i, value in enumerate(fetchall[i - 1]))
            if temp_row[col_01] == row_dic[col_01] and temp_row[col_02] == row_dic[col_02]:
                dic2[row_dic[col_02]] = array.copy()
                dic[row_dic[col_01]] = {'data': dic2.copy(), 'yn_positive': yn_positive}
            else:
                # 1번 key 값이 바뀜 / 2번 key 값이 바뀜
                if temp_row[col_01] != row_dic[col_01]:
                    # 1번키값 바뀔경우 처음부터 다시 시작
                    dic2 = collections.OrderedDict()
                    yn_positive = 1
                else:
                    pass
                array.clear()
                array.append(row_dic)
                dic2[row_dic[col_02]] = array.copy()
                if dic2[row_dic[col_02]][0]['mn_bungae'] < 0:
                    yn_positive = 0
                dic[row_dic[col_01]] = {'data': dic2.copy(), 'yn_positive': yn_positive}

    return dic


#===주민번호와 일반문자열을 복호화하여 json으로 생성
def dencrypt_json(cursor, new_aes_cipher=None, com_aes_chipher=None, **cols):
    """
    :param cursor: 쿼리 실행 결과
    :param cols: 주민번호가 저장되는 컬럼(복수 가능)
    :return: JSON 쿼리 결과 LIST
    """
    r = dict()
    rs = []
    for row in cursor.fetchall():
        for i, value in enumerate(row):
            target = cursor.description[i][0]
            if target in cols.get('soc',[]):
                if not value:
                    r[target] = value
                elif len(value) == 10:
                    r[target] = value
                else:
                    if com_aes_chipher:
                        r[target] = com_aes_chipher.decrypt(data=value, crypt_kind="social")
                        if new_aes_cipher:
                            r[target] = new_aes_cipher.encrypt(data=r[target], crypt_kind="social")
                    else:
                        r[target] = value
            elif target in cols.get('str',[]):
                if com_aes_chipher:
                    r[target] = com_aes_chipher.decrypt(data=value, crypt_kind="string")
                    if new_aes_cipher:
                        r[target] = new_aes_cipher.encrypt(data=r[target], crypt_kind="string")
                else:
                    r[target] = value
            else:
                r[target] = value
        rs.append(r.copy())
    return rs
