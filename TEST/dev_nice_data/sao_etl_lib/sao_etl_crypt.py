#-*- coding: utf-8 -*-

import base64, hashlib
from Crypto.Cipher import AES

BS = 16
pad = lambda s: s + (BS - len(s) % BS) * chr(BS - len(s) % BS).encode()
unpad = lambda s: s[:-ord(s[len(s)-1:])]



class _Aes(object):
    def __init__(self, key, mode):
        self.key = key
        self.mode = mode

    def encrypt(self, message):
        message = message.encode()
        raw = pad(message)
        cipher = AES.new(self.key, self.mode, self.__iv())
        enc = cipher.encrypt(raw)
        return base64.b64encode(enc).decode('utf-8')

    def decrypt(self, enc):
        enc = base64.b64decode(enc)
        cipher = AES.new(self.key, self.mode, self.__iv())
        dec = cipher.decrypt(enc)
        return unpad(dec).decode('utf-8')

    def __iv(self):
        return chr(0) * 16


class AESCipher(object):
    def __init__(self, key):
        self.key = hashlib.sha256(key.encode()).digest()
        self.mode = AES.MODE_CBC

    def encrypt(self, message):
        return _Aes(self.key, self.mode).encrypt(message)

    def decrypt(self, enc):
        return _Aes(self.key, self.mode).decrypt(enc)


class SSCCipher(object):
    def __init__(self, key):
        self.key = key
        self.mode = AES.MODE_ECB

    def encrypt(self, message):
        return _Aes(self.key, self.mode).encrypt(message)

    def decrypt(self, enc):
        return _Aes(self.key, self.mode).decrypt(enc)


class CryptHelper:
    def __init__(self, dec_key, enc_key):
        self.dec_ase = SSCCipher(key=dec_key)
        self.enc_ase = SSCCipher(key=enc_key)

    #     if key:
    #         self.ase = SSCCipher(key=key)
    #     else:
    #         self.ase = SSCCipher(key=self.get_key())
    #
    #
    def get_key(self, request=None):
        key = "E86916E2CF3846B9BB6880CBC0447C35"

        # cno = None
        #
        # if cno in SAO_CRYPTOCANA:
        #     key = SAO_CRYPTOCANA[cno]

        return key

    def encrypt_string(self, data):
        try:
            if data.strip() == "":
                return data
            else:
                return self.enc_ase.encrypt(message=data)
        except Exception as e:
            return data

    def decrypt_string(self, data):
        try:
            if data.strip() == "":
                return data
            else:
                return self.dec_ase.decrypt(enc = data)
        except:
            return data

    def encrypt_social(self, data):
        if data and len(data) == 13:
            data1 = data[:7]
            data2 = data[7:]
            data = data1 + self.encrypt_string(data2)
            return  data
        else:
            return data

    def decrypt_social(self, data):
        if data and len(data) >= 13:
            data1 = data[:7]
            data2 = data[7:]
            data = data1 + self.decrypt_string(data2)
            return data
        else:
            return data

    def encrypt_row(self, row, cols):
        for key in cols.keys():
            if key in row:
                if cols[key] == "string":
                    row[key] = self.encrypt_string(data=row[key])
                elif "string:" in cols[key]:
                    tmp = cols[key].split(":")
                    if len(tmp) > 1:
                        index = int(tmp[1])
                        row[key] = row[key][:index] + self.encrypt_string(data=row[key][index:])
                    else:
                        row[key] = self.encrypt_string(data=row[key])
                else:
                    row[key] = self.encrypt_social(data=row[key])
        #return row

    def encrypt_data(self, data, cols):
        if isinstance(data, list):
            for dic in data:
                self.encrypt_row(row=dic, cols=cols)
        elif isinstance(data, dict):
            self.encrypt_row(row=data, cols=cols)
        #return data

    def decrypt_row(self, row, cols):
        for key in cols.keys():
            if key in row:
                if cols[key] == "string":
                    row[key] = self.decrypt_string(data=row[key])
                elif cols[key] == "social":
                    row[key] = self.decrypt_social(data=row[key])
                elif "string:" in cols[key]:
                    tmp = cols[key].split(":")
                    if len(tmp) > 1:
                        index = int(tmp[1])
                        row[key] = row[key][:index] + self.decrypt_string(data=row[key][index:])
                    else:
                        row[key] = self.decrypt_string(data=row[key])
                else:
                    row[key] = eval(cols[key])
                    # return row

    def decrypt_data(self, data, cols):
        if isinstance(data, list):
            for dic in data:
                self.decrypt_row(row=dic, cols=cols)
        elif isinstance(data, dict):
            self.decrypt_row(row=data, cols=cols)
            # return data

    def dec_enc_social(self, data):
        data1 = self.decrypt_social(data)
        if len(data1) == 13:
            data2 = self.encrypt_social(data1)
        else:
            data2 = data1
        return data2

    def dec_enc_data(self, data, cols):
        if isinstance(data, list):
            for dic in data:
                self.decrypt_row(row=dic, cols=cols)
                self.encrypt_row(row=dic, cols=cols)
        elif isinstance(data, dict):
            self.decrypt_row(row=data, cols=cols)
            self.encrypt_row(row=data, cols=cols)

            # return data

    #아래부터는 model 에서 사용하는것 같음
    @staticmethod
    def static_encrypt_str(key, string):
        try:
            if string.strip() == "":
                return string
            else:
                return SSCCipher(key=key).encrypt(string)
        except:
            return string

    @staticmethod
    def static_decrypt_str(key, string):
        try:
            if string.strip() == "":
                return string
            else:
                return SSCCipher(key=key).decrypt(string)
        except:
            return string

    @staticmethod
    def static_encrypt_soc(key, string):
        if string and len(string) == 13:
            data1 = string[:7]
            data2 = string[7:]
            string = data1 + SSCCipher(key=key).encrypt(data2)
            return string
        else:
            return string

    @staticmethod
    def static_decrypt_soc(key, string):
        if string and len(string) >= 13:
            data1 = string[:7]
            data2 = string[7:]
            try:
                string = data1 + SSCCipher(key=key).decrypt(data2)
            except:
                pass
            return string
        else:
            return string

