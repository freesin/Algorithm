# -*- coding: utf-8 -*-

DATABASES = {
    'default': {
        'ENGINE': 'django.db.backends.mysql',
        'NAME': 'DATAOFFER',
        'USER': 'dataoffer',
        'PASSWORD': 'DataOffer12!@',
        'HOST': '172.16.113.138',
        'PORT': '23306',
        'OPTIONS': {
            'init_command': 'SET sql_mode="STRICT_TRANS_TABLES"'
        }
    }
}
