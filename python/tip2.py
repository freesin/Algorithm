#https://scent2d.tistory.com/73

#버프 프록시를 이용하여 URL 디코딩 시 한글일 경우 정확히 보기 힘든 경우가 있다.


#따라서, 파이썬 urllib 모듈을 이용하여 아래와 같이 URL 인코딩/디코딩 할 수 있다.

from urllib import parse

b = 'gisu=17&gb_semok=0&gb_trade=Y&cd_acctits=10800&slip_dt_from=20210101&slip_dt_to=20211231&dt_from=20210101&dt_to=20211231&yn_total=1%2C2&yn_samedtrmn=1&cd_trade=&nm_trade=&nm_krcom=(%EC%A3%BC)%ED%8F%AC%EC%9C%A0%EC%9D%B8%ED%8F%AC%ED%85%8D&cd_prt=1&title_gbn=1&nm_acctit=%EC%99%B8%EC%83%81%EB%A7%A4%EC%B6%9C%EA%B8%88&dn_trade=6&cd_trades=&cno=52578&ccode=biz202205030001743&user_id=story3585&ym_insa=2021&cno_taxnum=18065&wehago_s=129063130818085427943176452188793174397&oldview=0&yn_empty=0&yn_exceptelec=0&yn_na=0&yn_privacy=0&yn_printtaildate=0&yn_printtailpage=0&yn_sealprinting=0'
b = parse.unquote(b)
print(b)
b = b.replace("=", ':').replace('&','\n').replace('%2C', ',').replace('?','\n')
print(b)
print('--------------------------------------------')
gg = '%EC%99%B8%EC%83%81%EB%A7%A4%EC%B6%9C%EA%B8%88'
print( parse.unquote(gg))
