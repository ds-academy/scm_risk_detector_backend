"""
DB_CONFIG 를 관리하며, 해당 config안에 실제 스마트인재개발원에서 활용하는 DB server의 정보를 입력할것

*DEFAULT CONFIGURATION 임에 주의 -> Yaml 파일이 없을 경우 아래 정보로 접속한다*

"""
db_config = {
    'user': 'cgi_24K_bigdata26_p3_1',  # USER NAME
    'password': 'smhrd1',  # PASSWORD
    'host': 'project-db-cgi.smhrd.com',  # HOST NAME
    'port': 3307,
    'database': 'SCM_',  # DATABASE NAME
    'name': 'MYSQL'
}
