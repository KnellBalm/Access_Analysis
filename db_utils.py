"""
Database Utility Module
=======================
다양한 DB(PostgreSQL, MariaDB)에서 데이터를 로드하여 pandas DataFrame으로 반환하는 유틸리티.
db_config.json 파일에서 DB 설정을 읽어와서 사용합니다.

사용법:
    from db_utils import get_dataframe, list_databases, get_connection
    
    # 데이터베이스 목록 확인
    list_databases()
    
    # SQL 쿼리 실행하여 DataFrame 반환
    df = get_dataframe("koroad_portal", "SELECT * FROM access_logs LIMIT 100")
    
    # 테이블 전체 로드 (스키마 지정 가능)
    df = get_dataframe("koroad_portal", table_name="access_logs", schema="public")
    
    # 데이터 저장 (스키마 지정 가능)
    save_dataframe(df, "koroad_portal", "processed_logs", schema="analysis")
"""

import json
import os
from pathlib import Path
from typing import Optional, Union
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine


# .env 파일 로드
load_dotenv()

# 설정 파일 경로 (.env의 DB_CONFIG_FILE 또는 기본값 db_config.json)
CONFIG_NAME = os.getenv("DB_CONFIG_FILE", "db_config.json")
CONFIG_PATH = Path(__file__).parent / CONFIG_NAME


def _load_config() -> dict:
    """db_config.json 파일에서 설정을 로드합니다."""
    if not CONFIG_PATH.exists():
        raise FileNotFoundError(
            f"설정 파일을 찾을 수 없습니다: {CONFIG_PATH}\n"
            "db_config.json 파일을 생성해 주세요."
        )
    
    with open(CONFIG_PATH, 'r', encoding='utf-8') as f:
        return json.load(f)


def list_databases() -> None:
    """사용 가능한 데이터베이스 목록을 출력합니다."""
    config = _load_config()
    databases = config.get("databases", {})
    
    print("=" * 60)
    print("사용 가능한 데이터베이스 목록")
    print("=" * 60)
    
    for name, info in databases.items():
        print(f"\n📦 {name}")
        print(f"   유형: {info.get('type', 'unknown')}")
        #print(f"   호스트: {info.get('host')}:{info.get('port')}")
        print(f"   DB명: {info.get('database')}")
        print(f"   설명: {info.get('description', '-')}")
    
    print("\n" + "=" * 60)


def get_connection(db_name: str):
    """
    지정된 데이터베이스에 대한 연결 객체를 반환합니다.
    
    Args:
        db_name: db_config.json에 정의된 데이터베이스 이름
        
    Returns:
        데이터베이스 연결 객체
    """
    config = _load_config()
    databases = config.get("databases", {})
    
    if db_name not in databases:
        available = ", ".join(databases.keys())
        raise ValueError(
            f"'{db_name}' 데이터베이스를 찾을 수 없습니다.\n"
            f"사용 가능한 DB: {available}"
        )
    
    db_config = databases[db_name]
    db_type = db_config.get("type", "").lower()
    
    if db_type == "postgresql":
        try:
            import psycopg2
        except ImportError:
            raise ImportError(
                "psycopg2가 설치되지 않았습니다.\n"
                "설치: pip install psycopg2-binary"
            )
        
        return psycopg2.connect(
            host=db_config["host"],
            port=db_config["port"],
            user=db_config["user"],
            password=db_config["password"],
            database=db_config["database"]
        )
    
    elif db_type == "mariadb":
        try:
            import pymysql
        except ImportError:
            raise ImportError(
                "pymysql이 설치되지 않았습니다.\n"
                "설치: pip install pymysql"
            )
        
        return pymysql.connect(
            host=db_config["host"],
            port=db_config["port"],
            user=db_config["user"],
            password=db_config["password"],
            database=db_config["database"],
            charset='utf8mb4'
        )
    
    else:
        raise ValueError(
            f"지원하지 않는 DB 유형: {db_type}\n"
            "지원 유형: postgresql, mariadb"
        )


def get_engine(db_name: str):
    """
    지정된 데이터베이스에 대한 SQLAlchemy engine 객체를 반환합니다.
    (pandas의 to_sql 등을 사용할 때 권장됩니다)
    """
    config = _load_config()
    databases = config.get("databases", {})
    
    if db_name not in databases:
        raise ValueError(f"'{db_name}' 데이터베이스를 찾을 수 없습니다.")
    
    db_config = databases[db_name]
    db_type = db_config.get("type", "").lower()
    
    # 드라이버 설정
    driver = "postgresql" if db_type == "postgresql" else "mysql+pymysql"
    
    url = f"{driver}://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['database']}"
    return create_engine(url)


def save_dataframe(
    df: pd.DataFrame,
    db_name: str,
    table_name: str,
    schema: Optional[str] = None,
    if_exists: str = 'append',
    index: bool = False
) -> None:
    """
    DataFrame을 지정된 데이터베이스의 테이블로 저장합니다.
    
    Args:
        df: 저장할 pandas DataFrame
        db_name: db_config.json에 정의된 데이터베이스 이름
        table_name: 저장할 테이블 이름
        schema: 데이터베이스 스키마 이름 (PostgreSQL 등에서 사용)
        if_exists: 테이블이 이미 존재할 경우 처리 방식 ('fail', 'replace', 'append')
        index: DataFrame의 인덱스를 컬럼으로 포함할지 여부
    """
    engine = get_engine(db_name)
    try:
        df.to_sql(
            name=table_name,
            con=engine,
            schema=schema,
            if_exists=if_exists,
            index=index,
            method='multi',  # 대량 삽입 성능 향상
            chunksize=1000   # 한 번에 처리할 행 수
        )
        target = f"{schema}.{table_name}" if schema else table_name
        print(f"✅ 성공: {len(df):,}행이 '{db_name}'의 '{target}' 테이블에 {if_exists} 되었습니다.")
    except Exception as e:
        print(f"❌ 실패: {str(e)}")
        raise
    finally:
        engine.dispose()


def get_dataframe(
    db_name: str,
    query: Optional[str] = None,
    table_name: Optional[str] = None,
    schema: Optional[str] = None,
    limit: Optional[int] = None
) -> pd.DataFrame:
    """
    지정된 데이터베이스에서 쿼리를 실행하고 결과를 DataFrame으로 반환합니다.
    
    Args:
        db_name: db_config.json에 정의된 데이터베이스 이름
        query: 실행할 SQL 쿼리 (table_name과 함께 사용 불가)
        table_name: 로드할 테이블 이름 (query와 함께 사용 불가)
        schema: 스키마 이름 (table_name 사용 시 적용)
        limit: 가져올 최대 행 수 (table_name 사용 시에만 적용)
        
    Returns:
        쿼리 결과가 담긴 pandas DataFrame
        
    Examples:
        # SQL 쿼리로 데이터 로드
        df = get_dataframe("koroad_portal", "SELECT * FROM access_logs WHERE date > '2024-01-01'")
        
        # 테이블 전체 로드
        df = get_dataframe("koroad_portal", table_name="access_logs")
        
        # 테이블에서 상위 1000개 행만 로드
        df = get_dataframe("koroad_portal", table_name="access_logs", limit=1000)
    """
    if query is None and table_name is None:
        raise ValueError("query 또는 table_name 중 하나를 지정해야 합니다.")
    
    if query is not None and table_name is not None:
        raise ValueError("query와 table_name을 동시에 지정할 수 없습니다.")
    
    # 테이블 이름으로 쿼리 생성
    if table_name is not None:
        full_table_name = f"{schema}.{table_name}" if schema else table_name
        if limit is not None:
            query = f"SELECT * FROM {full_table_name} LIMIT {limit}"
        else:
            query = f"SELECT * FROM {full_table_name}"
    
    # 연결 및 쿼리 실행
    conn = get_connection(db_name)
    try:
        df = pd.read_sql(query, conn)
        return df
    finally:
        conn.close()


def get_tables(db_name: str, schema: Optional[str] = None) -> pd.DataFrame:
    """
    지정된 데이터베이스의 테이블 목록을 반환합니다.
    
    Args:
        db_name: db_config.json에 정의된 데이터베이스 이름
        schema: 필터링할 스키마 이름 (PostgreSQL 기본값: 'public')
        
    Returns:
        테이블 목록이 담긴 DataFrame
    """
    config = _load_config()
    databases = config.get("databases", {})
    
    if db_name not in databases:
        raise ValueError(f"'{db_name}' 데이터베이스를 찾을 수 없습니다.")
    
    db_type = databases[db_name].get("type", "").lower()
    
    if db_type == "postgresql":
        target_schema = schema if schema else 'public'
        query = f"""
            SELECT table_name, table_type
            FROM information_schema.tables
            WHERE table_schema = '{target_schema}'
            ORDER BY table_name
        """
    elif db_type == "mariadb":
        schema_filter = f"AND TABLE_SCHEMA = '{schema}'" if schema else "AND TABLE_SCHEMA = DATABASE()"
        query = f"""
            SELECT TABLE_NAME as table_name, TABLE_TYPE as table_type
            FROM information_schema.TABLES
            WHERE 1=1 {schema_filter}
            ORDER BY TABLE_NAME
        """
    else:
        raise ValueError(f"지원하지 않는 DB 유형: {db_type}")
    
    return get_dataframe(db_name, query)


# 편의 함수
def preview(db_name: str, table_name: str, schema: Optional[str] = None, n: int = 5) -> pd.DataFrame:
    """테이블의 처음 n개 행을 미리보기합니다."""
    return get_dataframe(db_name, table_name=table_name, schema=schema, limit=n)
