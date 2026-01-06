from sqlalchemy import create_engine, MetaData, Table, select, text
from dotenv import load_dotenv
import os

load_dotenv()

# ================= 配置区域 =================
# 1. 定义源数据库 (SQLite)
SQLITE_DB_PATH = 'main.db' # 替换为你的 sqlite 文件路径
sqlite_url = f'sqlite:///{SQLITE_DB_PATH}'

# 2. 定义目标数据库 (MySQL)
# 替换为你的 MySQL 信息: user:password@host:port/db_name
mysql_url = os.getenv('CS_DATABASE').replace("aiomysql","pymysql")

# ===========================================

def migrate_data():
    # 创建引擎
    src_engine = create_engine(sqlite_url)
    dest_engine = create_engine(mysql_url)

    # 建立连接
    src_conn = src_engine.connect()
    dest_conn = dest_engine.connect()

    # 反射：自动获取数据库中的表结构
    src_metadata = MetaData()
    src_metadata.reflect(bind=src_engine)
    
    # 目标库的 metadata (用于确保表名匹配)
    dest_metadata = MetaData()
    dest_metadata.reflect(bind=dest_engine)

    # 开启事务
    trans = dest_conn.begin()

    try:
        print(">>> 开始迁移...")

        # A. 重要：关闭 MySQL 的外键约束检查
        # 这样我们可以不用关心表之间插入的先后顺序
        dest_conn.execute(text("SET FOREIGN_KEY_CHECKS = 0;"))

        # B. 遍历 SQLite 中的每一张表
        for table_name in src_metadata.tables:
            src_table = src_metadata.tables[table_name]
            
            # 检查该表是否在 MySQL 中存在
            if table_name not in dest_metadata.tables:
                print(f"跳过: 表 {table_name} 在 MySQL 中不存在")
                continue
            
            dest_table = dest_metadata.tables[table_name]

            print(f"正在迁移表: {table_name} ...", end="")

            # C. 读取 SQLite 数据
            # select(src_table) 相当于 "SELECT * FROM table"
            stmt = select(src_table)
            result_proxy = src_conn.execute(stmt)
            rows = result_proxy.fetchall()
            
            if not rows:
                print(" 空表 (跳过)")
                continue

            # 将 row 对象转换为字典列表 (MySQL 批量插入需要)
            # SQLAlchemy 的 result 行可以直接作为字典使用，但为了保险，显式转换
            data_to_insert = [row._asdict() for row in rows]

            # D. 写入 MySQL (使用 Core 的批量插入)
            # 这里的 chunk_size 是为了防止一次插入数据包太大导致 MySQL 报错
            # 如果数据量巨大（百万级），建议分块插入
            chunk_size = 1000
            for i in range(0, len(data_to_insert), chunk_size):
                chunk = data_to_insert[i : i + chunk_size]
                dest_conn.execute(dest_table.insert(), chunk)
            
            print(f" 完成 ({len(rows)} 行)")

        # E. 恢复外键约束检查
        dest_conn.execute(text("SET FOREIGN_KEY_CHECKS = 1;"))
        
        # 提交事务
        trans.commit()
        print(">>> 迁移成功！所有数据已保存。")

    except Exception as e:
        # 发生错误回滚
        trans.rollback()
        print(f"\n!!! 迁移失败，已回滚。错误信息: {e}")
    
    finally:
        src_conn.close()
        dest_conn.close()

if __name__ == "__main__":
    migrate_data()