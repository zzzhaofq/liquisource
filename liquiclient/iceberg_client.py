#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import threading

from pyiceberg.catalog import load_catalog
from pyspark.sql import SparkSession

from liquiclient.config import get_property, get_property_or_none

# ============================================================
# SparkSession 全局缓存（按 catalog 名称缓存，避免重复创建）
# ============================================================
_spark_sessions = {}
_lock = threading.Lock()


def _build_storage_config(prefix):
    """
    根据存储类型构建对应的配置参数
    支持: s3(AWS S3) / minio / cos(腾讯云COS) / oss(阿里云OSS)
    """
    s3_type = get_property_or_none(prefix + ".s3.type")
    if not s3_type:
        # 没有配置存储类型，尝试兼容旧的纯 S3 配置
        s3_endpoint = get_property_or_none(prefix + ".s3.endpoint")
        if s3_endpoint:
            return {
                "s3.endpoint": s3_endpoint,
                "s3.secret_key": get_property(prefix + ".s3.secret_key"),
                "s3.access_key": get_property(prefix + ".s3.access_key"),
            }
        return {}

    s3_type = s3_type.strip().lower()
    config = {}

    if s3_type == "s3":
        # AWS S3
        config["s3.endpoint"] = get_property(prefix + ".s3.endpoint")
        config["s3.secret_key"] = get_property(prefix + ".s3.secret_key")
        config["s3.access_key"] = get_property(prefix + ".s3.access_key")
        region = get_property_or_none(prefix + ".s3.region")
        if region:
            config["s3.region"] = region

    elif s3_type == "minio":
        # MinIO (S3兼容)
        config["s3.endpoint"] = get_property(prefix + ".s3.endpoint")
        config["s3.secret_key"] = get_property(prefix + ".s3.secret_key")
        config["s3.access_key"] = get_property(prefix + ".s3.access_key")
        config["s3.path_style"] = "true"

    elif s3_type == "cos":
        # 腾讯云 COS (cosn://)
        config["s3.endpoint"] = get_property(prefix + ".s3.endpoint")
        config["s3.secret_key"] = get_property(prefix + ".s3.secret_key")
        config["s3.access_key"] = get_property(prefix + ".s3.access_key")
        # COS Hadoop 文件系统配置
        config["fs.cosn.userinfo.secretId"] = get_property(prefix + ".s3.secret_key")
        config["fs.cosn.userinfo.secretKey"] = get_property(prefix + ".s3.access_key")
        config["fs.cosn.bucket.endpoint_suffix"] = get_property(prefix + ".s3.endpoint")
        config["fs.cosn.impl"] = "org.apache.hadoop.fs.CosFileSystem"
        config["fs.AbstractFileSystem.cosn.impl"] = "org.apache.hadoop.fs.CosN"

    elif s3_type == "oss":
        # 阿里云 OSS (oss://)
        config["s3.endpoint"] = get_property(prefix + ".s3.endpoint")
        config["s3.secret_key"] = get_property(prefix + ".s3.secret_key")
        config["s3.access_key"] = get_property(prefix + ".s3.access_key")
        # OSS Hadoop 文件系统配置
        config["fs.oss.accessKeyId"] = get_property(prefix + ".s3.secret_key")
        config["fs.oss.accessKeySecret"] = get_property(prefix + ".s3.access_key")
        config["fs.oss.endpoint"] = get_property(prefix + ".s3.endpoint")
        config["fs.oss.impl"] = "org.apache.hadoop.fs.aliyun.oss.AliyunOSSFileSystem"
        config["fs.oss.connection.secure.enabled"] = "false"
        config["fs.oss.connection.maximum"] = "2048"

    else:
        raise ValueError(f"不支持的存储类型: {s3_type}，可选值: s3, minio, cos, oss")

    return config


def _collect_rest_catalog_config(prefix):
    """
    收集 REST Catalog 专有配置（认证 + Credential Vending header）

    读取以下配置项（均为可选）:
      - {prefix}.catalog.credential : OAuth2 客户端凭证
      - {prefix}.catalog.token      : Bearer Token
      - {prefix}.catalog.scope      : OAuth2 scope
    """
    config = {}
    credential = get_property_or_none(prefix + ".catalog.credential")
    if credential:
        config["credential"] = credential
    token = get_property_or_none(prefix + ".catalog.token")
    if token:
        config["token"] = token
    scope = get_property_or_none(prefix + ".catalog.scope")
    if scope:
        config["scope"] = scope

    # 禁用 Credential Vending（pyiceberg 默认请求 vended-credentials，
    # 若 Polaris 服务端未配置存储凭证会报错，DDL 操作不需要此功能）
    config["header.X-Iceberg-Access-Delegation"] = ""
    return config


def _collect_jdbc_catalog_config(prefix):
    """
    收集 JDBC Catalog 专有配置

    读取以下配置项（均为可选）:
      - {prefix}.catalog.jdbc.user       : 数据库用户名
      - {prefix}.catalog.jdbc.password   : 数据库密码
      - {prefix}.catalog.jdbc.*          : 其他常见 JDBC 参数（如 useSSL、schema-version 等）

    返回:
        dict，key 形如 "jdbc.user" / "jdbc.password" / "jdbc.xxx"
    """
    jdbc_config = {}
    user = get_property_or_none(prefix + ".catalog.jdbc.user")
    if user is not None:
        jdbc_config["jdbc.user"] = user
    password = get_property_or_none(prefix + ".catalog.jdbc.password")
    if password is not None:
        jdbc_config["jdbc.password"] = password

    # schema-version：Iceberg JDBC Catalog 元数据表结构版本，默认 V1
    schema_version = get_property_or_none(prefix + ".catalog.jdbc.schema-version")
    jdbc_config["jdbc.schema-version"] = schema_version if schema_version else "V1"

    # 禁用 catalog 缓存：JDBC Catalog 元数据存储于外部数据库，
    # 开启缓存可能导致读到过期表结构（其他进程 DDL 后本进程感知不到）
    cache_enabled = get_property_or_none(prefix + ".catalog.cache-enabled")
    jdbc_config["cache-enabled"] = cache_enabled if cache_enabled else "false"

    # 常见扩展 JDBC 参数（预留常用键，get_property 无法枚举全部前缀）
    for extra_key in ("useSSL", "verifyServerCertificate",
                      "serverTimezone", "characterEncoding", "connectionTimeout"):
        val = get_property_or_none(prefix + ".catalog.jdbc." + extra_key)
        if val is not None:
            jdbc_config["jdbc." + extra_key] = val

    return jdbc_config


def _build_catalog_config(prefix, for_pyiceberg=False):
    """
    构建 Iceberg Catalog 配置（按 catalog_type 分支加载对应字段，互斥）

    参数:
        prefix: 配置项前缀，默认 catalog 为 "iceberg"，集群 catalog 为 "{cluster}.iceberg"
        for_pyiceberg: 是否用于 pyiceberg 客户端
            - True : 供 pyiceberg 使用，会做 type/字段名兼容映射（jdbc→sql, jdbc.user→user）
            - False: 供 Spark 使用，保留 Spark Iceberg 原生字段名

    返回:
        (catalog_name, config_dict)
    """
    catalog_name = get_property(prefix + ".catalog.name")
    catalog_type = get_property(prefix + ".catalog.type").strip().lower()
    catalog_uri = get_property(prefix + ".catalog.uri")
    warehouse = get_property(prefix + ".catalog.warehouse")

    config = {
        "type": catalog_type,
        "uri": catalog_uri,
        "warehouse": warehouse,
    }

    # 按 catalog 类型分支加载专有配置（REST / JDBC 互斥）
    if catalog_type == "rest":
        config.update(_collect_rest_catalog_config(prefix))
    elif catalog_type == "jdbc":
        config.update(_collect_jdbc_catalog_config(prefix))
        if for_pyiceberg:
            # pyiceberg 使用 SqlCatalog（type=sql），字段名也不同，需做兼容映射
            config["type"] = "sql"
            if "jdbc.user" in config:
                config["user"] = config.pop("jdbc.user")
            if "jdbc.password" in config:
                config["password"] = config.pop("jdbc.password")
            # pyiceberg SqlCatalog 不识别以下 Spark 专有字段，需剔除
            config.pop("cache-enabled", None)
            config.pop("jdbc.schema-version", None)
    else:
        raise ValueError(
            f"不支持的 catalog 类型: {catalog_type}，当前支持: rest / jdbc"
        )

    # 存储配置（S3 / MinIO / COS / OSS），与 catalog_type 无关，两种类型都需要
    config.update(_build_storage_config(prefix))

    return catalog_name, config


# 获取iceberg catalog实例
def get_iceberg_client():
    catalog_name, config = _build_catalog_config("iceberg", for_pyiceberg=True)
    return load_catalog(catalog_name, **config)


# 获取iceberg集群catalog实例
def get_iceberg_cluster_client(cluster):
    catalog_name, config = _build_catalog_config(cluster + ".iceberg", for_pyiceberg=True)
    return load_catalog(catalog_name, **config)


def _get_catalog_config(cluster=None):
    """
    获取 catalog 配置（供 PySpark 使用）

    参数:
        cluster: 集群名称，为 None 时使用默认配置

    返回:
        (catalog_name, config_dict) 元组
    """
    prefix = cluster + ".iceberg" if cluster else "iceberg"
    return _build_catalog_config(prefix, for_pyiceberg=False)


# ============================================================
# PySpark SparkSession 管理
# ============================================================

def _build_spark_session(catalog_name, catalog_config):
    """
    构建配置了 Iceberg Catalog 的 SparkSession

    参数:
        catalog_name: catalog 名称，将作为 Spark SQL 中的 catalog 前缀
        catalog_config: dict，包含 catalog 连接配置

    返回:
        SparkSession 实例
    """
    # Iceberg Spark Runtime JAR（首次运行会自动从 Maven 下载）
    iceberg_version = "1.7.1"
    iceberg_spark_jar = f"org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:{iceberg_version}"

    builder = SparkSession.builder \
        .appName(f"iceberg-{catalog_name}") \
        .config("spark.jars.packages", iceberg_spark_jar) \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config(f"spark.sql.catalog.{catalog_name}", "org.apache.iceberg.spark.SparkCatalog")

    # 设置 catalog 类型（REST / JDBC，互斥）
    catalog_type = catalog_config.get("type", "rest")
    builder = builder.config(f"spark.sql.catalog.{catalog_name}.type", catalog_type)

    # 设置 catalog URI
    uri = catalog_config.get("uri")
    if uri:
        builder = builder.config(f"spark.sql.catalog.{catalog_name}.uri", uri)

    # 设置 warehouse
    warehouse = catalog_config.get("warehouse")
    if warehouse:
        builder = builder.config(f"spark.sql.catalog.{catalog_name}.warehouse", warehouse)

    # 按 catalog 类型分支加载专有配置（REST / JDBC 互斥）
    if catalog_type == "rest":
        # REST Catalog 认证配置
        credential = catalog_config.get("credential")
        if credential:
            builder = builder.config(f"spark.sql.catalog.{catalog_name}.credential", credential)

        token = catalog_config.get("token")
        if token:
            builder = builder.config(f"spark.sql.catalog.{catalog_name}.token", token)

        scope = catalog_config.get("scope")
        if scope:
            builder = builder.config(f"spark.sql.catalog.{catalog_name}.scope", scope)

        # header 配置（如禁用 Credential Vending，仅 REST Catalog 有意义）
        for key, value in catalog_config.items():
            if key.startswith("header."):
                builder = builder.config(f"spark.sql.catalog.{catalog_name}.{key}", value)

    elif catalog_type == "jdbc":
        # JDBC Catalog 配置（jdbc.user / jdbc.password / jdbc.*）
        for key, value in catalog_config.items():
            if key.startswith("jdbc."):
                builder = builder.config(f"spark.sql.catalog.{catalog_name}.{key}", value)

        # cache-enabled：JDBC Catalog 场景强烈建议禁用元数据缓存，
        # 避免读取到其他进程 DDL 后的过期表结构
        cache_enabled = catalog_config.get("cache-enabled")
        if cache_enabled is not None:
            builder = builder.config(
                f"spark.sql.catalog.{catalog_name}.cache-enabled", cache_enabled
            )

    # 存储相关配置（S3 / MinIO / COS / OSS），与 catalog_type 无关
    for key, value in catalog_config.items():
        if key.startswith("s3.") or key.startswith("fs."):
            builder = builder.config(f"spark.sql.catalog.{catalog_name}.{key}", value)

    # Spark SQL 全局配置（Iceberg 场景通用，REST/JDBC 都需要）
    #   - maxToStringFields: 打印宽表 schema 不被截断
    #   - caseSensitive:     Iceberg 表列名大小写敏感，与 Spark 默认行为对齐
    builder = builder.config("spark.sql.debug.maxToStringFields", "300")
    builder = builder.config("spark.sql.caseSensitive", "true")

    # 设置默认 catalog
    builder = builder.config("spark.sql.defaultCatalog", catalog_name)

    spark = builder.getOrCreate()
    return spark


def get_spark_session(catalog_name, catalog_config):
    """
    获取或创建 SparkSession（线程安全，按 catalog_name 缓存）

    参数:
        catalog_name: catalog 名称
        catalog_config: catalog 配置字典

    返回:
        SparkSession 实例
    """
    with _lock:
        if catalog_name in _spark_sessions:
            session = _spark_sessions[catalog_name]
            # 检查 session 是否仍然有效
            try:
                session.sql("SELECT 1")
                return session
            except Exception:
                # session 已失效，重新创建
                del _spark_sessions[catalog_name]

        session = _build_spark_session(catalog_name, catalog_config)
        _spark_sessions[catalog_name] = session
        return session


# ============================================================
# Iceberg SQL 执行（基于 PySpark）
# ============================================================

def execute_iceberg_sql(sql, cluster=None):
    """
    通过 PySpark 执行 Iceberg SQL 语句

    支持 Spark SQL 的所有 Iceberg 语法，包括但不限于:
      - CREATE TABLE / CREATE TABLE IF NOT EXISTS
      - DROP TABLE / DROP TABLE IF EXISTS
      - ALTER TABLE (ADD/DROP/RENAME COLUMN, SET TBLPROPERTIES, ...)
      - INSERT INTO / INSERT OVERWRITE
      - SELECT / MERGE INTO
      - CREATE/DROP NAMESPACE/SCHEMA/DATABASE

    参数:
        sql: SQL 语句字符串（Spark SQL 语法）
        cluster: 集群名称，为 None 时使用默认 catalog

    返回:
        pyspark DataFrame（查询结果）

    示例:
        execute_iceberg_sql('''
            CREATE TABLE IF NOT EXISTS my_db.users (
                id BIGINT NOT NULL COMMENT '用户ID',
                name STRING COMMENT '用户名',
                created_at TIMESTAMP
            )
            USING iceberg
            PARTITIONED BY (day(created_at))
        ''')

        # 插入数据
        execute_iceberg_sql("INSERT INTO my_db.users VALUES (1, 'test', current_timestamp())")

        # 查询数据
        df = execute_iceberg_sql("SELECT * FROM my_db.users")
        df.show()
    """
    catalog_name, catalog_config = _get_catalog_config(cluster)
    spark = get_spark_session(catalog_name, catalog_config)
    sql = sql.strip().rstrip(';')
    return spark.sql(sql)


def execute_iceberg_sql_batch(sql_statements, cluster=None):
    """
    批量执行多条 Iceberg SQL 语句

    参数:
        sql_statements: SQL 语句列表 或 用分号分隔的多条 SQL 字符串
        cluster: 集群名称，为 None 时使用默认 catalog

    返回:
        list[DataFrame]: 每条 SQL 的执行结果列表
    """
    if isinstance(sql_statements, str):
        # 按分号拆分（简单拆分，不处理字符串内的分号）
        sql_statements = [s.strip() for s in sql_statements.split(';') if s.strip()]

    catalog_name, catalog_config = _get_catalog_config(cluster)
    spark = get_spark_session(catalog_name, catalog_config)

    results = []
    for sql in sql_statements:
        sql = sql.strip().rstrip(';')
        results.append(spark.sql(sql))
    return results


def stop_iceberg_spark(catalog_name=None):
    """
    停止 Iceberg 使用的 SparkSession

    参数:
        catalog_name: 指定要停止的 catalog 对应的 session，为 None 时停止所有
    """
    with _lock:
        if catalog_name:
            session = _spark_sessions.pop(catalog_name, None)
            if session:
                session.stop()
        else:
            for name, session in _spark_sessions.items():
                try:
                    session.stop()
                except Exception:
                    pass
            _spark_sessions.clear()
