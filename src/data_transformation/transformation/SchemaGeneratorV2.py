#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Standalone ClickHouse schema generator (numeric + string columns) with per-customer string key mapping export.
Connection via clickhouse_connect (get_client/ping/command/query).
"""

import argparse
import json
import logging
import os
from datetime import datetime
from typing import Dict, List, Optional

import clickhouse_connect  # 参考脚本同款依赖

# -----------------------------
# ClickHouse client helpers
# -----------------------------
def build_client(host: str, port: int, username: str, password: str, database: str, timeout: int):
    """
    建立 ClickHouse 客户端（HTTP），与参考脚本一致的参数命名。
    """
    return clickhouse_connect.get_client(
        host=host,
        port=port,
        username=username,
        password=password,
        database=database,
        send_receive_timeout=timeout,
    )

def test_connection(client) -> bool:
    try:
        client.ping()
        return True
    except Exception as e:
        logging.error("Ping ClickHouse 失败: %s", e)
        return False

def describe_table(client, table_name: str) -> List[Dict[str, str]]:
    """
    使用 DESCRIBE TABLE 获取列信息；解析 name/type 字段。
    """
    res = client.query(f"DESCRIBE TABLE {table_name}")
    schema = []
    # DESCRIBE 返回列：name, type, default_type, default_expression, comment, codec_expression, ttl_expression
    for row in res.result_rows:
        # 容错：只取前两个
        name = str(row[0])
        col_type = str(row[1])
        schema.append({"name": name, "type": col_type})
    return schema

def makedirs(path: str) -> None:
    os.makedirs(path, exist_ok=True)

def dedupe_preserve_order(seq: List[str]) -> List[str]:
    return list(dict.fromkeys(seq))


# -----------------------------
# Mapping normalize helpers
# -----------------------------
def normalize_string_mapping(raw: Dict) -> Dict:
    """
    归一化字符串映射为：
    {
      "customers": { "<id>": {"key_count": int, "keys": [str, ...]}, ... },
      "max_keys_per_customer": int,
      "metadata": {...}
    }
    支持两种输入结构：
      A) {"customers": {...}, "max_keys_per_customer": K, ...}
      B) {"<id>": {...}, "max_keys_per_customer": K, "metadata": {...}}  // 顶层平铺
    """
    if not isinstance(raw, dict):
        raise RuntimeError("String mapping must be a JSON object.")
    if "customers" in raw and isinstance(raw["customers"], dict):
        customers = raw["customers"]
        max_keys = int(raw.get("max_keys_per_customer", 0))
        meta = raw.get("metadata", {})
    else:
        known_meta = {"max_keys_per_customer", "metadata"}
        customers = {k: v for k, v in raw.items() if k not in known_meta}
        max_keys = int(raw.get("max_keys_per_customer", 0))
        meta = raw.get("metadata", {})

    for cid, entry in customers.items():
        if not isinstance(entry, dict):
            raise RuntimeError(f"String mapping customer '{cid}' value must be object.")
        keys = entry.get("keys", [])
        if not isinstance(keys, list):
            raise RuntimeError(f"String mapping customer '{cid}' missing/invalid 'keys' list.")
        keys = [str(k) for k in keys]
        keys = dedupe_preserve_order(keys)
        entry["keys"] = keys
        kc = int(entry.get("key_count", len(keys)))
        if kc != len(keys):
            logging.warning(
                "Customer '%s' key_count=%s != len(keys)=%s，已按 len(keys) 修正。",
                cid, kc, len(keys)
            )
            entry["key_count"] = len(keys)

    return {"customers": customers, "max_keys_per_customer": max_keys, "metadata": meta}

def compute_required_string_columns(norm: Optional[Dict]) -> int:
    if not norm:
        return 0
    customers = norm.get("customers", {})
    observed_max = 0
    for _, entry in customers.items():
        kc = int(entry.get("key_count", len(entry.get("keys", []))))
        observed_max = max(observed_max, kc)
    declared_max = int(norm.get("max_keys_per_customer", 0))
    return max(observed_max, declared_max)


# -----------------------------
# Core generator
# -----------------------------
class SchemaGeneratorV2:
    def __init__(
            self,
            numeric_mapping_file: str,
            string_mapping_file: Optional[str],
            client,
            target_table: str,
            drop_before_create: bool = False,
            use_low_cardinality_strings: bool = False,
            export_string_mapping_path: Optional[str] = None,
    ):
        self.client = client
        self.target_table = target_table
        self.drop_before_create = drop_before_create
        self.use_lc = use_low_cardinality_strings
        self.export_string_mapping_path = export_string_mapping_path

        # 读取数值映射
        with open(numeric_mapping_file, "r", encoding="utf-8") as f:
            self.numeric_mapping = json.load(f)
        if not isinstance(self.numeric_mapping, dict) or "max_columns" not in self.numeric_mapping or "customers" not in self.numeric_mapping:
            raise RuntimeError("Numeric mapping 必须包含 'max_columns' 与 'customers'。")

        maxc = self.numeric_mapping["max_columns"]
        self.int_columns = int(maxc.get("int_columns", 0))
        self.float_columns = int(maxc.get("float_columns", 0))
        if self.int_columns < 0 or self.float_columns < 0:
            raise RuntimeError("int_columns/float_columns 必须为非负。")

        # 读取字符串映射（可选）
        self.string_norm = None
        self.string_columns = 0
        if string_mapping_file:
            with open(string_mapping_file, "r", encoding="utf-8") as f:
                raw = json.load(f)
            self.string_norm = normalize_string_mapping(raw)
            self.string_columns = compute_required_string_columns(self.string_norm)

        logging.info(
            "Loaded max columns -> int=%d, float=%d, string=%d",
            self.int_columns, self.float_columns, self.string_columns
        )

    def generate_create_table_ddl(self) -> str:
        logging.info("Generating DDL for %s", self.target_table)
        ddl_parts = [f"CREATE TABLE IF NOT EXISTS {self.target_table} ("]
        cols = []

        # 标准元数据列（保持与原版一致）
        cols.extend([
            "timestampMs DateTime64(3) CODEC(ZSTD(1))",
            "flowId LowCardinality(String) CODEC(ZSTD(1))",
            "flowStartTimeMs DateTime64(3) CODEC(ZSTD(1))",
            "customerId Int32 CODEC(ZSTD(1))",
            "clientId String CODEC(ZSTD(1))",
            "sessionId UInt256 CODEC(ZSTD(1))",
            "inSession UInt8 CODEC(ZSTD(1))",
            "userSessionId Int64 CODEC(ZSTD(1))",
            "inUserSession UInt8 CODEC(ZSTD(1))",
            "platform LowCardinality(String) CODEC(ZSTD(1))",
            "platformSubcategory LowCardinality(String) CODEC(ZSTD(1))",
            "appName LowCardinality(String) CODEC(ZSTD(1))",
            "appBuild LowCardinality(String) CODEC(ZSTD(1))",
            "appVersion LowCardinality(String) CODEC(ZSTD(1))",
            "browserName LowCardinality(String) DEFAULT '' CODEC(ZSTD(1))",
            "browserVersion LowCardinality(String) DEFAULT '' CODEC(ZSTD(1))",
            "userId String DEFAULT '' CODEC(ZSTD(1))",
            "deviceManufacturer LowCardinality(String) DEFAULT '' CODEC(ZSTD(1))",
            "deviceMarketingName LowCardinality(String) DEFAULT '' CODEC(ZSTD(1))",
            "deviceModel LowCardinality(String) DEFAULT '' CODEC(ZSTD(1))",
            "deviceHardwareType LowCardinality(String) DEFAULT '' CODEC(ZSTD(1))",
            "deviceName LowCardinality(String) DEFAULT '' CODEC(ZSTD(1))",
            "deviceCategory LowCardinality(String) DEFAULT '' CODEC(ZSTD(1))",
            "deviceOperatingSystem LowCardinality(String) DEFAULT '' CODEC(ZSTD(1))",
            "deviceOperatingSystemVersion LowCardinality(String) DEFAULT '' CODEC(ZSTD(1))",
            "deviceOperatingSystemFamily LowCardinality(String) DEFAULT '' CODEC(ZSTD(1))",
            "country Int64 DEFAULT 0 CODEC(ZSTD(1))",
            "state Int64 DEFAULT 0 CODEC(ZSTD(1))",
            "city Int64 DEFAULT 0 CODEC(ZSTD(1))",
            "countryIso LowCardinality(String) CODEC(ZSTD(1))",
            "sub1Iso LowCardinality(String) CODEC(ZSTD(1))",
            "sub2Iso String CODEC(ZSTD(1))",
            "cityGid Int32 CODEC(ZSTD(1))",
            "dma Int16 CODEC(ZSTD(1))",
            "postalCode String DEFAULT '' CODEC(ZSTD(1))",
            "isp Int32 DEFAULT 0 CODEC(ZSTD(1))",
            "netSpeed LowCardinality(String) DEFAULT '' CODEC(ZSTD(1))",
            "sensorVersion LowCardinality(String) CODEC(ZSTD(1))",
            "appType LowCardinality(String) CODEC(ZSTD(1))",
            "asn Int32 DEFAULT 0 CODEC(ZSTD(1))",
            "timezoneOffsetMins Int32 DEFAULT 0 CODEC(ZSTD(1))",
            "connType Int32 DEFAULT 0 CODEC(ZSTD(1))",
            "watermarkMs DateTime64(3) CODEC(ZSTD(1))",
            "partitionId Int32 CODEC(ZSTD(1))",
            "retentionDate Date CODEC(ZSTD(1))",
        ])

        # 数值扩展列
        for i in range(1, self.int_columns + 1):
            cols.append(f"int{i} Int32 DEFAULT 0 CODEC(ZSTD(1))")
        for i in range(1, self.float_columns + 1):
            cols.append(f"float{i} Float32 DEFAULT 0 CODEC(ZSTD(1))")

        # 字符串扩展列
        if self.string_columns > 0:
            coltype = "LowCardinality(String)" if self.use_lc else "String"
            for i in range(1, self.string_columns + 1):
                cols.append(f"string{i} {coltype} DEFAULT '' CODEC(ZSTD(1))")

        ddl_parts.append("    " + ",\n    ".join(cols))
        ddl_parts.extend([
            ") ENGINE = MergeTree()",
            "PARTITION BY toYYYYMM(timestampMs)",
            "ORDER BY (customerId, clientId, sessionId, timestampMs)",
            "SETTINGS index_granularity = 8192;"
        ])

        ddl = "\n".join(ddl_parts)
        logging.info("DDL generated.")
        return ddl

    def build_and_export_string_mapping(self) -> Optional[str]:
        if not self.string_norm:
            logging.info("未提供 string-mapping，跳过导出。")
            return None

        out = {
            "max_string_columns": self.string_columns,
            "customers": {}
        }
        for cid, entry in self.string_norm.get("customers", {}).items():
            keys = dedupe_preserve_order([str(k) for k in entry.get("keys", [])])
            mapping = {k: f"string{i+1}" for i, k in enumerate(keys)}
            reverse = {f"string{i+1}": k for i, k in enumerate(keys)}
            out["customers"][cid] = {
                "string_columns": len(keys),
                "string_keys": keys,
                "string_mapping": mapping,
                "reverse_string_mapping": reverse,
            }

        # 选择输出路径
        if self.export_string_mapping_path:
            final_path = self.export_string_mapping_path
        else:
            makedirs("output/reports")
            table_suffix = self.target_table.split(".")[-1]
            final_path = f"output/reports/string_key_mapping_{table_suffix}.json"

        makedirs(os.path.dirname(final_path))
        with open(final_path, "w", encoding="utf-8") as f:
            json.dump(out, f, indent=2, ensure_ascii=False)
        logging.info("已导出字符串键映射: %s", final_path)
        return final_path

    def create_target_table(self) -> bool:
        try:
            if not test_connection(self.client):
                logging.error("连接 ClickHouse 失败。")
                return False

            if self.drop_before_create:
                try:
                    self.client.command(f"DROP TABLE IF EXISTS {self.target_table}")
                    logging.info("已尝试删除旧表（如存在）。")
                except Exception as e:
                    logging.warning("DROP 失败（继续）：%s", e)

            ddl = self.generate_create_table_ddl()
            makedirs("output/reports")
            ddl_file = f"output/reports/create_table_{self.target_table.split('.')[-1]}.sql"
            with open(ddl_file, "w", encoding="utf-8") as f:
                f.write(ddl)
            logging.info("DDL 已保存到: %s", ddl_file)

            self.client.command(ddl)
            logging.info("表创建成功。")

            # 验证
            schema = describe_table(self.client, self.target_table)
            logging.info("验证：列数=%d。", len(schema))
            return True
        except Exception as e:
            logging.error("创建表失败：%s", e)
            return False

    def write_table_info(self) -> Optional[str]:
        try:
            schema = describe_table(self.client, self.target_table)
            info = {
                "table_name": self.target_table,
                "total_columns": len(schema),
                "int_columns": sum(1 for c in schema if c["name"].startswith("int")),
                "float_columns": sum(1 for c in schema if c["name"].startswith("float")),
                "string_columns": sum(1 for c in schema if c["name"].startswith("string")),
                "metadata_columns": sum(1 for c in schema if not c["name"].startswith(("int","float","string"))),
                "created_at": datetime.now().isoformat()
            }
            makedirs("output/reports")
            path = "output/reports/table_info.json"
            with open(path, "w", encoding="utf-8") as f:
                json.dump(info, f, indent=2, ensure_ascii=False)
            logging.info("表信息已保存到: %s", path)
            return path
        except Exception as e:
            logging.error("写入表信息失败：%s", e)
            return None


# -----------------------------
# CLI
# -----------------------------
def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Generate ClickHouse table (numeric + string) and export per-customer string key mapping (clickhouse_connect)."
    )
    # 映射输入
    p.add_argument("--numeric-mapping", required=True, help="数值映射 JSON（包含 max_columns/customers）")
    p.add_argument("--string-mapping", required=False, help="字符串映射 JSON（可选）")
    p.add_argument("--export-string-mapping", required=False, help="导出每客户 stringN 映射的输出路径")
    # 目标表/DDL
    p.add_argument("--target-table", required=True, help="目标表名（db.table 或 table）")
    p.add_argument("--drop", action="store_true", help="创建前先 DROP 旧表")
    p.add_argument("--lowcard-strings", action="store_true", help="stringN 使用 LowCardinality(String)")
    # ClickHouse 连接参数（与参考脚本命名一致）
    p.add_argument("--host", default="localhost", help="ClickHouse host")
    p.add_argument("--port", type=int, default=8123, help="ClickHouse HTTP port")
    p.add_argument("--username", default="default", help="ClickHouse username")
    p.add_argument("--password", default="", help="ClickHouse password")
    p.add_argument("--database", default="default", help="缺省数据库上下文")
    p.add_argument("--timeout", type=int, default=300, help="send/receive 超时（秒）")
    # 日志
    p.add_argument("--log-level", default="INFO", help="日志级别（DEBUG/INFO/WARNING/ERROR）")
    return p.parse_args()

def main():
    args = parse_args()
    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s - %(levelname)s - %(message)s",
    )

    client = build_client(args.host, args.port, args.username, args.password, args.database, args.timeout)

    gen = SchemaGeneratorV2(
        numeric_mapping_file=args.numeric_mapping,
        string_mapping_file=args.string_mapping,
        client=client,
        target_table=args.target_table,
        drop_before_create=args.drop,
        use_low_cardinality_strings=args.lowcard_strings,
        export_string_mapping_path=args.export_string_mapping,
    )

    gen.build_and_export_string_mapping()
    ok = gen.create_target_table()
    if ok:
        gen.write_table_info()
    else:
        raise SystemExit(1)

    # 关闭连接
    try:
        client.close()
    except Exception:
        pass


if __name__ == "__main__":
    main()
