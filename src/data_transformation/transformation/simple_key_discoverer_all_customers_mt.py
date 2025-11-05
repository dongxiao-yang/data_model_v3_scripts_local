#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Simple key discovery (ALL customers, multithreaded, memory-friendly) with multi-day support.

特性
- 自动发现“每天”出现过的所有 customerId；支持单日 --date 或多日区间 --date-start/--date-end（含端点）。
- 将每一天按 chunk-mins 切分（允许 >60，必须是 60 的倍数，且能整除 1440），
  每个时间片并发执行 SQL（ThreadPoolExecutor），每个任务独立创建 ClickHouse client。
- 使用 UNION ALL + DISTINCT + arrayJoin(mapKeys(...))，避免 groupArray/flatten 的大数组。
- 跨多天：对同一客户的 keys 做并集，产出 per-customer 的 int/float 映射与全体 max_columns。

依赖
    pip install clickhouse-connect

示例
    # 单天
    python simple_key_discoverer_all_customers_mt.py \
      --date 2025-10-08 \
      --source-table default.eco_cross_page_flow_pt1m_local_20251008_3cust \
      --output output/mappings/key_mapping_mul_cust.json \
      --chunk-mins 120 --max-workers 8 --log-level INFO

    # 多天（含端点）
    python simple_key_discoverer_all_customers_mt.py \
      --date-start 2025-10-08 --date-end 2025-10-10 \
      --source-table default.eco_cross_page_flow_pt1m_local_20251008_3cust \
      --output output/mappings/key_mapping_mul_cust.json \
      --chunk-mins 180 --max-workers 8 --log-level INFO
"""

import argparse
import json
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Set, Tuple

import os
from concurrent.futures import ThreadPoolExecutor, as_completed

# 可选：从项目 settings 读取默认值（若存在）
DEFAULTS: Dict[str, object] = {}
try:
    from src.config import settings as _settings  # type: ignore
    DEFAULTS.update({
        "host": _settings.SOURCE_DB.host,
        "port": _settings.SOURCE_DB.port,
        "username": _settings.SOURCE_DB.username,
        "password": _settings.SOURCE_DB.password,
        "database": _settings.SOURCE_DB.database,
        "timeout": _settings.SOURCE_DB.timeout,
        "source_table": _settings.SOURCE_TABLE,
        "output": getattr(_settings, "KEY_MAPPING_FILE", "output/mappings/key_mapping.json"),
        "date": getattr(_settings, "TRANSFORMATION_DATE", None),
    })
except Exception:
    pass

import clickhouse_connect

LOG = logging.getLogger("key_discoverer_all_customers_mt")

# ---------------------------
# ClickHouse 连接
# ---------------------------
def build_client(host: str, port: int, username: str, password: str, database: str, timeout: int):
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
        LOG.error("Ping ClickHouse 失败: %s", e)
        return False

# ---------------------------
# 时间片与日期工具
# ---------------------------
def build_chunks_for_day(date_str: str, chunk_minutes: int) -> List[Tuple[str, str]]:
    """
    生成一天内的时间片 [start, end]（end 为当片最后一秒）。
    约束：
      - chunk_minutes 必须能整除 1440；
      - 若 chunk_minutes >= 60，则还必须是 60 的倍数。
    """
    if 1440 % chunk_minutes != 0:
        raise ValueError(f"chunk-mins 必须能整除 1440（一天总分钟数），当前: {chunk_minutes}")
    if chunk_minutes >= 60 and (chunk_minutes % 60 != 0):
        raise ValueError(f"chunk-mins >= 60 时必须是 60 的倍数，当前: {chunk_minutes}")
    day_start = datetime.strptime(date_str, "%Y-%m-%d")
    chunks = []
    total = 1440 // chunk_minutes
    for i in range(total):
        start = day_start + timedelta(minutes=i * chunk_minutes)
        end = start + timedelta(minutes=chunk_minutes) - timedelta(seconds=1)
        chunks.append((start.strftime("%Y-%m-%d %H:%M:%S"), end.strftime("%Y-%m-%d %H:%M:%S")))
    return chunks

def iter_days(date: str = None, date_start: str = None, date_end: str = None) -> List[str]:
    """
    返回需要处理的日期列表（YYYY-MM-DD）。
    - 若提供 --date，则返回单元素列表；
    - 否则使用 --date-start 与 --date-end 构建闭区间日期列表；
    - 若均未提供，抛错。
    """
    if date:
        # 单天
        _ = datetime.strptime(date, "%Y-%m-%d")  # 校验格式
        return [date]
    if not (date_start and date_end):
        raise SystemExit("请提供 --date（单天）或 --date-start 与 --date-end（多天）。")
    d0 = datetime.strptime(date_start, "%Y-%m-%d")
    d1 = datetime.strptime(date_end, "%Y-%m-%d")
    if d1 < d0:
        raise SystemExit("--date-end 需不早于 --date-start")
    out = []
    cur = d0
    while cur <= d1:
        out.append(cur.strftime("%Y-%m-%d"))
        cur += timedelta(days=1)
    return out

# ---------------------------
# 当天所有 customerId
# ---------------------------
def day_range(date_str: str) -> Tuple[str, str]:
    start = datetime.strptime(date_str, "%Y-%m-%d")
    next_day = start + timedelta(days=1)
    return start.strftime("%Y-%m-%d 00:00:00"), next_day.strftime("%Y-%m-%d 00:00:00")

def fetch_all_customers_for_date(client, source_table: str, date_str: str) -> List[int]:
    start, next_day = day_range(date_str)
    sql = f"""
        SELECT DISTINCT customerId
        FROM {source_table}
        WHERE timestampMs >= '{start}'
          AND timestampMs <  '{next_day}'
    """
    LOG.info("扫描当天所有 customerId（%s ~ 次日 00:00:00）", start)
    res = client.query(sql)
    customers: List[int] = []
    for (cid,) in res.result_rows:
        try:
            customers.append(int(cid))
        except Exception:
            LOG.warning("遇到非整型 customerId 值：%r（忽略）", cid)
    customers.sort()
    LOG.info("发现 %d 个 customerId：%s", len(customers), customers[:20] if len(customers) > 20 else customers)
    return customers

# ---------------------------
# 省内存 SQL（UNION ALL + DISTINCT）
# ---------------------------
def build_union_all_keys_sql(source_table: str, start_ts: str, end_ts: str, customer_id: int, value_type: str) -> str:
    """
    生成 UNION ALL + DISTINCT 的键提取 SQL。
    value_type: "int" 或 "float"
    """
    assert value_type in ("int", "float")
    if value_type == "int":
        cols = [f"metricIntGroup{i}" for i in range(1, 16)]
    else:
        cols = [f"metricFloatGroup{i}" for i in range(1, 16)]

    parts = []
    for c in cols:
        parts.append(
            f"""
            SELECT arrayJoin(mapKeys({c})) AS key
            FROM {source_table}
            WHERE timestampMs >= '{start_ts}'
              AND timestampMs <= '{end_ts}'
              AND customerId = {customer_id}
            """
        )
    union_body = "\nUNION ALL\n".join(parts)
    sql = f"SELECT DISTINCT key FROM (\n{union_body}\n)"
    return sql

# ---------------------------
# 单时间片任务（在线程中执行）
# ---------------------------
def _run_chunk_query(conn_params: Dict, source_table: str, customer_id: int, start_ts: str, end_ts: str) -> Tuple[List[str], List[str]]:
    """
    并发任务：对单个时间片执行两条查询：
      - UNION ALL + DISTINCT 拿所有 int 组的唯一键
      - UNION ALL + DISTINCT 拿所有 float 组的唯一键
    返回 (int_keys, float_keys)
    """
    client = build_client(**conn_params)
    try:
        # int keys
        sql_int = build_union_all_keys_sql(source_table, start_ts, end_ts, customer_id, value_type="int")
        res_int = client.query(sql_int)
        int_keys = [str(r[0]) for r in res_int.result_rows if r and str(r[0]).strip()]

        # float keys
        sql_float = build_union_all_keys_sql(source_table, start_ts, end_ts, customer_id, value_type="float")
        res_float = client.query(sql_float)
        float_keys = [str(r[0]) for r in res_float.result_rows if r and str(r[0]).strip()]

        return (int_keys, float_keys)
    finally:
        try:
            client.close()
        except Exception:
            pass

# ---------------------------
# 并发发现：单 customer / 单天
# ---------------------------
def discover_all_keys_for_customer_day_mt(conn_params: Dict,
                                          source_table: str,
                                          date_str: str,
                                          customer_id: int,
                                          chunk_minutes: int,
                                          max_workers: int) -> Tuple[Set[str], Set[str]]:
    """
    并发版本：对单个 customer 的某一天并行查询所有时间片，并合并结果。
    返回：当天该客户发现到的 (int_key_set, float_key_set)
    """
    chunks = build_chunks_for_day(date_str, chunk_minutes)
    total_chunks = len(chunks)
    LOG.info("客户 %s，日期 %s：开始（并发=%d），总分片=%d，chunk-mins=%d",
             customer_id, date_str, max_workers, total_chunks, chunk_minutes)

    int_keys: Set[str] = set()
    float_keys: Set[str] = set()

    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futures = []
        for (start_ts, end_ts) in chunks:
            futures.append(ex.submit(_run_chunk_query, conn_params, source_table, customer_id, start_ts, end_ts))

        done = 0
        for fut in as_completed(futures):
            try:
                ints, floats = fut.result()
                if ints:
                    int_keys.update(ints)
                if floats:
                    float_keys.update(floats)
            except Exception as e:
                LOG.warning("客户 %s，日期 %s：某分片任务失败：%s", customer_id, date_str, e)
            finally:
                done += 1
                if done % 10 == 0 or done == total_chunks:
                    LOG.info("客户 %s，日期 %s：进度 %d/%d，累计 keys：int=%d, float=%d",
                             customer_id, date_str, done, total_chunks, len(int_keys), len(float_keys))

    return int_keys, float_keys

# ---------------------------
# 生成嵌套映射（最终落盘）
# ---------------------------
def build_nested_mapping(source_table: str,
                         customer_to_keys: Dict[int, Tuple[Set[str], Set[str]]],
                         dates_processed: List[str]) -> Dict:
    customers_block: Dict[str, Dict] = {}
    max_int_cols = 0
    max_float_cols = 0

    for cid, (int_set, float_set) in customer_to_keys.items():
        uniq_int = sorted(int_set)
        uniq_float = sorted(float_set)

        int_mapping = {k: f"int{i}" for i, k in enumerate(uniq_int, 1)}
        float_mapping = {k: f"float{i}" for i, k in enumerate(uniq_float, 1)}
        rev_int = {v: k for k, v in int_mapping.items()}
        rev_float = {v: k for k, v in float_mapping.items()}

        customers_block[str(cid)] = {
            "int_keys": uniq_int,
            "float_keys": uniq_float,
            "int_mapping": int_mapping,
            "float_mapping": float_mapping,
            "reverse_int_mapping": rev_int,
            "reverse_float_mapping": rev_float,
            "int_columns": len(uniq_int),
            "float_columns": len(uniq_float),
        }

        max_int_cols = max(max_int_cols, len(uniq_int))
        max_float_cols = max(max_float_cols, len(uniq_float))

    return {
        "metadata": {
            "generated_at": datetime.now().isoformat(),
            "source_table": source_table,
            "dates": dates_processed,
            "note": "Per-customer mappings across multiple days; each customer independently maps metrics to column positions starting from int1/float1. Table uses max columns across all customers."
        },
        "max_columns": {
            "int_columns": max_int_cols,
            "float_columns": max_float_cols
        },
        "customers": customers_block
    }

# ---------------------------
# CLI
# ---------------------------
def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Discover metric keys for ALL customers from ClickHouse map columns (multithreaded, memory-friendly), with multi-day support."
    )
    p.add_argument("--host", default=DEFAULTS.get("host"), help="ClickHouse host")
    p.add_argument("--port", type=int, default=DEFAULTS.get("port", 8123), help="ClickHouse HTTP port")
    p.add_argument("--username", default=DEFAULTS.get("username", "default"), help="ClickHouse username")
    p.add_argument("--password", default=DEFAULTS.get("password", ""), help="ClickHouse password")
    p.add_argument("--database", default=DEFAULTS.get("database", "default"), help="Default database")
    p.add_argument("--timeout", type=int, default=DEFAULTS.get("timeout", 300), help="send/receive timeout (sec)")

    p.add_argument("--source-table", default=DEFAULTS.get("source_table"),
                   help="Fully qualified table name containing map columns")

    # 日期：单日或区间（二选一）
    p.add_argument("--date", default=DEFAULTS.get("date"),
                   help="YYYY-MM-DD date to process (single day)")
    p.add_argument("--date-start", help="YYYY-MM-DD start date (inclusive) for multi-day scan")
    p.add_argument("--date-end", help="YYYY-MM-DD end date (inclusive) for multi-day scan")

    p.add_argument("--chunk-mins", type=int, default=120,
                   help="Chunk size in minutes; must divide 1440. If >=60, must also be a multiple of 60.")
    p.add_argument("--max-workers", type=int, default=8,
                   help="Max concurrent workers for chunk queries")
    p.add_argument("--output", default=DEFAULTS.get("output", "output/mappings/key_mapping.json"),
                   help="Output JSON path for nested mapping")

    p.add_argument("--log-level", default="INFO", help="Logging level (DEBUG, INFO, WARNING, ERROR)")
    return p.parse_args()

def main():
    args = parse_args()
    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s - %(levelname)s - %(message)s"
    )

    if not args.source_table:
        raise SystemExit("缺少 --source-table，或未在 settings.py 中提供 SOURCE_TABLE")

    # 决定要处理的日期列表
    dates = iter_days(date=args.date, date_start=args.date_start, date_end=args.date_end)
    LOG.info("待处理日期：%s", ", ".join(dates))

    # 预校验 chunk 配置
    _ = build_chunks_for_day(dates[0], args.chunk_mins)  # 仅用于参数校验（每个日子都会再构造）

    LOG.info("连接 ClickHouse: host=%s port=%s user=%s db=%s timeout=%ss",
             args.host, args.port, args.username, args.database, args.timeout)
    # 主连接用于探活与每日的 customer 扫描
    main_client = build_client(args.host, args.port, args.username, args.password, args.database, args.timeout)
    if not test_connection(main_client):
        raise SystemExit("连接 ClickHouse 失败，请检查网络/凭据/权限")

    # 跨多天：为每个客户维护一个全局集合（并集）
    customer_to_keys: Dict[int, Tuple[Set[str], Set[str]]] = {}

    # 逐日处理
    for d in dates:
        # 1) 获取当天所有 customerId
        customers = fetch_all_customers_for_date(main_client, args.source_table, d)
        if not customers:
            LOG.warning("日期 %s 未发现任何客户，跳过。", d)
            continue

        # 2) 逐客户并发跑该日的时间片；把结果并入全局集合
        conn_params = {
            "host": args.host,
            "port": args.port,
            "username": args.username,
            "password": args.password,
            "database": args.database,
            "timeout": args.timeout,
        }

        for cid in customers:
            day_ints, day_floats = discover_all_keys_for_customer_day_mt(
                conn_params,
                source_table=args.source_table,
                date_str=d,
                customer_id=cid,
                chunk_minutes=args.chunk_mins,
                max_workers=args.max_workers,
            )
            if cid not in customer_to_keys:
                customer_to_keys[cid] = (set(), set())
            customer_to_keys[cid][0].update(day_ints)
            customer_to_keys[cid][1].update(day_floats)
            LOG.info("客户 %s，日期 %s 完成：int=%d(+%d), float=%d(+%d)",
                     cid, d,
                     len(customer_to_keys[cid][0]), len(day_ints),
                     len(customer_to_keys[cid][1]), len(day_floats))

    # 主连接可关闭
    try:
        main_client.close()
    except Exception:
        pass

    if not customer_to_keys:
        raise SystemExit("在所给日期范围内未发现任何客户/键，请检查表名或日期是否正确。")

    # 3) 生成映射并落盘
    mapping = build_nested_mapping(args.source_table, customer_to_keys, dates_processed=dates)
    os.makedirs(os.path.dirname(args.output), exist_ok=True)
    with open(args.output, "w", encoding="utf-8") as f:
        json.dump(mapping, f, ensure_ascii=False, indent=2, sort_keys=True)

    LOG.info("已保存映射文件：%s", args.output)
    LOG.info("最大列数：int=%d, float=%d",
             mapping["max_columns"]["int_columns"],
             mapping["max_columns"]["float_columns"])
    LOG.info("完成！")

if __name__ == "__main__":
    main()
