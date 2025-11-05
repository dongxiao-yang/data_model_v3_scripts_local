import argparse
import json
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Set, Tuple
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
import clickhouse_connect

LOG = logging.getLogger("tag_group_key_discoverer")

# 默认值设置
DEFAULTS = {
    "host": "clickhouse-ds-00.us-east4.prod.gcp.conviva.com",
    "port": 8123,
    "username": "default",
    "password": "",
    "database": "default",
    "timeout": 300,
    "source_table": "eco_cross_page_flow_pt1m_local",
    "output": "output/mappings/tag_group_key_mapping.json",
    "date": None,
}

# 建立ClickHouse客户端
def build_client(host: str, port: int, username: str, password: str, database: str, timeout: int):
    """建立ClickHouse客户端"""
    return clickhouse_connect.get_client(
        host=host,
        port=port,
        username=username,
        password=password,
        database=database,
        send_receive_timeout=timeout,
    )

# 测试与ClickHouse的连接
def test_connection(client) -> bool:
    """测试与ClickHouse的连接"""
    try:
        client.ping()
        return True
    except Exception as e:
        LOG.error("Ping ClickHouse失败: %s", e)
        return False

# 生成时间块（按天）
def build_chunks_for_day(date_str: str, chunk_minutes: int) -> List[Tuple[str, str]]:
    """为单一天生成时间块"""
    if 1440 % chunk_minutes != 0:
        raise ValueError(f"chunk-mins必须能整除1440（一天的总分钟数），当前: {chunk_minutes}")
    if chunk_minutes >= 60 and (chunk_minutes % 60 != 0):
        raise ValueError(f"chunk-mins >= 60时，必须是60的倍数，当前: {chunk_minutes}")

    day_start = datetime.strptime(date_str, "%Y-%m-%d")
    chunks = []
    total = 1440 // chunk_minutes
    for i in range(total):
        start = day_start + timedelta(minutes=i * chunk_minutes)
        end = start + timedelta(minutes=chunk_minutes) - timedelta(seconds=1)
        chunks.append((start.strftime("%Y-%m-%d %H:%M:%S"), end.strftime("%Y-%m-%d %H:%M:%S")))
    return chunks

# 生成日期范围
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

# 生成一天的起始和结束时间
def day_range(date_str: str) -> Tuple[str, str]:
    """生成一天的起始和结束时间"""
    start = datetime.strptime(date_str, "%Y-%m-%d")
    next_day = start + timedelta(days=1)
    return start.strftime("%Y-%m-%d 00:00:00"), next_day.strftime("%Y-%m-%d 00:00:00")

# 查询指定日期的所有客户ID
def fetch_all_customers_for_date(client, source_table: str, date_str: str) -> List[int]:
    """获取指定日期的所有客户ID"""
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

# 生成 UNION ALL 查询 SQL（一次查询所有 tag group 键）
def build_union_all_keys_sql(source_table: str, start_ts: str, end_ts: str, customer_id: int) -> str:
    """
    生成 UNION ALL + DISTINCT 的键提取 SQL，针对所有 tag group。
    """
    # 构造 tagGroup1 到 tagGroup15 的列名
    tag_groups = [f"tagGroup{i}" for i in range(1, 16)]
    parts = []

    for tag_group in tag_groups:
        parts.append(
            f"""
            SELECT arrayJoin(mapKeys({tag_group})) AS key
            FROM {source_table}
            WHERE timestampMs >= '{start_ts}'
              AND timestampMs <= '{end_ts}'
              AND customerId = {customer_id}
            """
        )

    union_body = "\nUNION ALL\n".join(parts)
    sql = f"SELECT DISTINCT key FROM (\n{union_body}\n)"
    return sql

# 执行查询并返回所有tag group的键
def _run_chunk_query(conn_params: Dict, source_table: str, customer_id: int, start_ts: str, end_ts: str) -> Set[str]:
    """执行单个时间块的查询，返回所有tag group的唯一键"""
    client = build_client(**conn_params)
    try:
        # 获取所有tag group的键
        sql = build_union_all_keys_sql(source_table, start_ts, end_ts, customer_id)
        res = client.query(sql)
        keys = {str(r[0]) for r in res.result_rows if r and str(r[0]).strip()}
        return keys
    finally:
        try:
            client.close()
        except Exception:
            pass

# 并发执行查询任务，收集所有tag group的键
def discover_all_keys_for_customer_day_mt(conn_params: Dict,
                                          source_table: str,
                                          date_str: str,
                                          customer_id: int,
                                          chunk_minutes: int,
                                          max_workers: int) -> Set[str]:
    """并发执行查询任务，收集指定日期所有tag group的键"""
    chunks = build_chunks_for_day(date_str, chunk_minutes)
    total_chunks = len(chunks)
    LOG.info("客户 %s，日期 %s：开始（并发=%d），总分片=%d，chunk-mins=%d",
             customer_id, date_str, max_workers, total_chunks, chunk_minutes)

    all_keys: Set[str] = set()

    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futures = []
        for (start_ts, end_ts) in chunks:
            futures.append(ex.submit(_run_chunk_query, conn_params, source_table, customer_id, start_ts, end_ts))

        done = 0
        for fut in as_completed(futures):
            try:
                keys = fut.result()
                if keys:
                    all_keys.update(keys)
            except Exception as e:
                LOG.warning("客户 %s，日期 %s：某分片任务失败：%s", customer_id, date_str, e)
            finally:
                done += 1
                if done % 10 == 0 or done == total_chunks:
                    LOG.info("客户 %s，日期 %s：进度 %d/%d，累计 keys：%d",
                             customer_id, date_str, done, total_chunks, len(all_keys))

    return all_keys

# 生成嵌套映射（最终落盘）
def build_nested_mapping(source_table: str,
                         customer_to_keys: Dict[int, Set[str]],
                         dates_processed: List[str]) -> Dict:
    customers_block: Dict[str, Dict] = {}
    max_keys_per_customer = 0

    for cid, keys in customer_to_keys.items():
        customers_block[str(cid)] = {
            "keys": sorted(keys),
            "key_count": len(keys)
        }
        max_keys_per_customer = max(max_keys_per_customer, len(keys))

    return {
        "metadata": {
            "generated_at": datetime.now().isoformat(),
            "source_table": source_table,
            "dates": dates_processed,
            "note": "每个客户的tag group键映射。"
        },
        "max_keys_per_customer": max_keys_per_customer,
        "customers": customers_block
    }

# 解析命令行参数
def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Discover tag group keys for ALL customers from ClickHouse map columns (multithreaded, memory-friendly), with multi-day support."
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
    p.add_argument("--output", default=DEFAULTS.get("output", "output/mappings/tag_group_key_mapping.json"),
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

    # 连接 ClickHouse 并验证连接
    main_client = build_client(args.host, args.port, args.username, args.password, args.database, args.timeout)
    if not test_connection(main_client):
        raise SystemExit("连接 ClickHouse 失败，请检查网络/凭据/权限")

    # 逐日处理
    customer_to_keys: Dict[int, Set[str]] = {}

    for d in dates:
        # 获取当天所有客户
        customers = fetch_all_customers_for_date(main_client, args.source_table, d)
        if not customers:
            LOG.warning("日期 %s 未发现任何客户，跳过。", d)
            continue

        # 逐客户并发处理
        conn_params = {
            "host": args.host,
            "port": args.port,
            "username": args.username,
            "password": args.password,
            "database": args.database,
            "timeout": args.timeout,
        }

        for cid in customers:
            all_keys = discover_all_keys_for_customer_day_mt(
                conn_params,
                source_table=args.source_table,
                date_str=d,
                customer_id=cid,
                chunk_minutes=args.chunk_mins,
                max_workers=args.max_workers,
            )
            customer_to_keys[cid] = all_keys

    # 主连接可关闭
    try:
        main_client.close()
    except Exception:
        pass

    # 生成映射并保存
    mapping = build_nested_mapping(args.source_table, customer_to_keys, dates_processed=dates)
    os.makedirs(os.path.dirname(args.output), exist_ok=True)
    with open(args.output, "w", encoding="utf-8") as f:
        json.dump(mapping, f, ensure_ascii=False, indent=2, sort_keys=True)

    LOG.info("已保存映射文件：%s", args.output)
    LOG.info("最大键数量：%d", mapping["max_keys_per_customer"])
    LOG.info("完成！")

if __name__ == "__main__":
    main()
