# coding=utf-8
"""
数据处理模块

提供数据读取和检测功能：
- read_all_today_titles: 从存储后端读取当天所有标题
- detect_latest_new_titles: 检测最新批次的新增标题

Author: TrendRadar Team
"""

from datetime import datetime, timedelta
from pathlib import Path
import sqlite3
from typing import Dict, List, Tuple, Optional, Set

from trendradar.utils.url import normalize_url


def read_all_today_titles_from_storage(
    storage_manager,
    current_platform_ids: Optional[List[str]] = None,
) -> Tuple[Dict, Dict, Dict]:
    """
    从存储后端读取当天所有标题（SQLite 数据）

    Args:
        storage_manager: 存储管理器实例
        current_platform_ids: 当前监控的平台 ID 列表（用于过滤）

    Returns:
        Tuple[Dict, Dict, Dict]: (all_results, id_to_name, title_info)
    """
    try:
        news_data = storage_manager.get_today_all_data()

        if not news_data or not news_data.items:
            return {}, {}, {}

        all_results = {}
        final_id_to_name = {}
        title_info = {}

        for source_id, news_list in news_data.items.items():
            # 按平台过滤
            if current_platform_ids is not None and source_id not in current_platform_ids:
                continue

            # 获取来源名称
            source_name = news_data.id_to_name.get(source_id, source_id)
            final_id_to_name[source_id] = source_name

            if source_id not in all_results:
                all_results[source_id] = {}
                title_info[source_id] = {}

            for item in news_list:
                title = item.title
                ranks = item.ranks or [item.rank]
                first_time = item.first_time or item.crawl_time
                last_time = item.last_time or item.crawl_time
                count = item.count
                rank_timeline = item.rank_timeline

                all_results[source_id][title] = {
                    "ranks": ranks,
                    "url": item.url or "",
                    "mobileUrl": item.mobile_url or "",
                }

                title_info[source_id][title] = {
                    "first_time": first_time,
                    "last_time": last_time,
                    "count": count,
                    "ranks": ranks,
                    "url": item.url or "",
                    "mobileUrl": item.mobile_url or "",
                    "rank_timeline": rank_timeline,
                }

        return all_results, final_id_to_name, title_info

    except Exception as e:
        print(f"[存储] 从存储后端读取数据失败: {e}")
        return {}, {}, {}


def read_all_today_titles(
    storage_manager,
    current_platform_ids: Optional[List[str]] = None,
    quiet: bool = False,
) -> Tuple[Dict, Dict, Dict]:
    """
    读取当天所有标题（从存储后端）

    Args:
        storage_manager: 存储管理器实例
        current_platform_ids: 当前监控的平台 ID 列表（用于过滤）
        quiet: 是否静默模式（不打印日志）

    Returns:
        Tuple[Dict, Dict, Dict]: (all_results, id_to_name, title_info)
    """
    all_results, final_id_to_name, title_info = read_all_today_titles_from_storage(
        storage_manager, current_platform_ids
    )

    if not quiet:
        if all_results:
            total_count = sum(len(titles) for titles in all_results.values())
            print(f"[存储] 已从存储后端读取 {total_count} 条标题")
        else:
            print("[存储] 当天暂无数据")

    return all_results, final_id_to_name, title_info


def _parse_news_datetime(date_str: str, time_str: str) -> Optional[datetime]:
    """解析新闻库里的日期和抓取时间。"""
    if not date_str or not time_str:
        return None

    time_candidates = [time_str]
    if "-" in time_str and ":" not in time_str:
        time_candidates.append(time_str.replace("-", ":"))

    for candidate in time_candidates:
        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M"):
            try:
                return datetime.strptime(f"{date_str} {candidate}", fmt)
            except ValueError:
                continue
    return None


def _get_local_news_db_dir(storage_manager) -> Optional[Path]:
    """获取本地新闻 SQLite 目录。"""
    backend = storage_manager.get_backend()
    data_dir = getattr(backend, "data_dir", None) or getattr(storage_manager, "data_dir", None)
    if not data_dir:
        return None

    news_dir = Path(data_dir) / "news"
    if not news_dir.exists():
        return None
    return news_dir


def _collect_recent_url_signatures(
    storage_manager,
    current_platform_ids: Optional[List[str]],
    latest_date: str,
    latest_time: str,
    lookback_hours: int = 48,
) -> Dict[str, Set[str]]:
    """收集最近 lookback_hours 小时内出现过的新闻 URL 签名。"""
    latest_dt = _parse_news_datetime(latest_date, latest_time)
    news_dir = _get_local_news_db_dir(storage_manager)
    if latest_dt is None or news_dir is None:
        return {}

    window_start = latest_dt - timedelta(hours=lookback_hours)
    platform_filter = set(current_platform_ids) if current_platform_ids is not None else None
    recent_signatures: Dict[str, Set[str]] = {}

    for db_path in sorted(news_dir.glob('*.db')):
        try:
            db_date = datetime.strptime(db_path.stem, "%Y-%m-%d")
        except ValueError:
            continue

        if db_date.date() < window_start.date() or db_date.date() > latest_dt.date():
            continue

        conn = sqlite3.connect(db_path)
        try:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT platform_id, url, first_crawl_time
                FROM news_items
                WHERE url != ''
            """)
            for source_id, url, first_crawl_time in cursor.fetchall():
                if platform_filter is not None and source_id not in platform_filter:
                    continue

                item_dt = _parse_news_datetime(db_path.stem, first_crawl_time)
                if item_dt is None:
                    continue
                if not (window_start <= item_dt < latest_dt):
                    continue

                normalized_url = normalize_url(url, source_id)
                if not normalized_url:
                    continue

                recent_signatures.setdefault(source_id, set()).add(normalized_url)
        finally:
            conn.close()

    return recent_signatures


def detect_latest_new_titles_from_storage(
    storage_manager,
    current_platform_ids: Optional[List[str]] = None,
) -> Dict:
    """
    从存储后端检测最新批次的新增标题

    Args:
        storage_manager: 存储管理器实例
        current_platform_ids: 当前监控的平台 ID 列表（用于过滤）

    Returns:
        Dict: 新增标题 {source_id: {title: title_data}}
    """
    try:
        # 获取最新抓取数据
        latest_data = storage_manager.get_latest_crawl_data()
        if not latest_data or not latest_data.items:
            return {}

        # 获取所有历史数据
        all_data = storage_manager.get_today_all_data()
        if not all_data or not all_data.items:
            # 没有历史数据（第一次抓取），不应该有"新增"标题
            return {}

        # 获取最新批次时间
        latest_time = latest_data.crawl_time

        # 步骤1：收集最新批次的标题（last_crawl_time = latest_time 的标题）
        latest_titles = {}
        for source_id, news_list in latest_data.items.items():
            if current_platform_ids is not None and source_id not in current_platform_ids:
                continue
            latest_titles[source_id] = {}
            for item in news_list:
                latest_titles[source_id][item.title] = {
                    "ranks": [item.rank],
                    "url": item.url or "",
                    "mobileUrl": item.mobile_url or "",
                }

        # 步骤2：收集历史标题
        # 关键逻辑：一个标题只要其 first_crawl_time < latest_time，就是历史标题
        # 这样即使同一标题有多条记录（URL 不同），只要任何一条是历史的，该标题就算历史
        historical_titles = {}
        for source_id, news_list in all_data.items.items():
            if current_platform_ids is not None and source_id not in current_platform_ids:
                continue

            historical_titles[source_id] = set()
            for item in news_list:
                first_time = item.first_time or item.crawl_time
                # 如果该记录的首次出现时间早于最新批次，则该标题是历史标题
                if first_time < latest_time:
                    historical_titles[source_id].add(item.title)

        # 检查是否是当天第一次抓取（没有任何历史标题）
        # 如果所有平台的历史标题集合都为空，说明只有一个抓取批次
        # 在这种情况下，将所有最新批次的标题视为"新增"（用于增量模式的第一次推送）
        has_historical_data = any(len(titles) > 0 for titles in historical_titles.values())
        if not has_historical_data:
            # 当天第一次爬取仍要继续执行跨天 URL 去重，避免隔夜重复推送。
            new_titles = latest_titles
        else:
            # 步骤3：找出新增标题 = 最新批次标题 - 历史标题
            new_titles = {}
            for source_id, source_latest_titles in latest_titles.items():
                historical_set = historical_titles.get(source_id, set())
                source_new_titles = {}

                for title, title_data in source_latest_titles.items():
                    if title not in historical_set:
                        source_new_titles[title] = title_data

                if source_new_titles:
                    new_titles[source_id] = source_new_titles

        # 步骤4：对新闻源做跨天 URL 去重，避免隔夜重复推送
        dedup_config = getattr(storage_manager, "dedup_config", {}) or {}
        dedup_enabled = dedup_config.get("ENABLED", True)
        hotlist_dedup_enabled = dedup_config.get("APPLY_TO", {}).get("HOTLIST", True)
        lookback_hours = int(dedup_config.get("LOOKBACK_HOURS", 48) or 48)

        recent_signatures = {}
        if dedup_enabled and hotlist_dedup_enabled and lookback_hours > 0:
            recent_signatures = _collect_recent_url_signatures(
                storage_manager,
                current_platform_ids,
                latest_data.date,
                latest_time,
                lookback_hours=lookback_hours,
            )
        if recent_signatures:
            filtered_new_titles = {}
            for source_id, source_titles in new_titles.items():
                known_signatures = recent_signatures.get(source_id, set())
                filtered_titles = {}

                for title, title_data in source_titles.items():
                    normalized_url = normalize_url(title_data.get("url", ""), source_id)
                    if normalized_url and normalized_url in known_signatures:
                        continue
                    filtered_titles[title] = title_data

                if filtered_titles:
                    filtered_new_titles[source_id] = filtered_titles

            new_titles = filtered_new_titles

        return new_titles

    except Exception as e:
        print(f"[存储] 从存储后端检测新标题失败: {e}")
        return {}


def detect_latest_new_titles(
    storage_manager,
    current_platform_ids: Optional[List[str]] = None,
    quiet: bool = False,
) -> Dict:
    """
    检测当日最新批次的新增标题（从存储后端）

    Args:
        storage_manager: 存储管理器实例
        current_platform_ids: 当前监控的平台 ID 列表（用于过滤）
        quiet: 是否静默模式（不打印日志）

    Returns:
        Dict: 新增标题 {source_id: {title: title_data}}
    """
    new_titles = detect_latest_new_titles_from_storage(storage_manager, current_platform_ids)
    if new_titles and not quiet:
        total_new = sum(len(titles) for titles in new_titles.values())
        print(f"[存储] 从存储后端检测到 {total_new} 条新增标题")
    return new_titles
