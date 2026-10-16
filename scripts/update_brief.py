#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Daily Macro Intelligence Brief - Automated Data Pipeline (scripts/update_brief.py)

역할:
1. 국내 주요 경제/금융 뉴스 RSS 피드(매일경제, 한국경제, 연합뉴스 등) 실시간 수집 및 정제
2. 네이버 금융(코스피, 코스닥, 원/달러 환율, WTI 유가) 시장 지표 크롤링 및 폴백(Fallback) 처리
3. Google Gemini API (google-genai SDK)를 활용하여 '경제 인텔리전스 데이터 정제 엔진' 스키마에 맞춘 JSON 생성
4. 스키마 무결성 검증 후 루트 경로의 `data.json`을 원자적(Atomic)으로 안전하게 갱신
"""

import json
import logging
import os
import re
import shutil
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import feedparser
import requests
from bs4 import BeautifulSoup

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("update_brief")

# 경로 설정
ROOT_DIR = Path(__file__).resolve().parent.parent
DATA_JSON_PATH = ROOT_DIR / "data.json"
BACKUP_JSON_PATH = ROOT_DIR / "data.json.bak"
TEMP_JSON_PATH = ROOT_DIR / "data.json.tmp"

# 한국 표준시 (KST, UTC+9)
KST = timezone(timedelta(hours=9))
WEEKDAY_KO = ["월요일", "화요일", "수요일", "목요일", "금요일", "토요일", "일요일"]

# 수집 대상 경제/증시 RSS 피드 목록
RSS_FEEDS = [
    {
        "source": "매일경제(경제)",
        "url": "https://www.mk.co.kr/rss/30100041/",
    },
    {
        "source": "매일경제(증권)",
        "url": "https://www.mk.co.kr/rss/50200011/",
    },
    {
        "source": "한국경제(경제)",
        "url": "https://www.hankyung.com/feed/economy",
    },
    {
        "source": "한국경제(증권)",
        "url": "https://www.hankyung.com/feed/finance",
    },
    {
        "source": "연합뉴스(경제)",
        "url": "https://www.yna.co.kr/rss/economy.xml",
    },
    {
        "source": "연합인포맥스/마켓",
        "url": "https://www.yna.co.kr/rss/market.xml",
    },
]

# HTTP 요청 공통 헤더
DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/129.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
}

# =====================================================================
# 시스템 프롬프트 & 출력 JSON 스키마 정의 (경제 인텔리전스 데이터 정제 엔진)
# =====================================================================
SYSTEM_INSTRUCTION = """당신은 기관 투자자 및 애널리스트를 위한 '경제 인텔리전스 데이터 정제 엔진(Macro & Equity Intelligence Refinement Engine)'입니다.
제공된 최신 경제·금융 뉴스 기사 목록과 실시간 시장 지표 데이터를 분석하여, 엄격한 검증 프레임워크를 따르는 JSON 리포트 데이터를 생성하십시오.

[핵심 작성 원칙]
1. 과장된 수식어나 추측성 표현을 배제하고, 수치·공시·팩트 중심의 건조하고 명확한 애널리스트 보고서 문체(한국어)를 유지하십시오.
2. `macro_core_highlights`:
   - 오늘 시장을 관통하는 핵심 거시/산업 테마 3가지를 선정하고, `rank`는 `"01"`, `"02"`, `"03"`으로 부여하십시오.
3. `market_indicators`:
   - 입력으로 제공된 실시간 시장 지표(`KOSPI`, `KOSDAQ`, `USD/KRW`, `WTI CRUDE`) 4개 항목의 수치를 그대로 반영하거나 최신 팩트에 맞게 정밀하게 포맷팅하십시오.
   - `direction`은 전일 대비 상승 시 `"UP"`, 하락 시 `"DOWN"`만 허용됩니다.
4. `top_news_reports`:
   - 가장 파급력이 큰 핵심 뉴스 5건(`id`: 1 ~ 5)을 엄선하여 구조화하십시오.
   - `observed_facts`: 기사에서 확인된 객관적 사실과 수치를 2~3개의 문자열 배열로 요약하십시오.
   - `transmission_pathway`:
     - `channel`: 원인이 결과로 이어지는 인과 사슬을 반드시 `"단계1 -> 단계2 -> 단계3"` 형식(`->` 구분자 사용)으로 작성하십시오.
     - `impact`: 해당 경로가 거시 경제, 산업 밸류체인 또는 기업 실적에 미치는 실질적 영향을 기술하십시오.
   - `falsification_condition`: 이 분석 가설이 기각(무효화)될 수 있는 구체적인 경계 조건(수치, 정책 변화, 수율/실적 미달 등)을 명시하십시오.
   - `analyst_tip`: 투자자 및 의사결정자가 후속 점검해야 할 핵심 체크포인트를 한 문장으로 제시하십시오.
5. 출력은 마크다운 코드블록(```json) 없이 순수하고 유효한 JSON 객체만 반환해야 합니다.

[출력 JSON 스키마 규격]
{
  "report_metadata": {
    "generated_at": "ISO-8601 UTC 타임스탬프 (예: 2026-10-16T06:00:00Z)",
    "display_date": "한국어 날짜 표기 (예: 2026년 10월 16일 금요일)",
    "timezone": "Asia/Seoul",
    "data_version": "v1.0-Automated"
  },
  "macro_core_highlights": [
    {
      "rank": "01",
      "title": "핵심 테마 제목",
      "summary": "핵심 요약 설명"
    }
  ],
  "market_indicators": [
    {
      "name": "KOSPI | KOSDAQ | USD/KRW | WTI CRUDE",
      "value": "현재 지수/가격 문자열",
      "change": "전일 대비 변동폭 문자열 (부호 포함, 예: +12.34, -5.20)",
      "change_percentage": "전일 대비 등락률 문자열 (부호 및 % 포함, 예: +0.45%, -0.32%)",
      "direction": "UP 또는 DOWN"
    }
  ],
  "top_news_reports": [
    {
      "id": 1,
      "headline": "핵심 뉴스 헤드라인",
      "observed_facts": [
        "관측된 객관적 팩트 1",
        "관측된 객관적 팩트 2"
      ],
      "transmission_pathway": {
        "channel": "발단 요인 -> 중간 전이 과정 -> 최종 시장 반응",
        "impact": "산업 및 거시적 파급 효과 요약"
      },
      "falsification_condition": "분석 가설이 기각되는 구체적 조건",
      "analyst_tip": "애널리스트 핵심 체크포인트"
    }
  ]
}
"""


def clean_html_text(raw_html: str) -> str:
    """HTML 태그 및 불필요한 공백을 제거하여 순수 텍스트만 반환합니다."""
    if not raw_html:
        return ""
    soup = BeautifulSoup(raw_html, "html.parser")
    text = soup.get_text(separator=" ", strip=True)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def fetch_rss_articles(max_per_feed: int = 10, max_total: int = 35) -> List[Dict[str, str]]:
    """복수의 경제 뉴스 RSS 피드에서 최신 헤드라인과 본문 요약을 수집합니다."""
    articles: List[Dict[str, str]] = []
    seen_titles = set()

    for feed_info in RSS_FEEDS:
        source_name = feed_info["source"]
        url = feed_info["url"]
        try:
            logger.info(f"RSS 피드 수집 중: {source_name} ({url})")
            resp = requests.get(url, headers=DEFAULT_HEADERS, timeout=12)
            resp.raise_for_status()

            parsed = feedparser.parse(resp.content)
            count = 0
            for entry in parsed.entries:
                if count >= max_per_feed:
                    break

                title = clean_html_text(entry.get("title", ""))
                if not title or len(title) < 6:
                    continue

                # 중복 제목 필터링 (앞 20자 기준 정규화 비교)
                norm_key = re.sub(r"[^가-힣a-zA-Z0-9]", "", title)[:20]
                if norm_key in seen_titles:
                    continue
                seen_titles.add(norm_key)

                summary_raw = (
                    entry.get("summary", "")
                    or entry.get("description", "")
                    or ""
                )
                summary = clean_html_text(summary_raw)
                if len(summary) > 350:
                    summary = summary[:350] + "..."

                pub_date = entry.get("published", entry.get("updated", ""))

                articles.append(
                    {
                        "source": source_name,
                        "title": title,
                        "summary": summary,
                        "published": pub_date,
                    }
                )
                count += 1

            logger.info(f"  -> {source_name}: {count}건 수집 완료")
        except Exception as exc:
            logger.warning(f"  -> {source_name} RSS 수집 실패 (건너뜀): {exc}")

    logger.info(f"총 {len(articles)}건의 고유 경제 뉴스 수집 완료")
    return articles[:max_total]


def load_existing_data() -> Optional[Dict[str, Any]]:
    """기존 data.json 파일을 읽어 폴백(Fallback) 기준 데이터로 활용합니다."""
    if not DATA_JSON_PATH.exists():
        return None
    try:
        with open(DATA_JSON_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as exc:
        logger.warning(f"기존 data.json 로드 실패: {exc}")
        return None


def _parse_naver_sise_index(soup: BeautifulSoup, code: str) -> Optional[Dict[str, str]]:
    """네이버 금융 국내증시 메인 페이지에서 KOSPI/KOSDAQ 지수를 추출합니다."""
    try:
        now_el = soup.select_one(f"#{code}_now")
        change_el = soup.select_one(f"#{code}_change")
        if not now_el or not change_el:
            return None

        value_str = now_el.get_text(strip=True)
        change_raw = change_el.get_text(" ", strip=True)

        # 하락 여부 판별
        is_down = ("하락" in change_raw) or ("-" in change_raw)
        direction = "DOWN" if is_down else "UP"
        sign = "-" if is_down else "+"

        # 숫자와 퍼센트 추출
        numbers = re.findall(r"\d+(?:,\d+)*(?:\.\d+)?", change_raw)
        if len(numbers) >= 2:
            change_val = f"{sign}{numbers[0]}"
            change_pct = f"{sign}{numbers[1]}%"
        elif len(numbers) == 1:
            change_val = f"{sign}{numbers[0]}"
            change_pct = f"{sign}0.00%"
        else:
            return None

        return {
            "name": code,
            "value": value_str,
            "change": change_val,
            "change_percentage": change_pct,
            "direction": direction,
        }
    except Exception:
        return None


def _parse_naver_marketindex(soup: BeautifulSoup, selector: str, name: str, suffix: str = "", prefix: str = "") -> Optional[Dict[str, str]]:
    """네이버 금융 시장지표 페이지에서 환율 및 국제유가를 추출합니다."""
    try:
        item = soup.select_one(selector)
        if not item:
            return None

        val_el = item.select_one(".value")
        chg_el = item.select_one(".change")
        blind_els = item.select(".blind")
        if not val_el or not chg_el:
            return None

        raw_val = val_el.get_text(strip=True)
        raw_chg = chg_el.get_text(strip=True)
        full_text = item.get_text(" ", strip=True)

        head_info = item.select_one(".head_info")
        is_down = False
        if head_info and ("point_dn" in (head_info.get("class") or [])):
            is_down = True
        elif "하락" in full_text:
            is_down = True

        direction = "DOWN" if is_down else "UP"
        sign = "-" if is_down else "+"

        # 등락률 계산 (현재가 및 변동폭 기반 역산)
        val_float = float(raw_val.replace(",", ""))
        chg_float = float(raw_chg.replace(",", ""))
        prev_val = val_float + chg_float if is_down else val_float - chg_float
        pct_float = (chg_float / prev_val * 100.0) if prev_val > 0 else 0.0

        return {
            "name": name,
            "value": f"{prefix}{raw_val}{suffix}",
            "change": f"{sign}{raw_chg}",
            "change_percentage": f"{sign}{pct_float:.2f}%",
            "direction": direction,
        }
    except Exception:
        return None


def fetch_market_indicators(existing_data: Optional[Dict[str, Any]] = None) -> List[Dict[str, str]]:
    """
    네이버 금융에서 KOSPI, KOSDAQ, USD/KRW, WTI CRUDE 실시간 지표를 수집합니다.
    수집에 실패한 개별 지표는 기존 data.json 값 또는 안전한 기본값으로 대체합니다.
    """
    # 기본 폴백 지표 설정
    fallback_map: Dict[str, Dict[str, str]] = {
        "KOSPI": {
            "name": "KOSPI",
            "value": "6,082.90",
            "change": "+49.55",
            "change_percentage": "+0.82%",
            "direction": "UP",
        },
        "KOSDAQ": {
            "name": "KOSDAQ",
            "value": "833.15",
            "change": "-3.94",
            "change_percentage": "-0.47%",
            "direction": "DOWN",
        },
        "USD/KRW": {
            "name": "USD/KRW",
            "value": "1,336.80원",
            "change": "-6.18",
            "change_percentage": "-0.46%",
            "direction": "DOWN",
        },
        "WTI CRUDE": {
            "name": "WTI CRUDE",
            "value": "$78.20",
            "change": "-0.95",
            "change_percentage": "-1.20%",
            "direction": "DOWN",
        },
    }

    if existing_data and isinstance(existing_data.get("market_indicators"), list):
        for item in existing_data["market_indicators"]:
            if isinstance(item, dict) and item.get("name") in fallback_map:
                fallback_map[item["name"]] = item

    collected: Dict[str, Dict[str, str]] = {}

    # 1. 국내 증시 (KOSPI, KOSDAQ)
    try:
        logger.info("네이버 금융 국내증시 지표 수집 중...")
        sise_resp = requests.get("https://finance.naver.com/sise/", headers=DEFAULT_HEADERS, timeout=10)
        sise_resp.raise_for_status()
        sise_soup = BeautifulSoup(sise_resp.text, "html.parser")

        kospi = _parse_naver_sise_index(sise_soup, "KOSPI")
        if kospi:
            collected["KOSPI"] = kospi

        kosdaq = _parse_naver_sise_index(sise_soup, "KOSDAQ")
        if kosdaq:
            collected["KOSDAQ"] = kosdaq
    except Exception as exc:
        logger.warning(f"국내증시 지표 크롤링 실패 (폴백 사용): {exc}")

    # 2. 환율 및 유가 (USD/KRW, WTI CRUDE)
    try:
        logger.info("네이버 금융 시장지표(환율/유가) 수집 중...")
        mi_resp = requests.get("https://finance.naver.com/marketindex/", headers=DEFAULT_HEADERS, timeout=10)
        mi_resp.raise_for_status()
        mi_soup = BeautifulSoup(mi_resp.text, "html.parser")

        usd_krw = _parse_naver_marketindex(
            mi_soup, "#exchangeList li.on", name="USD/KRW", suffix="원"
        )
        if usd_krw:
            collected["USD/KRW"] = usd_krw

        wti = _parse_naver_marketindex(
            mi_soup, "#oilGoldList li.on", name="WTI CRUDE", prefix="$"
        )
        if wti:
            collected["WTI CRUDE"] = wti
    except Exception as exc:
        logger.warning(f"환율/유가 지표 크롤링 실패 (폴백 사용): {exc}")

    ordered_keys = ["KOSPI", "KOSDAQ", "USD/KRW", "WTI CRUDE"]
    final_indicators = []
    for key in ordered_keys:
        if key in collected:
            logger.info(
                f"  -> [실시간 반영] {key}: {collected[key]['value']} ({collected[key]['change_percentage']})"
            )
            final_indicators.append(collected[key])
        else:
            logger.info(
                f"  -> [폴백 유지] {key}: {fallback_map[key]['value']} ({fallback_map[key]['change_percentage']})"
            )
            final_indicators.append(fallback_map[key])

    return final_indicators


def build_user_prompt(
    articles: List[Dict[str, str]],
    indicators: List[Dict[str, str]],
    now_kst: datetime,
    now_utc: datetime,
) -> str:
    """Gemini 모델에 전달할 프롬프트를 조립합니다."""
    display_date = f"{now_kst.year}년 {now_kst.month}월 {now_kst.day}일 {WEEKDAY_KO[now_kst.weekday()]}"
    generated_at = now_utc.strftime("%Y-%m-%dT%H:%M:%SZ")

    articles_text_lines = []
    for idx, art in enumerate(articles, start=1):
        articles_text_lines.append(
            f"[{idx}] ({art['source']}) 제목: {art['title']}\n"
            f"    요약: {art['summary']}\n"
            f"    발행시각: {art['published']}"
        )
    articles_block = "\n\n".join(articles_text_lines)

    indicators_block = json.dumps(indicators, ensure_ascii=False, indent=2)

    return f"""다음은 {display_date} 기준 수집된 실시간 시장 지표와 주요 경제 뉴스 RSS 데이터입니다.
이를 바탕으로 기관급 데일리 매크로 인텔리전스 브리프(`data.json`)를 생성하십시오.

[메타데이터 고정값]
- generated_at: "{generated_at}"
- display_date: "{display_date}"
- timezone: "Asia/Seoul"
- data_version: "v1.0-Automated"

[실시간 시장 지표 데이터 (market_indicators에 그대로 반영)]
{indicators_block}

[수집된 최신 경제/금융 뉴스 기사 목록]
{articles_block}
"""


def call_gemini_intelligence_engine(user_prompt: str) -> Dict[str, Any]:
    """
    google-genai SDK를 호출하여 구조화된 JSON 리포트 데이터를 생성합니다.
    (환경에 따라 google-generativeai 패키지도 호환 지원)
    """
    api_key = os.environ.get("GEMINI_API_KEY", "").strip()
    if not api_key:
        raise RuntimeError(
            "환경변수 'GEMINI_API_KEY'가 설정되어 있지 않습니다. "
            "GitHub Actions Secrets 또는 로컬 환경변수를 확인하십시오."
        )

    model_candidates = [
        os.environ.get("GEMINI_MODEL", "gemini-2.5-flash").strip(),
        "gemini-2.0-flash",
    ]

    last_error: Optional[Exception] = None

    # 1순위: 최신 공식 SDK (`google-genai`)
    try:
        from google import genai
        from google.genai import types

        client = genai.Client(api_key=api_key)
        for model_name in model_candidates:
            try:
                logger.info(f"Gemini API 호출 중 (SDK: google-genai, Model: {model_name})...")
                response = client.models.generate_content(
                    model=model_name,
                    contents=user_prompt,
                    config=types.GenerateContentConfig(
                        system_instruction=SYSTEM_INSTRUCTION,
                        response_mime_type="application/json",
                        temperature=0.2,
                    ),
                )
                raw_text = (response.text or "").strip()
                if not raw_text:
                    raise ValueError("Gemini API 응답 텍스트가 비어 있습니다.")
                return json.loads(raw_text)
            except Exception as err:
                logger.warning(f"모델 '{model_name}' 호출 실패: {err}")
                last_error = err
    except ImportError:
        logger.info("google-genai 패키지를 찾을 수 없어 google-generativeai 폴백을 시도합니다.")
        try:
            import google.generativeai as legacy_genai

            legacy_genai.configure(api_key=api_key)
            for model_name in model_candidates:
                try:
                    logger.info(f"Gemini API 호출 중 (SDK: google-generativeai, Model: {model_name})...")
                    model = legacy_genai.GenerativeModel(
                        model_name=model_name,
                        system_instruction=SYSTEM_INSTRUCTION,
                        generation_config={"response_mime_type": "application/json", "temperature": 0.2},
                    )
                    response = model.generate_content(user_prompt)
                    raw_text = (response.text or "").strip()
                    if not raw_text:
                        raise ValueError("Gemini API 응답 텍스트가 비어 있습니다.")
                    return json.loads(raw_text)
                except Exception as err:
                    logger.warning(f"레거시 SDK 모델 '{model_name}' 호출 실패: {err}")
                    last_error = err
        except ImportError as imp_err:
            raise RuntimeError(
                "google-genai 라이브러리가 설치되어 있지 않습니다. `pip install -r requirements.txt`를 실행하세요."
            ) from imp_err

    raise RuntimeError(f"모든 Gemini 모델 호출에 실패했습니다. 마지막 오류: {last_error}")


def validate_and_normalize_schema(
    data: Dict[str, Any],
    fallback_indicators: List[Dict[str, str]],
    now_kst: datetime,
    now_utc: datetime,
) -> Dict[str, Any]:
    """생성된 JSON이 프론트엔드(`index.html`) 렌더링 스키마를 완벽히 충족하는지 검증 및 보정합니다."""
    if not isinstance(data, dict):
        raise ValueError("최상위 JSON 구조가 객체(dict)가 아닙니다.")

    # 1. report_metadata 보정
    display_date = f"{now_kst.year}년 {now_kst.month}월 {now_kst.day}일 {WEEKDAY_KO[now_kst.weekday()]}"
    generated_at = now_utc.strftime("%Y-%m-%dT%H:%M:%SZ")

    meta = data.get("report_metadata")
    if not isinstance(meta, dict):
        meta = {}
    data["report_metadata"] = {
        "generated_at": generated_at,
        "display_date": meta.get("display_date") or display_date,
        "timezone": "Asia/Seoul",
        "data_version": "v1.0-Automated",
    }

    # 2. macro_core_highlights 검증
    highlights = data.get("macro_core_highlights")
    if not isinstance(highlights, list) or len(highlights) < 3:
        raise ValueError("macro_core_highlights 항목이 3개 미만입니다.")
    for idx, item in enumerate(highlights[:3], start=1):
        if not isinstance(item, dict):
            raise ValueError("macro_core_highlights 내부 요소가 객체가 아닙니다.")
        item["rank"] = f"0{idx}"
        if not item.get("title") or not item.get("summary"):
            raise ValueError(f"macro_core_highlights[{idx}]에 title 또는 summary가 누락되었습니다.")
    data["macro_core_highlights"] = highlights[:3]

    # 3. market_indicators 검증 (누락 시 수집된 실시간 지표로 강제 보정)
    indicators = data.get("market_indicators")
    if not isinstance(indicators, list) or len(indicators) < 4:
        data["market_indicators"] = fallback_indicators
    else:
        for ind in indicators:
            if ind.get("direction") not in ("UP", "DOWN"):
                ind["direction"] = "DOWN" if str(ind.get("change", "")).startswith("-") else "UP"
        data["market_indicators"] = indicators[:4]

    # 4. top_news_reports 검증
    reports = data.get("top_news_reports")
    if not isinstance(reports, list) or len(reports) < 5:
        raise ValueError("top_news_reports 항목이 5개 미만입니다.")

    normalized_reports = []
    for idx, rep in enumerate(reports[:5], start=1):
        if not isinstance(rep, dict):
            raise ValueError(f"top_news_reports[{idx}]가 객체가 아닙니다.")
        rep["id"] = idx
        if not rep.get("headline"):
            raise ValueError(f"top_news_reports[{idx}]에 headline이 누락되었습니다.")

        facts = rep.get("observed_facts")
        if not isinstance(facts, list) or len(facts) == 0:
            raise ValueError(f"top_news_reports[{idx}]에 observed_facts 배열이 비어 있습니다.")

        pathway = rep.get("transmission_pathway")
        if not isinstance(pathway, dict) or not pathway.get("channel") or not pathway.get("impact"):
            raise ValueError(f"top_news_reports[{idx}]의 transmission_pathway가 유효하지 않습니다.")

        # '->' 구분자가 없을 경우 화살표 기호 정규화
        channel_str = str(pathway["channel"])
        for arrow_char in ("→", "⇒", "▶", ">"):
            if "->" not in channel_str and arrow_char in channel_str:
                channel_str = channel_str.replace(arrow_char, " -> ")
        pathway["channel"] = channel_str

        if not rep.get("falsification_condition") or not rep.get("analyst_tip"):
            raise ValueError(
                f"top_news_reports[{idx}]에 falsification_condition 또는 analyst_tip이 누락되었습니다."
            )
        normalized_reports.append(rep)

    data["top_news_reports"] = normalized_reports
    return data


def save_json_atomically(data: Dict[str, Any]) -> None:
    """
    기존 data.json을 백업한 뒤 임시 파일에 먼저 쓰고 검증 후 원자적(Atomic)으로 교체합니다.
    중간 오류 발생 시 빈 파일이나 손상된 파일이 남지 않도록 보장합니다.
    """
    if DATA_JSON_PATH.exists():
        shutil.copy2(DATA_JSON_PATH, BACKUP_JSON_PATH)

    try:
        with open(TEMP_JSON_PATH, "w", encoding="utf-8") as tmp_file:
            json.dump(data, tmp_file, ensure_ascii=False, indent=2)
            tmp_file.write("\n")

        # 파일 크기 및 재파싱 무결성 확인
        if TEMP_JSON_PATH.stat().st_size < 100:
            raise IOError("생성된 임시 JSON 파일 크기가 비정상적으로 작습니다.")

        with open(TEMP_JSON_PATH, "r", encoding="utf-8") as check_file:
            json.load(check_file)

        # 원자적 교체
        os.replace(TEMP_JSON_PATH, DATA_JSON_PATH)
        logger.info(f"성공적으로 '{DATA_JSON_PATH.name}' 파일을 갱신했습니다.")
    except Exception as exc:
        logger.error(f"파일 저장 중 오류 발생: {exc}")
        if TEMP_JSON_PATH.exists():
            TEMP_JSON_PATH.unlink(missing_ok=True)
        if BACKUP_JSON_PATH.exists() and not DATA_JSON_PATH.exists():
            shutil.copy2(BACKUP_JSON_PATH, DATA_JSON_PATH)
            logger.info("백업 파일로부터 기존 data.json을 복원했습니다.")
        raise
    finally:
        if BACKUP_JSON_PATH.exists():
            BACKUP_JSON_PATH.unlink(missing_ok=True)


def main() -> int:
    logger.info("=== Daily Macro Intelligence Brief 자동 갱신 파이프라인 시작 ===")
    now_utc = datetime.now(timezone.utc)
    now_kst = now_utc.astimezone(KST)

    existing_data = load_existing_data()

    # 1. RSS 뉴스 수집
    articles = fetch_rss_articles(max_per_feed=10, max_total=35)
    if not articles:
        logger.error("수집된 RSS 뉴스 기사가 없습니다. 기존 data.json을 유지하고 종료합니다.")
        return 0 if existing_data else 1

    # 2. 실시간 시장 지표 수집 (실패 시 기존 data.json 지표 폴백)
    indicators = fetch_market_indicators(existing_data=existing_data)

    # 3. Gemini API 호출 및 스키마 검증
    try:
        user_prompt = build_user_prompt(
            articles=articles,
            indicators=indicators,
            now_kst=now_kst,
            now_utc=now_utc,
        )
        raw_result = call_gemini_intelligence_engine(user_prompt)
        validated_data = validate_and_normalize_schema(
            data=raw_result,
            fallback_indicators=indicators,
            now_kst=now_kst,
            now_utc=now_utc,
        )
        save_json_atomically(validated_data)
        logger.info("=== 파이프라인 정상 완료 ===")
        return 0
    except Exception as exc:
        logger.error(f"Gemini 정제 또는 파일 갱신 실패 (기존 data.json 보존): {exc}")
        return 1


if __name__ == "__main__":
    sys.exit(main())

