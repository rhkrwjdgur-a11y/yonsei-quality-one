# ==============================================================================
# [api/index.py - PART 1 / 2]
# YONSEI QUALITY ONE 백엔드 API 엔진 (9개 구글 시트 실시간 파서 & 공정 이력 DB 로더)
# ==============================================================================
import json
import os
import re
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

import gspread
import requests
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials as OAuthCredentials
from google.oauth2.service_account import Credentials as ServiceAccountCredentials
from pydantic import BaseModel

# ==============================================================================
# 1. 기본 상수 · 한국 표준시(KST, UTC+9) · 기본값 정의
# ==============================================================================
KST_TZ = timezone(timedelta(hours=9))


def now_kst() -> datetime:
    return datetime.now(KST_TZ).replace(tzinfo=None)


FISCAL_MONTHS = ["3월", "4월", "5월", "6월", "7월", "8월", "9월", "10월", "11월", "12월", "1월", "2월"]
DAIRY_CATEGORIES = ["냉장우유 카톤팩", "냉장우유 병", "발효유/음료 카톤팩", "발효유/음료 병"]
RT_CATEGORIES = ["상온 멸균팩", "상온 프리즈마"]

AUDIT_STATUS_LIST = ["심사준비중", "심사중", "서류심사중", "현장심사중", "심사종료"]
AUDIT_STATUS_CONFIG = {
    "심사준비중": {"pct": 20, "color": "#64748B"},
    "심사중": {"pct": 50, "color": "#0284C7"},
    "서류심사중": {"pct": 75, "color": "#2563EB"},
    "현장심사중": {"pct": 85, "color": "#D97706"},
    "심사종료": {"pct": 100, "color": "#10B981"}
}

SHEET_NAMES = {
    "cost": "원가절감 현황",
    "claim": "클레임 건수",
    "supplier": "협력업체 심사 결과 DB",
    "audit_db": "심사점수 및 일정 관리 DB",
    "label_status": "표시사항 현황",
    "raw_label_db": "원자재 표시사항 DB",
    "process_soy": "[상온]두유 배양검사 일보",
    "process_rt": "[상온]상온제품 공정 관리 일보",
    "process_milk": "[우유] 유제품 포장 공정관리 일보"
}

HIST_TABS = {
    "dairy_prod": "_이력DB_유제품포장",
    "dairy_ccp": "_이력DB_유제품공정CCP",
    "rt_prod": "_이력DB_상온생산",
    "rt_ccp": "_이력DB_상온CCP",
    "soy_inc": "_이력DB_두유배양"
}

AUDIT_COLS = ["고유ID", "심사일자", "심사명", "진행현황", "점수", "비고", "수정일시"]
MANUAL_KPI_TAB = "수동KPI_설정"
MANUAL_KPI_COLS = [
    "kpi_key", "target_text", "actual_text", "sub_badge",
    "target_num", "actual_num", "signal", "notes_title", "notes_joined", "updated_at"
]
SPECIAL_BOARD_TAB = "대시보드_특이사항_보드"

DEFAULT_MANUAL_KPIS = {
    "kpi_8": {
        "no": "3",
        "title": "3. 식품안전 및 HACCP 강화",
        "short_title": "3. HACCP 강화",
        "target_text": "11건 이상",
        "actual_text": "11건, 100% 달성",
        "compact_val": "11건",
        "sub_badge": "목표 100% 달성 완료",
        "target_num": 11.0,
        "actual_num": 11.0,
        "suffix": "",
        "unit_label": "(단위 : 건)",
        "signal": "green",
        "has_chart": True,
        "notes_title": "식품안전 및 HACCP 강화 추진 실적",
        "notes": [
            "연간 식품안전 및 HACCP 유효성 검증·개선 과제 11건 이행 완료",
            "현장 작업자 정기 위생·CCP 교육 및 검증 프로세스 고도화"
        ]
    },
    "kpi_9": {
        "no": "4",
        "title": "4. 핵심불량(클레임) 발생 저감화",
        "short_title": "4. 핵심불량 저감",
        "target_text": "38건 이하",
        "actual_text": "5건",
        "compact_val": "5건",
        "sub_badge": "목표(38건 이하) 대비 안정 관리",
        "target_num": 38.0,
        "actual_num": 5.0,
        "suffix": "",
        "unit_label": "(단위 : 건)",
        "signal": "green",
        "has_chart": True,
        "notes_title": "핵심불량(클레임) 중점 저감 내역",
        "notes": [
            "연간 허용 한도(38건 이하) 대비 현재 누적 5건으로 안정적 관리 중",
            "설비 접착·이물·파손 등 핵심 중대 불량 요인 사전 예방 점검"
        ]
    },
    "kpi_10": {
        "no": "5",
        "title": "5. 부자재 관리 고도화",
        "short_title": "5. 부자재 관리",
        "target_text": "부자재폐기 0건",
        "actual_text": "0건",
        "compact_val": "0건",
        "sub_badge": "부자재 폐기 0건 유지",
        "target_num": 0.0,
        "actual_num": 0.0,
        "suffix": "건",
        "unit_label": "(단위 : 건)",
        "signal": "green",
        "has_chart": False,
        "notes_title": "부자재 폐기 Zero화 관리 내역",
        "notes": [
            "표시사항 사전 심의 및 법규 개정 선제 반영으로 부자재 오인쇄·폐기 0건 유지",
            "포장재 발주 전 표시사항 더블체크 프로세스 상시 가동"
        ]
    },
    "kpi_11": {
        "no": "6",
        "title": "6. 협력업체 관리강화",
        "short_title": "6. 협력업체 관리",
        "target_text": "실행률 88.3% 이상",
        "actual_text": "77.9%",
        "compact_val": "77.9%",
        "sub_badge": "목표 88.3% 대비 집중 관리 중",
        "target_num": 88.3,
        "actual_num": 77.9,
        "suffix": "%",
        "unit_label": "(단위 : %)",
        "signal": "red",
        "has_chart": True,
        "notes_title": "협력업체 현장·서류 관리강화 내역",
        "notes": [
            "연간 협력업체 정기 서류·현장심사 및 개선조치 실행률 점검 진행",
            "등급 외 및 미제출 업체 대상 집중 피드백 및 보완 점검"
        ]
    },
    "kpi_5": {
        "no": "7",
        "title": "7. 심사점수",
        "short_title": "7. 심사점수",
        "target_text": "94.28점 이상",
        "actual_text": "91.56점",
        "compact_val": "91.56점",
        "sub_badge": "목표 대비 -2.72점",
        "target_num": 94.28,
        "actual_num": 91.56,
        "suffix": "",
        "unit_label": "(단위 : 점)",
        "signal": "red",
        "has_chart": True,
        "notes_title": "심사점수 주요 관리 내역",
        "notes": [
            "정기 위생·품질 심사 결과 집계 및 미비점 개선 조치 진행",
            "현장 CCP 모니터링 및 중점 위생관리 항목 상시 점검"
        ]
    },
    "kpi_12": {
        "no": "8",
        "title": "8. 식품안전 법적 RISK 최소화",
        "short_title": "8. 법적 RISK",
        "target_text": "행정처분 0건",
        "actual_text": "0건",
        "compact_val": "0건",
        "sub_badge": "행정처분 0건 · 중요건 조사 대응",
        "target_num": 0.0,
        "actual_num": 0.0,
        "suffix": "건",
        "unit_label": "(단위 : 건)",
        "signal": "green",
        "has_chart": False,
        "notes_title": "중요건 행정기관 조사 3건 대응",
        "notes": [
            "카톤발효유 : 3월",
            "락토프리(명가) : 6월",
            "호상발효유 : 10월"
        ]
    }
}

DEFAULT_LABEL_MEMO = (
    "• 원재료 변경 품목 전성분 및 알레르기 유발물질 표시 교차 점검 진행\n"
    "• 표시사항 신규·변경 의뢰 건 부자재 발주 전 최종 확인필 여부 상시 체크"
)

DEFAULT_CLAIM_ISSUES = [
    {"구분": "클레임추적", "특이사항": "주요 클레임 접수 품목 생산·멸균·배양 이력 점검", "상태": "확인 필요", "진행단계": 1, "제품키워드": "", "소비기한": ""},
    {"구분": "공정·부적합", "특이사항": "카톤·멸균팩 상하부 접착 및 표시사항 인쇄 상태 집중 모니터링", "상태": "진행 중", "진행단계": 2, "제품키워드": "", "소비기한": ""}
]


# ==============================================================================
# 2. Google 인증 · 시트 열기 · 공통 수학/날짜/정규화 유틸리티
# ==============================================================================
def api_call_with_retry(func, *args, **kwargs):
    for attempt in range(4):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            err_msg = str(e)
            if "429" in err_msg or "Quota exceeded" in err_msg or "RESOURCE_EXHAUSTED" in err_msg:
                time.sleep(2.2 * (attempt + 1))
                continue
            raise
    return func(*args, **kwargs)


def get_sheet_urls_map() -> dict:
    raw = os.environ.get("SHEET_URLS_JSON", "").strip()
    if not raw:
        return {}
    try:
        parsed = json.loads(raw)
        return parsed if isinstance(parsed, dict) else {}
    except Exception:
        return {}


def build_google_credentials():
    raw_cred = os.environ.get("GOOGLE_CREDENTIALS_JSON", "").strip()
    if raw_cred:
        try:
            info = json.loads(raw_cred)
            if "refresh_token" in info and "client_id" in info:
                creds = OAuthCredentials(
                    token=None,
                    refresh_token=info["refresh_token"],
                    token_uri=info.get("token_uri", "https://oauth2.googleapis.com/token"),
                    client_id=info["client_id"],
                    client_secret=info["client_secret"]
                )
                creds.refresh(Request())
                return creds, None
            if info.get("type") == "service_account" or "private_key" in info:
                scopes = [
                    "https://www.googleapis.com/auth/spreadsheets",
                    "https://www.googleapis.com/auth/drive"
                ]
                creds = ServiceAccountCredentials.from_service_account_info(info, scopes=scopes)
                return creds, None
        except Exception as e:
            return None, f"GOOGLE_CREDENTIALS_JSON 인증 실패: {e}"

    return None, "환경변수 GOOGLE_CREDENTIALS_JSON이 설정되지 않았습니다."


def open_sheet_threadsafe(creds, sheet_urls_map: dict, key_or_title: str):
    if creds is None:
        return None, "인증 자격증명이 없습니다."

    client = gspread.authorize(creds)
    errors = []

    if key_or_title in sheet_urls_map:
        raw_val = str(sheet_urls_map[key_or_title]).strip()
        if raw_val:
            # 괄호나 설명 문구가 섞여 있어도 스프레드시트 고유 Key 또는 순수 URL 자동 추출
            m_key = re.search(r"/spreadsheets/d/([a-zA-Z0-9\-_]+)", raw_val)
            try:
                if m_key:
                    return api_call_with_retry(client.open_by_key, m_key.group(1)), None
                if raw_val.startswith("http"):
                    return api_call_with_retry(client.open_by_url, raw_val), None
                return api_call_with_retry(client.open_by_key, raw_val), None
            except Exception as e:
                errors.append(f"URL/ID 열기 실패({key_or_title}): {e}")

    target_title = SHEET_NAMES.get(key_or_title, key_or_title)
    for candidate in [target_title, target_title.strip(), target_title.strip() + " "]:
        try:
            return api_call_with_retry(client.open, candidate), None
        except Exception as e:
            errors.append(f"파일명 '{candidate}' 열기 실패: {e}")

    return None, " / ".join(errors)


def get_values_unformatted(ws) -> list:
    try:
        return api_call_with_retry(ws.get_all_values, value_render_option="UNFORMATTED_VALUE")
    except Exception:
        return api_call_with_retry(ws.get_all_values)


def get_values_formatted(ws) -> list:
    return api_call_with_retry(ws.get_all_values)


def safe_ws_update(ws, payload: list):
    try:
        api_call_with_retry(ws.update, values=payload, range_name="A1", value_input_option="USER_ENTERED")
    except TypeError:
        api_call_with_retry(ws.update, "A1", payload, value_input_option="USER_ENTERED")


def rows_to_records(rows: list) -> List[Dict[str, str]]:
    if not rows or len(rows) < 2:
        return []
    hdr = [str(h).strip() for h in rows[0]]
    data = []
    for r in rows[1:]:
        if not any(str(x).strip() != "" for x in r):
            continue
        r_pad = [str(x).strip() for x in r] + [""] * max(0, len(hdr) - len(r))
        data.append(dict(zip(hdr, r_pad[:len(hdr)])))
    return data


def to_float(val: Any, default: float = 0.0) -> float:
    if val is None:
        return default
    s = str(val).strip()
    if s in ["", "-", "nan", "None", "#######"]:
        return default
    try:
        cleaned = re.sub(r"[^\d.-]", "", s)
        return float(cleaned) if cleaned not in ["", "-", "."] else default
    except Exception:
        return default


def parse_date_obj(val: Any) -> Optional[date]:
    if val is None:
        return None
    if isinstance(val, datetime):
        return val.date()
    if isinstance(val, date):
        return val
    s = str(val).strip()
    if not s or "###" in s:
        return None
    if re.match(r"^\d{5}(\.\d+)?$", s):
        try:
            dt = datetime(1899, 12, 30) + timedelta(days=float(s))
            if 2018 <= dt.year <= 2035:
                return dt.date()
        except Exception:
            pass
    m = re.search(r"(20\d{2})\D+(\d{1,2})\D+(\d{1,2})", s)
    if m:
        try:
            return date(int(m.group(1)), int(m.group(2)), int(m.group(3)))
        except ValueError:
            return None
    m2 = re.match(r"^(\d{2})[\.\-/](\d{1,2})[\.\-/](\d{1,2})", s)
    if m2:
        try:
            return date(2000 + int(m2.group(1)), int(m2.group(2)), int(m2.group(3)))
        except ValueError:
            return None
    return None


def calc_trend_and_forecast_split(y_values: list, is_recorded_list: list = None, total_periods: int = 12):
    if is_recorded_list is not None:
        valid_idx = [i for i, flag in enumerate(is_recorded_list) if flag]
    else:
        valid_idx = [i for i, v in enumerate(y_values) if to_float(v, 0.0) > 0]

    valid_y = [float(y_values[i]) for i in valid_idx]

    if len(valid_idx) >= 2:
        n = len(valid_idx)
        mean_x = sum(valid_idx) / n
        mean_y = sum(valid_y) / n
        denom = sum((x - mean_x) ** 2 for x in valid_idx)
        slope = sum((x - mean_x) * (y - mean_y) for x, y in zip(valid_idx, valid_y)) / denom if denom > 0 else 0.0
        intercept = mean_y - slope * mean_x
        full_line = [round(max(float(slope * i + intercept), 0.0), 2) for i in range(total_periods)]
    elif len(valid_idx) == 1:
        slope = 0.0
        full_line = [round(valid_y[0], 2)] * total_periods
    else:
        return [None] * total_periods, [None] * total_periods, [0.0] * total_periods, 0.0, 0, -1

    last_idx = max(valid_idx)
    actual_line = [full_line[i] if i <= last_idx else None for i in range(total_periods)]
    forecast_line = [full_line[i] if i >= last_idx else None for i in range(total_periods)]

    return actual_line, forecast_line, full_line, round(float(slope), 4), len(valid_idx), last_idx


def normalize_comp_name(name: str, core_only: bool = False) -> str:
    if not name:
        return ""
    s = str(name).strip()
    if core_only:
        s = re.sub(r"\([^)]*\)", "", s)
        s = s.split("_")[0]
    for token in ["농업회사법인", "주식회사", "유한회사", "(주)", "㈜", "(유)", "(법인)", " ", "_", "\n", "\t", ","]:
        s = s.replace(token, "")
    s = s.replace("롯데웰푸드", "롯데푸드")
    return s.strip()


def clean_product_for_match(name: str) -> str:
    if not name:
        return ""
    s = str(name).strip()
    s = re.sub(r"\(\s*\d+(?:\.\d+)?\s*(?:mL|ml|ML|㎖|L|g|kg)\s*\)", "", s)
    s = re.sub(r"(?<!\d)\d{2,4}\s*(?:mL|ml|ML|㎖)(?!\w)", "", s)
    s = re.sub(r"\s+1[0127]\d{3}\b", "", s)
    s = re.sub(r"[\s\(\)\[\]\{\}\-_·,/&\+!]+", "", s).lower()
    return s


def is_product_match(record_prod: str, selected_prod: str, kw_text: str = "") -> bool:
    rec_raw = str(record_prod or "").strip()
    if not rec_raw or rec_raw == "-":
        return False

    rec_parts = [p.strip() for p in rec_raw.split("/") if p.strip()]
    rec_cleans = [clean_product_for_match(p) for p in rec_parts] + [clean_product_for_match(rec_raw)]

    if selected_prod and selected_prod not in ["", "ALL", "전체 (제품 미선택 · 소비기한으로만 검색)"]:
        sel_clean = clean_product_for_match(selected_prod)
        if not sel_clean:
            return False
        matched_sel = False
        for rc in rec_cleans:
            if not rc:
                continue
            if sel_clean == rc or (len(sel_clean) >= 2 and sel_clean in rc) or (len(rc) >= 3 and rc in sel_clean):
                matched_sel = True
                break
        if not matched_sel:
            return False

    if kw_text and kw_text.strip():
        kw_clean = clean_product_for_match(kw_text)
        kw_raw_low = kw_text.strip().lower()
        if kw_clean:
            if not any(kw_clean in rc for rc in rec_cleans if rc) and (kw_raw_low not in rec_raw.lower()):
                return False

    return True


def parse_exp_query_info(exp_input: str) -> dict:
    s = str(exp_input or "").strip()
    if not s:
        return {"empty": True}

    m_full = re.search(r"(20\d{2})\D+(\d{1,2})\D+(\d{1,2})", s)
    if m_full:
        yr, mm, dd = int(m_full.group(1)), int(m_full.group(2)), int(m_full.group(3))
        return {
            "empty": False,
            "iso_full": f"{yr:04d}-{mm:02d}-{dd:02d}",
            "mm_dd_dash": f"{mm:02d}-{dd:02d}",
            "mm_dd_dot": f"{mm:02d}.{dd:02d}",
            "m_d_dot": f"{mm}.{dd}",
            "raw": s
        }

    m_two = re.search(r"^(\d{2})\D+(\d{1,2})\D+(\d{1,2})$", s)
    if m_two:
        yr, mm, dd = 2000 + int(m_two.group(1)), int(m_two.group(2)), int(m_two.group(3))
        return {
            "empty": False,
            "iso_full": f"{yr:04d}-{mm:02d}-{dd:02d}",
            "mm_dd_dash": f"{mm:02d}-{dd:02d}",
            "mm_dd_dot": f"{mm:02d}.{dd:02d}",
            "m_d_dot": f"{mm}.{dd}",
            "raw": s
        }

    m_short = re.search(r"^(\d{1,2})\D+(\d{1,2})$", s)
    if m_short:
        mm, dd = int(m_short.group(1)), int(m_short.group(2))
        return {
            "empty": False,
            "iso_full": "",
            "mm_dd_dash": f"{mm:02d}-{dd:02d}",
            "mm_dd_dot": f"{mm:02d}.{dd:02d}",
            "m_d_dot": f"{mm}.{dd}",
            "raw": s
        }

    return {"empty": False, "iso_full": "", "mm_dd_dash": "", "mm_dd_dot": "", "m_d_dot": "", "raw": s}


def is_exp_match(raw_exp: str, iso_exp: str, q_info: dict) -> bool:
    if q_info.get("empty", True):
        return True

    raw_s = re.sub(r"\s+", "", str(raw_exp or ""))
    iso_s = re.sub(r"\s+", "", str(iso_exp or ""))

    if q_info.get("iso_full"):
        if q_info["iso_full"] in iso_s:
            return True
        yr_2 = q_info["iso_full"][2:4]
        mm_dd_dot = q_info["mm_dd_dot"]
        if f"{q_info['iso_full'][:4]}.{mm_dd_dot}" in raw_s or f"{yr_2}.{mm_dd_dot}" in raw_s:
            return True
        return False

    if q_info.get("mm_dd_dash"):
        if iso_s and any(tok.endswith(q_info["mm_dd_dash"]) for tok in iso_s.split(",")):
            return True
        if q_info["mm_dd_dot"] in raw_s or q_info["m_d_dot"] in raw_s:
            return True
        return False

    q_raw = re.sub(r"\s+", "", q_info.get("raw", ""))
    return (q_raw in raw_s) or (q_raw in iso_s)


# ==============================================================================
# 3. [1] 원가절감 현황 & [2] 클레임 건수 시트 파서
# ==============================================================================
def fetch_cost_sheet(creds, sheet_urls_map: dict):
    sh, err = open_sheet_threadsafe(creds, sheet_urls_map, "cost")
    default_targets = {"부문": 1.54, "식품안전팀": 1.0, "품질관리팀": 0.54}
    if sh is None:
        return [], default_targets, err

    try:
        ws = api_call_with_retry(sh.get_worksheet, 0)
        rows = get_values_unformatted(ws)
        targets = default_targets.copy()

        if len(rows) >= 1:
            r0_join = " ".join([str(x) for x in rows[0]])
            nums_uk = re.findall(r"(\d+(?:\.\d+)?)\s*억", r0_join)
            if len(nums_uk) >= 3:
                targets = {
                    "부문": float(nums_uk[0]),
                    "식품안전팀": float(nums_uk[1]),
                    "품질관리팀": float(nums_uk[2])
                }

        hdr_row_idx = 1
        for idx, r in enumerate(rows[:5]):
            if sum(1 for c in r if re.sub(r"\s+", "", str(c)) == "원가절감") >= 2:
                hdr_row_idx = idx
                break

        hdr_cells = [re.sub(r"\s+", "", str(c)) for c in rows[hdr_row_idx]]
        act_cols = [i for i, h in enumerate(hdr_cells) if h == "원가절감"]
        if len(act_cols) < 3:
            act_cols = [1, 6, 11]

        b_c, fs_c, qc_c = act_cols[0], act_cols[1], act_cols[2]
        parsed = []
        for r in rows[hdr_row_idx + 1:]:
            r_pad = list(r) + [""] * max(0, qc_c + 4 - len(r))
            m_name = str(r_pad[max(0, b_c - 1)]).strip()
            if m_name in FISCAL_MONTHS:
                b_act = round(to_float(r_pad[b_c]), 1)
                b_tar = round(to_float(r_pad[b_c + 1]), 1)
                fs_act = round(to_float(r_pad[fs_c]), 1)
                fs_tar = round(to_float(r_pad[fs_c + 1]), 1)
                qc_act = round(to_float(r_pad[qc_c]), 1)
                qc_tar = round(to_float(r_pad[qc_c + 1]), 1)
                is_rec = (b_act > 0) or (fs_act > 0) or (qc_act > 0)
                parsed.append({
                    "월": m_name,
                    "부문_실적(만원)": b_act,
                    "부문_기준(만원)": b_tar,
                    "부문_달성률(%)": round((b_act / b_tar) * 100, 1) if b_tar > 0 else 0.0,
                    "식품안전팀_실적(만원)": fs_act,
                    "식품안전팀_기준(만원)": fs_tar,
                    "식품안전팀_달성률(%)": round((fs_act / fs_tar) * 100, 1) if fs_tar > 0 else 0.0,
                    "품질관리팀_실적(만원)": qc_act,
                    "품질관리팀_기준(만원)": qc_tar,
                    "품질관리팀_달성률(%)": round((qc_act / qc_tar) * 100, 1) if qc_tar > 0 else 0.0,
                    "_기록여부": is_rec
                })
        return parsed, targets, None
    except Exception as e:
        return [], default_targets, str(e)


def fetch_claim_sheet(creds, sheet_urls_map: dict):
    sh, err = open_sheet_threadsafe(creds, sheet_urls_map, "claim")
    if sh is None:
        return [], [], err

    try:
        ws_all = api_call_with_retry(sh.worksheets)
        target_ws = ws_all[0] if ws_all else None
        if len(ws_all) > 1:
            for w in ws_all:
                sample = get_values_formatted(w)[:8]
                flat_txt = " ".join([" ".join(r) for r in sample])
                if "유형" in flat_txt and ("3월" in flat_txt or "생산" in flat_txt):
                    target_ws = w
                    break

        rows = get_values_unformatted(target_ws)
        header_row_idx = 1
        for idx, r in enumerate(rows[:10]):
            r_str = [str(c).strip() for c in r]
            if "유형" in r_str and "합계" in r_str:
                header_row_idx = idx
                break

        hdr = [str(c).strip() for c in rows[header_row_idx]]
        cat_col = hdr.index("유형") if "유형" in hdr else 1
        sum_col = hdr.index("합계") if "합계" in hdr else 2
        year_col = max(0, cat_col - 1)

        month_col_map = {}
        for idx, cell_txt in enumerate(hdr):
            if cell_txt in FISCAL_MONTHS:
                month_col_map[cell_txt] = idx
        for idx, m in enumerate(FISCAL_MONTHS):
            if m not in month_col_map:
                month_col_map[m] = sum_col + 3 + (idx * 3)

        cat_list = []
        current_year = ""
        for r in rows[header_row_idx + 2:]:
            if not any(str(x).strip() != "" for x in r):
                continue
            r_pad = list(r) + [""] * max(0, 45 - len(r))
            yr_raw = str(r_pad[year_col]).strip()
            cat_raw = str(r_pad[cat_col]).strip()

            if re.search(r"20\d{2}", yr_raw):
                m_yr = re.search(r"(20\d{2})", yr_raw)
                current_year = f"{m_yr.group(1)}년"

            if not current_year or not cat_raw or cat_raw in ["유형", "클레임 건수"]:
                continue

            t_cnt = int(round(to_float(r_pad[sum_col])))
            t_prod = int(round(to_float(r_pad[sum_col + 1])))
            t_ppm = round(to_float(r_pad[sum_col + 2]), 2)

            row_dict = {
                "연도": current_year,
                "유형": cat_raw,
                "합계_건수": t_cnt,
                "합계_생산수량": t_prod,
                "합계_PPM": t_ppm
            }
            for m in FISCAL_MONTHS:
                mc = month_col_map[m]
                m_cnt = int(round(to_float(r_pad[mc]))) if mc < len(r_pad) else 0
                m_prd = int(round(to_float(r_pad[mc + 1]))) if mc + 1 < len(r_pad) else 0
                m_ppm = round(to_float(r_pad[mc + 2]), 2) if mc + 2 < len(r_pad) else 0.0
                row_dict[f"{m}_건수"] = m_cnt
                row_dict[f"{m}_생산수량"] = m_prd
                row_dict[f"{m}_PPM"] = m_ppm
            cat_list.append(row_dict)

        if not cat_list:
            return [], [], "클레임 시트에서 유효한 데이터를 찾지 못했습니다."

        years_found = sorted(list({r["연도"] for r in cat_list}), reverse=True)
        yr_cur = "2026년" if "2026년" in years_found else years_found[0]
        yr_prev = "2025년" if "2025년" in years_found else (years_found[1] if len(years_found) > 1 else "")

        row_cur = next((r for r in cat_list if r["연도"] == yr_cur and r["유형"] == "합계"), None)
        row_prev = next((r for r in cat_list if r["연도"] == yr_prev and r["유형"] == "합계"), None) if yr_prev else None

        trend_rows = []
        for m in FISCAL_MONTHS:
            c26 = int(row_cur[f"{m}_건수"]) if row_cur else 0
            prod26 = int(row_cur[f"{m}_생산수량"]) if row_cur else 0
            p26 = float(row_cur[f"{m}_PPM"]) if row_cur else 0.0
            c25 = int(row_prev[f"{m}_건수"]) if row_prev else 0
            p25 = float(row_prev[f"{m}_PPM"]) if row_prev else 0.0
            trend_rows.append({
                "월": m,
                "2026년_건수": c26,
                "2026년_생산수량": prod26,
                "2026년_PPM": p26,
                "2025년_건수": c25,
                "2025년_PPM": p25,
                "목표_PPM": 1.12,
                "_기록여부": (prod26 > 0 or c26 > 0)
            })
        return trend_rows, cat_list, None
    except Exception as e:
        return [], [], str(e)


def build_division_claim_summary(cat_list: list, year_str: str, division_categories: list) -> dict:
    sub = [r for r in cat_list if r["연도"] == year_str and r["유형"] in division_categories]
    monthly = []
    for m in FISCAL_MONTHS:
        cnt_sum = sum(int(r.get(f"{m}_건수", 0)) for r in sub)
        prod_sum = sum(int(r.get(f"{m}_생산수량", 0)) for r in sub)
        ppm_val = round((cnt_sum / prod_sum) * 1_000_000, 2) if prod_sum > 0 else 0.0
        item = {
            "월": m,
            "합산_건수": cnt_sum,
            "합산_생산수량": prod_sum,
            "부문_통합_PPM": ppm_val,
            "_기록여부": (prod_sum > 0 or cnt_sum > 0)
        }
        for cat in division_categories:
            cr = next((x for x in sub if x["유형"] == cat), None)
            item[f"{cat}_건수"] = int(cr.get(f"{m}_건수", 0)) if cr else 0
            item[f"{cat}_PPM"] = float(cr.get(f"{m}_PPM", 0.0)) if cr else 0.0
        monthly.append(item)

    tot_c = sum(x["합산_건수"] for x in monthly)
    tot_p = sum(x["합산_생산수량"] for x in monthly)
    tot_ppm = round((tot_c / tot_p) * 1_000_000, 2) if tot_p > 0 else 0.0
    act_l, fc_l, full_l, slp, _, last_i = calc_trend_and_forecast_split(
        [x["부문_통합_PPM"] for x in monthly],
        [x["_기록여부"] for x in monthly]
    )
    return {
        "year": year_str,
        "categories": division_categories,
        "total_cnt": tot_c,
        "total_prod": tot_p,
        "total_ppm": tot_ppm,
        "slope": slp,
        "feb_pred": full_l[-1] if full_l else 0.0,
        "last_idx": last_i,
        "act_line": act_l,
        "fc_line": fc_l,
        "monthly": monthly,
        "raw": sub
    }


# ==============================================================================
# 4. [3] 3대 공정일보(상온 · 유제품 · 두유배양) 실시간 탭 & 누적 _이력DB_* 파서
# ==============================================================================
def get_sorted_date_worksheets(sh):
    if sh is None:
        return None, None, []
    ws_list = api_call_with_retry(sh.worksheets)
    today = now_kst()
    yesterday = today - timedelta(days=1)

    date_tabs = []
    for w in ws_list:
        t = w.title.strip()
        if t.startswith("_이력DB_") or any(ex in t for ex in ["양식", "복사", "백업", "사본", "대기", "식품안전나라"]):
            continue
        m = re.match(r"^(\d{1,2})[/\.\-](\d{1,2})$", t)
        if m:
            mm, dd = int(m.group(1)), int(m.group(2))
            yr = today.year if mm <= today.month + 1 else today.year - 1
            try:
                dt_obj = datetime(yr, mm, dd)
            except ValueError:
                dt_obj = datetime(today.year, 1, 1)
            date_tabs.append((dt_obj, w))

    past_or_today = sorted([x for x in date_tabs if x[0].date() <= today.date()], key=lambda x: x[0], reverse=True)
    future_tabs = sorted([x for x in date_tabs if x[0].date() > today.date()], key=lambda x: x[0], reverse=True)
    ordered_ws = [w for _, w in (past_or_today + future_tabs)]

    t_cands = {today.strftime("%m/%d"), f"{today.month}/{today.day}", today.strftime("%m.%d"), f"{today.month}.{today.day}"}
    y_cands = {yesterday.strftime("%m/%d"), f"{yesterday.month}/{yesterday.day}", yesterday.strftime("%m.%d"), f"{yesterday.month}.{yesterday.day}"}

    ws_today = next((w for w in ordered_ws if w.title.strip() in t_cands), None)
    ws_yest = next((w for w in ordered_ws if w.title.strip() in y_cands), None)

    if ws_today is None and len(ordered_ws) >= 1:
        ws_today = ordered_ws[0]
    if ws_yest is None and len(ordered_ws) >= 2:
        idx_t = ordered_ws.index(ws_today) if ws_today in ordered_ws else 0
        ws_yest = ordered_ws[idx_t + 1] if idx_t + 1 < len(ordered_ws) else ordered_ws[0]

    if ws_today is None and ws_list:
        non_db_ws = [w for w in ws_list if not w.title.strip().startswith("_이력DB_")]
        ws_today = non_db_ws[0] if non_db_ws else ws_list[0]

    return ws_today, ws_yest, ordered_ws


def eval_ccp3b_cell(val_str: str, std_str: str) -> str:
    s = str(val_str).strip()
    if s in ["", "-"]:
        return ""
    nums = [float(x) for x in re.findall(r"\d+(?:\.\d+)?", s)]
    if not nums:
        return "cell-green" if "적합" in s else ""
    ranges = re.findall(r"(\d+(?:\.\d+)?)\s*~\s*(\d+(?:\.\d+)?)", str(std_str))
    if not ranges:
        return "cell-green"
    all_ok = all(any(float(lo) <= n <= float(hi) for lo, hi in ranges) for n in nums)
    return "cell-green" if all_ok else "cell-red"


def eval_ster_metric(actual_str: str, std_str: str, sterilizer_ho: str, metric_kind: str) -> str:
    act_s = str(actual_str).strip()
    if act_s in ["", "-", "기록대기"]:
        return "cell-yellow"
    act_nums = [float(x) for x in re.findall(r"\d+(?:\.\d+)?", act_s)]
    if not act_nums:
        return "cell-yellow"
    act_val = act_nums[0]

    std_s = str(std_str).strip()
    if std_s in ["", "-"]:
        return "cell-green"

    target_val = None
    ho_num_m = re.search(r"(\d+)호기", str(sterilizer_ho))
    ho_num = ho_num_m.group(1) if ho_num_m else ""

    if ho_num and f"{ho_num}호기" in std_s:
        m_specific = re.search(rf"{ho_num}호기[^\d]*(\d+(?:\.\d+)?)", std_s)
        if m_specific:
            target_val = float(m_specific.group(1))
    elif "그외" in std_s:
        m_other = re.search(r"그외[^\d]*(\d+(?:\.\d+)?)", std_s)
        if m_other:
            target_val = float(m_other.group(1))

    if target_val is None:
        std_nums = [float(x) for x in re.findall(r"\d+(?:\.\d+)?", std_s)]
        if std_nums:
            target_val = min(std_nums)

    if target_val is None:
        return "cell-green"

    if metric_kind == "temp":
        return "cell-green" if act_val >= (target_val - 2.0) else "cell-red"
    if metric_kind == "press":
        return "cell-green" if act_val >= (target_val * 0.90) else "cell-red"
    if metric_kind == "hold":
        return "cell-green" if act_val >= (target_val - 0.8) else "cell-red"
    return "cell-green"


def is_same_tab_date_memo(memo_dt_str: str, tab_title: str) -> bool:
    m_tab = re.match(r"^(\d{1,2})[/\.\-](\d{1,2})$", str(tab_title).strip())
    if not m_tab or not str(memo_dt_str).strip():
        return False
    t_m, t_d = int(m_tab.group(1)), int(m_tab.group(2))

    s = str(memo_dt_str).strip()
    if re.search(r"(?:20)?25[\.\-/]", s):
        return False
    nums = [int(x) for x in re.findall(r"\d+", s)]
    if len(nums) >= 3:
        yr, mm, dd = nums[0], nums[1], nums[2]
        if yr in (26, 2026) and mm == t_m and dd == t_d:
            return True
    elif len(nums) == 2:
        if nums[0] == t_m and nums[1] == t_d:
            return True
    return False


def parse_rt_process_worksheet(ws):
    if ws is None:
        return None
    rows = get_values_formatted(ws)
    if len(rows) < 5:
        return None

    tab_title = ws.title.strip()
    prod_date_str = tab_title
    for r in rows[:4]:
        for c in r[:18]:
            if "생 산 일" in str(c) or re.search(r"202\d\.\s*\d{1,2}\.\s*\d{1,2}", str(c)):
                prod_date_str = str(c).replace("생 산 일 :", "").strip()
                break

    r_pack_hdr, r_ccp3b_hdr, r_ster_hdr = None, None, None
    for idx, r in enumerate(rows):
        b_txt = re.sub(r"\s+", "", str(r[1])) if len(r) > 1 else ""
        row_join = re.sub(r"\s+", "", "".join(str(x) for x in r[:12]))
        if r_pack_hdr is None and b_txt == "포장호기":
            r_pack_hdr = idx
        elif r_ccp3b_hdr is None and ("CCP-3B" in row_join or "포장기공정관리" in row_join):
            r_ccp3b_hdr = idx
        elif r_ccp3b_hdr is not None and r_ster_hdr is None and b_txt == "멸균기":
            r_ster_hdr = idx
            break

    if r_pack_hdr is None:
        r_pack_hdr = 2
    if r_ccp3b_hdr is None:
        r_ccp3b_hdr = min(15, len(rows) - 1)
    if r_ster_hdr is None:
        r_ster_hdr = min(r_ccp3b_hdr + 6, len(rows) - 1)

    ho_products, ho_rows_raw = {}, {}
    current_ho = ""
    for r in rows[r_pack_hdr + 1 : r_ccp3b_hdr]:
        r_pad = list(r) + [""] * max(0, 18 - len(r))
        b_val = str(r_pad[1]).strip()
        if re.match(r"^[A-Z가-힣0-9]+호기$", b_val):
            current_ho = b_val
            ho_rows_raw.setdefault(current_ho, [])
            ho_products.setdefault(current_ho, [])

        if not current_ho:
            continue

        slots = [
            (r_pad[2:6], [r_pad[6]], r_pad[7:9]),
            (r_pad[9:13], r_pad[13:15], r_pad[15:17])
        ]
        for p_cells, e_cells, t_cells in slots:
            p_name = next((str(x).strip() for x in p_cells if str(x).strip() not in ["", "제 품 명", "제  품  명"]), "")
            exp_v = next((str(x).strip() for x in e_cells if str(x).strip() != ""), "")
            t_parts = [str(x).strip() for x in t_cells if str(x).strip() != ""]
            t_str = " / ".join(t_parts)

            if p_name or exp_v:
                has_time = bool(re.search(r"\d{1,2}:\d{2}", t_str))
                if p_name and has_time and exp_v:
                    st_txt, st_cls = "확인완료", "cell-green"
                elif p_name and not exp_v:
                    st_txt, st_cls = "소비기한 누락", "cell-red"
                else:
                    st_txt, st_cls = "확인시간 대기", "cell-yellow"

                if p_name and p_name not in ho_products[current_ho]:
                    ho_products[current_ho].append(p_name)

                ho_rows_raw[current_ho].append({
                    "포장호기": current_ho,
                    "생산 제품명": p_name if p_name else "(제품명 미기입)",
                    "소비기한": exp_v if exp_v else "-",
                    "확인시간": t_str if t_str else "미기입(대기)",
                    "상태": st_txt,
                    "_cls": st_cls
                })

    lines_records = []
    for ho_k, items in ho_rows_raw.items():
        if items:
            lines_records.extend(items)
        else:
            lines_records.append({
                "포장호기": ho_k,
                "생산 제품명": "(비가동)",
                "소비기한": "-",
                "확인시간": "-",
                "상태": "비가동",
                "_cls": "cell-gray"
            })

    ccp3b_records = []
    r_3b_colhdr = r_ccp3b_hdr
    for sub_i in range(r_ccp3b_hdr, min(r_ccp3b_hdr + 3, r_ster_hdr)):
        if sum(1 for c in rows[sub_i][:18] if re.match(r"^[A-F]호기$", str(c).strip())) >= 3:
            r_3b_colhdr = sub_i
            break

    hdr_3b = [str(x).strip() for x in rows[r_3b_colhdr][:18]]
    ho_pos_list = sorted([(idx, cell) for idx, cell in enumerate(hdr_3b) if re.match(r"^[A-F]호기$", cell)], key=lambda x: x[0])

    first_ho_col = ho_pos_list[0][0] if ho_pos_list else 8
    ho_col_spans = {}
    for i_p, (c_start, h_lbl) in enumerate(ho_pos_list):
        c_end = ho_pos_list[i_p + 1][0] if i_p + 1 < len(ho_pos_list) else 17
        ho_col_spans[h_lbl] = (c_start, c_end)

    last_item_3b = ""
    for r in rows[r_3b_colhdr + 1 : r_ster_hdr]:
        r_pad = list(r) + [""] * max(0, 18 - len(r))
        if not any(str(x).strip() for x in r_pad[1:17]):
            continue
        item_nm = str(r_pad[1]).strip()
        if item_nm:
            last_item_3b = item_nm

        left_meta = [str(x).strip() for x in r_pad[2:first_ho_col] if str(x).strip() and str(x).strip() != item_nm]
        std_parts = [x for x in left_meta if any(u in x for u in ["~", "%", "℃", "㎕", "이상", "이하"])]
        eq_parts = [x for x in left_meta if x not in std_parts]

        std_nm = " ".join(std_parts)
        equip_nm = " ".join(eq_parts)
        if not last_item_3b and not std_nm:
            continue

        rec_3b = {"항목": last_item_3b, "관리기준": std_nm or "-", "적용설비": equip_nm or "-", "_cls_map": {}}
        for ho_lbl in ["A호기", "B호기", "C호기", "D호기", "E호기", "F호기"]:
            if ho_lbl in ho_col_spans:
                cs, ce = ho_col_spans[ho_lbl]
                vals_in_span = [str(x).strip() for x in r_pad[cs:ce] if str(x).strip() not in ["", "-"]]
                v_txt = " / ".join(vals_in_span) if vals_in_span else "-"
            else:
                v_txt = "-"
            rec_3b[ho_lbl] = v_txt
            rec_3b["_cls_map"][ho_lbl] = eval_ccp3b_cell(v_txt, rec_3b["관리기준"])
        ccp3b_records.append(rec_3b)

    sv_std_by_prod = {}
    for r in rows[r_ster_hdr:]:
        if len(r) >= 22:
            s_t = re.sub(r"\s+", " ", str(r[18])).strip()
            t_p = re.sub(r"\s+", " ", str(r[19])).strip()
            u_h = re.sub(r"\s+", " ", str(r[20])).strip()
            v_pr = re.sub(r"\s+", " ", str(r[21])).strip()
            if v_pr and v_pr not in ["호기별 제품", "제품명"] and any([s_t, t_p, u_h]):
                sv_std_by_prod[v_pr] = (s_t, t_p, u_h)

    ster_blocks = []
    cur_block = None
    for r in rows[r_ster_hdr + 1:]:
        r_pad = list(r) + [""] * max(0, 23 - len(r))
        b_cell = str(r_pad[1]).strip()

        if re.match(r"^\d+호기$", b_cell):
            if cur_block:
                ster_blocks.append(cur_block)
            cur_block = {"ster_ho": b_cell, "rows": [r_pad], "remark": ""}
        elif re.sub(r"\s+", "", b_cell) == "특이사항" and cur_block is not None:
            rem_chunks = []
            for x in r_pad[2:17]:
                xs = str(x).strip()
                if xs not in ["", "-"] and xs not in rem_chunks:
                    rem_chunks.append(xs)
            cur_block["remark"] = " ".join(rem_chunks)
            ster_blocks.append(cur_block)
            cur_block = None
        elif cur_block is not None and len(cur_block["rows"]) < 5:
            if b_cell == "":
                cur_block["rows"].append(r_pad)
            else:
                ster_blocks.append(cur_block)
                cur_block = None
    if cur_block:
        ster_blocks.append(cur_block)

    ster_records = []
    ster_special_notes = []
    ho_ster_summary, ho_remark_map = {}, {}

    for blk in ster_blocks:
        s_ho = blk["ster_ho"]
        b_rows = blk["rows"]
        pack_ho = next((str(br[2]).strip() for br in b_rows if str(br[2]).strip() not in ["", "-"]), "-")
        vol_str = next((str(br[3]).strip() for br in b_rows if str(br[3]).strip() not in ["", "-"]), "-")
        flt_str = next((str(br[13]).strip() or str(br[14]).strip() for br in b_rows if (str(br[13]).strip() or str(br[14]).strip()) not in ["", "-"]), "-")

        std_temp = next((re.sub(r"\s+", " ", str(br[18])).strip() for br in b_rows if str(br[18]).strip() not in ["", "-", "멸균온도"]), "")
        std_press = next((re.sub(r"\s+", " ", str(br[19])).strip() for br in b_rows if str(br[19])).strip() not in ["", "-", "균질압"]), "")
        std_hold = next((re.sub(r"\s+", " ", str(br[20])).strip() for br in b_rows if str(br[20])).strip() not in ["", "-", "멸균시간"]), "")
        std_v_prod = next((re.sub(r"\s+", " ", str(br[21])).strip() for br in b_rows if str(br[21]).strip() not in ["", "-", "호기별 제품"]), "")

        matched_prods = ho_products.get(pack_ho, [])
        matched_prod_str = ", ".join(matched_prods) if matched_prods else (std_v_prod or "-")

        if not (std_temp or std_press or std_hold) and matched_prods:
            for mp in matched_prods:
                for sv_k, (st_t, st_p, st_h) in sv_std_by_prod.items():
                    if sv_k in mp or mp in sv_k:
                        std_temp, std_press, std_hold = st_t, st_p, st_h
                        break

        rem_val = blk["remark"].strip()
        if rem_val:
            ster_special_notes.append(f"[{s_ho}·{pack_ho} {matched_prod_str}] {rem_val}")
            if pack_ho != "-":
                ho_remark_map[pack_ho] = rem_val

        has_record = False
        for br in b_rows:
            mon_time = str(br[4]).strip() or str(br[5]).strip()
            temp_v = str(br[6]).strip()
            press_v = str(br[7]).strip() or str(br[8]).strip()
            hold_v = str(br[9]).strip()
            flow_v = str(br[10]).strip() or str(br[11]).strip()
            cool_v = str(br[12]).strip()
            sign_v = str(br[15]).strip() or str(br[16]).strip()

            if any(x not in ["", "-"] for x in [mon_time, temp_v, press_v, hold_v, flow_v, cool_v]):
                has_record = True
                temp_disp = f"{temp_v}℃" + (f" (기준: {std_temp})" if std_temp else "")
                press_disp = f"{press_v}" + (f" (기준: {std_press})" if std_press else "")
                hold_disp = f"{hold_v}초" + (f" (기준: {std_hold})" if std_hold else "")

                c_temp = eval_ster_metric(temp_v, std_temp, s_ho, "temp")
                c_press = eval_ster_metric(press_v, std_press, s_ho, "press")
                c_hold = eval_ster_metric(hold_v, std_hold, s_ho, "hold")
                c_flt = "cell-green" if flt_str == "적합" else ("cell-red" if "부적합" in flt_str else "cell-yellow")
                c_time = "cell-green" if re.search(r"\d{1,2}:\d{2}", mon_time) else "cell-yellow"
                c_sign = "cell-green" if sign_v not in ["", "-"] else "cell-yellow"

                ster_records.append({
                    "멸균기_포장": f"{s_ho} ({pack_ho})",
                    "매칭 제품명": f"{matched_prod_str} ({vol_str})" if vol_str != "-" else matched_prod_str,
                    "확인시간": mon_time or "미기입",
                    "멸균온도": temp_disp,
                    "균질압": press_disp,
                    "멸균시간": hold_disp,
                    "유량_냉각": f"{flow_v or '-'} L/h / {cool_v or '-'}℃",
                    "여과망_서명": f"{flt_str} / {sign_v or '미서명'}",
                    "특이사항": rem_val if rem_val else "없음",
                    "_c_time": c_time,
                    "_c_temp": c_temp,
                    "_c_press": c_press,
                    "_c_hold": c_hold,
                    "_c_flt": c_flt if c_sign == "cell-green" else "cell-yellow",
                    "_c_rem": "cell-yellow" if rem_val else ""
                })
                if pack_ho != "-":
                    ho_ster_summary[pack_ho] = f"{s_ho} ({temp_v}℃ / {press_v}bar / {hold_v}초 / 여과망:{flt_str})"

        if not has_record and pack_ho != "-":
            ster_records.append({
                "멸균기_포장": f"{s_ho} ({pack_ho})",
                "매칭 제품명": f"{matched_prod_str} ({vol_str})" if vol_str != "-" else matched_prod_str,
                "확인시간": "기록대기",
                "멸균온도": f"- (기준: {std_temp})" if std_temp else "-",
                "균질압": f"- (기준: {std_press})" if std_press else "-",
                "멸균시간": f"- (기준: {std_hold})" if std_hold else "-",
                "유량_냉각": "-",
                "여과망_서명": f"{flt_str} / 미서명",
                "특이사항": rem_val if rem_val else "없음",
                "_c_time": "cell-yellow",
                "_c_temp": "cell-yellow",
                "_c_press": "cell-yellow",
                "_c_hold": "cell-yellow",
                "_c_flt": "cell-yellow",
                "_c_rem": "cell-yellow" if rem_val else ""
            })

    compact_rows = []
    for lr in lines_records:
        if lr["생산 제품명"] == "(비가동)":
            continue
        p_ho = lr["포장호기"]
        rem_txt = ho_remark_map.get(p_ho, "이상없음")
        compact_rows.append({
            "포장호기": p_ho,
            "생산 제품명": lr["생산 제품명"],
            "소비기한": lr["소비기한"],
            "확인시간": lr["확인시간"],
            "멸균기·CCP 요약": ho_ster_summary.get(p_ho, "-"),
            "특이사항": rem_txt,
            "_cls_status": lr["_cls"],
            "_cls_rem": "cell-yellow" if rem_txt != "이상없음" else "cell-green"
        })

    for r in rows[4:]:
        if len(r) >= 29:
            dt_s = str(r[27]).strip()
            if not is_same_tab_date_memo(dt_s, tab_title):
                continue
            memo_chunks = []
            for c in r[28:]:
                cs = str(c).strip()
                if cs and cs not in memo_chunks and "전달(특이)" not in cs and "생 산 일" not in cs:
                    memo_chunks.append(cs)
            memo_s = " / ".join(memo_chunks)
            if memo_s:
                ster_special_notes.append(f"[당일 전달사항] {memo_s}")

    return {
        "tab_title": tab_title,
        "prod_date": prod_date_str,
        "running_hos": [k for k, v in ho_products.items() if v],
        "lines_records": lines_records,
        "compact_rows": compact_rows,
        "ccp3b_records": ccp3b_records,
        "ster_records": ster_records,
        "ster_special_notes": ster_special_notes
    }


def parse_dairy_pack_worksheet(ws, line_label: str):
    if ws is None:
        return [], "-"
    rows = get_values_formatted(ws)
    w_date = ws.title.strip()
    for r in rows[:5]:
        joined = " ".join([str(c).strip() for c in r if str(c).strip()])
        m_dt = re.search(r"202\d[\.\-\s]+\d{1,2}[\.\-\s]+\d{1,2}", joined)
        if m_dt:
            w_date = m_dt.group(0).strip()
            break

    parsed_products = []
    for r_idx, r in enumerate(rows):
        first_cell = re.sub(r"\s+", "", str(r[0])) if len(r) > 0 else ""
        if first_cell in ["제품명", "품목"]:
            next_block_r = len(rows)
            for nr in range(r_idx + 1, min(r_idx + 20, len(rows))):
                nc = re.sub(r"\s+", "", str(rows[nr][0])) if len(rows[nr]) > 0 else ""
                if nc in ["제품명", "품목", "생산시간", "CCP검증모니터링"]:
                    next_block_r = nr
                    break

            for col_i in range(1, len(r)):
                p_name = re.sub(r"\s+", " ", str(r[col_i])).strip()
                if not p_name or p_name in ["제 품 명", "제품명", "품 목", "품목"] or "특이사항" in p_name:
                    continue

                vol_str, chk_time = "", "-"
                exp_list = []
                label_ok, top_ok, bot_ok, vol_ok, sens_ok = "-", "-", "-", "-", "-"

                for sub_r in rows[r_idx + 1 : next_block_r]:
                    row_lbl = re.sub(r"\s+", "", str(sub_r[0])) if len(sub_r) > 0 else ""
                    val = str(sub_r[col_i]).strip() if col_i < len(sub_r) else ""
                    if not val:
                        continue
                    if "㎖" in val or "mL" in val:
                        vol_str = val
                    elif "확인시간" in row_lbl:
                        chk_time = val
                    elif any(k in row_lbl for k in ["소비기한", "Tank"]) or re.match(r"^\d{2}\.\d{2}[A-Z]?$", row_lbl):
                        if val not in ["00.00", "-", "소비기한"] and "제조일로부터" not in val:
                            exp_list.append(val)
                    elif "표시사항" in row_lbl or "표기사항" in row_lbl:
                        label_ok = val
                    elif "상부접착" in row_lbl or "상/하부접착" in row_lbl:
                        top_ok = val
                        if "상/하부" in row_lbl:
                            bot_ok = val
                    elif "하부접착" in row_lbl:
                        bot_ok = val
                    elif "제품용량" in row_lbl:
                        vol_ok = val
                    elif "관능" in row_lbl:
                        sens_ok = val

                exp_joined = ", ".join(dict.fromkeys(exp_list)) if exp_list else "-"
                is_running = (exp_joined != "-") or (chk_time not in ["-", ""])
                full_p_name = f"{p_name} ({vol_str})" if vol_str else p_name

                parsed_products.append({
                    "라인구분": line_label,
                    "작성일": w_date,
                    "가동여부": "금일생산" if is_running else "미생산(00.00)",
                    "제품명": full_p_name,
                    "확인시간": chk_time,
                    "표기 소비기한": exp_joined,
                    "표시사항": label_ok,
                    "상부접착": top_ok,
                    "하부접착": bot_ok,
                    "제품용량": vol_ok,
                    "관능(5점법)": sens_ok
                })

    return parsed_products, w_date


SOY_STOP_LABELS = {
    "배양검사", "배양온도", "용량/포장기", "포장기", "작성자", "검사자",
    "품목", "생산일", "소비기한", "검사일", "생산시작", "생산종료",
    "총검사수량", "오염수량", "판정", "특이사항", "소비기한품목명표시",
    "확인시간", "제품명", "TRUE", "FALSE"
}


def calc_run_duration(start_str: str, end_str: str) -> str:
    m1 = re.search(r"(\d{1,2}):(\d{2})", str(start_str))
    m2 = re.search(r"(\d{1,2}):(\d{2})", str(end_str))
    if not m1 or not m2:
        return "-"
    s_min = int(m1.group(1)) * 60 + int(m1.group(2))
    e_min = int(m2.group(1)) * 60 + int(m2.group(2))
    if e_min < s_min:
        e_min += 24 * 60
    diff = e_min - s_min
    return f"{diff // 60}시간 {diff % 60:02d}분"


def extract_clean_right_values(sr: list, start_idx: int) -> list:
    out = []
    for raw_x in sr[start_idx + 1:]:
        xs = str(raw_x).strip()
        if not xs:
            continue
        norm_x = re.sub(r"\s+", "", xs).upper()
        if norm_x in SOY_STOP_LABELS or "소비기한품목명표시" in norm_x:
            break
        out.append(xs)
    return out


def resolve_soy_realtime_dates(tab_title: str, raw_prod_dt: str, raw_insp_dt: str, inc_days: str):
    today_d = now_kst().date()
    m_tab = re.match(r"^(\d{1,2})[/\.\-](\d{1,2})$", str(tab_title).strip())
    if not m_tab:
        return raw_prod_dt or "-", raw_insp_dt or "-"

    mm, dd = int(m_tab.group(1)), int(m_tab.group(2))
    yr = today_d.year if mm <= today_d.month + 1 else today_d.year - 1
    try:
        tab_d = date(yr, mm, dd)
    except ValueError:
        return raw_prod_dt or "-", raw_insp_dt or "-"

    c_insp = parse_date_obj(re.sub(r"\s+", "", str(raw_insp_dt)).rstrip("."))
    c_prod = parse_date_obj(re.sub(r"\s+", "", str(raw_prod_dt)).rstrip("."))

    offset = 7 if "7" in str(inc_days) else 5
    if c_insp and c_prod:
        diff = (c_insp - c_prod).days
        if 1 <= diff <= 20:
            offset = diff

    if c_prod is None or (c_insp and c_insp == today_d and tab_d != today_d):
        true_prod = (tab_d - timedelta(days=offset)).strftime("%Y-%m-%d")
        return true_prod, tab_d.strftime("%Y-%m-%d")

    return c_prod.strftime("%Y-%m-%d"), tab_d.strftime("%Y-%m-%d")


def parse_soy_incubation_worksheet(ws):
    if ws is None:
        return [], []
    rows = get_values_formatted(ws)
    if not rows:
        return [], []

    tab_title = ws.title.strip()
    anchors = []
    for r_i, r in enumerate(rows):
        for c_i, cell in enumerate(r):
            if re.sub(r"\s+", "", str(cell)) == "배양검사":
                anchors.append((r_i, c_i))

    blocks_parsed, hourly_all = [], []

    for _, (r0, c0) in enumerate(anchors):
        same_row_next_c = [c for (rr, c) in anchors if abs(rr - r0) <= 2 and c > c0]
        c_end = min(same_row_next_c) if same_row_next_c else c0 + 14

        same_col_next_r = [rr for (rr, c) in anchors if abs(c - c0) <= 2 and rr > r0]
        r_end = min(same_col_next_r) if same_col_next_r else min(len(rows), r0 + 48)

        sub_rows = [list(rows[rr][c0:min(c_end, len(rows[rr]))]) for rr in range(r0, r_end)]

        inc_days, inc_temp, vol_ml, machine_ho = "", "", "", ""
        writer_nm, inspector_nm = "", ""
        item_names, exp_dates = [], []
        prod_dt, insp_dt, t_start, t_end, total_smp = "", "", "", "", ""
        contam_cnt, judge_res, remark_txt = "", "", ""

        for sr_i, sr in enumerate(sub_rows):
            for sc_i, val in enumerate(sr):
                key = re.sub(r"\s+", "", str(val))
                if not key:
                    continue
                right_vals = extract_clean_right_values(sr, sc_i)

                if key == "배양검사" and not inc_days and right_vals:
                    inc_days = right_vals[0]
                elif key == "배양온도" and not inc_temp and right_vals:
                    inc_temp = right_vals[0]
                elif ("용량/포장기" in key or key == "포장기") and not machine_ho:
                    for rv in right_vals[:3]:
                        if "호기" in rv and not machine_ho:
                            machine_ho = rv
                        elif re.search(r"^\d{2,4}$", rv.replace("mL", "").replace("㎖", "").strip()) and not vol_ml:
                            vol_ml = rv
                elif key == "작성자" and not writer_nm and right_vals:
                    writer_nm = right_vals[0]
                elif key == "검사자" and not inspector_nm and right_vals:
                    inspector_nm = right_vals[0]
                elif key == "품목":
                    for rv in right_vals:
                        if rv not in item_names:
                            item_names.append(rv)
                    if sr_i + 1 < len(sub_rows):
                        next_sr = sub_rows[sr_i + 1]
                        next_first = re.sub(r"\s+", "", str(next_sr[0])) if next_sr else ""
                        if next_first not in SOY_STOP_LABELS:
                            for rv in [str(x).strip() for x in next_sr if str(x).strip()]:
                                norm_rv = re.sub(r"\s+", "", rv).upper()
                                if norm_rv in SOY_STOP_LABELS or "소비기한품목명표시" in norm_rv:
                                    continue
                                if not re.search(r"^202\d", rv) and rv not in item_names:
                                    item_names.append(rv)
                elif key == "생산일" and not prod_dt and right_vals:
                    prod_dt = right_vals[0]
                elif key == "소비기한":
                    for rv in right_vals:
                        if re.search(r"\d{2,4}[\.\-/]\s*\d{1,2}[\.\-/]\s*\d{1,2}", rv) and rv not in exp_dates:
                            exp_dates.append(rv)
                elif key == "검사일" and not insp_dt and right_vals:
                    insp_dt = right_vals[0]
                elif key == "생산시작" and not t_start and right_vals:
                    if re.search(r"\d{1,2}:\d{2}", right_vals[0]):
                        t_start = right_vals[0]
                elif key == "생산종료" and not t_end and right_vals:
                    if re.search(r"\d{1,2}:\d{2}", right_vals[0]):
                        t_end = right_vals[0]
                elif key == "총검사수량" and not total_smp and right_vals:
                    if re.search(r"\d+", right_vals[0]):
                        total_smp = right_vals[0]
                elif key == "오염수량":
                    look_cells = right_vals[:]
                    if sr_i + 1 < len(sub_rows):
                        look_cells.extend([
                            str(x).strip() for x in sub_rows[sr_i + 1]
                            if str(x).strip() and re.sub(r"\s+", "", str(x)).upper() not in SOY_STOP_LABELS
                        ])
                    for lv in look_cells:
                        if ("개" in lv or lv.isdigit()) and not contam_cnt:
                            contam_cnt = lv
                        elif lv in ["적합", "부적합"] and not judge_res:
                            judge_res = lv
                elif key == "특이사항" and right_vals:
                    remark_txt = " ".join(right_vals)

            first_t = str(sr[0]).strip() if sr else ""
            if re.match(r"^\d{2}\s*:$", first_t) and machine_ho:
                smp_v = " ".join([str(x).strip() for x in sr[1:6] if str(x).strip()])
                strp_v = " ".join([str(x).strip() for x in sr[6:9] if str(x).strip()])
                pap_v = " ".join([str(x).strip() for x in sr[9:12] if str(x).strip()])
                if smp_v or strp_v or pap_v:
                    hourly_all.append({
                        "포장기": machine_ho,
                        "시간대": first_t,
                        "샘플체크": smp_v or "-",
                        "Strip": strp_v or "-",
                        "Paper": pap_v or "-"
                    })

        has_valid_record = bool(machine_ho) and (bool(item_names) or bool(total_smp) or bool(t_start))
        if not has_valid_record:
            continue

        clean_inspector = re.sub(r"[\s\-_]", "", str(inspector_nm))
        is_inspected = bool(clean_inspector) and clean_inspector not in ["미입력", "미검사", "검사전", "대기", "없음"]

        if not is_inspected:
            insp_disp = "미입력 (검사전)"
            final_judge = "검사전"
            status_badge = "검사전"
        else:
            insp_disp = inspector_nm.strip()
            final_judge = judge_res if judge_res else "적합"
            if to_float(contam_cnt, 0) > 0 or "부적합" in final_judge:
                status_badge = "부적합"
            else:
                status_badge = "검사완료"

        true_prod_dt, true_insp_dt = resolve_soy_realtime_dates(tab_title, prod_dt, insp_dt, inc_days)

        blocks_parsed.append({
            "시트탭": tab_title,
            "포장기": machine_ho,
            "배양구분": f"{inc_days} ({inc_temp})".strip(" ()"),
            "용량(mL)": vol_ml or "-",
            "품목명": " / ".join(item_names) if item_names else "-",
            "생산일": true_prod_dt,
            "소비기한": " / ".join(exp_dates) if exp_dates else "-",
            "검사일(개봉일)": true_insp_dt,
            "생산 시작~종료": f"{t_start} ~ {t_end}" if (t_start or t_end) else "-",
            "실가동시간": calc_run_duration(t_start, t_end),
            "총 검사수량": total_smp or "0",
            "오염수량": contam_cnt if is_inspected and contam_cnt else ("-" if not is_inspected else (contam_cnt or "0 개")),
            "작성자": writer_nm.strip() if writer_nm.strip() else "-",
            "검사자": insp_disp,
            "검사완료여부": is_inspected,
            "진행상태": status_badge,
            "판정": final_judge,
            "특이사항": remark_txt or "없음"
        })

    return blocks_parsed, hourly_all


def fetch_process_rt_sheet(creds, sheet_urls_map: dict):
    sh_rt, err_rt = open_sheet_threadsafe(creds, sheet_urls_map, "process_rt")
    if sh_rt is None:
        return None, None, [], [], [], f"[상온공정일보] {err_rt}"
    try:
        ws_all = api_call_with_retry(sh_rt.worksheets)
        ws_rt_today, ws_rt_yest, _ = get_sorted_date_worksheets(sh_rt)
        rt_today_parsed = parse_rt_process_worksheet(ws_rt_today)
        rt_yest_parsed = parse_rt_process_worksheet(ws_rt_yest)

        ws_hist_p = next((w for w in ws_all if w.title.strip() == HIST_TABS["rt_prod"]), None)
        ws_hist_c = next((w for w in ws_all if w.title.strip() == HIST_TABS["rt_ccp"]), None)
        rt_hist_p = rows_to_records(get_values_formatted(ws_hist_p)) if ws_hist_p is not None else []
        rt_hist_c = rows_to_records(get_values_formatted(ws_hist_c)) if ws_hist_c is not None else []

        fsk_products = []
        ws_fsk = next(
            (w for w in ws_all if re.sub(r"\s+", "", w.title) == "제품명식품안전나라" or "식품안전나라" in w.title),
            None
        )
        if ws_fsk is not None:
            fsk_rows = get_values_formatted(ws_fsk)
            skip_hdrs = {"제품명", "품목명", "품목", "제 품 명", "번호", "no", "코드", "품목제조보고"}
            for r in fsk_rows:
                if not r:
                    continue
                col_a = re.sub(r"\s+", " ", str(r[0])).strip()
                if not col_a or col_a == "-" or col_a.lower() in skip_hdrs:
                    continue
                if re.match(r"^\d+$", col_a):
                    continue
                if col_a not in fsk_products:
                    fsk_products.append(col_a)

        return rt_today_parsed, rt_yest_parsed, rt_hist_p, rt_hist_c, fsk_products, None
    except Exception as e:
        return None, None, [], [], [], f"[상온공정일보] {e}"


def fetch_process_milk_sheet(creds, sheet_urls_map: dict):
    sh_milk, err_milk = open_sheet_threadsafe(creds, sheet_urls_map, "process_milk")
    if sh_milk is None:
        return [], [], "-", "-", [], [], [], f"[유제품포장일보] {err_milk}"
    try:
        ws_all = api_call_with_retry(sh_milk.worksheets)
        ws_m, ws_f = None, None
        ws_m_tpl, ws_f_tpl = None, None

        for w in ws_all:
            wt = w.title.strip()
            if wt == "우유":
                ws_m = w
            elif wt == "발효유":
                ws_f = w
            elif "우유" in wt and "복사용" in wt and ws_m_tpl is None:
                ws_m_tpl = w
            elif "발효유" in wt and "복사용" in wt and ws_f_tpl is None:
                ws_f_tpl = w

        m_rows, m_dt = parse_dairy_pack_worksheet(ws_m, "우유") if ws_m is not None else ([], "-")
        f_rows, f_dt = parse_dairy_pack_worksheet(ws_f, "발효유") if ws_f is not None else ([], "-")

        ws_hist_dp = next((w for w in ws_all if w.title.strip() == HIST_TABS["dairy_prod"]), None)
        ws_hist_dc = next((w for w in ws_all if w.title.strip() == HIST_TABS["dairy_ccp"]), None)
        dairy_hist_p = rows_to_records(get_values_formatted(ws_hist_dp)) if ws_hist_dp is not None else []
        dairy_hist_c = rows_to_records(get_values_formatted(ws_hist_dc)) if ws_hist_dc is not None else []

        dairy_prod_names = []
        for tpl_ws, lbl in [(ws_m or ws_m_tpl, "우유"), (ws_f or ws_f_tpl, "발효유")]:
            if tpl_ws is not None:
                tpl_list, _ = parse_dairy_pack_worksheet(tpl_ws, lbl)
                for r in tpl_list:
                    p_clean = str(r.get("제품명", "")).strip()
                    if p_clean and p_clean != "-" and p_clean not in dairy_prod_names:
                        dairy_prod_names.append(p_clean)

        return m_rows, f_rows, m_dt, f_dt, dairy_hist_p, dairy_hist_c, dairy_prod_names, None
    except Exception as e:
        return [], [], "-", "-", [], [], [], f"[유제품포장일보] {e}"


def fetch_process_soy_sheet(creds, sheet_urls_map: dict):
    sh_soy, err_soy = open_sheet_threadsafe(creds, sheet_urls_map, "process_soy")
    if sh_soy is None:
        return "-", {}, [], [], [], f"[두유배양일보] {err_soy}"
    try:
        ws_all = api_call_with_retry(sh_soy.worksheets)
        _, _, soy_date_ws_list = get_sorted_date_worksheets(sh_soy)
        soy_tabs_parsed = {}
        default_tab_title = "-"

        for w in soy_date_ws_list[:5]:
            t_title = w.title.strip()
            b_list, h_list = parse_soy_incubation_worksheet(w)
            if b_list:
                soy_tabs_parsed[t_title] = {"blocks": b_list, "hourly": h_list}
                if default_tab_title == "-":
                    default_tab_title = t_title

        if default_tab_title == "-" and soy_date_ws_list:
            first_ws = soy_date_ws_list[0]
            default_tab_title = first_ws.title.strip()
            b_list, h_list = parse_soy_incubation_worksheet(first_ws)
            soy_tabs_parsed[default_tab_title] = {"blocks": b_list, "hourly": h_list}

        def_pack = soy_tabs_parsed.get(default_tab_title, {"blocks": [], "hourly": []})
        ws_hist_soy = next((w for w in ws_all if w.title.strip() == HIST_TABS["soy_inc"]), None)
        soy_hist = rows_to_records(get_values_formatted(ws_hist_soy)) if ws_hist_soy is not None else []

        return default_tab_title, soy_tabs_parsed, def_pack["blocks"], def_pack["hourly"], soy_hist, None
    except Exception as e:
        return "-", {}, [], [], [], f"[두유배양일보] {e}"
# ============================ [api/index.py - PART 1 / 2 끝] ============================

# ==============================================================================
# [api/index.py - PART 2 / 2]
# 협력업체·심사·표시사항·원자재 파서 + 5분 캐시 병렬 엔진 + 양방향 저장/역추적 FastAPI 라우터
# ==============================================================================

# ==============================================================================
# 5. [4] 협력업체 심사 결과 DB 파서 (순수 Python 무결성 구현)
# ==============================================================================
def fetch_supplier_sheet(creds, sheet_urls_map: dict):
    sh, err = open_sheet_threadsafe(creds, sheet_urls_map, "supplier")
    if sh is None:
        return [], [], [], [], err

    try:
        ws_list = api_call_with_retry(sh.worksheets)
        ws1 = ws_list[0] if len(ws_list) >= 1 else None
        ws2 = ws_list[1] if len(ws_list) >= 2 else None
        if ws2 is None:
            return [], [], [], [], "협력업체 시트2가 없습니다."

        rows1 = get_values_formatted(ws1) if ws1 else []
        s1_records = rows_to_records(rows1)

        rows2 = get_values_formatted(ws2)
        if len(rows2) < 2:
            return [], [], [], s1_records, "협력업체 시트2(마스터 명단)가 비어 있습니다."

        exact_score_map: Dict[str, dict] = {}
        core_score_map: Dict[str, dict] = {}

        if s1_records and ("업체명" in s1_records[0]) and ("심사항목" in s1_records[0]):
            sorted_s1 = sorted(s1_records, key=lambda r: str(r.get("제출일시", "")))
            dedup_map: Dict[Tuple[str, str], dict] = {}
            for r in sorted_s1:
                n_exact = normalize_comp_name(r.get("업체명", ""), core_only=False)
                it_clean = str(r.get("심사항목", "")).strip()
                if n_exact:
                    r["_norm_exact"] = n_exact
                    r["_norm_core"] = normalize_comp_name(r.get("업체명", ""), core_only=True)
                    r["_item_clean"] = it_clean
                    dedup_map[(n_exact, it_clean)] = r

            grouped: Dict[str, List[dict]] = {}
            for (_, _), rec in dedup_map.items():
                grouped.setdefault(rec["_norm_exact"], []).append(rec)

            for norm_k, grp in grouped.items():
                total_pts = 0.0
                weak_items = []
                for s_row in grp:
                    adm_str = str(s_row.get("관리자최종점수", "")).strip()
                    ai_str = str(s_row.get("AI판정결과", "")).strip()
                    target_str = adm_str if adm_str != "" else ai_str
                    m_pt = re.search(r"(\d+(?:\.\d+)?)\s*점", target_str)
                    pt_val = float(m_pt.group(1)) if m_pt else to_float(target_str, 0.0)
                    total_pts += pt_val

                    if any(w in target_str for w in ["부적합", "미흡", "누락", "0점"]):
                        clean_it = re.sub(r"^\[[^\]]+\]\s*\(\d+\)\s*", "", s_row["_item_clean"])
                        weak_items.append(clean_it)

                info_pack = {
                    "score": int(round(min(total_pts, 100.0))),
                    "weak_count": len(weak_items),
                    "weak_items": weak_items
                }
                exact_score_map[norm_k] = info_pack
                core_k = grp[0].get("_norm_core", "")
                if core_k and core_k not in core_score_map:
                    core_score_map[core_k] = info_pack

        hdr2 = [str(c).replace(" ", "").strip() for c in rows2[0]]
        col_gubun = next((i for i, h in enumerate(hdr2) if "구분" in h), 0)
        col_comp = next((i for i, h in enumerate(hdr2) if "업체" in h), 1)
        col_item = next((i for i, h in enumerate(hdr2) if "품목" in h), 2)
        col_sc2 = next((i for i, h in enumerate(hdr2) if "점수" in h), -1)
        col_res2 = next((i for i, h in enumerate(hdr2) if "결과" in h), -1)
        col_note2 = next((i for i, h in enumerate(hdr2) if "비고" in h), -1)

        temp_list = []
        for r in rows2[1:]:
            if not any(str(x).strip() for x in r):
                continue
            r_pad = list(r) + [""] * max(0, 7 - len(r))
            gubun = str(r_pad[col_gubun]).strip()
            comp = str(r_pad[col_comp]).strip()
            item = str(r_pad[col_item]).strip()
            if not comp:
                continue

            nk_exact = normalize_comp_name(comp, core_only=False)
            nk_core = normalize_comp_name(comp, core_only=True)
            matched = exact_score_map.get(nk_exact) or core_score_map.get(nk_core)

            s2_score_txt = str(r_pad[col_sc2]).strip() if col_sc2 >= 0 else ""
            s2_res_txt = str(r_pad[col_res2]).strip() if col_res2 >= 0 else ""
            s2_note_txt = str(r_pad[col_note2]).strip() if col_note2 >= 0 else ""

            if s2_score_txt != "":
                if s2_score_txt == "미제출":
                    sc_num, sc_disp = -1, "미제출"
                else:
                    sc_num = int(round(to_float(s2_score_txt, -1)))
                    sc_disp = str(sc_num) if sc_num >= 0 else "미제출"
                weak_cnt = matched["weak_count"] if matched else 0
                weak_list = matched["weak_items"] if matched else []
            elif matched is not None:
                sc_num = matched["score"]
                sc_disp = str(sc_num)
                weak_cnt = matched["weak_count"]
                weak_list = matched["weak_items"]
            else:
                sc_num, sc_disp = -1, "미제출"
                weak_cnt, weak_list = 0, []

            if s2_res_txt in ["승인", "지도", "등급 외"]:
                grade = s2_res_txt
            else:
                grade = "승인" if sc_num >= 85 else ("지도" if sc_num >= 70 else "등급 외")

            note_final = s2_note_txt if s2_note_txt else (
                "미제출" if sc_disp == "미제출" else ("복수거래, 거래중지 등 검토" if grade == "등급 외" else "")
            )
            temp_list.append({
                "_sort_sc": sc_num,
                "구분": gubun,
                "업체명": comp,
                "품목": item,
                "점수": sc_disp,
                "결과": grade,
                "비고": note_final,
                "_weak_cnt": weak_cnt,
                "_weak_list": weak_list
            })

        temp_list.sort(key=lambda x: x["_sort_sc"], reverse=True)
        master_records, under_comp_rows, under_detail_rows = [], [], []

        for i, rec in enumerate(temp_list, start=1):
            master_records.append({
                "NO": i,
                "구분": rec["구분"],
                "업체명": rec["업체명"],
                "품목": rec["품목"],
                "점수": rec["점수"],
                "결과": rec["결과"],
                "비고": rec["비고"]
            })
            if rec["결과"] == "등급 외":
                under_comp_rows.append({
                    "업체명": rec["업체명"],
                    "점수": f"{rec['점수']}점" if rec["점수"] != "미제출" else "미제출",
                    "미비 항목": f"{rec['_weak_cnt']}개" if rec["점수"] != "미제출" else "미제출",
                    "조치": "복수거래, 거래중지 등 검토"
                })
                if rec["_weak_list"]:
                    under_detail_rows.append({
                        "대상 구분": rec["업체명"],
                        "분류": "서류심사 미흡",
                        "미비 사항": ", ".join(rec["_weak_list"])
                    })

        ws_under = next((w for w in ws_list if w.title.strip() == "등급외_미비사항"), None)
        if ws_under is not None:
            u_rows = get_values_formatted(ws_under)
            if len(u_rows) >= 2:
                sheet_uc, sheet_ud = [], []
                hdr_uc = [str(x).strip() for x in u_rows[0][:4]]
                hdr_ud = [str(x).strip() for x in u_rows[0][5:8]] if len(u_rows[0]) >= 8 else []

                for ur in u_rows[1:]:
                    ur_pad = list(ur) + [""] * max(0, 8 - len(ur))
                    if any(str(x).strip() for x in ur_pad[:4]) and len(hdr_uc) == 4:
                        sheet_uc.append(dict(zip(hdr_uc, [str(x).strip() for x in ur_pad[:4]])))
                    if any(str(x).strip() for x in ur_pad[5:8]) and len(hdr_ud) == 3:
                        sheet_ud.append(dict(zip(hdr_ud, [str(x).strip() for x in ur_pad[5:8]])))
                if sheet_uc:
                    under_comp_rows = sheet_uc
                if sheet_ud:
                    under_detail_rows = sheet_ud

        cnt_ok = sum(1 for r in master_records if r["결과"] == "승인")
        cnt_guide = sum(1 for r in master_records if r["결과"] == "지도")
        cnt_out = sum(1 for r in master_records if r["결과"] == "등급 외")
        cnt_unsub = sum(1 for r in master_records if r["점수"] == "미제출")
        tot_sup = len(master_records)

        grade_sum = [
            {"등급": "승인", "점수": "85-100점", "업체수": cnt_ok, "조치": "승인"},
            {"등급": "지도", "점수": "70-84점", "업체수": cnt_guide, "조치": "업체별 개선사항 피드백"},
            {"등급": "등급 외", "점수": "70점미만, 미제출", "업체수": cnt_out, "조치": "복수거래, 거래중지 등 검토"},
            {"등급": "합계", "점수": "-", "업체수": tot_sup, "조치": f"제출 {tot_sup - cnt_unsub}개사 / 미제출 {cnt_unsub}개사"}
        ]

        return master_records, grade_sum, under_comp_rows, under_detail_rows, None
    except Exception as e:
        return [], [], [], [], str(e)


# ==============================================================================
# 6. [5] 심사점수·일정 DB + 수동KPI + 특이사항 보드 / [6] 표시사항 / [7] 원자재 DB
# ==============================================================================
def fetch_audit_and_manual_db(creds, sheet_urls_map: dict):
    sh, err = open_sheet_threadsafe(creds, sheet_urls_map, "audit_db")
    if sh is None:
        return [], {}, None, None, err

    try:
        ws_list = api_call_with_retry(sh.worksheets)
        ws_sched = ws_list[0] if ws_list else None
        sched_records = []

        if ws_sched is not None:
            rows = get_values_formatted(ws_sched)
            if len(rows) >= 2:
                hdr = [str(c).strip() for c in rows[0]]
                for r in rows[1:]:
                    if not any(str(x).strip() for x in r):
                        continue
                    r_pad = [str(x).strip() for x in r] + [""] * max(0, len(hdr) - len(r))
                    item = dict(zip(hdr, r_pad[:len(hdr)]))
                    d_obj = parse_date_obj(item.get("심사일자", ""))
                    dt_norm = d_obj.strftime("%Y-%m-%d") if d_obj else item.get("심사일자", "")
                    st_val = item.get("진행현황", "심사준비중")
                    if st_val not in AUDIT_STATUS_LIST:
                        st_val = "심사준비중"
                    sched_records.append({
                        "고유ID": item.get("고유ID", "") or str(uuid.uuid4())[:8],
                        "심사일자": dt_norm,
                        "심사명": item.get("심사명", ""),
                        "진행현황": st_val,
                        "점수": item.get("점수", ""),
                        "비고": item.get("비고", ""),
                        "수정일시": item.get("수정일시", "")
                    })
                sched_records.sort(key=lambda x: x["심사일자"])

        saved_manual = {}
        ws_manual = next((w for w in ws_list if w.title.strip() == MANUAL_KPI_TAB), None)
        if ws_manual is not None:
            m_rows = get_values_formatted(ws_manual)
            if len(m_rows) >= 2:
                m_hdr = [str(c).strip() for c in m_rows[0]]
                for mr in m_rows[1:]:
                    if not any(str(x).strip() for x in mr):
                        continue
                    mr_pad = [str(x).strip() for x in mr] + [""] * max(0, len(m_hdr) - len(mr))
                    md = dict(zip(m_hdr, mr_pad[:len(m_hdr)]))
                    k_key = md.get("kpi_key", "")
                    if k_key in DEFAULT_MANUAL_KPIS:
                        notes_raw = md.get("notes_joined", "")
                        act_txt = md.get("actual_text", DEFAULT_MANUAL_KPIS[k_key]["actual_text"])
                        saved_manual[k_key] = {
                            "target_text": md.get("target_text", DEFAULT_MANUAL_KPIS[k_key]["target_text"]),
                            "actual_text": act_txt,
                            "compact_val": act_txt.split(",")[0].strip() if act_txt else DEFAULT_MANUAL_KPIS[k_key]["compact_val"],
                            "sub_badge": md.get("sub_badge", DEFAULT_MANUAL_KPIS[k_key]["sub_badge"]),
                            "target_num": to_float(md.get("target_num"), DEFAULT_MANUAL_KPIS[k_key]["target_num"]),
                            "actual_num": to_float(md.get("actual_num"), DEFAULT_MANUAL_KPIS[k_key]["actual_num"]),
                            "signal": md.get("signal", DEFAULT_MANUAL_KPIS[k_key]["signal"]),
                            "notes_title": md.get("notes_title", DEFAULT_MANUAL_KPIS[k_key]["notes_title"]),
                            "notes": [x.strip() for x in notes_raw.split("||") if x.strip()] if notes_raw else list(DEFAULT_MANUAL_KPIS[k_key]["notes"])
                        }

        loaded_lbl_memo = None
        loaded_clm_issues = None
        ws_sp = next((w for w in ws_list if w.title.strip() == SPECIAL_BOARD_TAB), None)
        if ws_sp is not None:
            sp_rows = get_values_formatted(ws_sp)
            if len(sp_rows) >= 2:
                for sr in sp_rows[1:]:
                    if len(sr) >= 2:
                        b_key = str(sr[0]).strip()
                        b_val = str(sr[1]).strip()
                        if b_key == "label_memo" and b_val:
                            loaded_lbl_memo = b_val
                        elif b_key == "claim_issues_json" and b_val:
                            try:
                                parsed_json = json.loads(b_val)
                                if isinstance(parsed_json, list):
                                    loaded_clm_issues = parsed_json
                            except Exception:
                                pass

        return sched_records, saved_manual, loaded_lbl_memo, loaded_clm_issues, None
    except Exception as e:
        return [], {}, None, None, f"[심사점수 및 일정 관리 DB] {e}"


def parse_factual_date(row_dict: dict):
    for col_key in ["등록일", "최종수정일", "완료일"]:
        raw = row_dict.get(col_key, "")
        d_obj = parse_date_obj(raw)
        if d_obj and 2018 <= d_obj.year <= 2035:
            return d_obj.strftime("%Y-%m-%d"), f"{d_obj.year}년", f"{d_obj.month}월", d_obj.year, d_obj.month
    return "원본날짜깨짐(#######)", "날짜미상", "날짜미상", 0, 0


def classify_label_status(status_str: str) -> str:
    s = str(status_str).strip()
    if s == "식품안전팀 검토중":
        return "식품안전팀 검토중"
    if s in ["개시/준비", "보류", "검토/개시", ""]:
        return "제외(개시/준비·보류)"
    return "검토완료(확인필)"


def fetch_label_status_sheet(creds, sheet_urls_map: dict):
    sh, err = open_sheet_threadsafe(creds, sheet_urls_map, "label_status")
    if sh is None:
        return [], err
    try:
        ws = api_call_with_retry(sh.get_worksheet, 0)
        rows = get_values_unformatted(ws)
        if len(rows) < 2:
            return [], "표시사항 현황 시트에 데이터가 없습니다."
        headers = [str(h).strip() for h in rows[0]]
        data = []
        for r in rows[1:]:
            if not any(str(x).strip() != "" for x in r):
                continue
            r_pad = [str(x).strip() for x in r] + [""] * max(0, len(headers) - len(r))
            item = dict(zip(headers, r_pad[:len(headers)]))
            dt_str, yr_str, mo_str, yr_num, mo_num = parse_factual_date(item)
            item["등록일자"] = dt_str
            item["등록연도"] = yr_str
            item["등록월"] = mo_str
            item["_yr_num"] = yr_num
            item["_mo_num"] = mo_num
            item["분류"] = classify_label_status(item.get("상태", ""))
            data.append(item)
        return data, None
    except Exception as e:
        return [], str(e)


def fetch_raw_material_db_sheet(creds, sheet_urls_map: dict):
    sh, err = open_sheet_threadsafe(creds, sheet_urls_map, "raw_label_db")
    if sh is None:
        return [], 0, err
    submitted = []
    total_master_count = 0
    try:
        ws_list = api_call_with_retry(sh.worksheets)
        if ws_list:
            rows_sub = get_values_formatted(ws_list[0])
            submitted = rows_to_records(rows_sub)
        ws_master = next((w for w in ws_list if w.title.strip() == "전체 원재료 명단"), None)
        if ws_master is not None:
            rows_m = get_values_formatted(ws_master)
            total_master_count = max(len([r for r in rows_m[1:] if any(str(x).strip() for x in r)]), 0)
    except Exception as e:
        return submitted, total_master_count, str(e)
    return submitted, total_master_count, None


# ==============================================================================
# 7. 심사 요약 계산 · 표시사항/원가 분석 집계 · 5분 인메모리 캐시 번들 엔진
# ==============================================================================
def get_active_audit_summary(sched_list: list, today_str: str) -> dict:
    today_d = parse_date_obj(today_str) or now_kst().date()
    if not sched_list:
        return {
            "has_data": False,
            "mode": "empty",
            "banner_text": "등록된 심사 일정이 없습니다",
            "status": "심사준비중",
            "pct": 0,
            "color": "#9CA3AF",
            "active_uid": ""
        }

    enriched = []
    for r in sched_list:
        d_obj = parse_date_obj(r.get("심사일자", ""))
        diff_d = (d_obj - today_d).days if d_obj else None
        enriched.append((d_obj, diff_d, r))

    today_rows = [x for x in enriched if x[0] == today_d]
    if today_rows:
        r = today_rows[0][2]
        st_nm = r["진행현황"] if r["진행현황"] in AUDIT_STATUS_CONFIG else "심사준비중"
        cfg = AUDIT_STATUS_CONFIG[st_nm]
        return {
            "has_data": True,
            "mode": "today",
            "banner_text": f"[금일 심사 · {r['심사일자'][5:]}] {r['심사명']}",
            "status": st_nm,
            "pct": cfg["pct"],
            "color": cfg["color"],
            "active_uid": r["고유ID"]
        }

    in_prog = [x for x in enriched if x[2].get("진행현황") in ["심사중", "서류심사중", "현장심사중"]]
    if in_prog:
        r = in_prog[0][2]
        st_nm = r["진행현황"]
        cfg = AUDIT_STATUS_CONFIG[st_nm]
        dt_short = r["심사일자"][5:] if len(r["심사일자"]) >= 10 else r["심사일자"]
        return {
            "has_data": True,
            "mode": "in_progress",
            "banner_text": f"[진행 중 · {dt_short}] {r['심사명']}",
            "status": st_nm,
            "pct": cfg["pct"],
            "color": cfg["color"],
            "active_uid": r["고유ID"]
        }

    future_rows = sorted([x for x in enriched if x[0] and x[0] > today_d], key=lambda x: x[0])
    if future_rows:
        _, d_left, r = future_rows[0]
        st_nm = r["진행현황"] if r["진행현황"] in AUDIT_STATUS_CONFIG else "심사준비중"
        cfg = AUDIT_STATUS_CONFIG[st_nm]
        return {
            "has_data": True,
            "mode": "upcoming",
            "banner_text": f"[차기 심사 · D-{d_left}] {r['심사명']} ({r['심사일자'][5:]})",
            "status": st_nm,
            "pct": cfg["pct"],
            "color": cfg["color"],
            "active_uid": r["고유ID"]
        }

    r = sorted(sched_list, key=lambda x: x.get("심사일자", ""), reverse=True)[0]
    st_nm = r["진행현황"] if r["진행현황"] in AUDIT_STATUS_CONFIG else "심사종료"
    cfg = AUDIT_STATUS_CONFIG[st_nm]
    dt_short = r["심사일자"][5:] if len(r["심사일자"]) >= 10 else r["심사일자"]
    return {
        "has_data": True,
        "mode": "past",
        "banner_text": f"[최근 완료 · {dt_short}] {r['심사명']}",
        "status": st_nm,
        "pct": cfg["pct"],
        "color": cfg["color"],
        "active_uid": r["고유ID"]
    }


def build_label_analytics(lbl_rows: list) -> dict:
    reviewing_rows = [r for r in lbl_rows if r.get("분류") == "식품안전팀 검토중"]
    completed_rows = [r for r in lbl_rows if r.get("분류") == "검토완료(확인필)"]
    valid_rows = [r for r in lbl_rows if r.get("분류") != "제외(개시/준비·보류)"]
    broken_date_cnt = sum(1 for r in lbl_rows if r.get("등록연도") == "날짜미상")

    r_25 = [r for r in valid_rows if r.get("_yr_num") == 2025]
    r_26 = [r for r in valid_rows if r.get("_yr_num") == 2026]

    max_m_26 = max([int(r.get("_mo_num", 0)) for r in r_26 if int(r.get("_mo_num", 0)) > 0] or [now_kst().month])
    active_m_25 = max(len({int(r.get("_mo_num", 0)) for r in r_25 if int(r.get("_mo_num", 0)) > 0}), 1) if r_25 else 12
    active_m_26 = max(len({int(r.get("_mo_num", 0)) for r in r_26 if int(r.get("_mo_num", 0)) > 0}), 1) if r_26 else max_m_26

    cnt_25_all, cnt_26_all = len(r_25), len(r_26)
    cnt_25_ytd = sum(1 for r in r_25 if 1 <= int(r.get("_mo_num", 0)) <= max_m_26)
    cnt_26_ytd = sum(1 for r in r_26 if 1 <= int(r.get("_mo_num", 0)) <= max_m_26)
    ytd_diff = cnt_26_ytd - cnt_25_ytd
    ytd_pct = round((ytd_diff / cnt_25_ytd) * 100, 1) if cnt_25_ytd > 0 else 0.0

    m_avg_25 = round(cnt_25_all / active_m_25, 1) if cnt_25_all > 0 else 0.0
    m_avg_26 = round(cnt_26_all / active_m_26, 1) if cnt_26_all > 0 else 0.0

    monthly_cmp = [
        {
            "월": f"{m}월",
            "2025년": sum(1 for r in r_25 if int(r.get("_mo_num", 0)) == m),
            "2026년": sum(1 for r in r_26 if int(r.get("_mo_num", 0)) == m)
        }
        for m in range(1, 13)
    ]

    cat_keys = sorted(list({(r.get("대분류(마케팅 카테고리)", "") or "미분류").strip() or "미분류" for r in (r_25 + r_26)}))
    cat_cmp = []
    for ck in cat_keys:
        c25 = sum(1 for r in r_25 if ((r.get("대분류(마케팅 카테고리)", "") or "미분류").strip() or "미분류") == ck)
        c26 = sum(1 for r in r_26 if ((r.get("대분류(마케팅 카테고리)", "") or "미분류").strip() or "미분류") == ck)
        cat_cmp.append({
            "카테고리": ck,
            "2025년_건수": c25,
            "2026년_건수": c26,
            "증감(건)": c26 - c25,
            "26년_비중(%)": round((c26 / max(cnt_26_all, 1)) * 100, 1)
        })
    cat_cmp.sort(key=lambda x: x["2026년_건수"], reverse=True)

    dept_keys = sorted(list({(r.get("요청부서", "") or "미기입").strip() or "미기입" for r in valid_rows}))
    dept_cmp = []
    for dk in dept_keys:
        d25 = sum(1 for r in r_25 if ((r.get("요청부서", "") or "미기입").strip() or "미기입") == dk)
        d26 = sum(1 for r in r_26 if ((r.get("요청부서", "") or "미기입").strip() or "미기입") == dk)
        dnow = sum(1 for r in reviewing_rows if ((r.get("요청부서", "") or "미기입").strip() or "미기입") == dk)
        if d25 > 0 or d26 > 0 or dnow > 0:
            dept_cmp.append({"요청부서": dk, "2025년": d25, "2026년": d26, "현재_검토중": dnow, "증감(26-25)": d26 - d25})
    dept_cmp.sort(key=lambda x: x["2026년"], reverse=True)

    site_counts: Dict[str, int] = {}
    pkg_counts: Dict[str, int] = {}
    for r in r_26:
        st_nm = (r.get("생산처", "") or "미기입").strip() or "미기입"
        pk_nm = (r.get("검토 포장재 유형", "") or "미기입").strip() or "미기입"
        site_counts[st_nm] = site_counts.get(st_nm, 0) + 1
        pkg_counts[pk_nm] = pkg_counts.get(pk_nm, 0) + 1

    top_sites = [{"생산처": k, "2026년_검토건수": v} for k, v in sorted(site_counts.items(), key=lambda x: x[1], reverse=True)[:10]]
    top_pkgs = [{"포장재유형": k, "2026년_검토건수": v} for k, v in sorted(pkg_counts.items(), key=lambda x: x[1], reverse=True)[:10]]

    keep_cols = ["*docNo", "상태", "제목", "요청부서", "대분류(마케팅 카테고리)", "보관방법", "생산처", "검토 포장재 유형", "등록일자", "최종수정자", "분류", "등록연도", "등록월"]
    slim_all = [{k: r.get(k, "") for k in keep_cols} for r in lbl_rows]
    slim_reviewing = [{k: r.get(k, "") for k in keep_cols} for r in reviewing_rows]

    return {
        "reviewing_cnt": len(reviewing_rows),
        "completed_cnt": len(completed_rows),
        "broken_date_cnt": broken_date_cnt,
        "max_m_26": max_m_26,
        "cnt_25_all": cnt_25_all,
        "cnt_26_all": cnt_26_all,
        "cnt_25_ytd": cnt_25_ytd,
        "cnt_26_ytd": cnt_26_ytd,
        "ytd_diff": ytd_diff,
        "ytd_pct": ytd_pct,
        "m_avg_25": m_avg_25,
        "m_avg_26": m_avg_26,
        "avg_diff": round(m_avg_26 - m_avg_25, 1),
        "monthly_cmp": monthly_cmp,
        "cat_cmp": cat_cmp,
        "dept_cmp": dept_cmp,
        "top_sites": top_sites,
        "top_pkgs": top_pkgs,
        "reviewing_list": slim_reviewing,
        "all_records": slim_all
    }


_BUNDLE_CACHE: Dict[str, Any] = {"ts": 0.0, "date_key": "", "data": None}
CACHE_TTL_SEC = 300.0


def build_full_dashboard_bundle(force_refresh: bool = False) -> dict:
    cur_kst = now_kst()
    today_str = cur_kst.strftime("%Y-%m-%d")
    now_ts = time.time()

    if (
        not force_refresh
        and _BUNDLE_CACHE["data"] is not None
        and _BUNDLE_CACHE["date_key"] == today_str
        and (now_ts - _BUNDLE_CACHE["ts"]) < CACHE_TTL_SEC
    ):
        return _BUNDLE_CACHE["data"]

    creds, auth_err = build_google_credentials()
    sheet_urls_map = get_sheet_urls_map()

    if creds is None:
        empty_bundle = {
            "ok": False,
            "loaded_at": cur_kst.strftime("%Y. %m. %d %H:%M:%S (KST)"),
            "loaded_date": today_str,
            "errors": [auth_err]
        }
        return empty_bundle

    with ThreadPoolExecutor(max_workers=3) as ex:
        f_cost = ex.submit(fetch_cost_sheet, creds, sheet_urls_map)
        f_claim = ex.submit(fetch_claim_sheet, creds, sheet_urls_map)
        f_rt = ex.submit(fetch_process_rt_sheet, creds, sheet_urls_map)
        f_milk = ex.submit(fetch_process_milk_sheet, creds, sheet_urls_map)
        f_soy = ex.submit(fetch_process_soy_sheet, creds, sheet_urls_map)
        f_sup = ex.submit(fetch_supplier_sheet, creds, sheet_urls_map)
        f_audit = ex.submit(fetch_audit_and_manual_db, creds, sheet_urls_map)
        f_lbl = ex.submit(fetch_label_status_sheet, creds, sheet_urls_map)
        f_raw = ex.submit(fetch_raw_material_db_sheet, creds, sheet_urls_map)

        cost_rows, cost_targets, err_cost = f_cost.result()
        claim_trend, claim_cat, err_claim = f_claim.result()
        rt_today, rt_yest, rt_hp, rt_hc, fsk_prods, err_rt = f_rt.result()
        milk_pack, ferm_pack, milk_dt, ferm_dt, dy_hp, dy_hc, dairy_prods, err_milk = f_milk.result()
        soy_def_tab, soy_map, soy_b, soy_h, soy_hist, err_soy = f_soy.result()
        sup_m, sup_g, sup_uc, sup_ud, err_sup = f_sup.result()
        audit_sched, saved_manual_kpis, saved_lbl_memo, saved_clm_issues, err_audit = f_audit.result()
        lbl_rows, err_lbl = f_lbl.result()
        raw_sub, raw_m_cnt, err_raw = f_raw.result()

    # 마스터 제품명 통합 ('제품명 식품안전나라' A열 최우선)
    master_prods = []
    seen_norm = set()

    def add_prod_unique(p_str: str):
        ps = str(p_str or "").strip()
        if not ps or ps in ["-", "(비가동)", "(제품명 미기입)"]:
            return
        nk = clean_product_for_match(ps)
        if nk and nk not in seen_norm:
            seen_norm.add(nk)
            master_prods.append(ps)

    for fp in fsk_prods:
        add_prod_unique(fp)
    for dp in dairy_prods:
        add_prod_unique(dp)
    for r in rt_hp:
        add_prod_unique(r.get("제품명", ""))
    for r in dy_hp:
        add_prod_unique(r.get("제품명", ""))

    # 1) 원가절감 파생 지표 (부문 + 식품안전팀 + 품질관리팀)
    rec_flags_cost = [bool(r.get("_기록여부", False)) for r in cost_rows] if cost_rows else None

    def summarize_cost_col(col_act: str, col_std: str, target_uk: float):
        vals = [float(r.get(col_act, 0.0)) for r in cost_rows]
        stds = [float(r.get(col_std, 0.0)) for r in cost_rows]
        sum_man = round(sum(vals), 1)
        sum_uk = round(sum_man / 10000, 2)
        target_man = round(target_uk * 10000, 1)
        rate = round((sum_man / target_man) * 100, 1) if target_man > 0 else 0.0
        act_l, fc_l, full_l, slope, mcnt, last_i = calc_trend_and_forecast_split(vals, rec_flags_cost)
        proj_man = round(sum_man + sum(full_l[last_i + 1:]), 1) if (0 <= last_i < 11) else sum_man
        proj_uk = round(proj_man / 10000, 2)
        proj_rate = round((proj_man / target_man) * 100, 1) if target_man > 0 else 0.0
        pass_m = sum(1 for v, s, fl in zip(vals, stds, rec_flags_cost or []) if fl and v >= s)
        return {
            "target_uk": target_uk,
            "actual_man": sum_man,
            "actual_uk": sum_uk,
            "rate": rate,
            "proj_man": proj_man,
            "proj_uk": proj_uk,
            "proj_rate": proj_rate,
            "slope": slope,
            "recorded_months": mcnt,
            "pass_months": pass_m,
            "last_idx": last_i,
            "act_line": act_l,
            "fc_line": fc_l,
            "full_line": full_l
        }

    cost_dept_sum = summarize_cost_col("부문_실적(만원)", "부문_기준(만원)", cost_targets["부문"])
    cost_fs_sum = summarize_cost_col("식품안전팀_실적(만원)", "식품안전팀_기준(만원)", cost_targets["식품안전팀"])
    cost_qc_sum = summarize_cost_col("품질관리팀_실적(만원)", "품질관리팀_기준(만원)", cost_targets["품질관리팀"])
    sig_cost = "green" if (cost_dept_sum["actual_uk"] >= cost_targets["부문"] or cost_dept_sum["proj_uk"] >= cost_targets["부문"]) else ("red" if cost_rows else "gray")

    # 2) 클레임 파생 지표 (전사 + 유제품/상온 부문별 2025·2026)
    claim_target_ppm = 1.12
    row_26_sum = next((r for r in claim_cat if r["연도"] == "2026년" and r["유형"] == "합계"), None)
    claim_actual_ppm = float(row_26_sum["합계_PPM"]) if row_26_sum else 0.0
    claim_total_cnt = int(row_26_sum["합계_건수"]) if row_26_sum else 0
    cl_act_l, cl_fc_l, cl_full_l, cl_slope, cl_m_cnt, cl_last_i = calc_trend_and_forecast_split(
        [float(r["2026년_PPM"]) for r in claim_trend],
        [bool(r["_기록여부"]) for r in claim_trend] if claim_trend else None
    )
    claim_feb_pred = cl_full_l[-1] if cl_full_l else 0.0
    sig_claim = "gray" if not row_26_sum else ("red" if claim_actual_ppm > claim_target_ppm else ("yellow" if cl_slope > 0.03 else "green"))

    avail_claim_years = sorted(list({r["연도"] for r in claim_cat}), reverse=True)
    div_claims = {}
    for yr in avail_claim_years:
        div_claims[yr] = {
            "dairy": build_division_claim_summary(claim_cat, yr, DAIRY_CATEGORIES),
            "rt": build_division_claim_summary(claim_cat, yr, RT_CATEGORIES)
        }

    # 3) 공정 요약 지표
    rt_t_lines = rt_today["lines_records"] if rt_today else []
    active_rt_cnt = sum(1 for r in rt_t_lines if r["생산 제품명"] != "(비가동)")
    wait_rt_cnt = sum(1 for r in rt_t_lines if r["_cls"] == "cell-yellow")
    red_rt_cnt = sum(1 for r in rt_t_lines if r["_cls"] == "cell-red") + (
        sum(1 for sr in rt_today["ster_records"] if "cell-red" in (sr["_c_temp"], sr["_c_press"], sr["_c_hold"], sr["_c_flt"])) if rt_today else 0
    )

    active_milk_cnt = sum(1 for r in milk_pack if "금일생산" in str(r.get("가동여부", "")))
    active_ferm_cnt = sum(1 for r in ferm_pack if "금일생산" in str(r.get("가동여부", "")))
    active_dairy_cnt = active_milk_cnt + active_ferm_cnt
    if active_dairy_cnt == 0 and dy_hp:
        active_dairy_cnt = sum(1 for r in dy_hp if r.get("생산일자") == today_str)

    soy_blocks_cnt = len(soy_b)
    soy_total_samples = sum(int(to_float(r.get("총 검사수량", 0), 0)) for r in soy_b)
    soy_uninspected_cnt = sum(1 for r in soy_b if not bool(r.get("검사완료여부", False)))
    soy_contam_total = sum(int(to_float(r.get("오염수량", 0), 0)) for r in soy_b if bool(r.get("검사완료여부", False)))

    # 4) 협력업체 요약
    target_exec = 88.3
    total_sup_cnt = len(sup_m)
    unsub_sup_cnt = sum(1 for r in sup_m if r.get("점수") == "미제출")
    sub_sup_cnt = total_sup_cnt - unsub_sup_cnt
    sup_exec_rate = round((sub_sup_cnt / total_sup_cnt) * 100, 1) if total_sup_cnt > 0 else 0.0
    ok_sup_cnt = sum(1 for r in sup_m if r.get("결과") == "승인")
    guide_sup_cnt = sum(1 for r in sup_m if r.get("결과") == "지도")
    out_sup_cnt = sum(1 for r in sup_m if r.get("결과") == "등급 외")
    sig_supplier = "green" if sup_exec_rate >= target_exec else ("yellow" if sup_exec_rate >= 75.0 else ("red" if total_sup_cnt > 0 else "gray"))

    # 5) 수동 KPI 병합 & 특이사항 보드
    merged_manual = {k: {**v, "notes": list(v["notes"])} for k, v in DEFAULT_MANUAL_KPIS.items()}
    for mk_k, mk_val in saved_manual_kpis.items():
        if mk_k in merged_manual:
            merged_manual[mk_k].update(mk_val)

    label_memo_final = saved_lbl_memo if saved_lbl_memo is not None else DEFAULT_LABEL_MEMO
    claim_issues_final = saved_clm_issues if saved_clm_issues is not None else [dict(x) for x in DEFAULT_CLAIM_ISSUES]

    # 6) 표시사항 & 원자재 요약
    label_analytics = build_label_analytics(lbl_rows)
    sub_raw_cnt = len(raw_sub)
    verified_raw_cnt = sum(1 for r in raw_sub if re.search(r"완료|적합", str(r.get("검증상태", ""))))
    allergy_raw_cnt = sum(1 for r in raw_sub if str(r.get("신규알레르기", "")).strip() != "")

    errors = []
    for label_nm, er in [
        ("원가절감", err_cost), ("클레임", err_claim), ("협력업체", err_sup),
        ("표시사항 현황", err_lbl), ("원자재 DB", err_raw)
    ]:
        if er:
            errors.append(f"[{label_nm}] {er}")
    for p_er in [err_rt, err_milk, err_soy, err_audit]:
        if p_er:
            errors.append(p_er)

    done_kst = now_kst()
    bundle = {
        "ok": True,
        "loaded_at": done_kst.strftime("%Y. %m. %d %H:%M:%S (KST)"),
        "loaded_time_short": done_kst.strftime("%H:%M"),
        "loaded_date": today_str,
        "errors": errors,
        "cost": {
            "targets": cost_targets,
            "signal": sig_cost,
            "dept": cost_dept_sum,
            "fs": cost_fs_sum,
            "qc": cost_qc_sum,
            "rows": cost_rows
        },
        "claim": {
            "target_ppm": claim_target_ppm,
            "actual_ppm": claim_actual_ppm,
            "total_cnt": claim_total_cnt,
            "slope": cl_slope,
            "recorded_months": cl_m_cnt,
            "feb_pred": claim_feb_pred,
            "last_idx": cl_last_i,
            "act_line": cl_act_l,
            "fc_line": cl_fc_l,
            "full_line": cl_full_l,
            "signal": sig_claim,
            "years": avail_claim_years,
            "trend_rows": claim_trend,
            "category_rows": claim_cat,
            "divisions": div_claims
        },
        "process": {
            "summary": {
                "rt_today_tab": rt_today["tab_title"] if rt_today else "-",
                "rt_yest_tab": rt_yest["tab_title"] if rt_yest else "-",
                "active_rt_cnt": active_rt_cnt,
                "wait_rt_cnt": wait_rt_cnt,
                "red_rt_cnt": red_rt_cnt,
                "active_dairy_cnt": active_dairy_cnt,
                "soy_default_tab": soy_def_tab,
                "soy_blocks_cnt": soy_blocks_cnt,
                "soy_total_samples": soy_total_samples,
                "soy_uninspected_cnt": soy_uninspected_cnt,
                "soy_contam_total": soy_contam_total,
                "today_notes": rt_today["ster_special_notes"] if rt_today else [],
                "yest_notes": rt_yest["ster_special_notes"] if rt_yest else []
            },
            "rt_today": rt_today,
            "rt_yest": rt_yest,
            "milk_date": milk_dt,
            "ferm_date": ferm_dt,
            "milk_pack": milk_pack,
            "ferm_pack": ferm_pack,
            "soy_default_tab": soy_def_tab,
            "soy_tabs_map": soy_map,
            "rt_hist_p": rt_hp,
            "rt_hist_c": rt_hc,
            "dairy_hist_p": dy_hp,
            "dairy_hist_c": dy_hc,
            "soy_hist": soy_hist,
            "fsk_products_cnt": len(fsk_prods),
            "master_products": master_prods
        },
        "supplier": {
            "target_exec": target_exec,
            "total_cnt": total_sup_cnt,
            "sub_cnt": sub_sup_cnt,
            "unsub_cnt": unsub_sup_cnt,
            "exec_rate": sup_exec_rate,
            "ok_cnt": ok_sup_cnt,
            "guide_cnt": guide_sup_cnt,
            "out_cnt": out_sup_cnt,
            "signal": sig_supplier,
            "grade_summary": sup_g,
            "master_rows": sup_m,
            "under_comp_rows": sup_uc,
            "under_detail_rows": sup_ud
        },
        "audit": {
            "summary": get_active_audit_summary(audit_sched, today_str),
            "schedules": audit_sched
        },
        "manual_kpis": merged_manual,
        "special_boards": {
            "label_memo": label_memo_final,
            "claim_issues": claim_issues_final
        },
        "label_status": label_analytics,
        "raw_db": {
            "master_cnt": raw_m_cnt,
            "sub_cnt": sub_raw_cnt,
            "verified_cnt": verified_raw_cnt,
            "allergy_cnt": allergy_raw_cnt,
            "rows": raw_sub
        }
    }

    _BUNDLE_CACHE["ts"] = time.time()
    _BUNDLE_CACHE["date_key"] = today_str
    _BUNDLE_CACHE["data"] = bundle
    return bundle


# ==============================================================================
# 8. FastAPI 앱 및 REST 엔드포인트 (읽기 · LOT 역추적 · 양방향 시트 저장 · AI 브리핑)
# ==============================================================================
app = FastAPI(title="YONSEI QUALITY ONE API", version="2.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"]
)


class AuditSchedulePayload(BaseModel):
    action: str  # "add" | "update" | "delete" | "quick_status"
    uid: Optional[str] = ""
    audit_date: Optional[str] = ""
    audit_name: Optional[str] = ""
    status: Optional[str] = "심사준비중"
    score: Optional[str] = ""
    memo: Optional[str] = ""


class ManualKpiPayload(BaseModel):
    kpi_key: str
    target_text: str
    actual_text: str
    sub_badge: str
    target_num: float = 0.0
    actual_num: float = 0.0
    signal: str = "green"
    notes_title: str = ""
    notes: List[str] = []


class SpecialBoardPayload(BaseModel):
    label_memo: Optional[str] = None
    claim_issues: Optional[List[Dict[str, Any]]] = None


@app.get("/api/health")
def api_health():
    has_cred = bool(os.environ.get("GOOGLE_CREDENTIALS_JSON", "").strip())
    has_urls = bool(os.environ.get("SHEET_URLS_JSON", "").strip())
    has_ai = bool(os.environ.get("GEMINI_API_KEY", "").strip())
    return {
        "ok": True,
        "kst_now": now_kst().strftime("%Y-%m-%d %H:%M:%S"),
        "env_configured": {"google_credentials": has_cred, "sheet_urls": has_urls, "gemini_api_key": has_ai}
    }


@app.get("/api/bundle")
def api_get_bundle(force: int = 0):
    return build_full_dashboard_bundle(force_refresh=(force == 1))


@app.get("/api/lot-trace")
def api_lot_trace(product: str = "", exp: str = "", kw: str = ""):
    bundle = build_full_dashboard_bundle(force_refresh=False)
    if not bundle.get("ok"):
        raise HTTPException(status_code=500, detail="시트 데이터가 아직 로드되지 않았습니다.")

    p_data = bundle["process"]
    rt_hp = p_data["rt_hist_p"]
    rt_hc = p_data["rt_hist_c"]
    soy_h = p_data["soy_hist"]
    dy_hp = p_data["dairy_hist_p"]
    dy_hc = p_data["dairy_hist_c"]

    exp_q = parse_exp_query_info(exp)

    matched_rt = [
        r for r in rt_hp
        if is_product_match(r.get("제품명", ""), product, kw)
        and is_exp_match(r.get("표기소비기한_원문", ""), r.get("표준소비기한_검색용", ""), exp_q)
    ]
    matched_rt.sort(key=lambda x: x.get("생산일자", ""), reverse=True)

    rt_keys = [(str(r.get("생산일자", "")).strip(), str(r.get("포장호기", "")).strip(), str(r.get("제품명", "")).strip()) for r in matched_rt]
    matched_soy = []
    seen_soy_idx = set()
    for idx, sr in enumerate(soy_h):
        direct_ok = (
            is_product_match(sr.get("품목명", ""), product, kw)
            and is_exp_match(sr.get("표기소비기한_원문", ""), sr.get("표준소비기한_검색용", ""), exp_q)
        )
        cross_ok = any(
            (str(sr.get("생산일자", "")).strip() == r_dt)
            and (str(sr.get("포장기", "")).strip() == r_ho or is_product_match(sr.get("품목명", ""), r_pnm, ""))
            for r_dt, r_ho, r_pnm in rt_keys
        )
        if (direct_ok or cross_ok) and idx not in seen_soy_idx:
            seen_soy_idx.add(idx)
            matched_soy.append(sr)
    matched_soy.sort(key=lambda x: x.get("생산일자", ""), reverse=True)

    matched_dy = []
    for dr in dy_hp:
        if is_product_match(dr.get("제품명", ""), product, kw) and is_exp_match(dr.get("표기소비기한_원문", ""), dr.get("표준소비기한_검색용", ""), exp_q):
            d_dt = str(dr.get("생산일자", "")).strip()
            d_ln = str(dr.get("라인구분", "")).strip()
            ccp_lines = []
            for cr in dy_hc:
                if str(cr.get("생산일자", "")).strip() == d_dt and str(cr.get("라인구분", "")).strip() == d_ln:
                    rcat = str(cr.get("기록분류", ""))
                    if rcat == "포장생산시간":
                        ccp_lines.append(f"[생산시간] {cr.get('기록내용_수치', '-')}")
                    elif rcat == "CCP모니터링":
                        ccp_lines.append(f"[{cr.get('세부항목', 'CCP')}] {cr.get('기록내용_수치', '-')}")
                    elif rcat == "특이사항":
                        rv = str(cr.get("특이사항", "이상없음")).strip()
                        if rv and rv not in ["이상없음", "-"]:
                            ccp_lines.append(f"[특이사항] {rv}")
            dr_copy = dict(dr)
            dr_copy["_ccp_summary"] = " / ".join(ccp_lines) if ccp_lines else "이상없음"
            matched_dy.append(dr_copy)
    matched_dy.sort(key=lambda x: x.get("생산일자", ""), reverse=True)

    rt_dt_tab_set = {(r.get("생산일자", ""), r.get("원본탭명", "")) for r in matched_rt}
    matched_rt_ccp = [c for c in rt_hc if (c.get("생산일자", ""), c.get("원본탭명", "")) in rt_dt_tab_set]

    return {
        "ok": True,
        "counts": {"rt": len(matched_rt), "soy": len(matched_soy), "dairy": len(matched_dy)},
        "matched_rt": matched_rt,
        "matched_rt_ccp": matched_rt_ccp,
        "matched_soy": matched_soy,
        "matched_dairy": matched_dy
    }


@app.post("/api/audit-schedule")
def api_save_audit_schedule(body: AuditSchedulePayload):
    creds, auth_err = build_google_credentials()
    if creds is None:
        raise HTTPException(status_code=500, detail=auth_err)

    sheet_urls_map = get_sheet_urls_map()
    sh, err = open_sheet_threadsafe(creds, sheet_urls_map, "audit_db")
    if sh is None:
        raise HTTPException(status_code=500, detail=f"심사 DB 시트를 열 수 없습니다: {err}")

    bundle = build_full_dashboard_bundle(force_refresh=False)
    cur_list = [dict(x) for x in bundle.get("audit", {}).get("schedules", [])]
    now_s = now_kst().strftime("%Y-%m-%d %H:%M:%S")

    if body.action == "add":
        if not (body.audit_name or "").strip():
            raise HTTPException(status_code=400, detail="심사명을 입력해 주세요.")
        cur_list.append({
            "고유ID": str(uuid.uuid4())[:8],
            "심사일자": (body.audit_date or now_kst().strftime("%Y-%m-%d")).strip(),
            "심사명": body.audit_name.strip(),
            "진행현황": body.status if body.status in AUDIT_STATUS_LIST else "심사준비중",
            "점수": (body.score or "").strip(),
            "비고": (body.memo or "").strip(),
            "수정일시": now_s
        })
    elif body.action == "update":
        for item in cur_list:
            if item["고유ID"] == body.uid:
                if body.audit_date:
                    item["심사일자"] = body.audit_date.strip()
                if body.audit_name:
                    item["심사명"] = body.audit_name.strip()
                if body.status in AUDIT_STATUS_LIST:
                    item["진행현황"] = body.status
                item["점수"] = (body.score or "").strip()
                item["비고"] = (body.memo or "").strip()
                item["수정일시"] = now_s
    elif body.action == "quick_status":
        target_uid = body.uid
        if not target_uid and cur_list:
            target_uid = bundle["audit"]["summary"].get("active_uid", cur_list[0]["고유ID"])
        for item in cur_list:
            if item["고유ID"] == target_uid and body.status in AUDIT_STATUS_LIST:
                item["진행현황"] = body.status
                item["수정일시"] = now_s
    elif body.action == "delete":
        cur_list = [x for x in cur_list if x["고유ID"] != body.uid]

    cur_list.sort(key=lambda x: x.get("심사일자", ""))

    ws_sched = api_call_with_retry(sh.get_worksheet, 0)
    payload = [AUDIT_COLS] + [[str(r.get(c, "")) for c in AUDIT_COLS] for r in cur_list]
    api_call_with_retry(ws_sched.clear)
    safe_ws_update(ws_sched, payload)

    if _BUNDLE_CACHE["data"] is not None:
        today_str = now_kst().strftime("%Y-%m-%d")
        _BUNDLE_CACHE["data"]["audit"]["schedules"] = cur_list
        _BUNDLE_CACHE["data"]["audit"]["summary"] = get_active_audit_summary(cur_list, today_str)

    return {
        "ok": True,
        "message": "심사 일정 및 진행현황이 구글 시트에 저장되었습니다.",
        "audit": _BUNDLE_CACHE["data"]["audit"] if _BUNDLE_CACHE["data"] else {"schedules": cur_list}
    }


@app.post("/api/manual-kpi")
def api_save_manual_kpi(body: ManualKpiPayload):
    if body.kpi_key not in DEFAULT_MANUAL_KPIS:
        raise HTTPException(status_code=400, detail="유효하지 않은 KPI 키입니다.")

    creds, auth_err = build_google_credentials()
    if creds is None:
        raise HTTPException(status_code=500, detail=auth_err)

    sheet_urls_map = get_sheet_urls_map()
    sh, err = open_sheet_threadsafe(creds, sheet_urls_map, "audit_db")
    if sh is None:
        raise HTTPException(status_code=500, detail=f"심사 DB 시트를 열 수 없습니다: {err}")

    bundle = build_full_dashboard_bundle(force_refresh=False)
    all_manual = {k: dict(v) for k, v in bundle.get("manual_kpis", DEFAULT_MANUAL_KPIS).items()}

    updated_obj = {
        **all_manual[body.kpi_key],
        "target_text": body.target_text.strip(),
        "actual_text": body.actual_text.strip(),
        "compact_val": body.actual_text.split(",")[0].strip(),
        "sub_badge": body.sub_badge.strip(),
        "target_num": float(body.target_num),
        "actual_num": float(body.actual_num),
        "signal": body.signal if body.signal in ["green", "yellow", "red", "gray"] else "green",
        "notes_title": body.notes_title.strip(),
        "notes": [x.strip() for x in body.notes if x.strip()]
    }
    all_manual[body.kpi_key] = updated_obj

    ws_list = api_call_with_retry(sh.worksheets)
    ws_manual = next((w for w in ws_list if w.title.strip() == MANUAL_KPI_TAB), None)
    if ws_manual is None:
        ws_manual = api_call_with_retry(sh.add_worksheet, title=MANUAL_KPI_TAB, rows=30, cols=12)

    now_s = now_kst().strftime("%Y-%m-%d %H:%M:%S")
    payload = [MANUAL_KPI_COLS]
    for k, v in all_manual.items():
        payload.append([
            str(k),
            str(v.get("target_text", "")),
            str(v.get("actual_text", "")),
            str(v.get("sub_badge", "")),
            str(v.get("target_num", 0.0)),
            str(v.get("actual_num", 0.0)),
            str(v.get("signal", "green")),
            str(v.get("notes_title", "")),
            "||".join(v.get("notes", [])),
            now_s
        ])
    api_call_with_retry(ws_manual.clear)
    safe_ws_update(ws_manual, payload)

    if _BUNDLE_CACHE["data"] is not None:
        _BUNDLE_CACHE["data"]["manual_kpis"] = all_manual

    return {"ok": True, "message": "KPI 설정이 구글 시트에 영구 저장되었습니다.", "manual_kpis": all_manual}


@app.post("/api/special-boards")
def api_save_special_boards(body: SpecialBoardPayload):
    creds, auth_err = build_google_credentials()
    if creds is None:
        raise HTTPException(status_code=500, detail=auth_err)

    sheet_urls_map = get_sheet_urls_map()
    sh, err = open_sheet_threadsafe(creds, sheet_urls_map, "audit_db")
    if sh is None:
        raise HTTPException(status_code=500, detail=f"심사 DB 시트를 열 수 없습니다: {err}")

    bundle = build_full_dashboard_bundle(force_refresh=False)
    cur_sp = bundle.get("special_boards", {"label_memo": DEFAULT_LABEL_MEMO, "claim_issues": DEFAULT_CLAIM_ISSUES})

    new_memo = body.label_memo if body.label_memo is not None else cur_sp["label_memo"]
    new_issues = body.claim_issues if body.claim_issues is not None else cur_sp["claim_issues"]

    ws_list = api_call_with_retry(sh.worksheets)
    ws_sp = next((w for w in ws_list if w.title.strip() == SPECIAL_BOARD_TAB), None)
    if ws_sp is None:
        ws_sp = api_call_with_retry(sh.add_worksheet, title=SPECIAL_BOARD_TAB, rows=20, cols=4)

    now_s = now_kst().strftime("%Y-%m-%d %H:%M:%S")
    payload = [
        ["항목키", "저장내용", "수정일시"],
        ["label_memo", str(new_memo), now_s],
        ["claim_issues_json", json.dumps(new_issues, ensure_ascii=False), now_s]
    ]
    api_call_with_retry(ws_sp.clear)
    safe_ws_update(ws_sp, payload)

    if _BUNDLE_CACHE["data"] is not None:
        _BUNDLE_CACHE["data"]["special_boards"] = {"label_memo": new_memo, "claim_issues": new_issues}

    return {
        "ok": True,
        "message": "특이사항 보드가 구글 시트에 영구 저장되었습니다.",
        "special_boards": {"label_memo": new_memo, "claim_issues": new_issues}
    }


@app.post("/api/ai-briefing")
def api_generate_ai_briefing():
    bundle = build_full_dashboard_bundle(force_refresh=False)
    if not bundle.get("ok"):
        raise HTTPException(status_code=500, detail="데이터가 준비되지 않았습니다.")

    ps = bundle["process"]["summary"]
    lb = bundle["label_status"]
    au = bundle["audit"]["summary"]
    cl = bundle["claim"]
    cs = bundle["cost"]["dept"]

    factual_fallback = (
        f"[금일 핵심 브리핑] 상온공정({ps['rt_today_tab']}) {ps['active_rt_cnt']}개 호기 가동 중(특이사항 {len(ps['today_notes'])}건), "
        f"두유배양({ps['soy_default_tab']}) 미검사 대기 {ps['soy_uninspected_cnt']}호기입니다. "
        f"식안팀 표시사항 검토중 {lb['reviewing_cnt']}건이며, 심사 현황은 '{au['status']}({au['pct']}%)' 단계입니다. "
        f"부문 누적 원가절감 {cs['actual_uk']}억원({cs['rate']}%), 클레임 {cl['actual_ppm']}ppm입니다."
    )

    api_key = os.environ.get("GEMINI_API_KEY", "").strip()
    if not api_key:
        return {"ok": True, "source": "factual_engine", "briefing": factual_fallback}

    prompt_txt = (
        "다음 연세유업 아산공장 품질안전부문 실시간 지표를 바탕으로 추정 없이 오직 팩트 수치만 요약하여 "
        "부문장 출근 보고용 3줄 핵심 브리핑을 한국어로 간결하게 작성해줘.\n"
        f"- 상온공정({ps['rt_today_tab']}): 가동 {ps['active_rt_cnt']}건, 기준이탈 {ps['red_rt_cnt']}건, 금일특이사항: {ps['today_notes']}, 전일({ps['rt_yest_tab']})특이사항: {ps['yest_notes']}\n"
        f"- 유제품포장: 금일 {ps['active_dairy_cnt']}건 / 두유배양({ps['soy_default_tab']}): 총 {ps['soy_blocks_cnt']}호기 중 미검사 {ps['soy_uninspected_cnt']}건, 오염 {ps['soy_contam_total']}건\n"
        f"- 표시사항 심의: 현재 검토중 {lb['reviewing_cnt']}건 (동기간 전년비 {lb['ytd_pct']:+.1f}%)\n"
        f"- 심사 진행: {au['banner_text']} ({au['status']} {au['pct']}%)\n"
        f"- 원가절감 누적 {cs['actual_uk']}억(연말예상 {cs['proj_uk']}억) / 클레임 누적 {cl['actual_ppm']}ppm(목표 1.12 이하)"
    )

    for endpoint in [
        f"https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash:generateContent?key={api_key}",
        f"https://aiplatform.googleapis.com/v1/publishers/google/models/gemini-2.5-flash:generateContent?key={api_key}"
    ]:
        try:
            resp = requests.post(
                endpoint,
                json={"contents": [{"role": "user", "parts": [{"text": prompt_txt}]}]},
                timeout=12
            )
            if resp.status_code == 200:
                rj = resp.json()
                cands = rj.get("candidates", [])
                if cands:
                    parts = cands[0].get("content", {}).get("parts", [])
                    if parts and parts[0].get("text"):
                        return {"ok": True, "source": "gemini_api", "briefing": parts[0]["text"].strip()}
        except Exception:
            continue

    return {"ok": True, "source": "factual_engine", "briefing": factual_fallback}
# ============================ [api/index.py - PART 2 / 2 끝] ============================
