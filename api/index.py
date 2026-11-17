# ==============================================================================
# [api/index.py - PART 1 / 2]
# Q-Safety 백엔드 통합 엔진 (미생물 일보 실시간·DB 연동 + 법규 '대시보드' 탭 동기화 포함)
# ==============================================================================
import hashlib
import json
import os
import re
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

import gspread
import requests
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, HTMLResponse
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials as OAuthCredentials
from google.oauth2.service_account import Credentials as ServiceAccountCredentials
from pydantic import BaseModel

# ==============================================================================
# 0. Vercel FastAPI 최상단 진입점(Entrypoint) 및 CORS 설정
# ==============================================================================
app = FastAPI(title="Q-Safety API", version="6.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"]
)

# ==============================================================================
# 1. 기본 상수 · 한국 표준시(KST, UTC+9) · 시트 매핑 정의
# ==============================================================================
KST_TZ = timezone(timedelta(hours=9))


def now_kst() -> datetime:
    return datetime.now(KST_TZ).replace(tzinfo=None)


FISCAL_MONTHS = ["3월", "4월", "5월", "6월", "7월", "8월", "9월", "10월", "11월", "12월", "1월", "2월"]
DAIRY_CATEGORIES = ["냉장우유 카톤팩", "냉장우유 병", "발효유/음료 카톤팩", "발효유/음료 병"]
RT_CATEGORIES = ["상온 멸균팩", "상온 프리즈마"]

AUDIT_TYPE_LIST = ["방문심사", "협력업체 방문"]
AUDIT_STATUS_LIST = ["심사준비중", "심사중", "서류심사중", "현장심사중", "심사종료"]
AUDIT_STATUS_CONFIG = {
    "심사준비중": {"pct": 20, "color": "#64748B"},
    "심사중": {"pct": 50, "color": "#0284C7"},
    "서류심사중": {"pct": 75, "color": "#003378"},
    "현장심사중": {"pct": 85, "color": "#D97706"},
    "심사종료": {"pct": 100, "color": "#10B981"}
}

MICRO_STATUS_LIST = ["자동연동", "출고적합", "검사중", "재검·출고보류"]
LAW_PREP_LIST = ["검토전", "부자재변경중", "반영완료"]

# '법규정보' 시트 내 '대시보드' 탭 표준 헤더
LAW_DASH_TAB = "대시보드"
LAW_DASH_COLS = ["법규ID", "구분", "시행일", "법규명", "준비상태", "핵심요약", "관련부서", "수정일시"]

CLAIM_HIST_STATUS_LIST = ["출고대기", "검사중", "원인·조치중", "적합(종결)", "부적합"]
CLAIM_HIST_COLS = [
    "고유ID", "등록일", "중요도", "제품명", "소비기한",
    "발생공정_원인유형", "상세사유", "추가검사_조치내역",
    "진행상태", "최종판정", "대시보드노출", "수정일시"
]

SHEET_NAMES = {
    "cost": "원가절감 현황",
    "claim": "클레임 건수",
    "supplier": "협력업체 심사 결과 DB",
    "audit_db": "심사점수 및 일정 관리 DB",
    "label_status": "표시사항 현황",
    "raw_label_db": "원자재 표시사항 DB",
    "process_soy": "[상온]두유 배양검사 일보",
    "process_rt": "[상온]상온제품 공정 관리 일보",
    "process_milk": "[우유] 유제품 포장 공정관리 일보",
    "process_micro": "미생물 일보",
    "law_info": "법규정보",
    "sup_audit_kpi": "협력업체, 심사 점수",
    "claim_nc_hist": "클레임, 부적합 이력"
}

SHEET_FALLBACK_TITLES = {
    "process_micro": ["미생물 일보", "[품질] 미생물 검사일보", "미생물 검사일보", "미생물일보"],
    "law_info": ["법규정보", "법규 정보"],
    "sup_audit_kpi": ["협력업체, 심사 점수", "협력업체, 심사점수", "협력업체 심사점수"],
    "claim_nc_hist": ["클레임, 부적합 이력", "클레임 부적합 이력", "클레임 부적합 내역", "클레임, 부적합 내역"]
}

HIST_TABS = {
    "dairy_prod": "_이력DB_유제품포장",
    "dairy_ccp": "_이력DB_유제품공정CCP",
    "rt_prod": "_이력DB_상온생산",
    "rt_ccp": "_이력DB_상온CCP",
    "soy_inc": "_이력DB_두유배양",
    "micro_db": "_이력DB_미생물"
}

AUDIT_SCHED_TAB = "일정"
AUDIT_COLS = ["고유ID", "구분", "심사일자", "심사명", "진행현황", "점수", "비고", "수정일시"]
MANUAL_KPI_TAB = "수동KPI_설정"
MANUAL_KPI_COLS = [
    "kpi_key", "target_text", "actual_text", "sub_badge",
    "target_num", "actual_num", "signal", "notes_title", "notes_joined", "updated_at"
]
SPECIAL_BOARD_TAB = "대시보드_특이사항_보드"

KPI_LEVEL_CRITERIA = {
    "supplier_exec": {
        "kpi_name": "협력업체 AUDIT 실행",
        "weight_pts": 15,
        "formula": "연세유업 방문(40%) + 연세유업 서류(20%) + 연세생건 방문(20%) + 이롬 방문(20%)",
        "prev_year": "86.3% (서류 81.34% · 방문 105.3% · 생건 68.8%)",
        "levels": [
            {"lv": "Lev.1", "std": "88.3% ↑", "min": 88.3},
            {"lv": "Lev.2", "std": "87.3% ↑", "min": 87.3},
            {"lv": "Lev.3", "std": "86.3% ↑", "min": 86.3},
            {"lv": "Lev.4", "std": "85.3% ↑", "min": 85.3},
            {"lv": "Lev.5", "std": "85.3% 미만", "min": -999.0}
        ]
    },
    "audit_imp": {
        "kpi_name": "심사 개선율",
        "weight_pts": 10,
        "formula": "개선건수 / 지적건수 × 100 (예산편성 미실행 과제는 지적 건수에서 제외)",
        "prev_year": "93.5% (262건 / 280건)",
        "levels": [
            {"lv": "Lev.1", "std": "94.5% ↑", "min": 94.5},
            {"lv": "Lev.2", "std": "94.0% ↑", "min": 94.0},
            {"lv": "Lev.3", "std": "93.5% ↑", "min": 93.5},
            {"lv": "Lev.4", "std": "93.0% ↑", "min": 93.0},
            {"lv": "Lev.5", "std": "93.0% 미만", "min": -999.0}
        ]
    },
    "audit_score": {
        "kpi_name": "심사 점수 (통과시 90점)",
        "weight_pts": 10,
        "formula": "평균점수(70%) + 통과율(30%) / 5개 등급 환산: 1(95)·2(90)·3(85)·4(80)·5(70)",
        "prev_year": "93.28점 (점수평균 63.28점 + 통과율 30점)",
        "levels": [
            {"lv": "Lev.1", "std": "94.28점 ↑", "min": 94.28},
            {"lv": "Lev.2", "std": "93.78점 ↑", "min": 93.78},
            {"lv": "Lev.3", "std": "93.28점 ↑", "min": 93.28},
            {"lv": "Lev.4", "std": "92.78점 ↑", "min": 92.78},
            {"lv": "Lev.5", "std": "92.78점 미만", "min": -999.0}
        ]
    }
}


def eval_kpi_level(val: float, crit_key: str) -> str:
    lv_list = KPI_LEVEL_CRITERIA.get(crit_key, {}).get("levels", [])
    for item in lv_list:
        if val >= item["min"]:
            return item["lv"]
    return "Lev.5"


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
        "target_text": "88.3% 이상 (Lev.1)",
        "actual_text": "92.10% (Lev.1)",
        "compact_val": "92.10%",
        "sub_badge": "목표 88.3%↑ 대비 Lev.1 달성",
        "target_num": 88.3,
        "actual_num": 92.1,
        "suffix": "%",
        "unit_label": "(단위 : %)",
        "signal": "green",
        "has_chart": True,
        "notes_title": "협력업체 현장·서류 관리강화 내역",
        "notes": [
            "연세유업·연세생건·이롬 협력업체 방문점검 및 서류평가 통합 집계",
            "등급 외 및 미제출 업체 대상 집중 피드백 및 보완 점검"
        ]
    },
    "kpi_5": {
        "no": "7",
        "title": "7. 심사점수",
        "short_title": "7. 심사점수",
        "target_text": "94.28점 이상 (Lev.1)",
        "actual_text": "91.95점 (Lev.5)",
        "compact_val": "91.95점",
        "sub_badge": "클릭 시 심사개선율 · 수검이력 상세 조회",
        "target_num": 94.28,
        "actual_num": 91.95,
        "suffix": "",
        "unit_label": "(단위 : 점)",
        "signal": "red",
        "has_chart": True,
        "notes_title": "외부심사 점수 관리 내역",
        "notes": [
            "외부 고객사·인증기관 수검 점수평균(70%) 및 통과율(30%) 실시간 반영",
            "상세 페이지에서 심사개선율 및 업체별 미개선 항목 통합 관리"
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

DEFAULT_MICRO_STATUS = {
    "mode": "자동연동",
    "status": "출고적합",
    "note": "",
    "updated_at": ""
}


# ==============================================================================
# 2. 스레드 안전 Google 인증 · 일괄 조회 · 제품명/주석 매칭 유틸리티
# ==============================================================================
_TOKEN_CACHE_LOCK = threading.Lock()
_TOKEN_CACHE = {"access_token": "", "exp_ts": 0.0}


def api_call_with_retry(func, *args, **kwargs):
    for attempt in range(4):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            err_msg = str(e)
            if any(k in err_msg for k in ["429", "Quota exceeded", "RESOURCE_EXHAUSTED", "SSL", "Connection", "503", "500"]):
                time.sleep(1.4 * (attempt + 1))
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


def create_thread_local_credentials():
    raw_cred = os.environ.get("GOOGLE_CREDENTIALS_JSON", "").strip()
    if not raw_cred:
        return None, "환경변수 GOOGLE_CREDENTIALS_JSON이 설정되지 않았습니다."

    try:
        info = json.loads(raw_cred)
        if "refresh_token" in info and "client_id" in info:
            with _TOKEN_CACHE_LOCK:
                now_ts = time.time()
                if not _TOKEN_CACHE["access_token"] or now_ts >= _TOKEN_CACHE["exp_ts"]:
                    seed_creds = OAuthCredentials(
                        token=None,
                        refresh_token=info["refresh_token"],
                        token_uri=info.get("token_uri", "https://oauth2.googleapis.com/token"),
                        client_id=info["client_id"],
                        client_secret=info["client_secret"]
                    )
                    seed_creds.refresh(Request())
                    _TOKEN_CACHE["access_token"] = seed_creds.token
                    _TOKEN_CACHE["exp_ts"] = now_ts + 2400.0
                tok = _TOKEN_CACHE["access_token"]

            isolated_creds = OAuthCredentials(
                token=tok,
                refresh_token=info["refresh_token"],
                token_uri=info.get("token_uri", "https://oauth2.googleapis.com/token"),
                client_id=info["client_id"],
                client_secret=info["client_secret"]
            )
            return isolated_creds, None

        if info.get("type") == "service_account" or "private_key" in info:
            scopes = [
                "https://www.googleapis.com/auth/spreadsheets",
                "https://www.googleapis.com/auth/drive"
            ]
            creds = ServiceAccountCredentials.from_service_account_info(info, scopes=scopes)
            return creds, None

        return None, "지원되지 않는 GOOGLE_CREDENTIALS_JSON 형식입니다."
    except Exception as e:
        return None, f"GOOGLE_CREDENTIALS_JSON 인증 실패: {e}"


def open_sheet_isolated(sheet_urls_map: dict, key_or_title: str):
    creds, auth_err = create_thread_local_credentials()
    if creds is None:
        return None, auth_err

    client = gspread.authorize(creds)
    errors = []

    if key_or_title in sheet_urls_map:
        raw_val = str(sheet_urls_map[key_or_title]).strip()
        if raw_val:
            m_key = re.search(r"/spreadsheets/d/([a-zA-Z0-9\-_]+)", raw_val)
            try:
                if m_key:
                    return api_call_with_retry(client.open_by_key, m_key.group(1)), None
                if raw_val.startswith("http"):
                    return api_call_with_retry(client.open_by_url, raw_val), None
                return api_call_with_retry(client.open_by_key, raw_val), None
            except Exception as e:
                errors.append(f"URL/ID 열기 실패({key_or_title}): {e}")

    base_title = SHEET_NAMES.get(key_or_title, key_or_title)
    alt_list = SHEET_FALLBACK_TITLES.get(key_or_title, [base_title])
    candidates = []
    for t in alt_list:
        for v in [t, t.strip(), t.strip() + " "]:
            if v not in candidates:
                candidates.append(v)

    for candidate in candidates:
        try:
            return api_call_with_retry(client.open, candidate), None
        except Exception as e:
            errors.append(f"'{candidate}': {e}")

    return None, " / ".join(errors[:2])


def batch_fetch_tabs(sh, tab_titles: List[str], render_opt: str = "FORMATTED_VALUE") -> Dict[str, list]:
    if sh is None or not tab_titles:
        return {}
    unique_titles = list(dict.fromkeys([t for t in tab_titles if t]))
    if not unique_titles:
        return {}
    ranges = [f"'{t.replace(chr(39), chr(39)*2)}'" for t in unique_titles]
    try:
        resp = api_call_with_retry(
            sh.values_batch_get,
            ranges,
            params={"valueRenderOption": render_opt}
        )
        v_ranges = resp.get("valueRanges", [])
        out = {}
        for idx, t_name in enumerate(unique_titles):
            out[t_name] = v_ranges[idx].get("values", []) if idx < len(v_ranges) else []
        return out
    except Exception:
        out = {}
        for t_name in unique_titles:
            try:
                ws = api_call_with_retry(sh.worksheet, t_name)
                out[t_name] = get_values_unformatted(ws) if render_opt == "UNFORMATTED_VALUE" else get_values_formatted(ws)
            except Exception:
                out[t_name] = []
        return out


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
            if 2018 <= dt.year <= 2038:
                return dt.date()
        except Exception:
            pass
    m = re.search(r"(20\d{2})\D+(\d{1,2})\D+(\d{1,2})", s)
    if m:
        try:
            return date(int(m.group(1)), int(m.group(2)), int(m.group(3)))
        except ValueError:
            return None
    m2 = re.search(r"(?<!\d)(\d{2})[\.\-/]\s*(\d{1,2})[\.\-/]\s*(\d{1,2})(?!\d)", s)
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
    s = re.sub(r"\(주\d+\)", "", s)
    s = re.sub(r"\(\s*\d+(?:\.\d+)?\s*(?:mL|ml|ML|㎖|L|g|kg)\s*\)", "", s)
    s = re.sub(r"(?<!\d)\d{2,4}\s*(?:mL|ml|ML|㎖|g)(?!\w)", "", s)
    s = re.sub(r"\s+1[0127]\d{3}\b", "", s)
    s = s.replace("Ωmega3", "오메가3").replace("omega3", "오메가3")
    s = re.sub(r"[\s\(\)\[\]\{\}\-_·,/&\+!]+", "", s).lower()
    return s


def is_product_match(record_prod: str, selected_prod: str, kw_text: str = "") -> bool:
    rec_raw = str(record_prod or "").strip()
    if not rec_raw or rec_raw == "-":
        return False

    rec_parts = [p.strip() for p in re.split(r"[/|,]", rec_raw) if p.strip()]
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
        # 토큰 기반 보조 매칭 (예: '푸르밀 딸기' <-> '푸르밀 요거스푼 딸기', '오늘좋은 딸기')
        if not matched_sel:
            sel_tokens = [clean_product_for_match(t) for t in re.split(r"\s+", re.sub(r"\([^)]*\)", "", selected_prod)) if len(clean_product_for_match(t)) >= 2]
            if len(sel_tokens) >= 2:
                full_rc = " ".join(rec_cleans)
                if all(tok in full_rc for tok in sel_tokens):
                    matched_sel = True
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
def fetch_cost_sheet(sheet_urls_map: dict):
    sh, err = open_sheet_isolated(sheet_urls_map, "cost")
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


def fetch_claim_sheet(sheet_urls_map: dict):
    sh, err = open_sheet_isolated(sheet_urls_map, "claim")
    if sh is None:
        return [], [], err

    try:
        ws_all = api_call_with_retry(sh.worksheets)
        target_ws = ws_all[0] if ws_all else None
        if len(ws_all) > 1:
            t_map = batch_fetch_tabs(sh, [w.title for w in ws_all[:3]], "UNFORMATTED_VALUE")
            for w in ws_all[:3]:
                sample = t_map.get(w.title, [])[:8]
                flat_txt = " ".join([" ".join([str(x) for x in r]) for r in sample])
                if "유형" in flat_txt and ("3월" in flat_txt or "생산" in flat_txt):
                    target_ws = w
                    rows = t_map.get(w.title, [])
                    break
            else:
                rows = get_values_unformatted(target_ws)
        else:
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
# 4. [3] 4대 현장·실험실 일보(상온 · 유제품 · 두유배양 · 미생물 검사일보) 정밀 파서
# ==============================================================================
def select_date_tab_titles(ws_titles: List[str]):
    today = now_kst()
    yesterday = today - timedelta(days=1)

    date_tabs = []
    for raw_t in ws_titles:
        t = raw_t.strip()
        if t.startswith("_이력DB_") or any(ex in t for ex in ["양식", "복사", "백업", "사본", "대기", "예비", "샘플", "식품안전나라"]):
            continue
        m = re.match(r"^(\d{1,2})[/\.\-](\d{1,2})$", t)
        if m:
            mm, dd = int(m.group(1)), int(m.group(2))
            yr = today.year if mm <= today.month + 1 else today.year - 1
            try:
                dt_obj = datetime(yr, mm, dd)
            except ValueError:
                dt_obj = datetime(today.year, 1, 1)
            date_tabs.append((dt_obj, raw_t))

    past_or_today = sorted([x for x in date_tabs if x[0].date() <= today.date()], key=lambda x: x[0], reverse=True)
    future_tabs = sorted([x for x in date_tabs if x[0].date() > today.date()], key=lambda x: x[0], reverse=True)
    ordered_titles = [t for _, t in (past_or_today + future_tabs)]

    t_cands = {today.strftime("%m/%d"), f"{today.month}/{today.day}", today.strftime("%m.%d"), f"{today.month}.{today.day}"}
    y_cands = {yesterday.strftime("%m/%d"), f"{yesterday.month}/{yesterday.day}", yesterday.strftime("%m.%d"), f"{yesterday.month}.{yesterday.day}"}

    t_today = next((t for t in ordered_titles if t.strip() in t_cands), None)
    t_yest = next((t for t in ordered_titles if t.strip() in y_cands), None)

    if t_today is None and len(ordered_titles) >= 1:
        t_today = ordered_titles[0]
    if t_yest is None and len(ordered_titles) >= 2:
        idx_t = ordered_titles.index(t_today) if t_today in ordered_titles else 0
        t_yest = ordered_titles[idx_t + 1] if idx_t + 1 < len(ordered_titles) else ordered_titles[0]

    if t_today is None and ws_titles:
        non_db = [t for t in ws_titles if not t.strip().startswith("_이력DB_") and not any(ex in t for ex in ["복사", "양식", "사본", "백업"])]
        t_today = non_db[0] if non_db else ws_titles[0]

    return t_today, t_yest, ordered_titles


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


def parse_rt_process_rows(tab_title_raw: str, rows: list):
    if not tab_title_raw or not rows or len(rows) < 5:
        return None

    tab_title = tab_title_raw.strip()
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
        std_press = next((re.sub(r"\s+", " ", str(br[19])).strip() for br in b_rows if str(br[19]).strip() not in ["", "-", "균질압"]), "")
        std_hold = next((re.sub(r"\s+", " ", str(br[20])).strip() for br in b_rows if str(br[20]).strip() not in ["", "-", "멸균시간"]), "")
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


def calc_time_span_text(time_str: str) -> Tuple[str, str, str]:
    times = re.findall(r"(\d{1,2}:\d{2})", str(time_str or ""))
    if len(times) >= 2:
        t_s, t_e = times[0], times[-1]
        if t_s in ["00:00", "0:00"] and t_e in ["00:00", "0:00"]:
            return "-", "-", "비가동"
        m1 = re.match(r"(\d{1,2}):(\d{2})", t_s)
        m2 = re.match(r"(\d{1,2}):(\d{2})", t_e)
        if m1 and m2:
            s_min = int(m1.group(1)) * 60 + int(m1.group(2))
            e_min = int(m2.group(1)) * 60 + int(m2.group(2))
            if e_min < s_min:
                e_min += 24 * 60
            diff = e_min - s_min
            dur = f"{diff // 60}시간 {diff % 60:02d}분" if diff > 0 else "-"
            return t_s, t_e, dur
    elif len(times) == 1:
        if times[0] not in ["00:00", "0:00"]:
            return times[0], "진행중", "가동중"
    return "-", "-", "비가동"


def parse_dairy_pack_rows(tab_title_raw: str, rows: list, line_label: str):
    if not rows:
        return [], [], "-"

    w_date = (tab_title_raw or "-").strip()
    for r in rows[:6]:
        joined = " ".join([str(c).strip() for c in r if str(c).strip()])
        m_dt = re.search(r"202\d[\.\-\s]+\d{1,2}[\.\-\s]+\d{1,2}", joined)
        if m_dt:
            w_date = m_dt.group(0).strip()
            break

    parsed_products = []
    run_times = []
    current_cat = line_label
    prod_time_sec_idx = -1

    for r_idx, r in enumerate(rows):
        c0_raw = str(r[0]).strip() if len(r) > 0 else ""
        c1_raw = str(r[1]).strip() if len(r) > 1 else ""
        c0 = re.sub(r"\s+", "", c0_raw)
        c1 = re.sub(r"\s+", "", c1_raw)

        if any(k in c0 for k in ["생산시간", "포장생산시간", "포장시간"]) or any(k in c1 for k in ["생산시간", "포장생산시간", "포장시간"]):
            if prod_time_sec_idx < 0:
                prod_time_sec_idx = r_idx

        if c0 and c0 not in ["제품명", "품목", "생산시간", "CCP검증모니터링", "특이사항"] and ("발효유" in c0 or "우유" in c0 or "카톤" in c0 or "호상" in c0 or "병" in c0):
            current_cat = re.sub(r"\s+", " ", c0_raw).strip()

        lbl_col = -1
        if c0 in ["제품명", "품목"]:
            lbl_col = 0
        elif c1 in ["제품명", "품목"]:
            lbl_col = 1

        if lbl_col < 0:
            continue

        next_block_r = len(rows)
        for nr in range(r_idx + 1, min(r_idx + 24, len(rows))):
            nc0 = re.sub(r"\s+", "", str(rows[nr][0])) if len(rows[nr]) > 0 else ""
            nc1 = re.sub(r"\s+", "", str(rows[nr][1])) if len(rows[nr]) > 1 else ""
            if nc0 in ["제품명", "품목", "생산시간", "포장시간", "CCP검증모니터링"] or nc1 in ["제품명", "품목", "생산시간", "포장시간", "CCP검증모니터링"]:
                next_block_r = nr
                break

        for col_i in range(lbl_col + 1, len(r)):
            p_name = re.sub(r"\s+", " ", str(r[col_i])).strip()
            if not p_name or re.sub(r"\s+", "", p_name) in ["제품명", "품목"] or "특이사항" in p_name:
                continue

            vol_str, chk_time = "", "-"
            exp_list = []
            label_ok, top_ok, bot_ok, vol_ok, sens_ok = "-", "-", "-", "-", "-"

            carry_lbl = ""
            for sub_i, sub_r in enumerate(rows[r_idx + 1 : next_block_r]):
                raw_lbl_c0 = re.sub(r"\s+", "", str(sub_r[0])) if len(sub_r) > 0 else ""
                raw_lbl_c = re.sub(r"\s+", "", str(sub_r[lbl_col])) if len(sub_r) > lbl_col else ""
                eff_lbl = raw_lbl_c or (raw_lbl_c0 if lbl_col == 0 else "")
                if eff_lbl:
                    carry_lbl = eff_lbl
                row_lbl = eff_lbl or carry_lbl

                val = str(sub_r[col_i]).strip() if col_i < len(sub_r) else ""
                if not val:
                    continue

                if sub_i == 0 and re.search(r"\d+\s*(?:㎖|mL|ml|L|g|kg)", val) and not re.search(r"\d{1,2}\.\d{1,2}", val):
                    vol_str = val
                elif "확인시간" in row_lbl:
                    chk_time = val if chk_time == "-" else f"{chk_time} / {val}"
                elif any(k in row_lbl for k in ["소비기한", "Tank", "유통기한"]) or re.match(r"^\d{2}\.\d{2}[A-Z]?$", row_lbl):
                    if val not in ["00.00", "0.0", "-", "소비기한"] and "제조일로부터" not in val:
                        if val not in exp_list:
                            exp_list.append(val)
                elif "표시사항" in row_lbl or "표기사항" in row_lbl:
                    label_ok = val
                elif any(k in row_lbl for k in ["상부접착", "상/하부접착", "실링상태", "실링"]):
                    top_ok = val
                    if "상/하부" in row_lbl:
                        bot_ok = val
                elif any(k in row_lbl for k in ["하부접착", "4본입", "수축라벨", "외포장"]):
                    bot_ok = val if bot_ok == "-" else f"{bot_ok} / {val}"
                elif "제품용량" in row_lbl or "내용량" in row_lbl:
                    vol_ok = val
                elif "관능" in row_lbl:
                    sens_ok = val

            exp_joined = ", ".join(exp_list) if exp_list else "-"
            is_running = (exp_joined != "-") or (chk_time not in ["-", "", "00:00"])
            full_p_name = f"{p_name} ({vol_str})" if vol_str else p_name

            parsed_products.append({
                "라인구분": current_cat,
                "작성일": w_date,
                "가동여부": "금일생산" if is_running else "미생산(00.00)",
                "제품명": full_p_name,
                "확인시간": chk_time,
                "표기 소비기한": exp_joined,
                "표시사항": label_ok,
                "상부/실링": top_ok,
                "하부/라벨": bot_ok,
                "제품용량": vol_ok,
                "관능(5점법)": sens_ok
            })

    if prod_time_sec_idx >= 0:
        end_pt_idx = len(rows)
        for nr in range(prod_time_sec_idx + 1, min(prod_time_sec_idx + 14, len(rows))):
            nc = re.sub(r"\s+", "", "".join(str(x) for x in rows[nr][:3]))
            if any(stop_k in nc for stop_k in ["CCP", "살균", "금속검출", "특이사항"]):
                end_pt_idx = nr
                break

        pt_block = rows[prod_time_sec_idx : end_pt_idx]
        seen_machines = set()

        for bi, br in enumerate(pt_block):
            for ci, cell in enumerate(br):
                cs = re.sub(r"\s+", " ", str(cell)).strip()
                if not cs or re.sub(r"\s+", "", cs) in ["생산시간", "포장시간", "구분", "호기", "시작", "종료"]:
                    continue
                if any(mk in cs for mk in ["호기", "카톤", "병", "호상", "연팩", "로타리", "라인", "팩", "컵"]) and not re.search(r"\d{1,2}:\d{2}", cs):
                    below_vals = []
                    for sub_b in pt_block[bi + 1 : min(bi + 4, len(pt_block))]:
                        if ci < len(sub_b) and re.search(r"\d{1,2}:\d{2}", str(sub_b[ci])):
                            below_vals.append(str(sub_b[ci]).strip())
                    right_val = str(br[ci + 1]).strip() if (ci + 1 < len(br) and re.search(r"\d{1,2}:\d{2}", str(br[ci + 1]))) else ""

                    raw_t_join = " ~ ".join(below_vals) if below_vals else right_val
                    if raw_t_join or below_vals is not None:
                        t_s, t_e, dur = calc_time_span_text(raw_t_join)
                        if cs not in seen_machines and (t_s != "-" or right_val or below_vals):
                            seen_machines.add(cs)
                            run_times.append({
                                "라인구분": line_label,
                                "포장호기": cs,
                                "생산 시작": t_s,
                                "생산 종료": t_e,
                                "시작~종료": f"{t_s} ~ {t_e}" if t_s != "-" else (raw_t_join or "비가동"),
                                "실가동시간": dur,
                                "가동상태": "가동" if t_s != "-" else "비가동"
                            })

    return parsed_products, run_times, w_date


def extract_hist_dairy_run_times(dairy_hist_c: list, target_date: str, line_filter: str = "ALL") -> list:
    out = []
    for r in dairy_hist_c:
        if r.get("생산일자") != target_date:
            continue
        ln = r.get("라인구분", "")
        if line_filter != "ALL" and ln not in line_filter and line_filter not in ln:
            continue
        if "생산시간" in str(r.get("기록분류", "")):
            ho_nm = str(r.get("세부항목", "-")).strip()
            raw_v = str(r.get("기록내용_수치", "-")).strip()
            t_s, t_e, dur = calc_time_span_text(raw_v)
            out.append({
                "라인구분": ln,
                "포장호기": ho_nm,
                "생산 시작": t_s,
                "생산 종료": t_e,
                "시작~종료": f"{t_s} ~ {t_e}" if t_s != "-" else raw_v,
                "실가동시간": dur,
                "가동상태": "가동" if t_s != "-" else "비가동"
            })
    return out


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


def parse_soy_incubation_rows(tab_title_raw: str, rows: list):
    if not rows:
        return [], []

    tab_title = tab_title_raw.strip()
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


def parse_micro_live_sheet(tab_title: str, rows: list) -> Optional[dict]:
    """'미생물 일보'의 [일보작성] 또는 [일요일_일보작성] 탭을 실시간 파싱"""
    if not rows or len(rows) < 35:
        return None

    def g(r_i: int, c_i: int) -> str:
        if r_i < len(rows) and c_i < len(rows[r_i]):
            return str(rows[r_i][c_i]).strip()
        return ""

    today_d = now_kst().date()
    judge_d = parse_date_obj(g(7, 18)) or parse_date_obj(g(2, 3)) or today_d
    milk_d = parse_date_obj(g(8, 18)) or (judge_d - timedelta(days=2))
    ferm_d = parse_date_obj(g(9, 18)) or (judge_d - timedelta(days=5))

    judge_iso = judge_d.strftime("%Y-%m-%d")
    milk_iso = milk_d.strftime("%Y-%m-%d")
    ferm_iso = ferm_d.strftime("%Y-%m-%d")
    weekdays_kr = ["월", "화", "수", "목", "금", "토", "일"]
    day_kr = weekdays_kr[judge_d.weekday()]
    judge_person = re.sub(r"[\s:]+", "", g(3, 3).replace("판정자", "")) or "-"

    # 하단 (주1)~(주12) 각주 사전 추출
    note_map = {}
    for r_i in range(68, len(rows)):
        line_txt = g(r_i, 0)
        if "(주" not in line_txt:
            continue
        for m in re.finditer(r"\(주(\d+)\)\s*([^(]+)", line_txt):
            note_map[f"주{m.group(1)}"] = re.sub(r"\s+", " ", m.group(2)).strip()

    def expand_aliases(p_nm: str) -> str:
        clean_p = re.sub(r"\s+", " ", p_nm).strip()
        m = re.search(r"\(주(\d+)\)", clean_p)
        extra = note_map.get(f"주{m.group(1)}", "") if m else ""
        if "건강박사" in clean_p:
            extra = f"{extra}, 헬스닥터" if extra else "헬스닥터"
        return f"{clean_p} | {extra}" if extra else clean_p

    # 작업자 실제 측정치 기입 여부 판별
    carton_cols = [2, 4, 6, 8, 10, 12, 14]
    has_tank_top = any(g(7, c) not in ["", "-"] for c in carton_cols)
    has_bot_input = False
    for r_i in range(41, min(len(rows), 73)):
        if re.search(r"Storage\s*Tank\s*:\s*\d+", g(r_i, 2), re.IGNORECASE):
            has_bot_input = True
            break
        if r_i >= 53 and g(r_i, 15) not in ["", "-", "유산균"]:
            has_bot_input = True
            break
    has_actual_input = has_tank_top or has_bot_input

    flat_records = []
    carton_summary = []
    bottle_rows = []
    oem_ferm_rows = []
    has_fail = False

    # 1) 1. 카톤우유 파싱
    for c_idx in carton_cols:
        p_name = re.sub(r"\s+", " ", g(5, c_idx)).strip()
        if not p_name:
            continue
        tank_s = g(7, c_idx) or "-"
        judge_s = g(8, c_idx) or "적합"
        if "부적합" in judge_s:
            has_fail = True

        active_lines = []
        nonzero_notes = []
        cur_ho = ""
        for r_i in range(10, min(len(rows), 37)):
            ho_c = g(r_i, 0)
            ln_c = g(r_i, 1)
            if ho_c:
                cur_ho = ho_c
            if not cur_ho:
                continue
            coli_v = g(r_i, c_idx)
            bac_v = g(r_i, c_idx + 1)
            if coli_v == "" and bac_v == "":
                continue
            loc_tag = cur_ho if "Storage" in cur_ho else f"{cur_ho}-{ln_c}"
            active_lines.append(loc_tag)
            if (coli_v not in ["0", "-", ""] or bac_v not in ["0", "-", ""]):
                nonzero_notes.append(f"{loc_tag}(대:{coli_v}/세:{bac_v})")

            if has_actual_input:
                flat_records.append({
                    "판정일자": judge_iso,
                    "요일": day_kr,
                    "검사일자_생산일": milk_iso,
                    "판정자": judge_person,
                    "대분류": "1. 카톤우유",
                    "업체_라인구분": "자사 카톤",
                    "대표품목명": p_name,
                    "연관품목_주석포함": expand_aliases(p_name),
                    "호기_용량": cur_ho,
                    "세부라인": ln_c or "-",
                    "Storage_Tank": tank_s,
                    "대장균군": coli_v or "0",
                    "세균수": bac_v or "0",
                    "효모_곰팡이": "-",
                    "유산균": "-",
                    "판정": "부적합" if "부적합" in judge_s else "적합"
                })

        carton_summary.append({
            "제품명": p_name,
            "검사일(생산일)": milk_iso,
            "Storage Tank": tank_s,
            "검사 라인수": f"{len(active_lines)}개 라인",
            "검출/실측 요약": ", ".join(nonzero_notes) if nonzero_notes else "전 라인 음성(0)",
            "판정": judge_s if has_actual_input else "입력대기"
        })

    # 2) 2-좌. 병우유 · 유크림 · 두유액
    cur_b_prod, cur_b_tank = "", "-"
    for r_i in range(41, min(len(rows), 73)):
        a_txt = re.sub(r"\s+", " ", g(r_i, 0)).strip()
        if a_txt.startswith("(주") or a_txt.startswith("※"):
            break
        if a_txt and a_txt not in ["제품명", "제 품 명", "검사일", "검 사 일", "구분", "구 분", "2. 병우유, OEM 등"]:
            cur_b_prod = a_txt
            cur_b_tank = "-"
        if not cur_b_prod:
            continue

        c_txt = g(r_i, 2)
        if re.search(r"Storage\s*Tank", c_txt, re.IGNORECASE):
            cur_b_tank = re.sub(r"Storage\s*Tank\s*:?\s*", "", c_txt, flags=re.IGNORECASE).strip() or "-"
        elif re.search(r"silo|st-", c_txt, re.IGNORECASE):
            cur_b_tank = c_txt

        e_v, g_v = g(r_i, 4), g(r_i, 6)
        if e_v == "" and g_v == "":
            continue

        vol_lbl = "Storage 원액" if re.search(r"Storage\s*Tank", c_txt, re.IGNORECASE) else (c_txt or "-")
        bottle_rows.append({
            "제품명": cur_b_prod,
            "검사일(생산일)": milk_iso,
            "Storage Tank": cur_b_tank,
            "용량/구분": vol_lbl,
            "대장균군": e_v,
            "세균수": g_v
        })
        if has_actual_input:
            flat_records.append({
                "판정일자": judge_iso,
                "요일": day_kr,
                "검사일자_생산일": milk_iso,
                "판정자": judge_person,
                "대분류": "2. 병우유·원액",
                "업체_라인구분": "자사 병/원액",
                "대표품목명": cur_b_prod,
                "연관품목_주석포함": expand_aliases(cur_b_prod),
                "호기_용량": vol_lbl,
                "세부라인": "-",
                "Storage_Tank": cur_b_tank,
                "대장균군": e_v,
                "세균수": g_v,
                "효모_곰팡이": "-",
                "유산균": "-",
                "판정": "적합"
            })

    # 3) 2-우. OEM 및 발효유 · 호상요구르트
    cur_vendor = ""
    is_ferm = False
    for r_i in range(41, min(len(rows), 73)):
        i_txt = re.sub(r"\s+", " ", g(r_i, 8)).strip()
        j_txt = re.sub(r"\s+", " ", g(r_i, 9)).strip()
        m_txt = g(r_i, 12)
        p_txt = g(r_i, 15)

        if re.sub(r"\s+", "", i_txt) in ["검사일", "구분"]:
            continue
        if m_txt == "대장균군":
            if "유산균" in p_txt:
                is_ferm = True
            continue
        if i_txt and i_txt != "업체명":
            cur_vendor = i_txt
        if not j_txt or j_txt in ["제 품 명", "제품명"]:
            continue

        coli_v = m_txt
        bac_v = g(r_i, 13) if is_ferm else g(r_i, 14)
        ym_v = g(r_i, 14) if is_ferm else "-"
        lac_v = p_txt if is_ferm else "-"

        if all(x in ["", "-"] for x in [coli_v, bac_v, ym_v, lac_v]):
            continue

        insp_used = ferm_iso if is_ferm else milk_iso
        cat_nm = "3. 발효유·호상" if is_ferm else "2. OEM·기타"
        oem_ferm_rows.append({
            "구분/업체": cur_vendor or "-",
            "제품명": j_txt,
            "검사일(생산일)": insp_used,
            "대장균군": coli_v or "-",
            "세균수": bac_v or "-",
            "효모·곰팡이": ym_v or "-",
            "유산균": lac_v or "-"
        })
        if has_actual_input:
            flat_records.append({
                "판정일자": judge_iso,
                "요일": day_kr,
                "검사일자_생산일": insp_used,
                "판정자": judge_person,
                "대분류": cat_nm,
                "업체_라인구분": cur_vendor or "-",
                "대표품목명": j_txt,
                "연관품목_주석포함": expand_aliases(j_txt),
                "호기_용량": "-",
                "세부라인": "-",
                "Storage_Tank": "-",
                "대장균군": coli_v or "-",
                "세균수": bac_v or "-",
                "효모_곰팡이": ym_v or "-",
                "유산균": lac_v or "-",
                "판정": "적합"
            })

    return {
        "tab_title": tab_title,
        "judge_date": judge_iso,
        "day_kr": day_kr,
        "milk_insp_date": milk_iso,
        "ferm_insp_date": ferm_iso,
        "judge_person": judge_person,
        "has_actual_input": has_actual_input,
        "has_fail": has_fail,
        "item_cnt": len(carton_summary) + len(bottle_rows) + len(oem_ferm_rows),
        "carton_summary": carton_summary,
        "bottle_rows": bottle_rows,
        "oem_ferm_rows": oem_ferm_rows,
        "flat_records": flat_records
    }


def fetch_process_micro_sheet(sheet_urls_map: dict):
    """'미생물 일보' 구글 시트의 실시간 작업탭(일보작성 / 일요일_일보작성) 및 [_이력DB_미생물] 통합 로드"""
    empty_micro = {
        "has_sheet": False,
        "auto_status": "출고적합",
        "auto_note": "우유 · 발효유 · 두유 완제품 미생물 규격 적합",
        "live_tabs": {},
        "default_live_tab": "",
        "hist_rows": [],
        "total_db_cnt": 0
    }
    sh, err = open_sheet_isolated(sheet_urls_map, "process_micro")
    if sh is None:
        return empty_micro, None  # 미생물 시트 미연결 시에도 에러 배너 없이 기본 상태 유지

    try:
        ws_all = api_call_with_retry(sh.worksheets)
        ws_titles = [w.title for w in ws_all]

        today_d = now_kst().date()
        weekdays_kr = ["월", "화", "수", "목", "금", "토", "일"]
        today_day_kr = weekdays_kr[today_d.weekday()]

        target_live = [t for t in ["일보작성", "일요일_일보작성"] if t in ws_titles]
        if not target_live and today_day_kr in ws_titles:
            target_live = [today_day_kr]

        t_db = next((t for t in ws_titles if t.strip() == HIST_TABS["micro_db"]), None)
        batch_map = batch_fetch_tabs(sh, target_live + ([t_db] if t_db else []), "FORMATTED_VALUE")

        live_parsed_map = {}
        for lt in target_live:
            parsed_l = parse_micro_live_sheet(lt, batch_map.get(lt, []))
            if parsed_l:
                # 평일(화~토)에는 '일요일_일보작성'이 이전 주 데이터면 노출 제외
                if lt == "일요일_일보작성" and today_d.weekday() not in (0, 6):
                    jd = parse_date_obj(parsed_l["judge_date"])
                    if jd and (today_d - jd).days > 2:
                        continue
                live_parsed_map[lt] = parsed_l

        db_records = rows_to_records(batch_map.get(t_db, [])) if t_db else []

        # 실시간 입력분이 아직 5분 트리거 전이라도 이력 조회 및 LOT 역추적에 즉시 잡히도록 병합
        existing_judge_dates = {r.get("판정일자", "") for r in db_records}
        merged_hist = list(db_records)
        for lp in live_parsed_map.values():
            if lp["has_actual_input"] and lp["judge_date"] not in existing_judge_dates:
                merged_hist.extend(lp["flat_records"])
        merged_hist.sort(key=lambda x: (x.get("판정일자", ""), x.get("대분류", "")), reverse=True)

        def_tab = "일보작성" if "일보작성" in live_parsed_map else (list(live_parsed_map.keys())[0] if live_parsed_map else "")
        main_live = live_parsed_map.get(def_tab)

        if main_live:
            j_mmdd = main_live["judge_date"][5:].replace("-", "/")
            m_mmdd = main_live["milk_insp_date"][5:].replace("-", "/")
            f_mmdd = main_live["ferm_insp_date"][5:].replace("-", "/")
            if main_live["has_actual_input"]:
                if main_live["has_fail"]:
                    auto_st = "재검·출고보류"
                    auto_nt = f"판정 {j_mmdd} — 부적합 항목 확인 필요"
                else:
                    auto_st = "출고적합"
                    auto_nt = f"판정 {j_mmdd} 완료 (우유 {m_mmdd} · 발효유 {f_mmdd} 전 품목 적합)"
            else:
                auto_st = "검사중"
                auto_nt = f"금일({j_mmdd}) 판정 기입 대기 · 우유({m_mmdd})·발효유({f_mmdd}) 배양중"
        elif merged_hist:
            latest_dt = merged_hist[0].get("판정일자", "")
            auto_st = "출고적합"
            auto_nt = f"최근 판정({latest_dt[5:]}) 전 품목 적합"
        else:
            auto_st = "검사중"
            auto_nt = "미생물 일보 판정 데이터 대기"

        # UI 전달 시 중복 용량 절약을 위해 live_tabs 내부의 flat_records는 제거
        clean_live_tabs = {}
        for k, v in live_parsed_map.items():
            vc = dict(v)
            vc.pop("flat_records", None)
            clean_live_tabs[k] = vc

        return {
            "has_sheet": True,
            "auto_status": auto_st,
            "auto_note": auto_nt,
            "live_tabs": clean_live_tabs,
            "default_live_tab": def_tab,
            "hist_rows": merged_hist,
            "total_db_cnt": len(merged_hist)
        }, None
    except Exception as e:
        return empty_micro, f"[미생물일보] {e}"


def fetch_process_rt_sheet(sheet_urls_map: dict):
    sh_rt, err_rt = open_sheet_isolated(sheet_urls_map, "process_rt")
    if sh_rt is None:
        return None, None, [], [], [], f"[상온공정일보] {err_rt}"
    try:
        ws_all = api_call_with_retry(sh_rt.worksheets)
        ws_titles = [w.title for w in ws_all]
        t_today, t_yest, _ = select_date_tab_titles(ws_titles)

        t_hp = next((t for t in ws_titles if t.strip() == HIST_TABS["rt_prod"]), None)
        t_hc = next((t for t in ws_titles if t.strip() == HIST_TABS["rt_ccp"]), None)
        t_fsk = next((t for t in ws_titles if re.sub(r"\s+", "", t) == "제품명식품안전나라" or "식품안전나라" in t), None)

        batch_map = batch_fetch_tabs(sh_rt, [t_today, t_yest, t_hp, t_hc, t_fsk])

        rt_today_parsed = parse_rt_process_rows(t_today, batch_map.get(t_today, [])) if t_today else None
        rt_yest_parsed = parse_rt_process_rows(t_yest, batch_map.get(t_yest, [])) if t_yest else None
        rt_hist_p = rows_to_records(batch_map.get(t_hp, [])) if t_hp else []
        rt_hist_c = rows_to_records(batch_map.get(t_hc, [])) if t_hc else []

        fsk_products = []
        if t_fsk:
            fsk_rows = batch_map.get(t_fsk, [])
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


def fetch_process_milk_sheet(sheet_urls_map: dict):
    sh_milk, err_milk = open_sheet_isolated(sheet_urls_map, "process_milk")
    if sh_milk is None:
        return [], [], [], [], "-", "-", [], [], [], f"[유제품포장일보] {err_milk}"
    try:
        ws_all = api_call_with_retry(sh_milk.worksheets)
        ws_titles = [w.title for w in ws_all]
        exclude_words = ["복사", "복사용", "양식", "사본", "백업", "대기", "예비", "샘플", "_이력DB_"]

        t_m = next((t for t in ws_titles if t.strip() == "우유"), None)
        t_f = next((t for t in ws_titles if t.strip() == "발효유"), None)

        if t_m is None:
            t_m = next((t for t in ws_titles if "우유" in t and "발효" not in t and not any(ex in t for ex in exclude_words)), None)
        if t_f is None:
            t_f = next((t for t in ws_titles if "발효유" in t and not any(ex in t for ex in exclude_words)), None)

        t_m_tpl = next((t for t in ws_titles if "우유" in t and "복사용" in t), None)
        t_f_tpl = next((t for t in ws_titles if "발효유" in t and "복사용" in t), None)

        t_dp = next((t for t in ws_titles if t.strip() == HIST_TABS["dairy_prod"]), None)
        t_dc = next((t for t in ws_titles if t.strip() == HIST_TABS["dairy_ccp"]), None)

        batch_map = batch_fetch_tabs(sh_milk, [t_m, t_f, t_m_tpl, t_f_tpl, t_dp, t_dc])

        m_rows, m_times, m_dt = parse_dairy_pack_rows(t_m, batch_map.get(t_m, []), "우유") if t_m else ([], [], "-")
        f_rows, f_times, f_dt = parse_dairy_pack_rows(t_f, batch_map.get(t_f, []), "발효유") if t_f else ([], [], "-")

        dairy_hist_p = rows_to_records(batch_map.get(t_dp, [])) if t_dp else []
        dairy_hist_c = rows_to_records(batch_map.get(t_dc, [])) if t_dc else []

        m_date_obj = parse_date_obj(m_dt)
        if not m_times and m_date_obj and dairy_hist_c:
            m_times = extract_hist_dairy_run_times(dairy_hist_c, m_date_obj.strftime("%Y-%m-%d"), "우유")
        f_date_obj = parse_date_obj(f_dt)
        if not f_times and f_date_obj and dairy_hist_c:
            f_times = extract_hist_dairy_run_times(dairy_hist_c, f_date_obj.strftime("%Y-%m-%d"), "발효유")

        dairy_prod_names = []
        for tpl_t, lbl in [(t_m or t_m_tpl, "우유"), (t_f or t_f_tpl, "발효유")]:
            if tpl_t:
                tpl_list, _, _ = parse_dairy_pack_rows(tpl_t, batch_map.get(tpl_t, []), lbl)
                for r in tpl_list:
                    p_clean = str(r.get("제품명", "")).strip()
                    if p_clean and p_clean != "-" and p_clean not in dairy_prod_names:
                        dairy_prod_names.append(p_clean)

        return m_rows, f_rows, m_times, f_times, m_dt, f_dt, dairy_hist_p, dairy_hist_c, dairy_prod_names, None
    except Exception as e:
        return [], [], [], [], "-", "-", [], [], [], f"[유제품포장일보] {e}"


def fetch_process_soy_sheet(sheet_urls_map: dict):
    sh_soy, err_soy = open_sheet_isolated(sheet_urls_map, "process_soy")
    if sh_soy is None:
        return "-", {}, [], [], [], f"[두유배양일보] {err_soy}"
    try:
        ws_all = api_call_with_retry(sh_soy.worksheets)
        ws_titles = [w.title for w in ws_all]
        _, _, ordered_soy_titles = select_date_tab_titles(ws_titles)

        top5_titles = ordered_soy_titles[:5]
        t_hist_soy = next((t for t in ws_titles if t.strip() == HIST_TABS["soy_inc"]), None)

        batch_map = batch_fetch_tabs(sh_soy, top5_titles + ([t_hist_soy] if t_hist_soy else []))

        soy_tabs_parsed = {}
        default_tab_title = "-"

        for raw_t in top5_titles:
            t_title = raw_t.strip()
            b_list, h_list = parse_soy_incubation_rows(raw_t, batch_map.get(raw_t, []))
            if b_list:
                soy_tabs_parsed[t_title] = {"blocks": b_list, "hourly": h_list}
                if default_tab_title == "-":
                    default_tab_title = t_title

        if default_tab_title == "-" and top5_titles:
            first_raw = top5_titles[0]
            default_tab_title = first_raw.strip()
            b_list, h_list = parse_soy_incubation_rows(first_raw, batch_map.get(first_raw, []))
            soy_tabs_parsed[default_tab_title] = {"blocks": b_list, "hourly": h_list}

        def_pack = soy_tabs_parsed.get(default_tab_title, {"blocks": [], "hourly": []})
        soy_hist = rows_to_records(batch_map.get(t_hist_soy, [])) if t_hist_soy else []

        return default_tab_title, soy_tabs_parsed, def_pack["blocks"], def_pack["hourly"], soy_hist, None
    except Exception as e:
        return "-", {}, [], [], [], f"[두유배양일보] {e}"
# ============================ [api/index.py - PART 1 / 2 끝] ============================
# ==============================================================================
# [api/index.py - PART 2 / 2]
# 법규 '대시보드' 탭 직통 저장 · D-365 불용방지 배지 · 미생물 자동판정 & LOT 역추적 4단 통합
# ==============================================================================

# ==============================================================================
# 5. [4] 협력업체 서류심사 DB & [4-2] '협력업체, 심사 점수'(+일정 탭) 정밀 파서
# ==============================================================================
def fetch_supplier_sheet(sheet_urls_map: dict):
    sh, err = open_sheet_isolated(sheet_urls_map, "supplier")
    if sh is None:
        return [], [], [], [], err

    try:
        ws_list = api_call_with_retry(sh.worksheets)
        if len(ws_list) < 2:
            return [], [], [], [], "협력업체 시트2가 없습니다."

        t1_title = ws_list[0].title
        t2_title = ws_list[1].title
        t_under_title = next((w.title for w in ws_list if w.title.strip() == "등급외_미비사항"), None)

        batch_map = batch_fetch_tabs(sh, [t1_title, t2_title] + ([t_under_title] if t_under_title else []))
        rows1 = batch_map.get(t1_title, [])
        rows2 = batch_map.get(t2_title, [])

        s1_records = rows_to_records(rows1)
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

        if t_under_title:
            u_rows = batch_map.get(t_under_title, [])
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


def normalize_audit_type(raw_type: str) -> str:
    s = str(raw_type or "").strip()
    if "협력" in s or "출장" in s:
        return "협력업체 방문"
    return "방문심사"


def parse_schedule_tab_rows(rows: list) -> list:
    if not rows or len(rows) < 2:
        return []

    hdr0_ns = [re.sub(r"\s+", "", str(c)) for c in rows[0]]
    if "심사명" in hdr0_ns or "고유ID" in hdr0_ns or ("구분" in hdr0_ns and any("일자" in x or "날짜" in x for x in hdr0_ns)):
        col_uid = next((i for i, h in enumerate(hdr0_ns) if "ID" in h.upper() or "고유" in h), -1)
        col_tp = next((i for i, h in enumerate(hdr0_ns) if "구분" in h or "분류" in h), -1)
        col_dt = next((i for i, h in enumerate(hdr0_ns) if "일자" in h or "날짜" in h or "일시" in h), -1)
        col_nm = next((i for i, h in enumerate(hdr0_ns) if "심사명" in h or "업체" in h or "내용" in h or "제목" in h), -1)
        col_st = next((i for i, h in enumerate(hdr0_ns) if "진행" in h or "현황" in h or "상태" in h), -1)
        col_sc = next((i for i, h in enumerate(hdr0_ns) if "점수" in h), -1)
        col_mm = next((i for i, h in enumerate(hdr0_ns) if "비고" in h or "메모" in h), -1)
        col_ts = next((i for i, h in enumerate(hdr0_ns) if "수정" in h), -1)

        out = []
        for r_idx, r in enumerate(rows[1:], start=2):
            if not any(str(x).strip() for x in r):
                continue
            def g(idx: int) -> str:
                return str(r[idx]).strip() if (0 <= idx < len(r)) else ""

            nm_v = g(col_nm)
            dt_raw = g(col_dt)
            if not nm_v and not dt_raw:
                continue
            d_obj = parse_date_obj(dt_raw)
            dt_norm = d_obj.strftime("%Y-%m-%d") if d_obj else dt_raw
            tp_norm = normalize_audit_type(g(col_tp) or "방문심사")
            st_raw = g(col_st) or "심사준비중"
            st_norm = st_raw if st_raw in AUDIT_STATUS_LIST else "심사준비중"
            uid_v = g(col_uid) or hashlib.md5(f"{tp_norm}|{dt_norm}|{nm_v}|{r_idx}".encode("utf-8")).hexdigest()[:8]

            out.append({
                "고유ID": uid_v,
                "구분": tp_norm,
                "심사일자": dt_norm,
                "심사명": nm_v,
                "진행현황": st_norm,
                "점수": g(col_sc),
                "비고": g(col_mm),
                "수정일시": g(col_ts)
            })
        out.sort(key=lambda x: x.get("심사일자", ""))
        return out

    out_dual = []
    anchors = []
    for r_i, r in enumerate(rows[:10]):
        for c_i, cell in enumerate(r):
            cs = re.sub(r"\s+", "", str(cell))
            if "방문심사" in cs:
                anchors.append((r_i, c_i, "방문심사"))
            elif "협력업체방문" in cs or ("협력업체" in cs and "방문" in cs):
                anchors.append((r_i, c_i, "협력업체 방문"))

    if anchors:
        anchors.sort(key=lambda x: (x[0], x[1]))
        for ar, ac, atype in anchors:
            next_c = min([x[1] for x in anchors if abs(x[0] - ar) <= 2 and x[1] > ac] or [ac + 6])
            hdr_r = ar + 1
            c_dt, c_nm, c_st, c_mm = ac, ac + 1, -1, -1
            for rr in range(ar + 1, min(ar + 4, len(rows))):
                seg = [re.sub(r"\s+", "", str(x)) for x in rows[rr][ac : min(next_c, len(rows[rr]))]]
                if any(k in "".join(seg) for k in ["일자", "날짜", "심사", "업체"]):
                    hdr_r = rr
                    for off, h_t in enumerate(seg):
                        if "일자" in h_t or "날짜" in h_t:
                            c_dt = ac + off
                        elif "심사명" in h_t or "업체" in h_t or "내용" in h_t:
                            c_nm = ac + off
                        elif "진행" in h_t or "상태" in h_t or "현황" in h_t:
                            c_st = ac + off
                        elif "비고" in h_t or "메모" in h_t:
                            c_mm = ac + off
                    break

            for dr_i, dr in enumerate(rows[hdr_r + 1:], start=1):
                nm_v = str(dr[c_nm]).strip() if c_nm < len(dr) else ""
                dt_raw = str(dr[c_dt]).strip() if c_dt < len(dr) else ""
                if not nm_v:
                    continue
                d_obj = parse_date_obj(dt_raw)
                dt_norm = d_obj.strftime("%Y-%m-%d") if d_obj else dt_raw
                st_v = str(dr[c_st]).strip() if (0 <= c_st < len(dr)) else "심사준비중"
                if st_v not in AUDIT_STATUS_LIST:
                    st_v = "심사준비중"
                mm_v = str(dr[c_mm]).strip() if (0 <= c_mm < len(dr)) else ""
                uid_v = hashlib.md5(f"{atype}|{dt_norm}|{nm_v}|{dr_i}".encode("utf-8")).hexdigest()[:8]
                out_dual.append({
                    "고유ID": uid_v,
                    "구분": atype,
                    "심사일자": dt_norm,
                    "심사명": nm_v,
                    "진행현황": st_v,
                    "점수": "",
                    "비고": mm_v,
                    "수정일시": ""
                })
        out_dual.sort(key=lambda x: x.get("심사일자", ""))
    return out_dual


def fetch_sup_and_audit_kpi_sheet(sheet_urls_map: dict):
    default_sup_kpi = {
        "has_sheet": False,
        "target_rate": 88.3,
        "actual_rate": 92.10,
        "level_str": "Lev.1",
        "signal": "green",
        "criteria": KPI_LEVEL_CRITERIA["supplier_exec"],
        "weight_rows": [],
        "visit_rows": [],
        "visit_counts": {"전체": 0, "연세유업": 0, "연세생건": 0, "이롬": 0}
    }
    default_aud_kpi = {
        "has_sheet": False,
        "score_target": 94.28,
        "score_actual": 91.95,
        "score_level": "Lev.5",
        "imp_target": 94.5,
        "imp_actual": 94.85,
        "imp_level": "Lev.1",
        "imp_cnt_text": "",
        "signal": "red",
        "score_criteria": KPI_LEVEL_CRITERIA["audit_score"],
        "imp_criteria": KPI_LEVEL_CRITERIA["audit_imp"],
        "formula_rows": [],
        "history_rows": []
    }

    sh, err = open_sheet_isolated(sheet_urls_map, "sup_audit_kpi")
    if sh is None:
        return default_sup_kpi, default_aud_kpi, None, f"[협력업체, 심사 점수] {err}"

    try:
        ws_all = api_call_with_retry(sh.worksheets)
        ws_titles = [w.title for w in ws_all]
        t_sup = next((t for t in ws_titles if "협력업체" in t), ws_titles[0] if ws_titles else None)
        t_aud = next((t for t in ws_titles if "심사" in t and "일정" not in t), ws_titles[1] if len(ws_titles) > 1 else None)
        t_sched = next((t for t in ws_titles if "일정" in t), None)

        batch_map = batch_fetch_tabs(sh, [t_sup, t_aud] + ([t_sched] if t_sched else []), "FORMATTED_VALUE")

        sup_kpi = dict(default_sup_kpi)
        s_rows = batch_map.get(t_sup, []) if t_sup else []
        if s_rows:
            sup_kpi["has_sheet"] = True
            weight_rows = []
            visit_rows = []
            sum_hdr_idx, detail_hdr_idx = -1, -1

            for r_i, r in enumerate(s_rows):
                r_clean = [str(c).strip() for c in r]
                r_ns = [re.sub(r"\s+", "", x) for x in r_clean]
                row_join = " ".join(r_clean)

                if r_i <= 3 and "AUDIT" in row_join.upper():
                    m_t = re.search(r"(\d{2,3}(?:\.\d+)?)\s*%", row_join)
                    if m_t:
                        sup_kpi["target_rate"] = float(m_t.group(1))

                if "현재점수" in "".join(r_ns) or ("Summary" in row_join and "%" in row_join):
                    m_a = re.search(r"(\d{2,3}(?:\.\d+)?)\s*%", row_join)
                    if m_a:
                        sup_kpi["actual_rate"] = float(m_a.group(1))

                if sum_hdr_idx < 0 and len(r_ns) >= 5 and r_ns[0] == "구분" and any("목표" in x for x in r_ns[:3]) and any("점검" in x for x in r_ns[:4]):
                    sum_hdr_idx = r_i

                if detail_hdr_idx < 0 and r_ns.count("점검일") >= 1 and r_ns.count("업체명") >= 1:
                    detail_hdr_idx = r_i

            if sum_hdr_idx >= 0:
                for sr in s_rows[sum_hdr_idx + 1 : min(sum_hdr_idx + 8, len(s_rows))]:
                    sr_pad = [str(x).strip() for x in sr] + [""] * max(0, 7 - len(sr))
                    g_nm = sr_pad[0]
                    if not g_nm or "세부내역" in g_nm or g_nm.startswith("2."):
                        break
                    if g_nm == "합계" and sr_pad[4]:
                        v_tot = to_float(sr_pad[4], 0.0)
                        if v_tot > 0:
                            sup_kpi["actual_rate"] = v_tot
                    weight_rows.append({
                        "구분": g_nm,
                        "연간 목표건수": sr_pad[1] or "-",
                        "점검수": sr_pad[2] or "-",
                        "실행율": sr_pad[3] or "-",
                        "평가 점수": sr_pad[4] or "-",
                        "적용 비율": sr_pad[5] or "-",
                        "비고": sr_pad[6] or "-"
                    })

            if detail_hdr_idx >= 0:
                hdr_ns = [re.sub(r"\s+", "", str(x)) for x in s_rows[detail_hdr_idx]]
                above_row = [str(x).strip() for x in s_rows[detail_hdr_idx - 1]] if detail_hdr_idx >= 1 else []
                dt_cols = [ci for ci, h in enumerate(hdr_ns) if h in ["점검일", "일자", "날짜"]]
                default_aff_names = ["연세유업", "연세생건", "이롬"]

                for b_idx, c_start in enumerate(dt_cols):
                    aff_label = default_aff_names[b_idx] if b_idx < len(default_aff_names) else f"그룹{b_idx+1}"
                    look_above = " ".join(above_row[c_start : min(c_start + 3, len(above_row))])
                    if "연세유업" in look_above or "유업" in look_above:
                        aff_label = "연세유업"
                    elif "생건" in look_above or "생활건강" in look_above:
                        aff_label = "연세생건"
                    elif "이롬" in look_above:
                        aff_label = "이롬"

                    for dr in s_rows[detail_hdr_idx + 1:]:
                        dt_v = str(dr[c_start]).strip() if c_start < len(dr) else ""
                        comp_v = str(dr[c_start + 1]).strip() if (c_start + 1) < len(dr) else ""
                        note_v = str(dr[c_start + 2]).strip() if (c_start + 2) < len(dr) else ""
                        if not comp_v or comp_v in ["-", "업체명", "합계"]:
                            continue
                        visit_rows.append({
                            "계열사 구분": aff_label,
                            "점검일": dt_v or "-",
                            "업체명": comp_v,
                            "비고": note_v or "-"
                        })

            sup_kpi["level_str"] = eval_kpi_level(sup_kpi["actual_rate"], "supplier_exec")
            sup_kpi["weight_rows"] = weight_rows
            sup_kpi["visit_rows"] = visit_rows
            sup_kpi["visit_counts"] = {
                "전체": len(visit_rows),
                "연세유업": sum(1 for r in visit_rows if r["계열사 구분"] == "연세유업"),
                "연세생건": sum(1 for r in visit_rows if r["계열사 구분"] == "연세생건"),
                "이롬": sum(1 for r in visit_rows if r["계열사 구분"] == "이롬")
            }
            sup_kpi["signal"] = "green" if sup_kpi["actual_rate"] >= sup_kpi["target_rate"] else ("yellow" if sup_kpi["actual_rate"] >= 85.3 else "red")

        aud_kpi = dict(default_aud_kpi)
        a_rows = batch_map.get(t_aud, []) if t_aud else []
        if a_rows:
            aud_kpi["has_sheet"] = True
            formula_rows = []
            history_rows = []
            hist_hdr_idx = -1

            for r_i, r in enumerate(a_rows):
                r_pad = [str(c).strip() for c in r] + [""] * max(0, 11 - len(r))
                r_ns = [re.sub(r"\s+", "", x) for x in r_pad]

                if r_i <= 4:
                    for cell_s in r_pad[:5]:
                        m_imp_t = re.search(r"(\d{2,3}(?:\.\d+)?)\s*%\s*↑", cell_s)
                        if m_imp_t:
                            aud_kpi["imp_target"] = float(m_imp_t.group(1))
                    for cell_s in r_pad[5:10]:
                        m_sc_t = re.search(r"(\d{2,3}(?:\.\d+)?)\s*점\s*↑", cell_s)
                        if m_sc_t:
                            aud_kpi["score_target"] = float(m_sc_t.group(1))

                if r_ns[0] == "심사개선율" and r_i < 12:
                    v_imp = to_float(r_pad[2], 0.0)
                    if v_imp > 0:
                        aud_kpi["imp_actual"] = round(v_imp, 2)
                elif r_ns[0] == "심사점수" and r_i < 12:
                    v_sc = to_float(r_pad[2], 0.0)
                    if v_sc > 0:
                        aud_kpi["score_actual"] = round(v_sc, 2)

                if r_i < 12 and r_ns[5] in ["점수평균", "통과율"]:
                    calc_num = to_float(r_pad[8], 0.0)
                    formula_rows.append({
                        "구분": r_pad[5],
                        "기준수치": f"{r_pad[6]}점" if r_ns[5] == "점수평균" else f"대상 {r_pad[6]}건",
                        "적용배율": f"{int(round(to_float(r_pad[7], 0) * 100))}%" if to_float(r_pad[7], 0) <= 1.0 else r_pad[7],
                        "환산점수": f"{calc_num:.2f}점" if calc_num > 0 else r_pad[8]
                    })
                elif r_i < 12 and r_ns[5] == "" and r_pad[6].isdigit() and formula_rows and formula_rows[-1]["구분"] == "통과율":
                    formula_rows[-1]["기준수치"] = f"통과 {r_pad[6]}건 / {formula_rows[-1]['기준수치']}"

                if hist_hdr_idx < 0 and r_ns[0] in ["심사일", "일자"] and ("심사업체" in r_ns[1] or "업체" in r_ns[1] or "기관" in r_ns[1]):
                    hist_hdr_idx = r_i

            tot_imp_sum, tot_iss_sum = 0, 0
            if hist_hdr_idx >= 0:
                for dr in a_rows[hist_hdr_idx + 1:]:
                    dr_pad = [str(x).strip() for x in dr] + [""] * max(0, 10 - len(dr))
                    dt_v, org_v = dr_pad[0], dr_pad[1]
                    if not dt_v and not org_v:
                        continue
                    if any(k in dt_v for k in ["합계", "평균", "총계"]) or any(k in org_v for k in ["합계", "평균", "총계"]):
                        continue

                    imp_c, iss_c, imp_r, sc_v, pass_v = dr_pad[2], dr_pad[3], dr_pad[4], dr_pad[5], dr_pad[6]
                    col_h_type = dr_pad[7]
                    col_i_open = " ".join([x for x in dr_pad[8:11] if x]).strip()

                    if imp_c.isdigit() and iss_c.isdigit():
                        tot_imp_sum += int(imp_c)
                        tot_iss_sum += int(iss_c)

                    history_rows.append({
                        "심사일": dt_v or "-",
                        "심사업체": org_v or "-",
                        "개선건수": imp_c or "-",
                        "지적건수": iss_c or "-",
                        "개선율(%)": f"{imp_r}%" if (imp_r and imp_r != "-" and "%" not in imp_r) else (imp_r or "-"),
                        "점수": sc_v or "-",
                        "통과여부": pass_v or "-",
                        "평가유형(H열)": col_h_type or "-",
                        "미개선 · 특이사항(I열)": col_i_open or "-"
                    })

            if tot_iss_sum > 0:
                aud_kpi["imp_cnt_text"] = f"개선 {tot_imp_sum}건 / 지적 {tot_iss_sum}건"
            aud_kpi["score_level"] = eval_kpi_level(aud_kpi["score_actual"], "audit_score")
            aud_kpi["imp_level"] = eval_kpi_level(aud_kpi["imp_actual"], "audit_imp")
            aud_kpi["formula_rows"] = formula_rows
            aud_kpi["history_rows"] = history_rows
            aud_kpi["signal"] = "green" if aud_kpi["score_actual"] >= aud_kpi["score_target"] else ("yellow" if aud_kpi["score_actual"] >= 92.78 else "red")

        sched_from_sak = parse_schedule_tab_rows(batch_map.get(t_sched, [])) if t_sched is not None else None
        return sup_kpi, aud_kpi, sched_from_sak, None
    except Exception as e:
        return default_sup_kpi, default_aud_kpi, None, f"[협력업체, 심사 점수] {e}"


# ==============================================================================
# 6. [5] '법규정보' 시트 (원본 탭 + '대시보드' 탭 양방향 연동 & 핵심 내용만 도출)
# ==============================================================================
def make_law_id(tab_name: str, law_name: str, enf_raw: str, notice_raw: str) -> str:
    raw_key = f"{tab_name.strip()}|{re.sub(r'\\s+', '', law_name)}|{re.sub(r'\\s+', '', enf_raw)}|{re.sub(r'\\s+', '', notice_raw)}"
    return hashlib.md5(raw_key.encode("utf-8")).hexdigest()[:10]


def extract_pure_law_substance(law_name: str, content_str: str) -> str:
    """
    '예고 확정대기', '개정고시 반영', '가. 건기식...' 같은 상투어를 모두 제거하고
    실제 어떤 물질·품목·기준이 바뀌는지 구체적 알맹이만 1줄로 추출
    """
    raw = str(content_str or "").strip()
    t = str(law_name or "").strip()
    if not raw or raw == "-":
        return f"{t} 개정 세부기준 확인"

    lines = [re.sub(r"\s+", " ", ln).strip() for ln in raw.splitlines() if ln.strip()]
    cleaned_lines = []
    for ln in lines:
        if re.match(r"^[\[【\(]?\s*(?:개정이유|주요내용|개정내용|추진배경|부칙|시행일|의견제출)\s*[\]】\)]?\s*[:\-]?$", ln):
            continue
        ln = re.sub(r"^(?:[가나다라마바사아자차카타파하]\.|[0-9]+\)|[0-9]+\.|[①②③④⑤⑥⑦⑧⑨⑩]|[-•▪▫▶▸ㅇ*])\s*", "", ln)
        ln = re.sub(r"\(\s*안\s*제[^)]+\)", "", ln)
        ln = re.sub(r"\(\s*제\d+조[^)]*\)", "", ln)
        ln = ln.strip(" :-")
        if len(ln) >= 4:
            cleaned_lines.append(ln)

    full_clean = " / ".join(cleaned_lines) if cleaned_lines else re.sub(r"\s+", " ", raw)

    # 알레르기 유발물질 구체 항목이 원문에 있으면 물질명까지 정확히 표출
    if "알레르기" in full_clean or "알레르기" in t:
        items_found = [x for x in ["캐슈넛", "피스타치오", "참깨", "아몬드", "헤이즐넛", "피칸", "브라질넛", "마카다미아", "호밀", "보리", "귀리", "연체동물", "갑각류"] if x in full_clean]
        if items_found:
            return f"알레르기 유발물질 의무표시 대상 확대 ({'·'.join(items_found)} 신규 추가)"
        for cl in cleaned_lines:
            if "알레르기" in cl or "표시대상" in cl:
                return cl[:74]
        return "알레르기 유발물질 의무표시 대상 품목 및 표시방법 개정"

    if "유전자변형" in full_clean or "GMO" in full_clean.upper() or "유전자변형" in t:
        if "완전표시" in full_clean or "비유전자" in full_clean or "Non-GMO" in full_clean:
            return "유전자변형식품(GMO) 표시대상 구체화 및 비유전자변형(Non-GMO) 표시요건 개정"
        return cleaned_lines[0][:74] if cleaned_lines else "유전자변형식품(GMO) 표시대상 원료 및 표시기준 개정"

    # 원문의 첫 1~2개 실질 조항을 자연스럽게 연결 (기계적 접두어 완전 배제)
    picked = []
    for cl in cleaned_lines:
        if any(skip in cl for skip in ["개정이유", "주요내용", "일부개정", "의견이 있는 기관"]):
            continue
        picked.append(cl)
        if len(" · ".join(picked)) >= 52 or len(picked) >= 2:
            break

    res = " · ".join(picked) if picked else full_clean
    return (res[:76] + "…") if len(res) > 77 else res


def compute_obsolescence_prep_tag(d_day: Optional[int], is_notice: bool, prep_status: str) -> Tuple[str, str]:
    """불용자재 방지 1년(D-365) 리드타임 기준 준비 시점 배지 반환"""
    if prep_status == "반영완료":
        return "조치완료", "badge-ok"
    if is_notice:
        if d_day is not None and 0 <= d_day <= 365:
            return "최종고시 대기(1년내)", "badge-purple"
        return "최종고시 대기", "badge-purple"
    if d_day is None:
        return "시행일 미정", "badge-neutral"
    if d_day < 0:
        return "시행중(상시준수)", "badge-ok"
    if d_day <= 90:
        return "즉시 도안반영(3개월내)", "badge-err"
    if d_day <= 365:
        return "불용방지 준비구간(1년내)", "badge-warn"
    months_left_to_prep = max(1, round((d_day - 365) / 30))
    return f"여유 ({months_left_to_prep}개월 뒤 착수)", "badge-neutral"


def parse_law_dashboard_tab(rows: list) -> Tuple[bool, Dict[str, dict]]:
    """
    '법규정보' 시트 내 '대시보드' 탭을 파싱하여 (초기화완료여부, {law_id_or_title: 설정객체}) 반환
    - 탭이 완전히 비어 있으면 initialized=False 반환 -> 최초 1회 기본 추천 목록 자동 기록
    - 헤더(1행)만 있고 데이터 행이 없으면 사용자가 전부 체크 해제한 상태(initialized=True, {})로 존중
    """
    if not rows:
        return False, {}
    hdr_ns = [re.sub(r"\s+", "", str(x)) for x in rows[0]]
    has_valid_hdr = any(k in "".join(hdr_ns) for k in ["법규ID", "법규명", "시행일", "준비상태"])
    if not has_valid_hdr:
        return False, {}

    c_id = next((i for i, h in enumerate(hdr_ns) if "ID" in h.upper()), 0)
    c_nm = next((i for i, h in enumerate(hdr_ns) if "법규명" in h or "법령명" in h), 3)
    c_prep = next((i for i, h in enumerate(hdr_ns) if "준비" in h or "상태" in h), 4)
    c_sum = next((i for i, h in enumerate(hdr_ns) if "요약" in h or "내용" in h), 5)

    pinned_map: Dict[str, dict] = {}
    for r in rows[1:]:
        if not any(str(x).strip() for x in r):
            continue
        lid = str(r[c_id]).strip() if c_id < len(r) else ""
        nm = str(r[c_nm]).strip() if c_nm < len(r) else ""
        prep = str(r[c_prep]).strip() if c_prep < len(r) else "검토전"
        if prep not in LAW_PREP_LIST:
            prep = "검토전"
        summ = str(r[c_sum]).strip() if c_sum < len(r) else ""
        key = lid or re.sub(r"\s+", "", nm)
        if key:
            pinned_map[key] = {"prep": prep, "summary": summ, "law_name": nm}
    return True, pinned_map


def sync_law_dashboard_worksheet(sh, all_laws_enriched: list):
    """체크된 대시보드 노출 법규 목록을 '법규정보' 시트의 [대시보드] 탭에 즉시 기록(체크 해제분은 자동 삭제)"""
    ws_all = api_call_with_retry(sh.worksheets)
    ws_dash = next((w for w in ws_all if w.title.strip() == LAW_DASH_TAB), None)
    if ws_dash is None:
        ws_dash = api_call_with_retry(sh.add_worksheet, title=LAW_DASH_TAB, rows=100, cols=10)

    now_s = now_kst().strftime("%Y-%m-%d %H:%M:%S")
    dash_items = [x for x in all_laws_enriched if x.get("show_on_dashboard")]
    dash_items.sort(key=lambda x: x.get("_sort_k", 99999))

    payload = [LAW_DASH_COLS]
    for it in dash_items:
        payload.append([
            str(it.get("law_id", "")),
            str(it.get("법규단계", "고시")),
            str(it.get("시행일", "-")),
            str(it.get("법규명", "")),
            str(it.get("prep_status", "검토전")),
            str(it.get("실무조치요약", "")),
            str(it.get("당사관련부서", "-")),
            now_s
        ])
    api_call_with_retry(ws_dash.clear)
    safe_ws_update(ws_dash, payload)


def fetch_law_info_sheet(sheet_urls_map: dict):
    sh, err = open_sheet_isolated(sheet_urls_map, "law_info")
    if sh is None:
        return [], False, {}, f"[법규정보] {err}"

    try:
        ws_all = api_call_with_retry(sh.worksheets)
        ws_titles = [w.title for w in ws_all]
        t_dash = next((t for t in ws_titles if t.strip() == LAW_DASH_TAB), None)
        target_tabs = [t for t in ws_titles if t != t_dash and any(yr in t for yr in ["2026", "2025", "2027"])]
        if not target_tabs:
            target_tabs = [t for t in ws_titles if t != t_dash][:4]

        batch_map = batch_fetch_tabs(sh, target_tabs + ([t_dash] if t_dash else []), "FORMATTED_VALUE")
        dash_initialized, dash_tab_map = parse_law_dashboard_tab(batch_map.get(t_dash, [])) if t_dash else (False, {})

        today_d = now_kst().date()
        all_laws = []

        for t_name in target_tabs:
            rows = batch_map.get(t_name, [])
            if len(rows) < 3:
                continue
            tab_has_notice = "예고" in t_name

            hdr_idx = -1
            col_ministry, col_law, col_content, col_notice_dt, col_enf_dt, col_rel = -1, -1, -1, -1, -1, -1
            for r_i, r in enumerate(rows[:12]):
                r_ns = [re.sub(r"\s+", "", str(x)) for x in r]
                if any("법규명" in x or "법령명" in x for x in r_ns) and any("내용" in x or "시행" in x or "예고" in x for x in r_ns):
                    hdr_idx = r_i
                    for c_i, cell_txt in enumerate(r_ns):
                        if ("부처" in cell_txt or "기관" in cell_txt) and col_ministry < 0:
                            col_ministry = c_i
                        elif ("법규명" in cell_txt or "법령명" in cell_txt or "고시명" in cell_txt) and col_law < 0:
                            col_law = c_i
                        elif "내용" in cell_txt and col_content < 0:
                            col_content = c_i
                        elif any(k in cell_txt for k in ["고시일", "공포일", "예고일", "입법예고"]) and col_notice_dt < 0:
                            col_notice_dt = c_i
                        elif ("시행일" in cell_txt or "시행예정" in cell_txt or "의견제출" in cell_txt) and col_enf_dt < 0:
                            col_enf_dt = c_i
                        elif ("관련" in cell_txt or "당사" in cell_txt or "부서" in cell_txt or "비고" in cell_txt) and col_rel < 0:
                            col_rel = c_i
                    break

            if hdr_idx < 0 or col_law < 0:
                continue

            c_law_end = col_content if col_content > col_law else col_law + 1
            next_after_content = min([c for c in [col_notice_dt, col_enf_dt, col_rel] if c > col_content] or [col_content + 1])

            last_ministry = ""
            for dr in rows[hdr_idx + 1:]:
                if not any(str(x).strip() for x in dr):
                    continue
                min_v = str(dr[col_ministry]).strip() if (0 <= col_ministry < len(dr)) else ""
                if min_v:
                    last_ministry = min_v

                law_parts = [str(x).strip() for x in dr[col_law : min(c_law_end, len(dr))] if str(x).strip()]
                law_nm = re.sub(r"\s+", " ", " ".join(dict.fromkeys(law_parts))).strip()
                if not law_nm or law_nm in ["법규명", "법령명"]:
                    continue

                cnt_parts = [str(x).strip() for x in dr[col_content : min(next_after_content, len(dr))] if str(x).strip()] if col_content >= 0 else []
                content_full = "\n".join(dict.fromkeys(cnt_parts)).strip()

                notice_raw = str(dr[col_notice_dt]).strip() if (0 <= col_notice_dt < len(dr)) else ""
                enf_raw = str(dr[col_enf_dt]).strip() if (0 <= col_enf_dt < len(dr)) else ""
                rel_raw = " ".join([str(x).strip() for x in dr[col_rel:] if str(x).strip()]) if (0 <= col_rel < len(dr)) else ""

                is_notice_item = tab_has_notice or any(k in law_nm for k in ["예고", "개정안", "개정령(안)", "고시(안)"])
                stage_type = "예고" if is_notice_item else "고시"

                enf_d = parse_date_obj(enf_raw)
                d_day = (enf_d - today_d).days if enf_d else None
                enf_iso = enf_d.strftime("%Y-%m-%d") if enf_d else (enf_raw or "미정")

                is_qs_dept = ("품질안전" in rel_raw) or ("식품안전" in rel_raw) or ("품질관리" in rel_raw) or ("전부문" in rel_raw) or ("전사" in rel_raw)
                auto_default_show = (not tab_has_notice) and is_qs_dept and (d_day is not None and d_day >= -14)

                lid = make_law_id(t_name, law_nm, enf_raw, notice_raw)
                pure_sub = extract_pure_law_substance(law_nm, content_full)

                all_laws.append({
                    "law_id": lid,
                    "탭구분": t_name.strip(),
                    "법규단계": stage_type,
                    "예고여부": is_notice_item,
                    "주무부처": last_ministry or "-",
                    "법규명": law_nm,
                    "자동조치요약": pure_sub,
                    "원문내용": content_full or "-",
                    "고시_예고일": notice_raw or "-",
                    "시행일": enf_iso,
                    "시행일_원문": enf_raw or "-",
                    "d_day": d_day,
                    "당사관련부서": rel_raw or "-",
                    "품질안전해당": is_qs_dept,
                    "auto_default": auto_default_show
                })

        # 만약 '대시보드' 탭이 만들어져 있으나 아직 헤더조차 없는 빈 시트라면 최초 1회 자동 세팅 기록
        if t_dash and not dash_initialized and all_laws:
            init_bundle = apply_law_preferences(all_laws, False, {}, {})
            try:
                sync_law_dashboard_worksheet(sh, init_bundle["all_laws"])
                dash_initialized = True
                dash_tab_map = {
                    x["law_id"]: {"prep": x["prep_status"], "summary": x["실무조치요약"], "law_name": x["법규명"]}
                    for x in init_bundle["dashboard_laws"]
                }
            except Exception:
                pass

        return all_laws, dash_initialized, dash_tab_map, None
    except Exception as e:
        return [], False, {}, f"[법규정보] {e}"


def apply_law_preferences(raw_laws: list, dash_initialized: bool, dash_tab_map: dict, legacy_prefs: dict) -> dict:
    enriched = []
    for item in raw_laws:
        lid = item["law_id"]
        norm_title = re.sub(r"\s+", "", item["법규명"])
        dash_entry = dash_tab_map.get(lid) or dash_tab_map.get(norm_title)
        leg_entry = legacy_prefs.get(lid, {})

        if dash_initialized:
            show_dash = dash_entry is not None
            prep = (dash_entry.get("prep") if dash_entry else None) or leg_entry.get("prep", "검토전")
            custom_sum = (dash_entry.get("summary") if dash_entry else "") or leg_entry.get("summary", "")
        else:
            mode = leg_entry.get("mode", "auto")
            show_dash = True if mode == "Pin" else (False if mode == "Hide" else bool(item.get("auto_default", False)))
            prep = leg_entry.get("prep", "검토전")
            custom_sum = leg_entry.get("summary", "")

        if prep not in LAW_PREP_LIST:
            prep = "검토전"

        # 이전에 저장된 요약문에 '[예고·확정대기]' 등 구버전 접두어가 남아 있으면 자동 정제
        clean_custom = re.sub(r"^\[예고·확정대기\]\s*", "", str(custom_sum or "").strip())
        clean_custom = re.sub(r"^(?:개정고시\s*반영|예고\s*단계\([^)]*\)|고시\s*시행\s*대비[^—\-]*)\s*[—\-]\s*", "", clean_custom).strip()

        d_val = item.get("d_day")
        if d_val is None:
            d_label, d_cls, sort_key = "일정미정", "badge-neutral", 99999
        elif d_val == 0:
            d_label, d_cls, sort_key = "D-Day", "badge-err", 0
        elif d_val > 0:
            d_label = f"D-{d_val}"
            d_cls = "badge-err" if d_val <= 90 else ("badge-warn" if d_val <= 365 else "badge-info")
            sort_key = d_val
        else:
            d_label, d_cls, sort_key = f"시행+{abs(d_val)}일", "badge-ok", 50000 + abs(d_val)

        prep_tag_txt, prep_tag_cls = compute_obsolescence_prep_tag(d_val, bool(item.get("예고여부")), prep)

        cp = dict(item)
        cp["prep_status"] = prep
        cp["prep_timing_tag"] = prep_tag_txt
        cp["prep_timing_cls"] = prep_tag_cls
        cp["custom_summary"] = clean_custom
        cp["실무조치요약"] = clean_custom if clean_custom else item["자동조치요약"]
        cp["show_on_dashboard"] = show_dash
        cp["d_day_label"] = d_label
        cp["d_day_cls"] = d_cls
        cp["_sort_k"] = sort_key
        enriched.append(cp)

    dashboard_list = [x for x in enriched if x["show_on_dashboard"]]
    dashboard_list.sort(key=lambda x: x["_sort_k"])

    all_sorted = sorted(enriched, key=lambda x: (0 if x["show_on_dashboard"] else 1, x["_sort_k"]))
    return {
        "dashboard_laws": dashboard_list,
        "all_laws": all_sorted,
        "active_cnt": len(dashboard_list),
        "prep_1yr_cnt": sum(1 for x in dashboard_list if x["d_day"] is not None and 0 <= x["d_day"] <= 365 and x["prep_status"] != "반영완료")
    }


# ==============================================================================
# 7. [6] '클레임, 부적합 이력' 시트 & [7] 보조 설정 DB 파서
# ==============================================================================
def fetch_claim_nc_history_sheet(sheet_urls_map: dict):
    sh, err = open_sheet_isolated(sheet_urls_map, "claim_nc_hist")
    empty_res = {
        "active_items": [],
        "all_items": [],
        "claim_rows": [],
        "nc_rows": [],
        "by_cause": [],
        "by_status": [],
        "counts": {"total": 0, "claim": 0, "nc": 0, "active": 0, "major": 0}
    }
    if sh is None:
        return empty_res, f"[클레임, 부적합 이력] {err}"

    try:
        ws_all = api_call_with_retry(sh.worksheets)
        ws_titles = [w.title for w in ws_all]
        t_clm = next((t for t in ws_titles if "클레임" in t), None)
        t_nc = next((t for t in ws_titles if "부적합" in t), None)

        batch_map = batch_fetch_tabs(sh, [t_clm, t_nc], "FORMATTED_VALUE")

        def parse_tab_items(rows: list, category_label: str) -> list:
            if not rows or len(rows) < 2:
                return []
            hdr = [str(c).strip() for c in rows[0]]
            out = []
            for r in rows[1:]:
                if not any(str(x).strip() for x in r):
                    continue
                r_pad = [str(x).strip() for x in r] + [""] * max(0, len(hdr) - len(r))
                d = dict(zip(hdr, r_pad[:len(hdr)]))
                st = d.get("진행상태", "검사중")
                if st not in CLAIM_HIST_STATUS_LIST:
                    st = "검사중"
                show_raw = str(d.get("대시보드노출", "Y")).strip().upper()
                show_flag = show_raw not in ["N", "FALSE", "0", "제외", "숨김"]
                out.append({
                    "고유ID": d.get("고유ID", "") or str(uuid.uuid4())[:8],
                    "구분": category_label,
                    "등록일": d.get("등록일", ""),
                    "중요도": d.get("중요도", "일반") or "일반",
                    "제품명": d.get("제품명", ""),
                    "소비기한": d.get("소비기한", ""),
                    "발생공정_원인유형": d.get("발생공정_원인유형", "기타") or "기타",
                    "상세사유": d.get("상세사유", ""),
                    "추가검사_조치내역": d.get("추가검사_조치내역", ""),
                    "진행상태": st,
                    "최종판정": d.get("최종판정", "판정대기") or "판정대기",
                    "대시보드노출": "Y" if show_flag else "N",
                    "수정일시": d.get("수정일시", "")
                })
            return out

        claim_rows = parse_tab_items(batch_map.get(t_clm, []), "클레임") if t_clm else []
        nc_rows = parse_tab_items(batch_map.get(t_nc, []), "부적합") if t_nc else []

        all_items = sorted(claim_rows + nc_rows, key=lambda x: x.get("등록일", ""), reverse=True)
        active_items = [x for x in all_items if x["대시보드노출"] == "Y"]
        active_items.sort(key=lambda x: (0 if x["중요도"] == "중점관리" else 1, 1 if x["진행상태"] == "적합(종결)" else 0))

        cause_map: Dict[str, int] = {}
        status_map: Dict[str, int] = {k: 0 for k in CLAIM_HIST_STATUS_LIST}
        for it in all_items:
            c_key = it["발생공정_원인유형"] or "기타"
            cause_map[c_key] = cause_map.get(c_key, 0) + 1
            st_key = it["진행상태"]
            status_map[st_key] = status_map.get(st_key, 0) + 1

        by_cause = [{"원인유형": k, "건수": v} for k, v in sorted(cause_map.items(), key=lambda x: x[1], reverse=True)]
        by_status = [{"상태": k, "건수": status_map.get(k, 0)} for k in CLAIM_HIST_STATUS_LIST]

        return {
            "active_items": active_items,
            "all_items": all_items,
            "claim_rows": claim_rows,
            "nc_rows": nc_rows,
            "by_cause": by_cause,
            "by_status": by_status,
            "counts": {
                "total": len(all_items),
                "claim": len(claim_rows),
                "nc": len(nc_rows),
                "active": len(active_items),
                "major": sum(1 for x in all_items if x["중요도"] == "중점관리")
            }
        }, None
    except Exception as e:
        return empty_res, f"[클레임, 부적합 이력] {e}"


def fetch_audit_and_manual_db(sheet_urls_map: dict):
    sh, err = open_sheet_isolated(sheet_urls_map, "audit_db")
    if sh is None:
        sh, err = open_sheet_isolated(sheet_urls_map, "sup_audit_kpi")
        if sh is None:
            return [], {}, None, None, {}, [], err

    try:
        ws_list = api_call_with_retry(sh.worksheets)
        ws_titles = [w.title for w in ws_list]
        t_sched_legacy = ws_titles[0] if ws_titles else None
        t_manual = next((t for t in ws_titles if t.strip() == MANUAL_KPI_TAB), None)
        t_sp = next((t for t in ws_titles if t.strip() == SPECIAL_BOARD_TAB), None)

        batch_map = batch_fetch_tabs(sh, [t_sched_legacy, t_manual, t_sp])
        legacy_sched = parse_schedule_tab_rows(batch_map.get(t_sched_legacy, [])) if t_sched_legacy else []

        saved_manual = {}
        if t_manual:
            m_rows = batch_map.get(t_manual, [])
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
        loaded_micro = None
        loaded_law_prefs = {}
        loaded_label_pins = []
        if t_sp:
            sp_rows = batch_map.get(t_sp, [])
            if len(sp_rows) >= 2:
                for sr in sp_rows[1:]:
                    if len(sr) >= 2:
                        b_key = str(sr[0]).strip()
                        b_val = str(sr[1]).strip()
                        b_ts = str(sr[2]).strip() if len(sr) >= 3 else ""
                        if b_key == "label_memo" and b_val:
                            loaded_lbl_memo = b_val.strip()
                        elif b_key == "micro_status_json" and b_val:
                            try:
                                parsed_m = json.loads(b_val)
                                if isinstance(parsed_m, dict):
                                    if not parsed_m.get("updated_at") and b_ts:
                                        parsed_m["updated_at"] = b_ts
                                    loaded_micro = parsed_m
                            except Exception:
                                pass
                        elif b_key == "law_prefs_json" and b_val:
                            try:
                                parsed_lp = json.loads(b_val)
                                if isinstance(parsed_lp, dict):
                                    loaded_law_prefs = parsed_lp
                            except Exception:
                                pass
                        elif b_key == "label_pins_json" and b_val:
                            try:
                                parsed_pins = json.loads(b_val)
                                if isinstance(parsed_pins, list):
                                    loaded_label_pins = [str(x).strip() for x in parsed_pins if str(x).strip()]
                            except Exception:
                                pass

        return legacy_sched, saved_manual, loaded_lbl_memo, loaded_micro, loaded_law_prefs, loaded_label_pins, None
    except Exception as e:
        return [], {}, None, None, {}, [], f"[설정 DB] {e}"


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


def make_label_item_key(row: dict) -> str:
    doc_no = str(row.get("*docNo", "")).strip()
    if doc_no and doc_no != "-":
        return doc_no
    raw = f"{row.get('요청부서', '')}|{row.get('제목', '')}"
    return hashlib.md5(raw.encode("utf-8")).hexdigest()[:10]


def strip_duplicate_dept_prefix(title_str: str, dept_str: str) -> str:
    t = str(title_str or "").strip()
    d = re.sub(r"\s+", "", str(dept_str or ""))
    m = re.match(r"^\[([^\]]+)\]\s*(.+)$", t)
    if m:
        bracket_inner = re.sub(r"\s+", "", m.group(1))
        if d and (d in bracket_inner or bracket_inner in d):
            return m.group(2).strip()
    return t


def fetch_label_status_sheet(sheet_urls_map: dict):
    sh, err = open_sheet_isolated(sheet_urls_map, "label_status")
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


def fetch_raw_material_db_sheet(sheet_urls_map: dict):
    sh, err = open_sheet_isolated(sheet_urls_map, "raw_label_db")
    if sh is None:
        return [], 0, err
    submitted = []
    total_master_count = 0
    try:
        ws_list = api_call_with_retry(sh.worksheets)
        if ws_list:
            t_first = ws_list[0].title
            t_master = next((w.title for w in ws_list if w.title.strip() == "전체 원재료 명단"), None)
            batch_map = batch_fetch_tabs(sh, [t_first] + ([t_master] if t_master else []))
            submitted = rows_to_records(batch_map.get(t_first, []))
            if t_master:
                rows_m = batch_map.get(t_master, [])
                total_master_count = max(len([r for r in rows_m[1:] if any(str(x).strip() for x in r)]), 0)
    except Exception as e:
        return submitted, total_master_count, str(e)
    return submitted, total_master_count, None


# ==============================================================================
# 8. 심사 요약 · 표시사항 분석 · 통합 번들 엔진 (미생물 자동판정 및 전 품목 자동합산)
# ==============================================================================
def get_active_audit_summary(sched_list: list, today_str: str) -> dict:
    today_d = parse_date_obj(today_str) or now_kst().date()
    if not sched_list:
        return {
            "has_data": False,
            "upcoming_banner": "등록된 방문심사 · 협력업체 방문 일정이 없습니다",
            "upcoming_type": "",
            "has_visit_gauge": False,
            "visit_title": "등록된 방문심사 일정이 없습니다",
            "status": "심사준비중",
            "pct": 0,
            "color": "#9CA3AF",
            "active_uid": ""
        }

    enriched_all = []
    for r in sched_list:
        d_obj = parse_date_obj(r.get("심사일자", ""))
        diff_d = (d_obj - today_d).days if d_obj else None
        enriched_all.append((d_obj, diff_d, r))

    future_or_today_all = sorted(
        [x for x in enriched_all if x[0] is not None and x[1] >= 0 and x[2].get("진행현황") != "심사종료"],
        key=lambda x: x[1]
    )
    if not future_or_today_all:
        future_or_today_all = sorted([x for x in enriched_all if x[0] is not None and x[1] >= 0], key=lambda x: x[1])

    if future_or_today_all:
        _, diff_v, nr = future_or_today_all[0]
        d_tag = "금일(D-Day)" if diff_v == 0 else f"D-{diff_v}"
        dt_s = nr["심사일자"][5:] if len(nr["심사일자"]) >= 10 else nr["심사일자"]
        up_banner = f"[{nr.get('구분', '방문심사')} · {d_tag}] {nr['심사명']} ({dt_s})"
        up_type = nr.get("구분", "방문심사")
    else:
        nr = sorted(sched_list, key=lambda x: x.get("심사일자", ""), reverse=True)[0]
        dt_s = nr["심사일자"][5:] if len(nr["심사일자"]) >= 10 else nr["심사일자"]
        up_banner = f"[최근 일정 · {nr.get('구분', '방문심사')}] {nr['심사명']} ({dt_s})"
        up_type = nr.get("구분", "방문심사")

    visit_only = [x for x in enriched_all if normalize_audit_type(x[2].get("구분", "방문심사")) == "방문심사"]
    if not visit_only:
        return {
            "has_data": True,
            "upcoming_banner": up_banner,
            "upcoming_type": up_type,
            "has_visit_gauge": False,
            "visit_title": "현재 등록된 '방문심사' 일정이 없습니다",
            "status": "심사준비중",
            "pct": 0,
            "color": "#9CA3AF",
            "active_uid": ""
        }

    v_today = [x for x in visit_only if x[0] == today_d]
    v_inprog = [x for x in visit_only if x[2].get("진행현황") in ["심사중", "서류심사중", "현장심사중"]]
    v_future = sorted([x for x in visit_only if x[0] and x[0] > today_d], key=lambda x: x[0])

    if v_today:
        vr = v_today[0][2]
        v_prefix = f"금일 방문심사 ({vr['심사일자'][5:]})"
    elif v_inprog:
        vr = v_inprog[0][2]
        v_prefix = f"진행중 방문심사 ({vr['심사일자'][5:]})"
    elif v_future:
        _, dl, vr = v_future[0]
        v_prefix = f"차기 방문심사 D-{dl} ({vr['심사일자'][5:]})"
    else:
        vr = sorted([x[2] for x in visit_only], key=lambda x: x.get("심사일자", ""), reverse=True)[0]
        v_prefix = f"최근 방문심사 ({vr['심사일자'][5:]})"

    st_nm = vr["진행현황"] if vr["진행현황"] in AUDIT_STATUS_CONFIG else "심사준비중"
    cfg = AUDIT_STATUS_CONFIG[st_nm]
    return {
        "has_data": True,
        "upcoming_banner": up_banner,
        "upcoming_type": up_type,
        "has_visit_gauge": True,
        "visit_title": f"[{v_prefix}] {vr['심사명']}",
        "status": st_nm,
        "pct": cfg["pct"],
        "color": cfg["color"],
        "active_uid": vr["고유ID"]
    }


def build_label_analytics(lbl_rows: list, pinned_ids: List[str]) -> dict:
    reviewing_rows = [r for r in lbl_rows if r.get("분류") == "식품안전팀 검토중"]
    completed_rows = [r for r in lbl_rows if r.get("분류") == "검토완료(확인필)"]
    valid_rows = [r for r in lbl_rows if r.get("분류") != "제외(개시/준비·보류)"]
    broken_date_cnt = sum(1 for r in lbl_rows if r.get("등록연도") == "날짜미상")

    r_25 = [r for r in valid_rows if r.get("_yr_num") == 2025]
    r_26 = [r for r in valid_rows if r.get("_yr_num") == 2026]
    completed_26_cnt = sum(1 for r in completed_rows if r.get("_yr_num") == 2026)

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

    rev_dept_map: Dict[str, int] = {}
    for r in reviewing_rows:
        dp = (r.get("요청부서", "") or "기타").strip() or "기타"
        rev_dept_map[dp] = rev_dept_map.get(dp, 0) + 1
    rev_dept_breakdown = [{"dept": k, "cnt": v} for k, v in sorted(rev_dept_map.items(), key=lambda x: x[1], reverse=True)]

    if rev_dept_breakdown:
        top_parts = [f"{x['dept']} {x['cnt']}" for x in rev_dept_breakdown[:4]]
        rest_cnt = sum(x["cnt"] for x in rev_dept_breakdown[4:])
        if rest_cnt > 0:
            top_parts.append(f"외 {rest_cnt}")
        rev_summary_line = f"{' · '.join(top_parts)}"
    else:
        rev_summary_line = "대기 0건"

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

    sorted_sites = sorted(site_counts.items(), key=lambda x: x[1], reverse=True)
    top_sites = [{"생산처": k, "2026년_검토건수": v} for k, v in sorted_sites[:10]]
    site_chart_items = [{"label": k, "count": v} for k, v in sorted_sites[:5]]
    etc_cnt = sum(v for _, v in sorted_sites[5:])
    if etc_cnt > 0:
        site_chart_items.append({"label": "기타 생산처", "count": etc_cnt})

    top_pkgs = [{"포장재유형": k, "2026년_검토건수": v} for k, v in sorted(pkg_counts.items(), key=lambda x: x[1], reverse=True)[:10]]

    pin_set = set(pinned_ids or [])
    keep_cols = ["*docNo", "상태", "제목", "요청부서", "대분류(마케팅 카테고리)", "보관방법", "생산처", "검토 포장재 유형", "등록일자", "최종수정자", "분류", "등록연도", "등록월"]

    slim_reviewing = []
    for r in reviewing_rows:
        ikey = make_label_item_key(r)
        clean_t = strip_duplicate_dept_prefix(r.get("제목", ""), r.get("요청부서", ""))
        item_d = {k: r.get(k, "") for k in keep_cols}
        item_d["_item_key"] = ikey
        item_d["_clean_title"] = clean_t
        item_d["_pinned"] = (ikey in pin_set)
        slim_reviewing.append(item_d)

    pinned_only = [x for x in slim_reviewing if x["_pinned"]]
    featured_list = pinned_only if len(pinned_only) > 0 else slim_reviewing[:6]
    slim_all = [{k: r.get(k, "") for k in keep_cols} for r in lbl_rows]

    return {
        "reviewing_cnt": len(reviewing_rows),
        "completed_cnt": len(completed_rows),
        "completed_26_cnt": completed_26_cnt,
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
        "rev_dept_breakdown": rev_dept_breakdown,
        "rev_summary_line": rev_summary_line,
        "has_manual_pins": len(pinned_only) > 0,
        "pinned_ids": list(pin_set),
        "featured_reviewing": featured_list,
        "monthly_cmp": monthly_cmp,
        "cat_cmp": cat_cmp,
        "dept_cmp": dept_cmp,
        "top_sites": top_sites,
        "site_chart": site_chart_items,
        "top_pkgs": top_pkgs,
        "reviewing_list": slim_reviewing,
        "all_records": slim_all
    }


_BUNDLE_CACHE: Dict[str, Any] = {"ts": 0.0, "date_key": "", "data": None}
_LAST_GOOD_SHEETS: Dict[str, Any] = {}
_BUNDLE_LOCK = threading.Lock()
CACHE_TTL_SEC = 180.0


def resolve_with_last_good(sheet_key: str, result_tuple: tuple, err_idx: int) -> tuple:
    err_val = result_tuple[err_idx]
    if not err_val:
        _LAST_GOOD_SHEETS[sheet_key] = result_tuple
        return result_tuple
    if sheet_key in _LAST_GOOD_SHEETS:
        return _LAST_GOOD_SHEETS[sheet_key]
    return result_tuple


def build_full_dashboard_bundle(force_refresh: bool = False) -> dict:
    cur_kst = now_kst()
    today_str = cur_kst.strftime("%Y-%m-%d")
    yest_dt = cur_kst - timedelta(days=1)
    now_ts = time.time()

    if (
        not force_refresh
        and _BUNDLE_CACHE["data"] is not None
        and _BUNDLE_CACHE["date_key"] == today_str
        and (now_ts - _BUNDLE_CACHE["ts"]) < CACHE_TTL_SEC
    ):
        return _BUNDLE_CACHE["data"]

    with _BUNDLE_LOCK:
        if (
            not force_refresh
            and _BUNDLE_CACHE["data"] is not None
            and _BUNDLE_CACHE["date_key"] == today_str
            and (time.time() - _BUNDLE_CACHE["ts"]) < CACHE_TTL_SEC
        ):
            return _BUNDLE_CACHE["data"]

        test_creds, auth_err = create_thread_local_credentials()
        if test_creds is None:
            if _BUNDLE_CACHE["data"] is not None:
                return _BUNDLE_CACHE["data"]
            return {
                "ok": False,
                "loaded_at": cur_kst.strftime("%Y. %m. %d %H:%M:%S (KST)"),
                "loaded_date": today_str,
                "errors": [auth_err]
            }

        sheet_urls_map = get_sheet_urls_map()

        with ThreadPoolExecutor(max_workers=4) as ex:
            f_cost = ex.submit(fetch_cost_sheet, sheet_urls_map)
            f_claim = ex.submit(fetch_claim_sheet, sheet_urls_map)
            f_rt = ex.submit(fetch_process_rt_sheet, sheet_urls_map)
            f_milk = ex.submit(fetch_process_milk_sheet, sheet_urls_map)
            f_soy = ex.submit(fetch_process_soy_sheet, sheet_urls_map)
            f_micro = ex.submit(fetch_process_micro_sheet, sheet_urls_map)
            f_sup = ex.submit(fetch_supplier_sheet, sheet_urls_map)
            f_audit = ex.submit(fetch_audit_and_manual_db, sheet_urls_map)
            f_lbl = ex.submit(fetch_label_status_sheet, sheet_urls_map)
            f_raw = ex.submit(fetch_raw_material_db_sheet, sheet_urls_map)
            f_law = ex.submit(fetch_law_info_sheet, sheet_urls_map)
            f_sak = ex.submit(fetch_sup_and_audit_kpi_sheet, sheet_urls_map)
            f_cnh = ex.submit(fetch_claim_nc_history_sheet, sheet_urls_map)

            cost_rows, cost_targets, err_cost = resolve_with_last_good("cost", f_cost.result(), 2)
            claim_trend, claim_cat, err_claim = resolve_with_last_good("claim", f_claim.result(), 2)
            rt_today, rt_yest, rt_hp, rt_hc, fsk_prods, err_rt = resolve_with_last_good("rt", f_rt.result(), 5)
            milk_pack, ferm_pack, milk_times, ferm_times, milk_dt, ferm_dt, dy_hp, dy_hc, dairy_prods, err_milk = resolve_with_last_good("milk", f_milk.result(), 9)
            soy_def_tab, soy_map, soy_b, soy_h, soy_hist, err_soy = resolve_with_last_good("soy", f_soy.result(), 5)
            micro_data, err_micro = resolve_with_last_good("micro", f_micro.result(), 1)
            sup_m, sup_g, sup_uc, sup_ud, err_sup = resolve_with_last_good("sup", f_sup.result(), 4)
            legacy_sched, saved_manual_kpis, saved_lbl_memo, saved_micro, saved_law_prefs, saved_lbl_pins, err_audit = resolve_with_last_good("audit", f_audit.result(), 6)
            lbl_rows, err_lbl = resolve_with_last_good("lbl", f_lbl.result(), 1)
            raw_sub, raw_m_cnt, err_raw = resolve_with_last_good("raw", f_raw.result(), 2)
            raw_laws, dash_init_flag, dash_tab_map, err_law = resolve_with_last_good("law", f_law.result(), 3)
            sup_kpi_sheet, aud_kpi_sheet, sak_sched, err_sak = resolve_with_last_good("sak", f_sak.result(), 3)
            claim_nc_data, err_cnh = resolve_with_last_good("cnh", f_cnh.result(), 1)

        audit_sched = sak_sched if (sak_sched is not None and len(sak_sched) > 0) else (legacy_sched if sak_sched is None else [])
        if sak_sched is not None and len(sak_sched) == 0 and legacy_sched:
            audit_sched = legacy_sched

        # 전 제품명 자동 수집 (상온 식품안전나라 + 유제품 포장일보 + 미생물 일보 대표/주석 품목)
        master_prods = []
        seen_norm = set()

        def add_prod_unique(p_str: str):
            ps = re.sub(r"\(주\d+\)", "", str(p_str or "")).strip()
            if not ps or ps in ["-", "(비가동)", "(제품명 미기입)"]:
                return
            nk = clean_product_for_match(ps)
            if nk and len(nk) >= 2 and nk not in seen_norm:
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
        for mr in micro_data.get("hist_rows", []):
            add_prod_unique(mr.get("대표품목명", ""))
            for alias_part in re.split(r"[|,]", str(mr.get("연관품목_주석포함", ""))):
                add_prod_unique(alias_part)

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

        rt_t_lines = rt_today["lines_records"] if rt_today else []
        rt_running_hos = rt_today["running_hos"] if rt_today else []
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
        soy_done_cnt = max(0, soy_blocks_cnt - soy_uninspected_cnt)
        soy_contam_total = sum(int(to_float(r.get("오염수량", 0), 0)) for r in soy_b if bool(r.get("검사완료여부", False)))

        total_sup_cnt = len(sup_m)
        unsub_sup_cnt = sum(1 for r in sup_m if r.get("점수") == "미제출")
        sub_sup_cnt = total_sup_cnt - unsub_sup_cnt
        doc_exec_rate = round((sub_sup_cnt / total_sup_cnt) * 100, 1) if total_sup_cnt > 0 else 0.0
        ok_sup_cnt = sum(1 for r in sup_m if r.get("결과") == "승인")
        guide_sup_cnt = sum(1 for r in sup_m if r.get("결과") == "지도")
        out_sup_cnt = sum(1 for r in sup_m if r.get("결과") == "등급 외")

        final_sup_target = sup_kpi_sheet["target_rate"] if sup_kpi_sheet.get("has_sheet") else 88.3
        final_sup_rate = sup_kpi_sheet["actual_rate"] if sup_kpi_sheet.get("has_sheet") else doc_exec_rate
        final_sup_lv = sup_kpi_sheet.get("level_str", eval_kpi_level(final_sup_rate, "supplier_exec"))
        sig_supplier = "green" if final_sup_rate >= final_sup_target else ("yellow" if final_sup_rate >= 85.3 else "red")

        merged_manual = {k: {**v, "notes": list(v["notes"])} for k, v in DEFAULT_MANUAL_KPIS.items()}
        for mk_k, mk_val in saved_manual_kpis.items():
            if mk_k in merged_manual:
                merged_manual[mk_k].update(mk_val)

        if sup_kpi_sheet.get("has_sheet"):
            vc = sup_kpi_sheet.get("visit_counts", {})
            merged_manual["kpi_11"].update({
                "target_text": f"{final_sup_target}% 이상 (Lev.1)",
                "actual_text": f"{final_sup_rate:.2f}% ({final_sup_lv})",
                "compact_val": f"{final_sup_rate:.2f}%",
                "sub_badge": f"방문점검 {vc.get('전체', 0)}개사(유업{vc.get('연세유업',0)}·생건{vc.get('연세생건',0)}·이롬{vc.get('이롬',0)})",
                "target_num": final_sup_target,
                "actual_num": final_sup_rate,
                "signal": sig_supplier
            })

        if aud_kpi_sheet.get("has_sheet"):
            sc_t = aud_kpi_sheet["score_target"]
            sc_a = aud_kpi_sheet["score_actual"]
            sc_lv = aud_kpi_sheet["score_level"]
            merged_manual["kpi_5"].update({
                "target_text": f"{sc_t}점 이상 (Lev.1)",
                "actual_text": f"{sc_a}점 ({sc_lv})",
                "compact_val": f"{sc_a}점",
                "sub_badge": f"수검 {len(aud_kpi_sheet.get('history_rows', []))}건 · 클릭 시 심사개선율 상세",
                "target_num": sc_t,
                "actual_num": sc_a,
                "signal": aud_kpi_sheet["signal"]
            })

        label_memo_final = (saved_lbl_memo if saved_lbl_memo is not None else DEFAULT_LABEL_MEMO).strip()

        # 미생물 출고 판정: '미생물 일보' 시트가 있으면 시트 실시간 자동판정을 기본으로 하되, 수동 오버라이드도 지원
        raw_m_pref = saved_micro if isinstance(saved_micro, dict) else dict(DEFAULT_MICRO_STATUS)
        m_mode = raw_m_pref.get("mode", "자동연동")
        if micro_data.get("has_sheet") and m_mode != "수동지정":
            micro_final = {
                "mode": "자동연동",
                "status": micro_data["auto_status"],
                "note": micro_data["auto_note"],
                "updated_at": "시트 실시간 연동"
            }
        else:
            micro_final = {
                "mode": m_mode,
                "status": raw_m_pref.get("status", micro_data.get("auto_status", "출고적합")),
                "note": raw_m_pref.get("note") or micro_data.get("auto_note", "우유 · 발효유 · 두유 완제품 미생물 규격 적합"),
                "updated_at": raw_m_pref.get("updated_at", "")
            }

        law_bundle = apply_law_preferences(raw_laws, dash_init_flag, dash_tab_map or {}, saved_law_prefs or {})
        label_analytics = build_label_analytics(lbl_rows, saved_lbl_pins or [])

        sub_raw_cnt = len(raw_sub)
        verified_raw_cnt = sum(1 for r in raw_sub if re.search(r"완료|적합", str(r.get("검증상태", ""))))
        allergy_raw_cnt = sum(1 for r in raw_sub if str(r.get("신규알레르기", "")).strip() != "")

        dairy_hist_dates = sorted(list({r.get("생산일자", "") for r in (dy_hp + dy_hc) if r.get("생산일자")}), reverse=True)
        dairy_hist_times_map = {
            dt_k: extract_hist_dairy_run_times(dy_hc, dt_k, "ALL")
            for dt_k in dairy_hist_dates
        }

        errors = []
        for label_nm, er in [
            ("원가절감", err_cost), ("클레임", err_claim), ("협력업체서류", err_sup),
            ("표시사항 현황", err_lbl), ("원자재 DB", err_raw)
        ]:
            if er:
                errors.append(f"[{label_nm}] {er}")
        for p_er in [err_rt, err_milk, err_soy, err_micro, err_audit, err_law, err_sak, err_cnh]:
            if p_er:
                errors.append(p_er)

        done_kst = now_kst()
        weekdays_kr = ["월", "화", "수", "목", "금", "토", "일"]
        kst_korean_full = f"{done_kst.year}년 {done_kst.month}월 {done_kst.day}일 ({weekdays_kr[done_kst.weekday()]})"

        bundle = {
            "ok": True,
            "loaded_at": done_kst.strftime("%Y. %m. %d %H:%M:%S (KST)"),
            "loaded_time_short": done_kst.strftime("%H:%M"),
            "loaded_date": today_str,
            "kst_korean_date": kst_korean_full,
            "today_mmdd": f"{done_kst.month:02d}/{done_kst.day:02d}",
            "yest_mmdd": f"{yest_dt.month:02d}/{yest_dt.day:02d}",
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
                    "rt_running_hos": rt_running_hos,
                    "active_rt_cnt": active_rt_cnt,
                    "wait_rt_cnt": wait_rt_cnt,
                    "red_rt_cnt": red_rt_cnt,
                    "active_milk_cnt": active_milk_cnt,
                    "active_ferm_cnt": active_ferm_cnt,
                    "active_dairy_cnt": active_dairy_cnt,
                    "soy_default_tab": soy_def_tab,
                    "soy_blocks_cnt": soy_blocks_cnt,
                    "soy_done_cnt": soy_done_cnt,
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
                "milk_run_times": milk_times,
                "ferm_run_times": ferm_times,
                "dairy_hist_times_map": dairy_hist_times_map,
                "soy_default_tab": soy_def_tab,
                "soy_tabs_map": soy_map,
                "micro": micro_data,
                "rt_hist_p": rt_hp,
                "rt_hist_c": rt_hc,
                "dairy_hist_p": dy_hp,
                "dairy_hist_c": dy_hc,
                "soy_hist": soy_hist,
                "fsk_products_cnt": len(fsk_prods),
                "master_products": master_prods
            },
            "supplier": {
                "target_exec": final_sup_target,
                "exec_rate": final_sup_rate,
                "doc_exec_rate": doc_exec_rate,
                "level_str": final_sup_lv,
                "total_cnt": total_sup_cnt,
                "sub_cnt": sub_sup_cnt,
                "unsub_cnt": unsub_sup_cnt,
                "ok_cnt": ok_sup_cnt,
                "guide_cnt": guide_sup_cnt,
                "out_cnt": out_sup_cnt,
                "signal": sig_supplier,
                "kpi_sheet": sup_kpi_sheet,
                "grade_summary": sup_g,
                "master_rows": sup_m,
                "under_comp_rows": sup_uc,
                "under_detail_rows": sup_ud
            },
            "audit": {
                "summary": get_active_audit_summary(audit_sched, today_str),
                "schedules": audit_sched,
                "kpi_sheet": aud_kpi_sheet
            },
            "law": law_bundle,
            "claim_nc_hist": claim_nc_data,
            "manual_kpis": merged_manual,
            "special_boards": {
                "label_memo": label_memo_final,
                "micro_status": micro_final
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

        _BUNDLE_CACHE["ts"] = time.time() - (CACHE_TTL_SEC - 15.0 if errors else 0.0)
        _BUNDLE_CACHE["date_key"] = today_str
        _BUNDLE_CACHE["data"] = bundle
        return bundle


# ==============================================================================
# 9. 정적 파일 · LOT 4단 역추적(미생물 통합) · 양방향 저장 API 엔드포인트
# ==============================================================================
class AuditSchedulePayload(BaseModel):
    action: str
    uid: Optional[str] = ""
    audit_type: Optional[str] = "방문심사"
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
    micro_status: Optional[Dict[str, Any]] = None


class LawPrefPayload(BaseModel):
    law_id: str
    mode: Optional[str] = None
    prep: Optional[str] = None
    summary: Optional[str] = None


class LabelPinPayload(BaseModel):
    item_key: str
    pinned: bool


class ClaimNcHistPayload(BaseModel):
    action: str
    uid: Optional[str] = ""
    category: str = "클레임"
    reg_date: Optional[str] = ""
    importance: Optional[str] = "일반"
    product_name: Optional[str] = ""
    exp_date: Optional[str] = ""
    cause_type: Optional[str] = "공정·설비"
    reason_detail: Optional[str] = ""
    extra_action: Optional[str] = ""
    status: Optional[str] = "검사중"
    final_judge: Optional[str] = "판정대기"
    show_dash: Optional[str] = "Y"


def find_public_file(filename: str) -> Optional[str]:
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    for candidate in [
        os.path.join(base_dir, "public", filename),
        os.path.join(os.getcwd(), "public", filename),
        os.path.join(os.path.dirname(os.path.abspath(__file__)), filename)
    ]:
        if os.path.isfile(candidate):
            return candidate
    return None


@app.get("/")
def serve_frontend():
    path = find_public_file("index.html")
    if path:
        return FileResponse(path, media_type="text/html")
    return HTMLResponse(content="<h3>public/index.html 파일을 확인해 주세요.</h3>", status_code=404)


@app.get("/img-symbol1.png")
def serve_yonsei_symbol():
    path = find_public_file("img-symbol1.png")
    if path:
        return FileResponse(path, media_type="image/png")
    raise HTTPException(status_code=404, detail="img-symbol1.png 파일을 찾을 수 없습니다.")


@app.get("/api/health")
def api_health():
    return {"ok": True, "kst_now": now_kst().strftime("%Y-%m-%d %H:%M:%S")}


@app.get("/api/bundle")
def api_get_bundle(force: int = 0):
    return build_full_dashboard_bundle(force_refresh=(force == 1))


@app.get("/api/lot-trace")
def api_lot_trace(product: str = "", exp: str = "", kw: str = ""):
    """
    LOT 원스톱 4단 교차 역추적:
    1) 상온 공정일보 & CCP
    2) 두유 배양검사 (실가동시간·오염판정)
    3) 유제품 포장일보 (포장 시작~종료 시간 · 살균기 온도/균질압 CCP 분리 도출)
    4) 미생물 검사일보 (해당 생산일 Tank 번호 · 호기/라인 · 대장균군/세균수/유산균 실측치 자동 연결)
    """
    bundle = build_full_dashboard_bundle(force_refresh=False)
    if not bundle.get("ok"):
        raise HTTPException(status_code=500, detail="시트 데이터가 아직 로드되지 않았습니다.")

    p_data = bundle["process"]
    rt_hp = p_data["rt_hist_p"]
    rt_hc = p_data["rt_hist_c"]
    soy_h = p_data["soy_hist"]
    dy_hp = p_data["dairy_hist_p"]
    dy_hc = p_data["dairy_hist_c"]
    micro_h = p_data.get("micro", {}).get("hist_rows", [])

    exp_q = parse_exp_query_info(exp)

    # 1. 상온 생산 매칭
    matched_rt = [
        r for r in rt_hp
        if is_product_match(r.get("제품명", ""), product, kw)
        and is_exp_match(r.get("표기소비기한_원문", ""), r.get("표준소비기한_검색용", ""), exp_q)
    ]
    matched_rt.sort(key=lambda x: x.get("생산일자", ""), reverse=True)

    # 2. 두유 배양 매칭
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

    # 3. 유제품 포장 + 포장 시작~종료 시간 + 살균기 온도/균질압 CCP 매칭
    matched_dy = []
    dy_prod_dates_matched = set()
    dy_prod_names_matched = []

    for dr in dy_hp:
        if is_product_match(dr.get("제품명", ""), product, kw) and is_exp_match(dr.get("표기소비기한_원문", ""), dr.get("표준소비기한_검색용", ""), exp_q):
            d_dt = str(dr.get("생산일자", "")).strip()
            d_ln = str(dr.get("라인구분", "")).strip()
            if d_dt:
                dy_prod_dates_matched.add(d_dt)
            if dr.get("제품명") and dr.get("제품명") not in dy_prod_names_matched:
                dy_prod_names_matched.append(dr.get("제품명"))

            time_lines, ccp_lines, note_lines = [], [], []
            for cr in dy_hc:
                if str(cr.get("생산일자", "")).strip() == d_dt and (str(cr.get("라인구분", "")).strip() in d_ln or d_ln in str(cr.get("라인구분", "")).strip()):
                    rcat = str(cr.get("기록분류", ""))
                    if "생산시간" in rcat:
                        time_lines.append(f"{cr.get('세부항목', '')}: {cr.get('기록내용_수치', '-')}")
                    elif "CCP" in rcat:
                        ccp_lines.append(f"{cr.get('세부항목', 'CCP')}: {cr.get('기록내용_수치', '-')}")
                    elif "특이사항" in rcat:
                        rv = str(cr.get("특이사항", "이상없음")).strip()
                        if rv and rv not in ["이상없음", "-"]:
                            note_lines.append(rv)

            dr_copy = dict(dr)
            dr_copy["포장기_시작~종료시간"] = " / ".join(time_lines) if time_lines else "-"
            dr_copy["살균기온도_균질압_CCP"] = " / ".join(ccp_lines) if ccp_lines else "-"
            dr_copy["현장특이사항"] = " / ".join(note_lines) if note_lines else "이상없음"
            matched_dy.append(dr_copy)
    matched_dy.sort(key=lambda x: x.get("생산일자", ""), reverse=True)

    # 4. 미생물 검사일보 매칭 (검사일자_생산일 == 포장 생산일자 교차 매칭 + (주1~주12) 주석 품목명 매칭)
    matched_micro = []
    has_prod_filter = bool((product and product != "ALL") or (kw and kw.strip()))

    for mr in micro_h:
        alias_str = f"{mr.get('대표품목명', '')} | {mr.get('연관품목_주석포함', '')}"
        m_insp_dt = str(mr.get("검사일자_생산일", "")).strip()

        if dy_prod_dates_matched:
            if m_insp_dt not in dy_prod_dates_matched:
                continue
            if has_prod_filter:
                if not is_product_match(alias_str, product, kw) and not any(is_product_match(alias_str, dp_nm, "") for dp_nm in dy_prod_names_matched):
                    continue
            else:
                if dy_prod_names_matched and not any(is_product_match(alias_str, dp_nm, "") for dp_nm in dy_prod_names_matched):
                    continue
            matched_micro.append(mr)
        elif exp_q.get("empty", True) and has_prod_filter:
            # 소비기한 없이 제품명만 검색한 경우 해당 제품의 최근 미생물 이력 전체 도출
            if is_product_match(alias_str, product, kw):
                matched_micro.append(mr)

    matched_micro.sort(key=lambda x: (x.get("검사일자_생산일", ""), x.get("판정일자", "")), reverse=True)

    rt_dt_tab_set = {(r.get("생산일자", ""), r.get("원본탭명", "")) for r in matched_rt}
    matched_rt_ccp = [c for c in rt_hc if (c.get("생산일자", ""), c.get("원본탭명", "")) in rt_dt_tab_set]

    return {
        "ok": True,
        "counts": {
            "rt": len(matched_rt),
            "soy": len(matched_soy),
            "dairy": len(matched_dy),
            "micro": len(matched_micro)
        },
        "matched_rt": matched_rt,
        "matched_rt_ccp": matched_rt_ccp,
        "matched_soy": matched_soy,
        "matched_dairy": matched_dy,
        "matched_micro": matched_micro[:120]
    }


@app.post("/api/audit-schedule")
def api_save_audit_schedule(body: AuditSchedulePayload):
    sheet_urls_map = get_sheet_urls_map()
    sh, err = open_sheet_isolated(sheet_urls_map, "sup_audit_kpi")
    using_sak = True
    if sh is None:
        using_sak = False
        sh, err = open_sheet_isolated(sheet_urls_map, "audit_db")
        if sh is None:
            raise HTTPException(status_code=500, detail=f"일정 저장용 구글 시트를 열 수 없습니다: {err}")

    if using_sak:
        ws_all = api_call_with_retry(sh.worksheets)
        ws_sched = next((w for w in ws_all if "일정" in w.title), None)
        if ws_sched is None:
            ws_sched = api_call_with_retry(sh.add_worksheet, title=AUDIT_SCHED_TAB, rows=150, cols=10)
    else:
        ws_sched = api_call_with_retry(sh.get_worksheet, 0)

    rows = get_values_formatted(ws_sched)
    cur_list = parse_schedule_tab_rows(rows)

    if not cur_list and _BUNDLE_CACHE["data"] is not None:
        cached_scheds = _BUNDLE_CACHE["data"].get("audit", {}).get("schedules", [])
        if cached_scheds:
            cur_list = [dict(x) for x in cached_scheds]

    today_str = now_kst().strftime("%Y-%m-%d")
    now_s = now_kst().strftime("%Y-%m-%d %H:%M:%S")

    if body.action == "add":
        if not (body.audit_name or "").strip():
            raise HTTPException(status_code=400, detail="심사명 또는 방문 업체명을 입력해 주세요.")
        cur_list.append({
            "고유ID": body.uid.strip() if (body.uid and body.uid.strip()) else str(uuid.uuid4())[:8],
            "구분": normalize_audit_type(body.audit_type),
            "심사일자": (body.audit_date or today_str).strip(),
            "심사명": body.audit_name.strip(),
            "진행현황": body.status if body.status in AUDIT_STATUS_LIST else "심사준비중",
            "점수": (body.score or "").strip(),
            "비고": (body.memo or "").strip(),
            "수정일시": now_s
        })
    elif body.action == "update":
        for item in cur_list:
            if item["고유ID"] == body.uid:
                if body.audit_type:
                    item["구분"] = normalize_audit_type(body.audit_type)
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
            target_uid = get_active_audit_summary(cur_list, today_str).get("active_uid", "")
        for item in cur_list:
            if item["고유ID"] == target_uid and normalize_audit_type(item.get("구분")) == "방문심사" and body.status in AUDIT_STATUS_LIST:
                item["진행현황"] = body.status
                item["수정일시"] = now_s
    elif body.action == "delete":
        cur_list = [x for x in cur_list if x["고유ID"] != body.uid]

    cur_list.sort(key=lambda x: x.get("심사일자", ""))
    payload = [AUDIT_COLS] + [[str(r.get(c, "")) for c in AUDIT_COLS] for r in cur_list]
    api_call_with_retry(ws_sched.clear)
    safe_ws_update(ws_sched, payload)

    new_summary = get_active_audit_summary(cur_list, today_str)
    if "sak" in _LAST_GOOD_SHEETS:
        prev_s, prev_a, _, _ = _LAST_GOOD_SHEETS["sak"]
        _LAST_GOOD_SHEETS["sak"] = (prev_s, prev_a, cur_list, None)
    if _BUNDLE_CACHE["data"] is not None:
        _BUNDLE_CACHE["data"]["audit"]["summary"] = new_summary
        _BUNDLE_CACHE["data"]["audit"]["schedules"] = cur_list

    return {
        "ok": True,
        "message": "'협력업체, 심사 점수' 시트의 [일정] 탭에 저장되었습니다.",
        "summary": new_summary,
        "schedules": cur_list
    }


def _read_and_write_special_board(updater_fn):
    sheet_urls_map = get_sheet_urls_map()
    sh, err = open_sheet_isolated(sheet_urls_map, "audit_db")
    if sh is None:
        sh, err = open_sheet_isolated(sheet_urls_map, "sup_audit_kpi")
        if sh is None:
            raise HTTPException(status_code=500, detail=f"설정 저장용 시트를 열 수 없습니다: {err}")

    ws_list = api_call_with_retry(sh.worksheets)
    ws_sp = next((w for w in ws_list if w.title.strip() == SPECIAL_BOARD_TAB), None)
    if ws_sp is None:
        ws_sp = api_call_with_retry(sh.add_worksheet, title=SPECIAL_BOARD_TAB, rows=30, cols=4)

    board_map = {
        "label_memo": DEFAULT_LABEL_MEMO,
        "micro_status_json": json.dumps(DEFAULT_MICRO_STATUS, ensure_ascii=False),
        "law_prefs_json": "{}",
        "label_pins_json": "[]"
    }
    sp_rows = get_values_formatted(ws_sp)
    if len(sp_rows) >= 2:
        for sr in sp_rows[1:]:
            if len(sr) >= 2 and str(sr[0]).strip():
                board_map[str(sr[0]).strip()] = str(sr[1]).strip()

    now_s = now_kst().strftime("%Y-%m-%d %H:%M:%S")
    updater_fn(board_map, now_s)

    payload = [["항목키", "저장내용", "수정일시"]] + [[k, v, now_s] for k, v in board_map.items()]
    api_call_with_retry(ws_sp.clear)
    safe_ws_update(ws_sp, payload)
    return board_map, now_s


@app.post("/api/special-boards")
def api_save_special_boards(body: SpecialBoardPayload):
    def _upd(b_map: dict, now_s: str):
        if body.label_memo is not None:
            b_map["label_memo"] = str(body.label_memo).strip()
        if body.micro_status is not None:
            mode_v = str(body.micro_status.get("mode", "수동지정")).strip()
            st_v = str(body.micro_status.get("status", "출고적합")).strip()
            if st_v == "자동연동" or mode_v == "자동연동":
                m_obj = {"mode": "자동연동", "status": "출고적합", "note": "", "updated_at": now_s}
            else:
                if st_v not in ["출고적합", "검사중", "재검·출고보류"]:
                    st_v = "출고적합"
                m_obj = {
                    "mode": "수동지정",
                    "status": st_v,
                    "note": str(body.micro_status.get("note", "")).strip(),
                    "updated_at": now_s
                }
            b_map["micro_status_json"] = json.dumps(m_obj, ensure_ascii=False)

    b_map, _ = _read_and_write_special_board(_upd)
    saved_m = json.loads(b_map.get("micro_status_json", "{}")) or dict(DEFAULT_MICRO_STATUS)

    micro_cached = _LAST_GOOD_SHEETS.get("micro", ({}, None))[0]
    if micro_cached.get("has_sheet") and saved_m.get("mode") != "수동지정":
        eff_micro = {
            "mode": "자동연동",
            "status": micro_cached.get("auto_status", "출고적합"),
            "note": micro_cached.get("auto_note", ""),
            "updated_at": "시트 실시간 연동"
        }
    else:
        eff_micro = saved_m

    new_sp = {
        "label_memo": b_map.get("label_memo", DEFAULT_LABEL_MEMO).strip(),
        "micro_status": eff_micro
    }
    if _BUNDLE_CACHE["data"] is not None:
        _BUNDLE_CACHE["data"]["special_boards"] = new_sp

    return {"ok": True, "message": "변경사항이 시트에 즉시 저장되었습니다.", "special_boards": new_sp}


@app.post("/api/law-pref")
def api_save_law_pref(body: LawPrefPayload):
    """
    체크한 법규를 '법규정보' 구글 시트 내 [대시보드] 탭에 실시간 기록하고,
    체크 해제한 법규는 [대시보드] 탭에서 즉시 삭제하여 다음 접속 시에도 100% 동일 유지
    """
    sheet_urls_map = get_sheet_urls_map()
    sh_law, err_law = open_sheet_isolated(sheet_urls_map, "law_info")
    if sh_law is None:
        raise HTTPException(status_code=500, detail=f"'법규정보' 시트를 열 수 없습니다: {err_law}")

    # 1) 현재 법규 목록 및 [대시보드] 탭 상태 로드
    if "law" in _LAST_GOOD_SHEETS and _LAST_GOOD_SHEETS["law"][0]:
        raw_laws_cur, dash_init_cur, dash_map_cur, _ = _LAST_GOOD_SHEETS["law"]
        dash_map_cur = dict(dash_map_cur or {})
    else:
        raw_laws_cur, dash_init_cur, dash_map_cur, _ = fetch_law_info_sheet(sheet_urls_map)
        dash_map_cur = dict(dash_map_cur or {})

    # 아직 초기화 전이었다면 현재 기본 노출 항목들로 맵 구성
    if not dash_init_cur:
        for lw in raw_laws_cur:
            if lw.get("auto_default"):
                dash_map_cur[lw["law_id"]] = {
                    "prep": "검토전",
                    "summary": lw.get("자동조치요약", ""),
                    "law_name": lw.get("법규명", "")
                }
        dash_init_cur = True

    target_law = next((x for x in raw_laws_cur if x["law_id"] == body.law_id), None)
    norm_nm = re.sub(r"\s+", "", target_law["법규명"]) if target_law else ""

    # 기존 엔트리 찾기 (ID 또는 법규명 기준)
    existing_entry = dash_map_cur.get(body.law_id) or (dash_map_cur.get(norm_nm) if norm_nm else None)
    if norm_nm and norm_nm in dash_map_cur and norm_nm != body.law_id:
        dash_map_cur.pop(norm_nm, None)

    # 2) Pin(추가/유지) vs Hide(삭제) 반영
    if body.mode == "Hide":
        dash_map_cur.pop(body.law_id, None)
    elif body.mode == "Pin":
        cur_p = (existing_entry.get("prep") if existing_entry else None) or "검토전"
        cur_s = (existing_entry.get("summary") if existing_entry else "") or (target_law["자동조치요약"] if target_law else "")
        dash_map_cur[body.law_id] = {
            "prep": body.prep if body.prep in LAW_PREP_LIST else cur_p,
            "summary": body.summary.strip() if body.summary is not None else cur_s,
            "law_name": target_law["법규명"] if target_law else ""
        }
    else:
        # 노출 중이거나 미노출 항목의 준비상태/요약 수정
        if existing_entry is not None:
            if body.prep in LAW_PREP_LIST:
                existing_entry["prep"] = body.prep
            if body.summary is not None:
                existing_entry["summary"] = body.summary.strip()
            dash_map_cur[body.law_id] = existing_entry

    # 보조 설정 보드에도 요약·준비상태 백업 보존 (체크 해제 후 다시 켰을 때 이전 메모 복원용)
    def _upd_aux(b_map: dict, _now_s: str):
        try:
            prefs = json.loads(b_map.get("law_prefs_json", "{}"))
            if not isinstance(prefs, dict):
                prefs = {}
        except Exception:
            prefs = {}
        cur_it = prefs.get(body.law_id, {"mode": "auto", "prep": "검토전", "summary": ""})
        if body.mode in ["Pin", "Hide"]:
            cur_it["mode"] = body.mode
        if body.prep in LAW_PREP_LIST:
            cur_it["prep"] = body.prep
        if body.summary is not None:
            cur_it["summary"] = body.summary.strip()
        prefs[body.law_id] = cur_it
        b_map["law_prefs_json"] = json.dumps(prefs, ensure_ascii=False)

    try:
        b_aux, _ = _read_and_write_special_board(_upd_aux)
        aux_prefs = json.loads(b_aux.get("law_prefs_json", "{}"))
    except Exception:
        aux_prefs = {}

    # 3) 최신 설정을 적용해 '법규정보' 시트의 [대시보드] 탭에 즉시 저장/삭제 반영
    new_law_bundle = apply_law_preferences(raw_laws_cur, True, dash_map_cur, aux_prefs)
    sync_law_dashboard_worksheet(sh_law, new_law_bundle["all_laws"])

    _LAST_GOOD_SHEETS["law"] = (raw_laws_cur, True, dash_map_cur, None)
    if _BUNDLE_CACHE["data"] is not None:
        _BUNDLE_CACHE["data"]["law"] = new_law_bundle

    return {
        "ok": True,
        "message": "'법규정보' 시트의 [대시보드] 탭에 즉시 반영되었습니다.",
        "law": new_law_bundle
    }


@app.post("/api/label-pin")
def api_save_label_pin(body: LabelPinPayload):
    def _upd(b_map: dict, _now_s: str):
        try:
            pins = json.loads(b_map.get("label_pins_json", "[]"))
            if not isinstance(pins, list):
                pins = []
        except Exception:
            pins = []

        pin_set = {str(x).strip() for x in pins if str(x).strip()}
        k = body.item_key.strip()
        if body.pinned:
            pin_set.add(k)
        else:
            pin_set.discard(k)
        b_map["label_pins_json"] = json.dumps(sorted(list(pin_set)), ensure_ascii=False)

    b_map, _ = _read_and_write_special_board(_upd)
    new_pins = json.loads(b_map.get("label_pins_json", "[]"))

    new_lbl_analytics = None
    if _BUNDLE_CACHE["data"] is not None:
        lbl_rows_cached = _LAST_GOOD_SHEETS.get("lbl", ([], None))[0]
        if lbl_rows_cached:
            new_lbl_analytics = build_label_analytics(lbl_rows_cached, new_pins)
            _BUNDLE_CACHE["data"]["label_status"] = new_lbl_analytics

    return {"ok": True, "message": "대표 표시사항 노출 설정이 저장되었습니다.", "label_status": new_lbl_analytics}


@app.post("/api/claim-nc-history")
def api_save_claim_nc_history(body: ClaimNcHistPayload):
    sheet_urls_map = get_sheet_urls_map()
    sh, err = open_sheet_isolated(sheet_urls_map, "claim_nc_hist")
    if sh is None:
        raise HTTPException(status_code=500, detail=f"'클레임, 부적합 이력' 시트를 열 수 없습니다: {err}")

    cat_norm = "부적합" if "부적합" in str(body.category) else "클레임"
    ws_all = api_call_with_retry(sh.worksheets)
    target_ws = next((w for w in ws_all if cat_norm in w.title), None)
    if target_ws is None:
        target_ws = api_call_with_retry(sh.add_worksheet, title=cat_norm, rows=200, cols=14)

    rows = get_values_formatted(target_ws)
    cur_records = []
    if len(rows) >= 2:
        hdr = [str(c).strip() for c in rows[0]]
        for r in rows[1:]:
            if not any(str(x).strip() for x in r):
                continue
            r_pad = [str(x).strip() for x in r] + [""] * max(0, len(hdr) - len(r))
            d = dict(zip(hdr, r_pad[:len(hdr)]))
            cur_records.append({col: d.get(col, "") for col in CLAIM_HIST_COLS})

    today_s = now_kst().strftime("%Y-%m-%d")
    now_s = now_kst().strftime("%Y-%m-%d %H:%M:%S")

    def auto_judge_by_status(st_v: str, cur_j: str) -> str:
        if st_v == "적합(종결)":
            return "적합"
        if st_v == "부적합":
            return "부적합"
        return cur_j if cur_j not in ["", "적합", "부적합"] else "판정대기"

    if body.action == "add":
        st_clean = body.status if body.status in CLAIM_HIST_STATUS_LIST else "검사중"
        cur_records.insert(0, {
            "고유ID": body.uid.strip() if (body.uid and body.uid.strip()) else str(uuid.uuid4())[:8],
            "등록일": (body.reg_date or today_s).strip(),
            "중요도": "중점관리" if body.importance == "중점관리" else "일반",
            "제품명": (body.product_name or "").strip() or "-",
            "소비기한": (body.exp_date or "").strip() or "-",
            "발생공정_원인유형": (body.cause_type or "공정·설비").strip(),
            "상세사유": (body.reason_detail or "").strip() or "-",
            "추가검사_조치내역": (body.extra_action or "").strip() or "-",
            "진행상태": st_clean,
            "최종판정": auto_judge_by_status(st_clean, body.final_judge or ""),
            "대시보드노출": "N" if body.show_dash == "N" else "Y",
            "수정일시": now_s
        })
    elif body.action == "update_status":
        st_clean = body.status if body.status in CLAIM_HIST_STATUS_LIST else "검사중"
        for it in cur_records:
            if it["고유ID"] == body.uid:
                it["진행상태"] = st_clean
                it["최종판정"] = auto_judge_by_status(st_clean, it.get("최종판정", ""))
                it["수정일시"] = now_s
    elif body.action == "toggle_dash":
        for it in cur_records:
            if it["고유ID"] == body.uid:
                it["대시보드노출"] = "N" if it.get("대시보드노출", "Y") == "Y" else "Y"
                it["수정일시"] = now_s
    elif body.action == "delete":
        cur_records = [x for x in cur_records if x["고유ID"] != body.uid]

    payload = [CLAIM_HIST_COLS] + [[str(r.get(c, "")) for c in CLAIM_HIST_COLS] for r in cur_records]
    api_call_with_retry(target_ws.clear)
    safe_ws_update(target_ws, payload)

    refreshed_cnh, _ = fetch_claim_nc_history_sheet(sheet_urls_map)
    _LAST_GOOD_SHEETS["cnh"] = (refreshed_cnh, None)
    if _BUNDLE_CACHE["data"] is not None:
        _BUNDLE_CACHE["data"]["claim_nc_hist"] = refreshed_cnh

    return {
        "ok": True,
        "message": f"'{cat_norm}' 탭에 이력이 저장되었습니다.",
        "claim_nc_hist": refreshed_cnh
    }


@app.post("/api/manual-kpi")
def api_save_manual_kpi(body: ManualKpiPayload):
    if body.kpi_key not in DEFAULT_MANUAL_KPIS:
        raise HTTPException(status_code=400, detail="유효하지 않은 KPI 키입니다.")

    sheet_urls_map = get_sheet_urls_map()
    sh, err = open_sheet_isolated(sheet_urls_map, "audit_db")
    if sh is None:
        sh, err = open_sheet_isolated(sheet_urls_map, "sup_audit_kpi")
        if sh is None:
            raise HTTPException(status_code=500, detail=f"설정 시트를 열 수 없습니다: {err}")

    ws_list = api_call_with_retry(sh.worksheets)
    ws_manual = next((w for w in ws_list if w.title.strip() == MANUAL_KPI_TAB), None)
    if ws_manual is None:
        ws_manual = api_call_with_retry(sh.add_worksheet, title=MANUAL_KPI_TAB, rows=30, cols=12)

    all_manual = {k: {**v, "notes": list(v["notes"])} for k, v in DEFAULT_MANUAL_KPIS.items()}
    if _BUNDLE_CACHE["data"] and _BUNDLE_CACHE["data"].get("manual_kpis"):
        for k, v in _BUNDLE_CACHE["data"]["manual_kpis"].items():
            if k in all_manual:
                all_manual[k] = dict(v)

    all_manual[body.kpi_key] = {
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

    return {"ok": True, "message": "KPI 설정이 즉시 저장되었습니다.", "manual_kpis": all_manual}


@app.post("/api/ai-briefing")
def api_generate_ai_briefing():
    bundle = build_full_dashboard_bundle(force_refresh=False)
    if not bundle.get("ok"):
        raise HTTPException(status_code=500, detail="데이터가 준비되지 않았습니다.")

    ps = bundle["process"]["summary"]
    lb = bundle["label_status"]
    au = bundle["audit"]["summary"]
    lw = bundle["law"]
    cnh = bundle["claim_nc_hist"]["counts"]
    cs = bundle["cost"]["dept"]
    mc = bundle.get("special_boards", {}).get("micro_status", DEFAULT_MICRO_STATUS)

    factual_fallback = (
        f"[Q-Safety 금일 브리핑] 상온공정({ps['rt_today_tab']}) {ps['active_rt_cnt']}건 · 유제품(우유 {ps['active_milk_cnt']}·발효유 {ps['active_ferm_cnt']}) 가동 중이며, "
        f"두유배양({ps['soy_default_tab']}) 미검사 {ps['soy_uninspected_cnt']}호기, 미생물 판정 '{mc.get('status', '출고적합')}'입니다. "
        f"표시사항은 현재 {lb['reviewing_cnt']}건 심의 중(26년 누적 {lb['cnt_26_all']}건, 법규 D-Day {lw['active_cnt']}건 관리), "
        f"차기 일정은 '{au['upcoming_banner']}', 클레임·부적합 진행 {cnh['active']}건, 부문 누적 원가절감 {cs['actual_uk']}억원({cs['rate']}%)입니다."
    )

    api_key = os.environ.get("GEMINI_API_KEY", "").strip()
    if not api_key:
        return {"ok": True, "source": "factual_engine", "briefing": factual_fallback}

    prompt_txt = (
        "다음 연세유업 아산공장 품질안전부문 실시간 지표를 바탕으로 추정 없이 오직 팩트 수치만 요약하여 "
        "부문장 출근 보고용 3줄 핵심 브리핑을 한국어로 간결하게 작성해줘.\n"
        f"- 공정: 상온({ps['rt_today_tab']}) 가동 {ps['active_rt_cnt']}건·특이사항 {ps['today_notes']} / 유제품 {ps['active_dairy_cnt']}건(우유 {ps['active_milk_cnt']}·발효유 {ps['active_ferm_cnt']}) / 두유배양({ps['soy_default_tab']}) 미검사 {ps['soy_uninspected_cnt']}건 / 미생물: {mc.get('status')} ({mc.get('note')})\n"
        f"- 식안팀: 표시사항 현재 {lb['reviewing_cnt']}건 심의 중({lb['rev_summary_line']}), 26년 누적 {lb['cnt_26_all']}건, 법규 D-Day 추적 {lw['active_cnt']}건(1년내 불용방지 준비 {lw['prep_1yr_cnt']}건)\n"
        f"- 심사/이력: 도래 일정 '{au['upcoming_banner']}', 클레임·부적합 현재 추적 {cnh['active']}건(중점 {cnh['major']}건)"
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
