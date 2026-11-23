# ==============================================================================
# [api/index.py - PART 1 / 4]
# Q-Safety 백엔드 통합 엔진 (진입점 · 상수 · 인증 · 주석매칭 · 원가절감 & 클레임 파서)
# ==============================================================================
import hashlib
import hmac
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
from fastapi import Depends, FastAPI, Header, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, HTMLResponse
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials as OAuthCredentials
from google.oauth2.service_account import Credentials as ServiceAccountCredentials
from pydantic import BaseModel

# ==============================================================================
# 0. Vercel FastAPI 최상단 진입점(Entrypoint) 및 CORS 설정
# ==============================================================================
app = FastAPI(title="Q-Safety API", version="6.8.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"]
)

# ==============================================================================
# 1. 기본 상수 · 한국 표준시(KST, UTC+9) · 시트 매핑 · 미생물 각주 사전 정의
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
    "심사준비중": {"pct": 20, "color": "#3B82F6"},
    "심사중": {"pct": 50, "color": "#0284C7"},
    "서류심사중": {"pct": 75, "color": "#1D4ED8"},
    "현장심사중": {"pct": 85, "color": "#003378"},
    "심사종료": {"pct": 100, "color": "#0F3A7D"}
}

MICRO_STATUS_LIST = ["자동연동", "출고적합", "검사중", "재검·출고보류"]
MICRO_LIVE_TABS = ["일반 일보", "일요일 일보", "휴일 일보(날짜변경)", "일보작성", "일요일_일보작성", "연휴_일보작성"]
LAW_PREP_LIST = ["검토전", "부자재변경중", "반영완료"]

# '법규정보' 시트 내 '대시보드' 탭 및 1~2줄 요약 전용 '_법규요약DB' 탭 표준 컬럼
LAW_DASH_TAB = "대시보드"
LAW_DASH_COLS = ["법규ID", "구분", "시행일", "법규명", "준비상태", "핵심요약", "관련부서", "수정일시"]
LAW_SUMMARY_DB_TAB = "_법규요약DB"
LAW_SUMMARY_DB_COLS = ["법규ID", "탭구분", "시행일", "법규명", "핵심요약_2줄", "요약방식", "수정일시"]

# '표시사항 현황' 시트 내 '특이사항' 탭 이름
LABEL_SPECIAL_TAB = "특이사항"

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

# 포장일보 제품명 <-> 미생물 일보 대표품목·하단 각주(주1~주12) 표준 매핑 사전
DEFAULT_MICRO_FOOTNOTES = {
    "주1": "연세 전용목장 우유, 아산맑은우유, 더클래식, 더클래식우유, Homeplus simplus 1등급우유, Homeplus signature 1A우유, 심플러스 1A, 마켓컬리, 더 고소한 목장우유",
    "주2": "연세대학교자녀사랑키짱전용목장, 자녀사랑, 연세자녀사랑, 자녀사랑키짱, 키짱, 키즈닥터, 키즈닥터우유, 아워홈365일, 아워홈 키유, 키유",
    "주3": "세브란스 고칼슘, 아산맑은우유고칼슘, 고칼슘우유",
    "주4": "연세우유 바리스타, 바리스타 (스타벅스), 스타벅스밀크, 스타벅스우유, 바리스타",
    "주5": "Homeplus simplus 1등급우유 저지방고칼슘, 1A우유 저지방고칼슘, 전용목장우유 저지방, 컬리 저지방, 마켓컬리 저지방, Simplus 저지방",
    "주7": "프로바이오틱스 위Y, 비락 위요, 세브란스 위플러스, 곰곰 위티처, 위락, 세브란스 위락",
    "주8": "프로바이오틱스 오메가3, 세브란스 장플러스, 곰곰 장티처, 오메가-3",
    "주9": "전용목장우유 컬리 1.8L, 연세우유 쿠팡 2.3L, 쿠팡전용, sim+ 2300, Simplus 1A, Homeplus simplus 국내산 1급A 원유로 만든 1A우유",
   "주10": "연세딸기, 호상딸기, 푸르밀 요거스푼 딸기, 푸르밀 딸기, simplus 딸기, 오늘좋은 딸기, 오늘 좋은 떠먹는 딸기, 건국 하이요 딸기, 건국 딸기, 부산요네뜨 딸기",
    "주11": "연세플레인, 호상플레인, 푸르밀 요거스푼 플레인, 푸르밀 플레인, simplus 플레인, 오늘좋은 플레인, 오늘 좋은 떠먹는 플레인, 건국 하이요 플레인, 건국 플레인, 부산요네뜨 플레인",
    "주12": "연세복숭아, 호상복숭아, 푸르밀 요거스푼 복숭아, 푸르밀 복숭아, 오늘좋은 복숭아, 오늘 좋은 떠먹는 복숭아, 건국 복숭아, 부산요네뜨 복숭아"
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
        "short_title": "3. 식품안전 및 HACCP 강화",
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
        "short_title": "4. 핵심불량(클레임) 발생 저감화",
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
        "short_title": "5. 부자재 관리 고도화",
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
        "short_title": "6. 협력업체 관리강화",
        "target_text": "실행률 88.3% 이상",
        "actual_text": "시트 연동 대기",
        "compact_val": "-",
        "sub_badge": "'협력업체, 심사 점수' 시트 실시간 연동",
        "target_num": 88.3,
        "actual_num": 0.0,
        "suffix": "%",
        "unit_label": "(단위 : %)",
        "signal": "gray",
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
        "target_text": "94.28점 이상",
        "actual_text": "시트 연동 대기",
        "compact_val": "-",
        "sub_badge": "'협력업체, 심사 점수' 시트 실시간 연동",
        "target_num": 94.28,
        "actual_num": 0.0,
        "suffix": "",
        "unit_label": "(단위 : 점)",
        "signal": "gray",
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
        "short_title": "8. 식품안전 법적 RISK 최소화",
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

DEFAULT_LABEL_MEMO = ""

DEFAULT_MICRO_STATUS = {
    "mode": "자동연동",
    "status": "출고적합",
    "note": "",
    "updated_at": ""
}


# ==============================================================================
# 2. 스레드 안전 Google 인증 · 일괄 조회 · 제품명/주석(주1~주12) 정밀 매칭 유틸리티
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
    ranges = []
    for t in unique_titles:
        escaped_t = t.replace("'", "''")
        ranges.append(f"'{escaped_t}'")
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


def calc_weighted_trend_and_forecast(y_values: list, is_recorded_list: list = None, total_periods: int = 12):
    """원가절감용 최근 가중 선형회귀(WLS): 초반보다 최근 월 실적 흐름에 가중치를 부여하여 연말 예측"""
    if is_recorded_list is not None:
        valid_idx = [i for i, flag in enumerate(is_recorded_list) if flag]
    else:
        valid_idx = [i for i, v in enumerate(y_values) if to_float(v, 0.0) > 0]

    valid_y = [float(y_values[i]) for i in valid_idx]

    if len(valid_idx) >= 2:
        weights = [1.0 + 0.22 * rank for rank in range(len(valid_idx))]
        w_sum = sum(weights)
        mean_x = sum(w * x for w, x in zip(weights, valid_idx)) / w_sum
        mean_y = sum(w * y for w, y in zip(weights, valid_y)) / w_sum
        denom = sum(w * ((x - mean_x) ** 2) for w, x in zip(weights, valid_idx))
        slope = (
            sum(w * (x - mean_x) * (y - mean_y) for w, x, y in zip(weights, valid_idx, valid_y)) / denom
            if denom > 0 else 0.0
        )
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


def calc_trend_and_forecast_split(y_values: list, is_recorded_list: list = None, total_periods: int = 12):
    return calc_weighted_trend_and_forecast(y_values, is_recorded_list, total_periods)


def despike_prev_year_series(prev_ppms: List[float]) -> List[float]:
    """단일 PPM 시계열의 특별원인 이상치를 Q3(여름 피크)가 아닌 평월 중앙값(Median)으로 안정 보정"""
    valid = sorted([v for v in prev_ppms if v > 0])
    if len(valid) < 4:
        return list(prev_ppms)
    n = len(valid)
    q1 = valid[n // 4]
    med = valid[n // 2]
    q3 = valid[(3 * n) // 4]
    iqr = max(q3 - q1, 0.15)
    upper_fence = min(q3 + 1.5 * iqr, med * 2.0)
    return [round(med, 2) if v > upper_fence else v for v in prev_ppms]


def compute_clean_prev_seasonal_ppms(cat_list: list, prev_year_str: str, target_categories: Optional[list] = None) -> Optional[List[float]]:
    """
    전년도 세부 유형별 데이터에서 일회성 특별원인 사고(예: 25년 2월 발효유/음료 카톤팩 CIP 이상 97건, 168.7 PPM)를
    유형 단위로 먼저 감지하여 해당 유형의 평월 정상 PPM으로 치환한 뒤 순수 월별 계절 PPM을 재산출
    """
    if not prev_year_str:
        return None
    sub = [
        r for r in cat_list
        if r["연도"] == prev_year_str
        and r["유형"] != "합계"
        and (target_categories is None or r["유형"] in target_categories)
    ]
    if not sub:
        return None

    adj_cnt_by_m = [0.0] * len(FISCAL_MONTHS)
    tot_prd_by_m = [0] * len(FISCAL_MONTHS)

    for r in sub:
        m_cnts = [int(r.get(f"{m}_건수", 0)) for m in FISCAL_MONTHS]
        m_prds = [int(r.get(f"{m}_생산수량", 0)) for m in FISCAL_MONTHS]
        m_ppms = [
            (c / p) * 1_000_000.0 if p > 0 else float(r.get(f"{m}_PPM", 0.0))
            for m, c, p in zip(FISCAL_MONTHS, m_cnts, m_prds)
        ]

        active_ppms = sorted([p for p, prd in zip(m_ppms, m_prds) if prd > 0])
        med_ppm = active_ppms[len(active_ppms) // 2] if active_ppms else 0.0
        q1_p = active_ppms[len(active_ppms) // 4] if len(active_ppms) >= 4 else med_ppm
        q3_p = active_ppms[(3 * len(active_ppms)) // 4] if len(active_ppms) >= 4 else med_ppm
        iqr_p = max(q3_p - q1_p, max(med_ppm * 0.5, 0.3))
        fence_ppm = max(q3_p + 2.2 * iqr_p, med_ppm * 3.2, 3.5)

        active_cnts = sorted([c for c, prd in zip(m_cnts, m_prds) if prd > 0])
        med_cnt = active_cnts[len(active_cnts) // 2] if active_cnts else 0

        for idx in range(len(FISCAL_MONTHS)):
            prd = m_prds[idx]
            cnt = m_cnts[idx]
            ppm = m_ppms[idx]
            tot_prd_by_m[idx] += prd

            # 평월 대비 3배 이상 급등하고 15건 이상 발생한 일회성 대량 클레임은 해당 품목 평월 중앙값으로 치환
            if prd > 0 and cnt >= 15 and ppm > fence_ppm and cnt > max(med_cnt * 3, med_cnt + 15):
                normal_cnt = (med_ppm * prd) / 1_000_000.0
                adj_cnt_by_m[idx] += normal_cnt
            else:
                adj_cnt_by_m[idx] += cnt

    if sum(tot_prd_by_m) <= 0:
        return None

    return [
        round((adj_cnt_by_m[i] / tot_prd_by_m[i]) * 1_000_000.0, 3) if tot_prd_by_m[i] > 0 else 0.0
        for i in range(len(FISCAL_MONTHS))
    ]


def calc_seasonal_claim_forecast(
    cur_ppms: List[float],
    cur_prods: List[int],
    cur_cnts: List[int],
    prev_ppms: Optional[List[float]] = None,
    total_periods: int = 12
) -> dict:
    """
    SQC 기반 클레임 발생률 예측 엔진:
    1) 평월 생산량의 60% 미만인 미마감 가집계 월(예: 11월 중간집계)은 기준 추세 왜곡에서 제외
    2) 전년도 일회성 사고 이상치를 절사한 뒤 월별 계절성 지수(Seasonal Index) 산출
    3) 마감 완료 월의 가중 추세 레벨 × 전년도 동절기 계절 지수를 결합해 잔여 월 및 회계연도 말 누적 PPM 산출
    """
    rec_idx = [i for i in range(total_periods) if (cur_prods[i] > 0 or cur_cnts[i] > 0)]
    if not rec_idx:
        return {
            "act_line": [None] * total_periods,
            "fc_line": [None] * total_periods,
            "full_line": [0.0] * total_periods,
            "slope": 0.0,
            "recorded_months": 0,
            "last_idx": -1,
            "feb_pred": 0.0,
            "cum_pred_ppm": 0.0,
            "partial_month_idx": -1
        }

    last_rec_idx = max(rec_idx)
    pos_prods = sorted([cur_prods[i] for i in rec_idx if cur_prods[i] > 0])
    med_prod = pos_prods[len(pos_prods) // 2] if pos_prods else 0

    partial_idx = -1
    settled_idx = list(rec_idx)
    if len(rec_idx) >= 3 and med_prod > 0 and (0 < cur_prods[last_rec_idx] < med_prod * 0.60):
        partial_idx = last_rec_idx
        settled_idx = [i for i in rec_idx if i != last_rec_idx]

    # 1) 마감 완료 월 기준 가중 선형 추세 산출
    s_y = [float(cur_ppms[i]) for i in settled_idx]
    if len(settled_idx) >= 2:
        weights = [1.0 + 0.18 * r for r in range(len(settled_idx))]
        w_sum = sum(weights)
        mx = sum(w * x for w, x in zip(weights, settled_idx)) / w_sum
        my = sum(w * y for w, y in zip(weights, s_y)) / w_sum
        denom = sum(w * ((x - mx) ** 2) for w, x in zip(weights, settled_idx))
        slope = sum(w * (x - mx) * (y - my) for w, x, y in zip(weights, settled_idx, s_y)) / denom if denom > 0 else 0.0
        # 급격한 직선 발산 방지를 위해 월별 추세 기울기 완충(Damping)
        damped_slope = max(min(slope * 0.30, 0.03), -0.04)
        intercept = my - slope * mx
        base_trend = [max(float(slope * i + intercept), 0.1) for i in range(total_periods)]
        anchor_i = max(settled_idx)
        anchor_level = base_trend[anchor_i]
    else:
        slope = 0.0
        damped_slope = 0.0
        anchor_i = settled_idx[0]
        anchor_level = float(cur_ppms[anchor_i])
        base_trend = [anchor_level] * total_periods

    # 2) 전년도 이상치 제거 후 계절성 지수(Seasonal Index) 추출
    seasonal_factors = [1.0] * total_periods
    if prev_ppms and sum(1 for v in prev_ppms if v > 0) >= 6:
        clean_prev = despike_prev_year_series([float(v) for v in prev_ppms[:total_periods]])
        valid_prev = [v for v in clean_prev if v > 0]
        prev_mean = sum(valid_prev) / len(valid_prev) if valid_prev else 1.0
        if prev_mean > 0:
            for idx_m in range(total_periods):
                if clean_prev[idx_m] > 0:
                    raw_si = clean_prev[idx_m] / prev_mean
                    seasonal_factors[idx_m] = 0.35 + 0.65 * raw_si

    overlap_si = [seasonal_factors[i] for i in settled_idx if seasonal_factors[i] > 0]
    base_si_mean = (sum(overlap_si) / len(overlap_si)) if overlap_si else 1.0
    deseason_level = anchor_level / base_si_mean if base_si_mean > 0 else anchor_level

    # 3) 실적 추세선(act_line) 및 계절성 반영 예측선(fc_line) 결합
    full_line = []
    for i in range(total_periods):
        if i <= anchor_i:
            full_line.append(round(base_trend[i], 2))
        else:
            step = i - anchor_i
            trend_comp = max(deseason_level + (damped_slope * step), 0.15)
            pred_v = round(max(trend_comp * seasonal_factors[i], 0.15), 2)
            full_line.append(pred_v)

    act_line = [full_line[i] if i <= anchor_i else None for i in range(total_periods)]
    fc_start = anchor_i
    fc_line = [None] * total_periods
    fc_line[fc_start] = round(float(cur_ppms[fc_start]), 2)
    for i in range(fc_start + 1, total_periods):
        fc_line[i] = full_line[i]

    # 4) 회계연도 말(2월 마감 시) 최종 연간 누적 PPM 예상치 산출
    est_month_prod = med_prod if med_prod > 0 else 55_000_000
    tot_cnt_proj = sum(cur_cnts[i] for i in settled_idx)
    tot_prd_proj = sum(cur_prods[i] for i in settled_idx)
    for i in range(anchor_i + 1, total_periods):
        if i == partial_idx:
            rem_prod = max(est_month_prod - cur_prods[i], 0)
            tot_cnt_proj += cur_cnts[i] + (rem_prod * full_line[i] / 1_000_000.0)
            tot_prd_proj += max(est_month_prod, cur_prods[i])
        else:
            tot_cnt_proj += est_month_prod * full_line[i] / 1_000_000.0
            tot_prd_proj += est_month_prod

    cum_pred_ppm = round((tot_cnt_proj / tot_prd_proj) * 1_000_000.0, 2) if tot_prd_proj > 0 else 0.0

    return {
        "act_line": act_line,
        "fc_line": fc_line,
        "full_line": full_line,
        "slope": round(float(slope), 4),
        "recorded_months": len(rec_idx),
        "last_idx": anchor_i,
        "feb_pred": full_line[-1] if full_line else 0.0,
        "cum_pred_ppm": cum_pred_ppm,
        "partial_month_idx": partial_idx
    }


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
    s = re.sub(r"\(주\s*\d+\)", "", s)
    s = re.sub(r"\(\s*\d+(?:\.\d+)?\s*(?:mL|ml|ML|㎖|L|g|kg)\s*\)", "", s)
    s = re.sub(r"(?<!\d)\d+(?:\.\d+)?\s*(?:mL|ml|ML|㎖|L|g|kg)(?!\w)", "", s)
    s = re.sub(r"\s+1[0127]\d{3}\b", "", s)
    s = s.replace("Ωmega3", "오메가3").replace("Ωmega3", "오메가3").replace("omega3", "오메가3")
    s = s.replace("MilkTalk", "밀크톡").replace("Milk Talk", "밀크톡")
    s = re.sub(r"[\s\(\)\[\]\{\}\-_·,/&\+!]+", "", s).lower()
    return s


def extract_volume_ml_token(text: str) -> str:
    """제품명·검색어에서 용량 숫자(mL 환산, 예: 1.8L -> 1800, 900ml -> 900, 단독 1800 -> 1800) 추출"""
    s = str(text or "").strip()
    if not s:
        return ""
    m_l = re.search(r"(\d+(?:\.\d+)?)\s*(?:L|ℓ)(?!\w)", s, re.IGNORECASE)
    if m_l:
        return str(int(round(float(m_l.group(1)) * 1000)))
    m_ml = re.search(r"(\d{2,4})\s*(?:mL|ml|ML|㎖|g)(?!\w)", s)
    if m_ml:
        return m_ml.group(1)
    m_bare = re.search(r"(?:^|[\s\(\)\[\]\-_+/])(170|180|190|200|500|750|900|930|1000|1800|2300)(?:$|[\s\(\)\[\]\-_+/])", s)
    if m_bare:
        return m_bare.group(1)
    return ""


def expand_micro_alias_pool(rep_name: str, saved_alias_col: str = "", custom_note_map: dict = None) -> Tuple[str, str]:
    rep_s = re.sub(r"\s+", " ", str(rep_name or "")).strip()
    rep_ns = re.sub(r"\s+", "", rep_s)
    rep_low = rep_ns.lower()

    m_ju = re.search(r"\(주\s*(\d+)\)", rep_s)
    ju_key = f"주{m_ju.group(1)}" if m_ju else ""
    if not ju_key:
        if rep_ns in ["연세딸기", "호상딸기", "부산요네뜨딸기"]:
            ju_key = "주10"
        elif rep_ns in ["연세플레인", "호상플레인", "부산요네뜨플레인"]:
            ju_key = "주11"
        elif rep_ns in ["연세복숭아", "호상복숭아", "부산요네뜨복숭아"]:
            ju_key = "주12"

    parts = [rep_s]
    saved_s = str(saved_alias_col or "").strip()

    # 1순위: [_이력DB_미생물]에 당시 날짜 스냅샷으로 저장된 주석 데이터가 있으면 최우선 보존·신뢰!
    if saved_s and saved_s != "-":
        parts.append(saved_s)
    elif ju_key:
        if custom_note_map and custom_note_map.get(ju_key):
            parts.append(custom_note_map[ju_key])
        elif DEFAULT_MICRO_FOOTNOTES.get(ju_key):
            parts.append(DEFAULT_MICRO_FOOTNOTES[ju_key])

    if ju_key == "주1":
        parts.append("마켓컬리 900mL, 마켓컬리 카톤 900mL, 더클래식, 더 클래식")
    elif ju_key == "주5":
        parts.append("마켓컬리 저지방 900mL, 마켓컬리 저지방 카톤")
    elif ju_key == "주9":
        parts.append("마켓컬리 1800mL, 마켓컬리 병 1800mL, 컬리 1800mL")

    # 자사 수출품 Milk Talk <-> 밀크톡 한·영 표기 양방향 호환
    if "milktalk" in rep_low or "밀크톡" in rep_low:
        kor_mt = re.sub(r"milk\s*talk", "밀크톡", rep_s, flags=re.IGNORECASE)
        eng_mt = re.sub(r"밀크톡", "Milk Talk", rep_s)
        parts.extend([kor_mt, eng_mt])

    is_yogurt = any(k in rep_low for k in ["yogurt", "요거트", "요구르트"])
    if not is_yogurt and "milktalk" not in rep_low and "밀크톡" not in rep_low:
        if "건강박사" in rep_ns:
            parts.append("헬스닥터, 오메가3건강박사")
        if "골드플러스" in rep_ns:
            parts.append("연세골드플러스, 연세 골드플러스RT 우유, 골드플러스키즈, 골드플러스 1800mL")
        if "바나바잎" in rep_ns:
            parts.append("저당바나바잎, 연세바나바잎, 세브란스 저당 바나바잎")
        if "헛개" in rep_ns:
            parts.append("연세헛개, 헛개나무, 세브란스 헛개")
        if "활력플러스" in rep_ns:
            parts.append("활력플러스, 세브란스 활력플러스, 연세 활력플러스")
        if "a2" in rep_low and "greek" not in rep_low:
            parts.append("A2 단백우유, 세브란스 A2 단백 우유, 전용목장 A2단백우유, A2단백우유")
        if "어린이" in rep_ns:
            parts.append("어린이 전용우유, 어린이전용우유, 어린이 전용우유 170mL")
        if "무항생제" in rep_ns and "목초" not in rep_ns:
            parts.append("연세 무항생제 목장우유, 무항생제 목장우유 750mL")
        if "킴스" in rep_ns or "더고소한" in rep_ns:
            parts.append("더 고소한 (킴스숲골), 더 고소한 목장우유 900mL")

    joined = " | ".join(dict.fromkeys([p.strip() for p in parts if p.strip()]))
    ju_badge = f"({ju_key} 공통원액)" if ju_key else ""
    return joined, ju_badge


VARIANT_MODIFIERS = [
    "a2", "저지방", "무지방", "락토프리", "고칼슘", "무항생제", "유기농", "디카페인",
    "밀크톡", "yogurt", "요거트", "요구르트"
]


def _norm_modifier_str(s: str) -> str:
    out = re.sub(r"\s+", "", str(s or "")).lower()
    out = out.replace("milktalk", "밀크톡")
    out = out.replace("ωmega3", "오메가3").replace("omega3", "오메가3")
    return out


def _strip_pack_and_vol_tokens(s: str) -> str:
    out = str(s or "").strip()
    out = re.sub(r"\(\s*(?:카톤|병|병포장)?\s*\d+(?:\.\d+)?\s*(?:mL|ml|ML|㎖|L|g|kg)?\s*\)", " ", out)
    out = re.sub(r"\(\s*(?:카톤|병포장|병)\s*\)", " ", out)
    out = re.sub(r"(?:카톤팩|카톤|병포장|병우유|병)(?=\s|$|\d|\()", " ", out)
    out = re.sub(r"(?<!\d)(170|180|190|200|500|750|900|930|1000|1800|2300)(?:\s*(?:mL|ml|ML|㎖|g))?(?!\d)", " ", out)
    return clean_product_for_match(out)


def is_product_match(
    record_prod: str,
    selected_prod: str,
    kw_text: str = "",
    allow_no_vol_record: bool = False,
    record_line_hint: str = ""
) -> bool:
    """
    1) 목록 선택(selected_prod) 시: 부분일치 배제 및 1:1 정확 일치 적용
       - '마켓컬리' 선택 시 '마켓컬리 (병 1800mL)' 및 '마켓컬리 저지방' 완전 분리
       - '연세딸기' 선택 시 '연세' 브랜드명을 유지하여 '푸르밀 딸기'·'오늘좋은 딸기' 혼입 원천 차단
    2) 자유 키워드(kw_text) 검색 시에만 부분 포함 허용
    """
    rec_raw = str(record_prod or "").strip()
    if not rec_raw or rec_raw == "-":
        return False

    has_exact_sel = bool(selected_prod and selected_prod not in ["", "ALL", "전체"])
    has_kw = bool(kw_text and kw_text.strip())
    if not has_exact_sel and not has_kw:
        return True

    rec_candidates = [p.strip() for p in re.split(r"[|/,]", rec_raw) if p.strip()]
    if not rec_candidates:
        rec_candidates = [rec_raw]

    hint_ns = re.sub(r"\s+", "", str(record_line_hint or "")).lower()
    is_ferm_hint = ("발효" in hint_ns) or ("호상" in hint_ns)
    hint_is_bottle = ("병" in hint_ns) and (not is_ferm_hint)
    hint_is_carton = ("카톤" in hint_ns) and (not hint_is_bottle) and (not is_ferm_hint)

    queries = []
    if has_exact_sel:
        queries.append((selected_prod.strip(), True))
    if has_kw and (not has_exact_sel or kw_text.strip() != selected_prod.strip()):
        queries.append((kw_text.strip(), False))

    # '연세'는 자사 고유 브랜드 식별자이므로 불용어에서 제외!
    stop_words = {"우유", "발효유", "요구르트", "요거트", "카톤", "병포장"}

    for q_str, is_exact_mode in queries:
        q_vol = extract_volume_ml_token(q_str)
        q_ns = _norm_modifier_str(q_str)
        q_is_bottle = ("병" in q_ns) or (q_vol in ("1800", "2300", "750", "170"))
        q_is_carton = ("카톤" in q_ns) or (q_vol in ("900", "930", "1000", "200", "180"))
        q_core = _strip_pack_and_vol_tokens(q_str)

        q_toks = [_strip_pack_and_vol_tokens(t) for t in re.split(r"[\s\(\)\[\]/+\-]+", q_str)]
        sig_toks = [t for t in q_toks if len(t) >= 2 and t not in stop_words]

        cand_matched = False
        for cand in rec_candidates:
            cand_ns = _norm_modifier_str(cand)
            cand_vol = extract_volume_ml_token(cand)
            cand_is_bottle = ("병" in cand_ns) or (cand_vol in ("1800", "2300", "750", "170")) or (len(rec_candidates) == 1 and hint_is_bottle)
            cand_is_carton = ("카톤" in cand_ns) or (cand_vol in ("900", "930", "1000", "200", "180")) or (len(rec_candidates) == 1 and hint_is_carton)

            # (1) 용량 충돌 배제
            if q_vol:
                if cand_vol and cand_vol != q_vol:
                    continue
                if not cand_vol and not allow_no_vol_record:
                    continue

            # (2) 카톤 vs 병포장 엄격 분리 ('마켓컬리' 선택 시 '마켓컬리 병 1800mL' 즉시 차단)
            if is_exact_mode:
                if q_is_bottle != cand_is_bottle:
                    if not (allow_no_vol_record and q_is_bottle and hint_is_bottle and not cand_is_carton):
                        continue
                if q_is_carton and cand_is_bottle:
                    continue
            else:
                if q_is_bottle and cand_is_carton:
                    continue
                if q_is_carton and cand_is_bottle:
                    continue

            # (3) 파생 속성 충돌 배제 (저지방·고칼슘·무항생제·유기농·A2·락토프리·밀크톡 등)
            mod_conflict = False
            for mod in VARIANT_MODIFIERS:
                if (mod in q_ns) != (mod in cand_ns):
                    mod_conflict = True
                    break
            if mod_conflict:
                continue

            # (4) 순수 제품명 매칭 (목록 선택 시 정확 일치, 자유 검색 시에만 부분 일치 허용)
            rc_core = _strip_pack_and_vol_tokens(cand)
            if not q_core:
                if q_vol and cand_vol == q_vol:
                    cand_matched = True
                    break
                continue

            if is_exact_mode:
                if q_core == rc_core:
                    cand_matched = True
                    break
                # 자사 브랜드 접두어('연세'·'세브란스') 생략 차이만 허용하되 타사 브랜드('푸르밀'·'오늘좋은' 등) 혼입은 완전 차단
                q_strip_ys = re.sub(r"^(?:연세대학교|연세우유|연세|세브란스)", "", q_core)
                r_strip_ys = re.sub(r"^(?:연세대학교|연세우유|연세|세브란스)", "", rc_core)
                if len(q_strip_ys) >= 3 and q_strip_ys == r_strip_ys:
                    cand_matched = True
                    break
            else:
                if q_core == rc_core or (len(q_core) >= 2 and q_core in rc_core):
                    cand_matched = True
                    break
                if sig_toks and all(t in rc_core for t in sig_toks):
                    cand_matched = True
                    break

        if not cand_matched:
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
        yr_4 = q_info["iso_full"][:4]
        mm_dd_dot = q_info["mm_dd_dot"]
        if f"{yr_4}.{mm_dd_dot}" in raw_s or f"{yr_2}.{mm_dd_dot}" in raw_s:
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

        # 실적이 1건이라도 입력된 가장 최신 회계연도를 자동으로 당해 대표 연도로 선택 (빈 양식만 추가된 경우 기존 연도 유지)
        active_yr = years_found[0]
        for y_cand in years_found:
            r_sum_cand = next((r for r in cat_list if r["연도"] == y_cand and r["유형"] == "합계"), None)
            if r_sum_cand and (r_sum_cand.get("합계_생산수량", 0) > 0 or r_sum_cand.get("합계_건수", 0) > 0):
                active_yr = y_cand
                break
            sub_has = any((r.get("합계_생산수량", 0) > 0 or r.get("합계_건수", 0) > 0) for r in cat_list if r["연도"] == y_cand)
            if sub_has:
                active_yr = y_cand
                break

        m_yr_num = re.search(r"(\d{4})", active_yr)
        prev_yr_cand = f"{int(m_yr_num.group(1)) - 1}년" if m_yr_num else ""
        yr_prev = prev_yr_cand if prev_yr_cand in years_found else next((y for y in years_found if y < active_yr), "")

        row_cur = next((r for r in cat_list if r["연도"] == active_yr and r["유형"] == "합계"), None)
        row_prev = next((r for r in cat_list if r["연도"] == yr_prev and r["유형"] == "합계"), None) if yr_prev else None

        trend_rows = []
        for m in FISCAL_MONTHS:
            c_cur = int(row_cur[f"{m}_건수"]) if row_cur else 0
            prod_cur = int(row_cur[f"{m}_생산수량"]) if row_cur else 0
            p_cur = float(row_cur[f"{m}_PPM"]) if row_cur else 0.0
            c_prv = int(row_prev[f"{m}_건수"]) if row_prev else 0
            p_prv = float(row_prev[f"{m}_PPM"]) if row_prev else 0.0
            trend_rows.append({
                "월": m,
                "당해_연도": active_yr,
                "전년_연도": yr_prev or "전년",
                "당해_건수": c_cur,
                "당해_생산수량": prod_cur,
                "당해_PPM": p_cur,
                "전년_건수": c_prv,
                "전년_PPM": p_prv,
                # 기존 키 하위 호환 유지
                "2026년_건수": c_cur,
                "2026년_생산수량": prod_cur,
                "2026년_PPM": p_cur,
                "2025년_건수": c_prv,
                "2025년_PPM": p_prv,
                "목표_PPM": 1.12,
                "_기록여부": (prod_cur > 0 or c_cur > 0)
            })
        return trend_rows, cat_list, None
    except Exception as e:
        return [], [], str(e)


def build_year_total_claim_pack(cat_list: list, year_str: str, prev_year_str: str = "") -> dict:
    """선택한 임의 연도(2025·2026·2027년 등)에 대한 전사 합계 추이 및 유형별 이상치 제거 계절성 예측 산출"""
    row_cur = next((r for r in cat_list if r["연도"] == year_str and r["유형"] == "합계"), None)
    row_prev = next((r for r in cat_list if r["연도"] == prev_year_str and r["유형"] == "합계"), None) if prev_year_str else None

    cur_ppms, cur_prods, cur_cnts, raw_prev_ppms = [], [], [], []
    monthly_rows = []
    for m in FISCAL_MONTHS:
        cnt_v = int(row_cur.get(f"{m}_건수", 0)) if row_cur else 0
        prd_v = int(row_cur.get(f"{m}_생산수량", 0)) if row_cur else 0
        ppm_v = float(row_cur.get(f"{m}_PPM", 0.0)) if row_cur else 0.0
        p_cnt_v = int(row_prev.get(f"{m}_건수", 0)) if row_prev else 0
        p_ppm_v = float(row_prev.get(f"{m}_PPM", 0.0)) if row_prev else 0.0
        cur_ppms.append(ppm_v)
        cur_prods.append(prd_v)
        cur_cnts.append(cnt_v)
        raw_prev_ppms.append(p_ppm_v)
        monthly_rows.append({
            "월": m,
            "당해_건수": cnt_v,
            "당해_생산수량": prd_v,
            "당해_PPM": ppm_v,
            "전년_건수": p_cnt_v,
            "전년_PPM": p_ppm_v,
            "_기록여부": (prd_v > 0 or cnt_v > 0)
        })

    # 표에는 실제 전년 실적(3.1 PPM)을 그대로 남기되, 예측 모델에는 세부 유형 단위로 사고 건수(97건)를 제거한 순수 계절 PPM 주입!
    clean_prev_seasonal = compute_clean_prev_seasonal_ppms(cat_list, prev_year_str, None)
    if clean_prev_seasonal is None and row_prev:
        clean_prev_seasonal = despike_prev_year_series(raw_prev_ppms)

    fc_res = calc_seasonal_claim_forecast(cur_ppms, cur_prods, cur_cnts, clean_prev_seasonal)
    return {
        "year": year_str,
        "prev_year": prev_year_str,
        "total_cnt": int(row_cur["합계_건수"]) if row_cur else 0,
        "total_prod": int(row_cur["합계_생산수량"]) if row_cur else 0,
        "total_ppm": float(row_cur["합계_PPM"]) if row_cur else 0.0,
        "monthly": monthly_rows,
        **fc_res
    }


def build_division_claim_summary(cat_list: list, year_str: str, division_categories: list, prev_year_str: str = "") -> dict:
    sub = [r for r in cat_list if r["연도"] == year_str and r["유형"] in division_categories]
    sub_prev = [r for r in cat_list if r["연도"] == prev_year_str and r["유형"] in division_categories] if prev_year_str else []

    monthly = []
    cur_ppms, cur_prods, cur_cnts, raw_prev_ppms = [], [], [], []
    for m in FISCAL_MONTHS:
        cnt_sum = sum(int(r.get(f"{m}_건수", 0)) for r in sub)
        prod_sum = sum(int(r.get(f"{m}_생산수량", 0)) for r in sub)
        ppm_val = round((cnt_sum / prod_sum) * 1_000_000, 2) if prod_sum > 0 else 0.0

        p_cnt_sum = sum(int(r.get(f"{m}_건수", 0)) for r in sub_prev)
        p_prod_sum = sum(int(r.get(f"{m}_생산수량", 0)) for r in sub_prev)
        p_ppm_val = round((p_cnt_sum / p_prod_sum) * 1_000_000, 2) if p_prod_sum > 0 else 0.0

        cur_ppms.append(ppm_val)
        cur_prods.append(prod_sum)
        cur_cnts.append(cnt_sum)
        raw_prev_ppms.append(p_ppm_val)

        item = {
            "월": m,
            "합산_건수": cnt_sum,
            "합산_생산수량": prod_sum,
            "부문_통합_PPM": ppm_val,
            "전년_부문_PPM": p_ppm_val,
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

    clean_prev_div = compute_clean_prev_seasonal_ppms(cat_list, prev_year_str, division_categories)
    if clean_prev_div is None and sub_prev:
        clean_prev_div = despike_prev_year_series(raw_prev_ppms)

    fc_res = calc_seasonal_claim_forecast(cur_ppms, cur_prods, cur_cnts, clean_prev_div)

    return {
        "year": year_str,
        "prev_year": prev_year_str,
        "categories": division_categories,
        "total_cnt": tot_c,
        "total_prod": tot_p,
        "total_ppm": tot_ppm,
        "slope": fc_res["slope"],
        "feb_pred": fc_res["feb_pred"],
        "cum_pred_ppm": fc_res["cum_pred_ppm"],
        "last_idx": fc_res["last_idx"],
        "act_line": fc_res["act_line"],
        "fc_line": fc_res["fc_line"],
        "monthly": monthly,
        "raw": sub
    }
# ============================ [api/index.py - PART 1 / 4 끝] ============================
# ==============================================================================
# [api/index.py - PART 2 / 4]
# 4대 현장·실험실 일보 (상온 · 유제품[카톤/병 분리] · 두유배양 · 미생물 3탭 상시연동) 파서
# ==============================================================================
def heal_db_worksheet_if_dirty(sh, tab_title: Optional[str], raw_rows: list, row_fixer_fn) -> bool:
    """
    읽어온 DB 시트 행(raw_rows)에 잘못 저장된 날짜·품목명·주석이 있는지 검사하여,
    수정이 필요한 셀이 1개라도 있을 때만 구글 시트 원본 탭에 즉시 덮어쓰기(영구 교정) 수행.
    이미 정상인 경우에는 API 호출을 하지 않아 속도를 100% 유지함.
    """
    if sh is None or not tab_title or not raw_rows or len(raw_rows) < 2:
        return False

    hdr = [str(h).strip() for h in raw_rows[0]]
    col_idx_map = {h: i for i, h in enumerate(hdr) if h}
    new_matrix = [hdr]
    dirty = False

    for r in raw_rows[1:]:
        if not any(str(x).strip() != "" for x in r):
            continue
        row_vals = [str(x).strip() for x in r] + [""] * max(0, len(hdr) - len(r))
        row_vals = row_vals[:len(hdr)]
        orig_copy = list(row_vals)

        row_fixer_fn(row_vals, col_idx_map)

        if row_vals != orig_copy:
            dirty = True
        new_matrix.append(row_vals)

    if dirty:
        try:
            ws = api_call_with_retry(sh.worksheet, tab_title)
            api_call_with_retry(ws.clear)
            safe_ws_update(ws, new_matrix)
            return True
        except Exception:
            return False
    return False
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
        pattern_ho = rf"{ho_num}호기[^\d]*(\d+(?:\.\d+)?)"
        m_specific = re.search(pattern_ho, std_s)
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


def tab_title_to_iso_date(tab_title: str, ref_date: Optional[date] = None) -> Optional[str]:
    """상온 일보의 '=TODAY()' 수식 왜곡 방지를 위해 하단 탭 이름(예: 11/18)을 YYYY-MM-DD로 변환"""
    s = str(tab_title or "").strip()
    m = re.match(r"^(\d{1,2})[/\.\-](\d{1,2})(?![/\.\-\d])", s)
    if not m:
        return None
    mm, dd = int(m.group(1)), int(m.group(2))
    today_d = now_kst().date()
    if ref_date and abs((today_d - ref_date).days) > 200:
        yr = ref_date.year
    else:
        yr = today_d.year if mm <= today_d.month + 1 else today_d.year - 1
    try:
        return date(yr, mm, dd).strftime("%Y-%m-%d")
    except ValueError:
        return None

def add_months_inclusive(d: date, months: int) -> date:
    """제조일 산입(초일산입) N개월 소비기한 계산 (예: 2026-11-18 + 7개월 -> 2027-06-17, 말일 보정 포함)"""
    total_m = (d.year * 12 + (d.month - 1)) + months
    ny = total_m // 12
    nm = (total_m % 12) + 1
    max_day = 31 if nm == 12 else (date(ny, nm + 1, 1) - timedelta(days=1)).day
    if d.day == 1:
        return date(ny, nm, 1) - timedelta(days=1)
    return date(ny, nm, min(d.day - 1, max_day))


def extract_duration_spec_token(val: Any) -> str:
    """셀 문자열에서 '=TODAY()' 날짜를 배제하고 순수 기간 기준(예: '8개월', '18주', '180일', '1년')만 추출"""
    s = re.sub(r"\s+", "", str(val or ""))
    if not s or s == "-":
        return ""
    m_mo = re.search(r"(?<!\d)(\d{1,2})개월", s)
    if m_mo and 1 <= int(m_mo.group(1)) <= 36:
        return f"{int(m_mo.group(1))}개월"
    m_wk = re.search(r"(?<!\d)(\d{1,3})주", s)
    if m_wk and 1 <= int(m_wk.group(1)) <= 104:
        return f"{int(m_wk.group(1))}주"
    m_dy = re.search(r"(?<!\d)(\d{1,4})일", s)
    if m_dy and 5 <= int(m_dy.group(1)) <= 1095:
        return f"{int(m_dy.group(1))}일"
    m_yr = re.search(r"(?<!\d)([1-3])년", s)
    if m_yr:
        return f"{int(m_yr.group(1)) * 12}개월"
    return ""


def calc_rt_limit_date_from_spec(prod_d: date, spec_str: str) -> Optional[date]:
    s = str(spec_str or "").strip()
    if not s:
        return None
    m_mo = re.search(r"(\d+)개월", s)
    if m_mo:
        return add_months_inclusive(prod_d, int(m_mo.group(1)))
    m_wk = re.search(r"(\d+)주", s)
    if m_wk:
        return prod_d + timedelta(days=int(m_wk.group(1)) * 7 - 1)
    m_dy = re.search(r"(\d+)일", s)
    if m_dy:
        return prod_d + timedelta(days=int(m_dy.group(1)) - 1)
    return None


def parse_exp_dates_against_prod(exp_raw: Any, prod_d: date, expected_days: int = 14) -> List[date]:
    """
    표기 소비기한 문자열에서 복수 날짜(예: '11.30, 12.02', '2027. 06. 17 F1')를 모두 추출하고,
    구글 시트 숫자 서식으로 뒷자리 0이 잘린 '11.3'(-> 11.30), '12.1'(-> 12.10), '12.2'(-> 12.20)를 정밀 복원
    """
    s = str(exp_raw or "").strip()
    if not s or s in ("-", "00.00", "0.0", "미기입", "미기입(대기)"):
        return []

    out: List[date] = []
    # 1) 연도가 포함된 날짜 (YYYY.MM.DD 또는 YY.MM.DD)
    used_spans = []
    for m4 in re.finditer(r"(20\d{2})[\.\-/\s]+(\d{1,2})[\.\-/\s]+(\d{1,2})(?!\d)", s):
        try:
            d_obj = date(int(m4.group(1)), int(m4.group(2)), int(m4.group(3)))
            if d_obj not in out:
                out.append(d_obj)
            used_spans.append((m4.start(), m4.end()))
        except ValueError:
            pass

    s_rem = list(s)
    for st, ed in used_spans:
        for i in range(st, ed):
            s_rem[i] = " "
    s2 = "".join(s_rem)

    used_spans2 = []
    for m2 in re.finditer(r"(?<!\d)(\d{2})[\.\-/]\s*(\d{1,2})[\.\-/]\s*(\d{1,2})(?!\d)", s2):
        yr2, mm2, dd2 = int(m2.group(1)), int(m2.group(2)), int(m2.group(3))
        if 24 <= yr2 <= 35 and 1 <= mm2 <= 12 and 1 <= dd2 <= 31:
            try:
                d_obj = date(2000 + yr2, mm2, dd2)
                if d_obj not in out:
                    out.append(d_obj)
                used_spans2.append((m2.start(), m2.end()))
            except ValueError:
                pass

    s_rem2 = list(s2)
    for st, ed in used_spans2:
        for i in range(st, ed):
            s_rem2[i] = " "
    s3 = "".join(s_rem2)

    # 2) 월.일 단축 표기 (예: '12.05', '11.30, 12.02', 시트 자동숫자 '11.3' -> 11.30 복원)
    for ms in re.finditer(r"(?<!\d)(\d{1,2})[\./](\d{1,2})(?![\.\d])", s3):
        mm = int(ms.group(1))
        d_str = ms.group(2)
        dd = int(d_str)
        if not (1 <= mm <= 12 and 1 <= dd <= 31):
            continue

        def _resolve_yr(m_val: int, d_val: int) -> Optional[date]:
            yr = prod_d.year
            if prod_d.month >= 11 and m_val <= 2:
                yr += 1
            elif prod_d.month == 1 and m_val == 12:
                yr -= 1
            try:
                return date(yr, m_val, d_val)
            except ValueError:
                return None

        cand_single = _resolve_yr(mm, dd)
        cand_ten = _resolve_yr(mm, dd * 10) if (len(d_str) == 1 and dd in (1, 2, 3)) else None

        chosen = cand_single
        if cand_ten is not None:
            if cand_single is None or (cand_single <= prod_d and cand_ten > prod_d):
                chosen = cand_ten
            elif cand_single > prod_d and cand_ten > prod_d:
                target_d = prod_d + timedelta(days=expected_days)
                if abs((cand_ten - target_d).days) + 3 < abs((cand_single - target_d).days):
                    chosen = cand_ten

        if chosen and chosen not in out:
            out.append(chosen)

    out.sort()
    return out


def extract_rt_shelf_specs_from_rows(rows: list, spec_map: Dict[str, dict]):
    """상온 시트 하단 기준표(품목코드+제품명 구조 포함) 및 '제품명 식품안전나라' 탭에서 품목제조·실운영 기간 추출"""
    if not rows:
        return
    c_prod, c_mfg, c_ops, c_exp_ops = -1, -1, -1, -1
    for r in rows:
        if not r:
            continue
        r_ns = [re.sub(r"\s+", "", str(x)) for x in r[:28]]
        joined = "".join(r_ns)
        if ("제품명" in joined or "품목명" in joined or "품목제조" in joined) and ("실운영" in joined or "소비기한" in joined):
            for idx, h in enumerate(r_ns):
                if h in ("제품명", "품목명", "품목", "호기별제품") and c_prod < 0:
                    c_prod = idx
                elif "품목제조" in h and c_mfg < 0:
                    c_mfg = idx
                elif "수출" in h and "실운영" in h and c_exp_ops < 0:
                    c_exp_ops = idx
                elif h == "실운영" and c_ops < 0:
                    c_ops = idx
            continue

        if c_prod >= 0 and c_prod < len(r):
            raw_cell = re.sub(r"\s+", " ", str(r[c_prod])).strip()
            # 앞열이 5자리 품목코드(예: 10474)인 경우 바로 옆 열의 실제 제품명을 참조
            if re.match(r"^\d{4,6}$", raw_cell) and (c_prod + 1) < len(r):
                raw_cell = re.sub(r"\s+", " ", str(r[c_prod + 1])).strip()
            pk = clean_product_for_match(raw_cell)
            if len(pk) >= 2 and not pk.isdigit():
                mfg_t = extract_duration_spec_token(r[c_mfg]) if (0 <= c_mfg < len(r)) else ""
                ops_t = extract_duration_spec_token(r[c_ops]) if (0 <= c_ops < len(r)) else ""
                exp_t = extract_duration_spec_token(r[c_exp_ops]) if (0 <= c_exp_ops < len(r)) else ""
                if mfg_t or ops_t or exp_t:
                    cur = spec_map.setdefault(pk, {"mfg": "", "ops": "", "exp_ops": ""})
                    if mfg_t:
                        cur["mfg"] = mfg_t
                    if ops_t:
                        cur["ops"] = ops_t
                    if exp_t:
                        cur["exp_ops"] = exp_t
            continue

        # 헤더 없는 하단 기준표 스캔: 앞쪽 품목코드(숫자)는 건너뛰고 첫 한글/영문 품목명 + 우측 기간(개월·주·일) 추출
        cand_p, p_col = "", -1
        for ci in range(min(5, len(r))):
            cs = re.sub(r"\s+", " ", str(r[ci])).strip()
            if not cs or cs == "-" or re.match(r"^\d+(?:\.\d+)?$", cs):
                continue
            if re.match(r"^[A-F0-9]+호기$", cs) or cs in ("포장호기", "멸균기", "특이사항", "구분"):
                break
            if re.search(r"[가-힣A-Za-z]", cs):
                cand_p, p_col = cs, ci
                break

        pk2 = clean_product_for_match(cand_p)
        if len(pk2) >= 2 and p_col >= 0:
            durs = [extract_duration_spec_token(x) for x in r[p_col + 1 : min(len(r), p_col + 14)]]
            durs = [x for x in durs if x]
            if durs:
                cur = spec_map.setdefault(pk2, {"mfg": "", "ops": "", "exp_ops": ""})
                if not cur["mfg"]:
                    cur["mfg"] = durs[0]
                if len(durs) >= 2 and not cur["ops"]:
                    cur["ops"] = durs[1]
                if len(durs) >= 3 and not cur["exp_ops"]:
                    cur["exp_ops"] = durs[2]


def _norm_rt_pack_group(ho_raw: Any) -> str:
    """
    상온 포장재 동일성 기준 호기 그룹핑:
    - B호기 · D호기 : 동일 포장재를 공용하므로 'B·D호기' 단일 재고 그룹으로 통합 비교
    - A호기 · C호기 · E호기 · F호기 : 포장재가 서로 다르므로 각각 독립 호기로 분리 비교
    """
    h = re.sub(r"\s+", "", str(ho_raw or "")).upper()
    if h in ("B호기", "D호기", "B", "D"):
        return "B·D호기"
    return h or "-"


def fix_rt_today_shifted_exp(
    exp_raw: str,
    prod_iso: str,
    ho_raw: str,
    p_name: str,
    spec_map: Dict[str, dict],
    saved_ts_raw: str = ""
) -> Tuple[str, str]:
    """
    과거 날짜 탭의 소비기한 셀에 '=TODAY()' 연동 수식이 남아 있어 조회일/저장일 기준으로
    소비기한이 밀려버린 경우, 해당 포장재 그룹(B·D 통합 / A·C·E·F 개별)·품목 기준을 대조해
    원래 생산일(prod_d) 기준 소비기한으로 복원한다.
    """
    raw_s = re.sub(r"\s+", " ", str(exp_raw or "")).strip()
    prod_d = parse_date_obj(prod_iso)
    today_d = now_kst().date()
    if not raw_s or raw_s == "-" or not prod_d:
        return raw_s, ""

    exp_dates = parse_exp_dates_against_prod(raw_s, prod_d, expected_days=210)
    if not exp_dates:
        return raw_s, ""

    std_iso = ", ".join([d.strftime("%Y-%m-%d") for d in exp_dates])
    if prod_d >= today_d or len(exp_dates) != 1:
        return raw_s, std_iso

    actual_e = exp_dates[0]
    ref_cands = {today_d - timedelta(days=k) for k in range(4)}
    saved_d = parse_date_obj(saved_ts_raw)
    if saved_d and saved_d > prod_d:
        ref_cands.add(saved_d)

    ho_norm = re.sub(r"\s+", "", str(ho_raw or "").strip()).upper()
    ho_grp = _norm_rt_pack_group(ho_norm)
    pk = clean_product_for_match(p_name)
    spec = spec_map.get(f"{ho_norm}|{pk}") or spec_map.get(f"{ho_grp}|{pk}") or spec_map.get(pk, {})
    cand_specs = []
    for k_sp in ("ops", "mfg", "exp_ops"):
        sv = spec.get(k_sp, "")
        if sv and sv not in cand_specs:
            cand_specs.append(sv)

    for ref_d in sorted(ref_cands, reverse=True):
        gap_days = (ref_d - prod_d).days
        if gap_days < 2 or (ref_d != today_d and ref_d != saved_d and gap_days < 3):
            continue

        for sp_tok in cand_specs:
            shifted_lim = calc_rt_limit_date_from_spec(ref_d, sp_tok)
            if shifted_lim and actual_e == shifted_lim:
                true_lim = calc_rt_limit_date_from_spec(prod_d, sp_tok)
                if true_lim:
                    return true_lim.strftime("%Y. %m. %d"), true_lim.strftime("%Y-%m-%d")

        if not cand_specs and gap_days >= 4:
            for m_try in range(3, 19):
                if actual_e == add_months_inclusive(ref_d, m_try):
                    true_lim = add_months_inclusive(prod_d, m_try)
                    return true_lim.strftime("%Y. %m. %d"), true_lim.strftime("%Y-%m-%d")

    return raw_s, std_iso


def validate_rt_shelf_life_on_db(rt_hist_p: List[dict], spec_map: Dict[str, dict]) -> Dict[str, List[dict]]:
    """
    [_이력DB_상온생산] 확정 데이터 기준 검증:
    1) 포장재 동일 그룹([B·D호기 통합] vs [A / C / E / F호기 개별]) + 제품명 + 수출여부 단위로 시계열 그룹핑
    2) 호기별 품목제조보고 기간 초과(빨강) 및 실운영 기간 초과(노랑) 판별
    3) 동일 포장재 그룹 내 직전 생산 재고 소비기한 대비 날짜 역전(감소) 발생 시 즉시 부적합(빨강)
    """
    grouped: Dict[Tuple[str, str, bool], List[dict]] = {}
    for rec in rt_hist_p:
        ho_grp = _norm_rt_pack_group(rec.get("포장호기", ""))
        p_nm = str(rec.get("제품명", "")).strip()
        exp_raw = str(rec.get("표기소비기한_원문", "")).strip()
        is_exp_lot = ("수출" in p_nm) or ("export" in p_nm.lower()) or ("수출" in exp_raw)
        gk = (ho_grp, clean_product_for_match(p_nm), is_exp_lot)
        grouped.setdefault(gk, []).append(rec)

    alerts_by_date: Dict[str, List[dict]] = {}

    for (ho_grp, pk, is_exp_lot), recs in grouped.items():
        recs.sort(key=lambda x: (str(x.get("생산일자", "")), str(x.get("포장호기", ""))))

        prev_prod_iso = ""
        prev_ho_actual = ""
        prev_max_exp: Optional[date] = None
        prev_exp_raw = ""

        for r in recs:
            dt_iso = str(r.get("생산일자", "")).strip()
            ho = str(r.get("포장호기", ho_grp)).strip()
            ho_norm = re.sub(r"\s+", "", ho).upper()
            p_nm = str(r.get("제품명", "-")).strip()
            exp_raw = str(r.get("표기소비기한_원문", "-")).strip()
            prod_d = parse_date_obj(dt_iso)

            spec = spec_map.get(f"{ho_norm}|{pk}") or spec_map.get(f"{ho_grp}|{pk}") or spec_map.get(pk, {})
            mfg_s = spec.get("mfg", "")
            ops_s = spec.get("exp_ops", "") if (is_exp_lot and spec.get("exp_ops")) else (spec.get("ops") or mfg_s)
            exp_ops_s = spec.get("exp_ops", "")

            if not prod_d:
                r["_cls_exp"] = ""
                r["_exp_tip"] = ""
                continue

            exp_dates = parse_exp_dates_against_prod(exp_raw, prod_d, expected_days=210)
            if not exp_dates:
                r["_cls_exp"] = "cell-red"
                r["_exp_badge"] = "미기입"
                msg = f"[상온 {ho} · {p_nm}] 소비기한이 미기입 상태입니다. 지금 확인이 필요합니다."
                r["_exp_tip"] = msg
                r["_exp_alert"] = msg
                alerts_by_date.setdefault(dt_iso, []).append({"level": "red", "msg": msg})
                continue

            min_e, max_e = min(exp_dates), max(exp_dates)
            mfg_lim = calc_rt_limit_date_from_spec(prod_d, mfg_s)
            ops_lim = calc_rt_limit_date_from_spec(prod_d, ops_s)
            exp_ops_lim = calc_rt_limit_date_from_spec(prod_d, exp_ops_s)

            matches_export = bool(exp_ops_lim and abs((max_e - exp_ops_lim).days) <= 1)
            eff_mfg_lim = max([d for d in [mfg_lim, exp_ops_lim if matches_export else None] if d is not None], default=None)

            cls_v = "cell-green"
            tip_parts = []
            if ops_lim:
                tip_parts.append(f"실운영({ops_s}): {ops_lim.strftime('%Y.%m.%d')}")
            if mfg_lim and mfg_s != ops_s:
                tip_parts.append(f"품목제조({mfg_s}): {mfg_lim.strftime('%Y.%m.%d')}")
            if prev_max_exp and prev_prod_iso < dt_iso:
                tip_parts.append(f"직전 {prev_ho_actual}({prev_prod_iso[5:]}): {prev_max_exp.strftime('%Y.%m.%d')}")

            alert_msg = ""
            alert_lv = ""

            if min_e <= prod_d:
                cls_v = "cell-red"
                alert_lv = "red"
                alert_msg = f"[상온 {ho} · {p_nm}] 표기 소비기한({exp_raw})이 생산일({dt_iso}) 이전으로 오기입되었습니다. 지금 확인이 필요합니다."
            elif eff_mfg_lim and max_e > eff_mfg_lim:
                cls_v = "cell-red"
                alert_lv = "red"
                alert_msg = f"[상온 {ho} · {p_nm}] 표기 소비기한({exp_raw})이 품목제조보고 상한({mfg_s}, {eff_mfg_lim.strftime('%Y.%m.%d')})을 초과했습니다. 지금 확인이 필요합니다."
            elif prev_max_exp and prev_prod_iso < dt_iso and (not matches_export) and min_e < prev_max_exp:
                cls_v = "cell-red"
                alert_lv = "red"
                grp_lbl = f"동일 포장재({prev_ho_actual})" if prev_ho_actual != ho else f"동일 호기({ho})"
                alert_msg = f"[상온 {ho} · {p_nm}] 표기 소비기한({exp_raw})이 {grp_lbl} 직전 생산 재고({prev_prod_iso[5:].replace('-', '/')} 생산, {prev_exp_raw})보다 짧게 역전 기입되었습니다. 지금 확인이 필요합니다."
            elif ops_lim and (not matches_export) and max_e > (ops_lim + timedelta(days=1)):
                cls_v = "cell-yellow"
                alert_lv = "yellow"
                mfg_note = f" / 품목제조 {mfg_s} 이내" if eff_mfg_lim else ""
                alert_msg = f"[상온 {ho} · {p_nm}] 표기 소비기한({exp_raw})이 실운영 기준({ops_s}, {ops_lim.strftime('%Y.%m.%d')})보다 길게 기입되었습니다{mfg_note}. 연휴·수출 적용 확인이 필요합니다."
            elif ops_lim and (not matches_export) and min_e < (ops_lim - timedelta(days=15)):
                cls_v = "cell-yellow"
                alert_lv = "yellow"
                alert_msg = f"[상온 {ho} · {p_nm}] 표기 소비기한({exp_raw})이 실운영 기준({ops_lim.strftime('%Y.%m.%d')}) 대비 15일 이상 짧습니다. 연·월 오타 확인이 필요합니다."

            r["_cls_exp"] = cls_v
            r["_exp_tip"] = alert_msg if alert_msg else ("정상 (" + " / ".join(tip_parts) + ")" if tip_parts else "소비기한 정상")
            r["_exp_alert"] = alert_msg
            if alert_msg:
                alerts_by_date.setdefault(dt_iso, []).append({"level": alert_lv, "msg": alert_msg})

            if min_e > prod_d and (eff_mfg_lim is None or max_e <= eff_mfg_lim) and not matches_export:
                if prev_prod_iso != dt_iso:
                    prev_prod_iso = dt_iso
                    prev_max_exp = max_e
                    prev_exp_raw = exp_raw
                    prev_ho_actual = ho
                elif prev_max_exp is None or max_e >= prev_max_exp:
                    prev_max_exp = max_e
                    prev_exp_raw = exp_raw
                    prev_ho_actual = f"{prev_ho_actual}·{ho}" if ho not in prev_ho_actual else prev_ho_actual

    return alerts_by_date


def _ferm_sub_kind(ln_str: str, sec_str: str = "") -> str:
    s = re.sub(r"\s+", "", f"{ln_str} {sec_str}")
    if "호상" in s:
        return "호상"
    if "병" in s:
        return "병발효"
    if "카톤" in s:
        return "카톤발효"
    return "발효유"


def validate_dairy_shelf_life_on_db(dairy_hist_p: List[dict], ferm_days_map: Dict[Tuple[str, str], int]) -> Dict[str, List[dict]]:
    """
    [_이력DB_유제품포장] 확정 데이터 기준 검증:
    - 우유(카톤·병): 생산일 + 14일(제조일 포함 15일) 초과 시 부적합(빨강) / 전일 부족분 동일 날짜 중복은 적합(초록) 허용 / 본생산 소비기한 역전 시(빨강)
    - 발효유·호상: 세부유형(호상/병/카톤)별 최대 소비기한(생산일 + N - 1일) 초과 시 부적합(빨강) / 직전 본생산(max_e) 대비 당일 본생산(max_e) 역전 시(빨강)
    """
    grouped: Dict[Tuple[bool, str, str], List[dict]] = {}
    for rec in dairy_hist_p:
        ln = str(rec.get("라인구분", "")).strip()
        sec = str(rec.get("세부유형", "")).strip()
        p_nm = str(rec.get("제품명", "")).strip()
        is_ferm = ("발효" in ln) or ("호상" in ln) or ("발효" in sec) or ("호상" in sec)
        ln_grp = _ferm_sub_kind(ln, sec) if is_ferm else ("우유(병)" if "병" in ln else "우유(카톤)")
        gk = (is_ferm, ln_grp, clean_product_for_match(p_nm))
        grouped.setdefault(gk, []).append(rec)

    alerts_by_date: Dict[str, List[dict]] = {}

    for (is_ferm, ln_grp, pk), recs in grouped.items():
        recs.sort(key=lambda x: str(x.get("생산일자", "")))
        ferm_n_days = ferm_days_map.get((ln_grp, pk), 0) or ferm_days_map.get(("발효유", pk), 0)
        if is_ferm and ferm_n_days <= 0:
            # 반드시 동일한 세부유형(예: 호상끼리만, 병발효끼리만) 안에서만 연세/세브란스 접두어 차이 허용!
            pk_strip = re.sub(r"^(?:연세대학교|연세우유|연세|세브란스)", "", pk)
            if len(pk_strip) >= 2:
                for (m_grp, map_k), map_d in ferm_days_map.items():
                    if m_grp != ln_grp:
                        continue
                    mk_strip = re.sub(r"^(?:연세대학교|연세우유|연세|세브란스)", "", map_k)
                    if mk_strip == pk_strip:
                        ferm_n_days = map_d
                        break

        prev_prod_d: Optional[date] = None
        prev_min_e: Optional[date] = None
        prev_max_e: Optional[date] = None

        for r in recs:
            dt_iso = str(r.get("생산일자", "")).strip()
            ln_disp = str(r.get("라인구분", ln_grp)).strip()
            p_nm = str(r.get("제품명", "-")).strip()
            exp_raw = str(r.get("표기소비기한_원문", "-")).strip()
            prod_d = parse_date_obj(dt_iso)

            if not prod_d:
                r["_cls_exp"] = ""
                r["_exp_tip"] = ""
                continue

            exp_offset_hint = (ferm_n_days - 1) if (is_ferm and ferm_n_days > 0) else (22 if ln_grp == "호상" else (18 if is_ferm else 12))
            exp_dates = parse_exp_dates_against_prod(exp_raw, prod_d, expected_days=exp_offset_hint)

            if not exp_dates:
                r["_cls_exp"] = "cell-yellow"
                msg = f"[{ln_disp} · {p_nm}] 표기 소비기한이 미기입 상태입니다. 지금 확인이 필요합니다."
                r["_exp_tip"] = msg
                r["_exp_alert"] = msg
                alerts_by_date.setdefault(dt_iso, []).append({"level": "yellow", "msg": msg})
                continue

            min_e, max_e = min(exp_dates), max(exp_dates)
            limit_d = (
                (prod_d + timedelta(days=ferm_n_days - 1)) if (is_ferm and ferm_n_days > 0)
                else ((prod_d + timedelta(days=14)) if not is_ferm else None)
            )

            cls_v = "cell-green"
            alert_msg = ""
            alert_lv = ""

            if min_e <= prod_d:
                cls_v = "cell-red"
                alert_lv = "red"
                alert_msg = f"[{ln_disp} · {p_nm}] 표기 소비기한({exp_raw})이 생산일({dt_iso[5:].replace('-', '/')}) 이전으로 오기입되었습니다. 지금 확인이 필요합니다."
            elif limit_d and max_e > limit_d:
                cls_v = "cell-red"
                alert_lv = "red"
                std_lbl = f"최대 소비기한({ferm_n_days}일 기준, {limit_d.strftime('%m.%d')})" if is_ferm else f"제조일+14일 상한({limit_d.strftime('%m.%d')})"
                alert_msg = f"[{ln_disp} · {p_nm}] 표기 소비기한({exp_raw})이 {std_lbl}을 초과하여 기입되었습니다. 지금 확인이 필요합니다."
            elif is_ferm and prev_max_e and prev_prod_d and prev_prod_d < prod_d and (prod_d - prev_prod_d).days <= 30 and max_e < prev_max_e:
                cls_v = "cell-red"
                alert_lv = "red"
                alert_msg = f"[{ln_disp} · {p_nm}] 당일 소비기한({max_e.strftime('%m.%d')})이 직전 생산 재고({prev_prod_d.strftime('%m/%d')} 생산, {prev_max_e.strftime('%m.%d')})보다 짧게 역전 기입되었습니다. 지금 확인이 필요합니다."
            elif (not is_ferm) and prev_min_e and prev_prod_d and prev_prod_d < prod_d and (prod_d - prev_prod_d).days <= 10 and max_e < prev_min_e:
                cls_v = "cell-red"
                alert_lv = "red"
                alert_msg = f"[{ln_disp} · {p_nm}] 표기 소비기한({exp_raw})이 직전 생산({prev_prod_d.strftime('%m/%d')} 생산, {prev_min_e.strftime('%m.%d')})보다 과거 날짜로 역전 기입되었습니다. 지금 확인이 필요합니다."

            tip_txt = alert_msg
            if not tip_txt:
                lim_s = f"상한 {limit_d.strftime('%m.%d')}" if limit_d else "기준 이내"
                prv_s = f" / 직전 {(prev_max_e if is_ferm else prev_min_e).strftime('%m.%d')}" if (prev_prod_d and prev_prod_d < prod_d and (prev_max_e if is_ferm else prev_min_e)) else ""
                tip_txt = f"소비기한 정상 ({lim_s}{prv_s})"

            r["_cls_exp"] = cls_v
            r["_exp_tip"] = tip_txt
            r["_exp_alert"] = alert_msg
            if alert_msg:
                alerts_by_date.setdefault(dt_iso, []).append({"level": alert_lv, "msg": alert_msg})

            if min_e > prod_d and (limit_d is None or max_e <= limit_d):
                prev_prod_d = prod_d
                prev_min_e = min_e
                prev_max_e = max_e

    return alerts_by_date

def parse_rt_process_rows(tab_title_raw: str, rows: list):
    if not tab_title_raw or not rows or len(rows) < 5:
        return None

    tab_title = tab_title_raw.strip()
    prod_date_str = tab_title_to_iso_date(tab_title) or tab_title

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
    slot_spec_map: Dict[str, dict] = {}
    extract_rt_shelf_specs_from_rows(rows[35:], slot_spec_map)
    current_ho = ""
    for r in rows[r_pack_hdr + 1 : r_ccp3b_hdr]:
        r_pad = list(r) + [""] * max(0, 26 - len(r))
        b_val = str(r_pad[1]).strip()
        if re.match(r"^[A-Z가-힣0-9]+호기$", b_val):
            current_ho = b_val
            ho_rows_raw.setdefault(current_ho, [])
            ho_products.setdefault(current_ho, [])

        if not current_ho:
            continue

        slots = [
            (r_pad[2:6], [r_pad[6]], r_pad[7:9], r_pad[18:22]),
            (r_pad[9:13], r_pad[13:15], r_pad[15:17], r_pad[22:26])
        ]
        for p_cells, e_cells, t_cells, spec_cells in slots:
            p_name = next((str(x).strip() for x in p_cells if str(x).strip() not in ["", "제 품 명", "제  품  명"]), "")
            exp_v = next((str(x).strip() for x in e_cells if str(x).strip() != ""), "")
            t_parts = [str(x).strip() for x in t_cells if str(x).strip() != ""]
            t_str = " / ".join(t_parts)

            if p_name and len(spec_cells) >= 3:
                pk_s = clean_product_for_match(p_name)
                ho_norm_s = re.sub(r"\s+", "", current_ho).upper()
                ho_grp_s = "B·D호기" if ho_norm_s in ("B호기", "D호기", "B", "D") else (ho_norm_s or "-")
                mfg_tok = extract_duration_spec_token(spec_cells[0])
                ops_tok = extract_duration_spec_token(spec_cells[1])
                exp_ops_tok = extract_duration_spec_token(spec_cells[2])
                if pk_s and (mfg_tok or ops_tok or exp_ops_tok):
                    for sp_key in dict.fromkeys([f"{ho_norm_s}|{pk_s}", f"{ho_grp_s}|{pk_s}", pk_s]):
                        sp_entry = slot_spec_map.setdefault(sp_key, {"mfg": "", "ops": "", "exp_ops": ""})
                        if mfg_tok:
                            sp_entry["mfg"] = mfg_tok
                        if ops_tok:
                            sp_entry["ops"] = ops_tok
                        if exp_ops_tok:
                            sp_entry["exp_ops"] = exp_ops_tok

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
                    "_cls": st_cls,
                    "_cls_exp": "cell-green" if exp_v else "cell-red",
                    "_exp_tip": ""
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
    for sub_i in range(r_ccp3b_hdr, min(r_ccp3b_hdr + 4, r_ster_hdr)):
        if sum(1 for c in rows[sub_i][:26] if re.match(r"^[A-F]\s*호기$", str(c).strip())) >= 2:
            r_3b_colhdr = sub_i
            break

    hdr_3b = [re.sub(r"\s+", "", str(x)) for x in rows[r_3b_colhdr][:26]]
    ho_pos_list = sorted([(idx, cell) for idx, cell in enumerate(hdr_3b) if re.match(r"^[A-F]호기$", cell)], key=lambda x: x[0])

    first_ho_col = ho_pos_list[0][0] if ho_pos_list else 8
    ho_col_spans = {}
    for i_p, (c_start, h_lbl) in enumerate(ho_pos_list):
        if i_p + 1 < len(ho_pos_list):
            c_end = ho_pos_list[i_p + 1][0]
        else:
            c_end = min(c_start + 3, 24)
        ho_col_spans[h_lbl] = (c_start, c_end)

    last_item_3b = ""
    last_std_3b = ""
    for r in rows[r_3b_colhdr + 1 : r_ster_hdr]:
        r_pad = list(r) + [""] * max(0, 26 - len(r))
        if not any(str(x).strip() for x in r_pad[1:22]):
            continue
        item_nm = str(r_pad[1]).strip()
        if re.sub(r"\s+", "", item_nm) in ["멸균기", "구분", "점검항목"]:
            continue
        if item_nm:
            last_item_3b = item_nm

        left_meta = [str(x).strip() for x in r_pad[2:first_ho_col] if str(x).strip() and str(x).strip() != item_nm]
        std_parts = [x for x in left_meta if any(u in x for u in ["~", "%", "℃", "㎕", "이상", "이하", "±"])]
        eq_parts = [x for x in left_meta if x not in std_parts]

        std_nm = " ".join(std_parts)
        if std_nm:
            last_std_3b = std_nm
        use_std = std_nm or last_std_3b or "-"
        equip_nm = " ".join(eq_parts) or "-"

        if not last_item_3b and use_std == "-":
            continue

        rec_3b = {"항목": last_item_3b or "무균포장살균", "관리기준": use_std, "적용설비": equip_nm, "_cls_map": {}}
        has_any_ho_val = False
        for ho_lbl in ["A호기", "B호기", "C호기", "D호기", "E호기", "F호기"]:
            if ho_lbl in ho_col_spans:
                cs, ce = ho_col_spans[ho_lbl]
                vals_in_span = []
                for x in r_pad[cs:ce]:
                    xs = str(x).strip()
                    if xs not in ["", "-"] and xs not in vals_in_span:
                        vals_in_span.append(xs)
                v_txt = " / ".join(vals_in_span) if vals_in_span else "-"
            else:
                v_txt = "-"
            if v_txt != "-":
                has_any_ho_val = True
            rec_3b[ho_lbl] = v_txt
            rec_3b["_cls_map"][ho_lbl] = eval_ccp3b_cell(v_txt, rec_3b["관리기준"])

        if has_any_ho_val or std_nm:
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
        "ster_special_notes": ster_special_notes,
        "spec_map": slot_spec_map,
        "exp_alerts": []
    }


def calc_time_span_text(time_str: str) -> Tuple[str, str, str]:
    s = str(time_str or "").replace("：", ":").strip()
    times = re.findall(r"(?<!\d)(\d{1,2}:\d{2})(?!\d)", s)
    valid_times = [t for t in times if t not in ["00:00", "0:00"]]
    if not valid_times:
        return "-", "-", "비가동"

    def _to_min(hm: str) -> int:
        p = hm.split(":")
        return int(p[0]) * 60 + int(p[1])

    if len(times) >= 2:
        if len(times) >= 4 and len(times) % 2 == 0:
            total_diff = 0
            for i in range(0, len(times), 2):
                if times[i] in ["00:00", "0:00"] and times[i + 1] in ["00:00", "0:00"]:
                    continue
                m_s, m_e = _to_min(times[i]), _to_min(times[i + 1])
                if m_e < m_s:
                    m_e += 24 * 60
                total_diff += max(0, m_e - m_s)
            dur = f"{total_diff // 60}시간 {total_diff % 60:02d}분" if total_diff > 0 else "-"
            return times[0], times[-1], dur

        t_s, t_e = times[0], times[1]
        if t_s in ["00:00", "0:00"] and t_e in ["00:00", "0:00"]:
            return "-", "-", "비가동"
        s_min, e_min = _to_min(t_s), _to_min(t_e)
        if e_min < s_min:
            e_min += 24 * 60
        diff = e_min - s_min
        dur = f"{diff // 60}시간 {diff % 60:02d}분" if diff > 0 else "-"
        return t_s, t_e, dur

    only_t = valid_times[0]
    tilde_pos = s.find("~")
    time_pos = s.find(only_t)
    if tilde_pos != -1 and tilde_pos < time_pos:
        return "미기입", only_t, "종료확인"
    return only_t, "진행중", "가동중"


def classify_milk_sub_line(full_p_name: str, top_raw: str, bot_raw: str, seal_summary: str = "", is_top_section: bool = False) -> str:
    """우유 포장 시트 원칙: 상부접착과 하부접착이 모두 '-'인 품목만 병우유, 상단 표 및 나머지는 모두 카톤우유"""
    if is_top_section:
        return "우유(카톤)"

    t_s = str(top_raw or "").strip()
    b_s = str(bot_raw or "").strip()
    s_sum = re.sub(r"\s+", "", str(seal_summary or ""))

    # 1순위: 시트 상부접착·하부접착이 모두 '-' 이거나 DB 요약에 '캡속지' 또는 '상부:-/하부:-'가 명시된 경우만 병우유
    if t_s == "-" and b_s == "-":
        return "우유(병포장)"
    if "캡속지" in s_sum or ("상부:-" in s_sum and "하부:-" in s_sum):
        return "우유(병포장)"
    if t_s not in ["", "-"] or b_s not in ["", "-"] or "상부:O" in s_sum or "하부:O" in s_sum:
        return "우유(카톤)"

    # 2순위: 접착란이 모두 비어 있는 옛날 DB 행일 때만 전용 병용량으로 보조 판별
    nm_ns = re.sub(r"\s+", "", str(full_p_name or "")).lower()
    if any(bk in nm_ns for bk in ["2300", "1800", "750", "170", "1.8l", "2.3l", "골드플러스rt", "어린이전용우유", "쿠팡전용"]):
        return "우유(병포장)"
    return "우유(카톤)"


def build_run_time_card(line_label: str, ho_title: str, raw_time_str: str) -> dict:
    raw_s = str(raw_time_str or "").replace("：", ":").strip()
    t_s, t_e, dur = calc_time_span_text(raw_s)

    if t_s == "-":
        span_disp = "비가동 (00:00)"
    else:
        # 전일 야간 시작 일자(예: '15일 20:10') 및 괄호 메모(예: '(수출 키짱)') 보존
        m_day = re.search(r"(\d{1,2}\s*일)\s*" + re.escape(t_s), raw_s)
        day_prefix = re.sub(r"\s+", "", m_day.group(1)) + " " if m_day else ""

        times_all = re.findall(r"(?<!\d)(\d{1,2}:\d{2})(?!\d)", raw_s)
        if len(times_all) >= 4 and len(times_all) % 2 == 0:
            pairs = [f"{times_all[i]} ~ {times_all[i + 1]}" for i in range(0, len(times_all), 2)]
            span_disp = day_prefix + " / ".join(pairs)
        else:
            span_disp = f"{day_prefix}{t_s} ~ {t_e}"

        notes = [
            n.strip() for n in re.findall(r"\(([^)]+)\)", raw_s)
            if n.strip() and not re.search(r"카톤\s*/\s*병", n)
        ]
        if notes:
            span_disp = f"{span_disp} ({', '.join(dict.fromkeys(notes))})"

    return {
        "라인구분": line_label,
        "포장호기": ho_title,
        "생산 시작": t_s,
        "생산 종료": t_e,
        "시작~종료": span_disp,
        "실가동시간": dur,
        "가동상태": "가동" if t_s != "-" else "비가동"
    }


def split_combined_milk_time_entry(raw_val: str) -> List[dict]:
    """
    '카톤 7:26 ~ 16:10 병포장 15일 20:10 ~ 14:23' 또는
    '카톤 08:22 ~ 16:03 병포장 07:25 (수출 키짱) ~ 15:35' 처럼
    날짜나 괄호 메모가 중간에 포함되더라도 카톤/병포장 구간을 정확히 분리
    """
    s = str(raw_val or "").replace("：", ":").strip()
    m_c = re.search(r"카톤(?:\s*(?:포장|우유|팩|라인))?(?=\s*[:\-~0-9\(]|$)", s)
    m_b = re.search(r"병(?:\s*(?:포장|우유|라인))?(?=\s*[:\-~0-9\(]|$)", s)

    if m_c and m_b:
        if m_c.start() < m_b.start():
            c_seg = s[m_c.end():m_b.start()].lstrip(" :").strip(" /|,;")
            b_seg = s[m_b.end():].lstrip(" :").strip(" /|,;")
        else:
            b_seg = s[m_b.end():m_c.start()].lstrip(" :").strip(" /|,;")
            c_seg = s[m_c.end():].lstrip(" :").strip(" /|,;")
        return [
            build_run_time_card("우유(카톤)", "우유 카톤 생산시간", c_seg),
            build_run_time_card("우유(병포장)", "우유 병포장 생산시간", b_seg)
        ]

    if m_c and not m_b:
        c_seg = s[m_c.end():].lstrip(" :").strip(" /|,;")
        return [
            build_run_time_card("우유(카톤)", "우유 카톤 생산시간", c_seg),
            build_run_time_card("우유(병포장)", "우유 병포장 생산시간", "")
        ]

    if m_b and not m_c:
        b_seg = s[m_b.end():].lstrip(" :").strip(" /|,;")
        return [
            build_run_time_card("우유(카톤)", "우유 카톤 생산시간", ""),
            build_run_time_card("우유(병포장)", "우유 병포장 생산시간", b_seg)
        ]

    all_t = re.findall(r"(?<!\d)\d{1,2}:\d{2}(?!\d)", s)
    if len(all_t) >= 4:
        return [
            build_run_time_card("우유(카톤)", "우유 카톤 생산시간", f"{all_t[0]} ~ {all_t[1]}"),
            build_run_time_card("우유(병포장)", "우유 병포장 생산시간", f"{all_t[2]} ~ {all_t[3]}")
        ]
    if len(all_t) >= 2:
        return [
            build_run_time_card("우유(카톤)", "우유 카톤 생산시간", f"{all_t[0]} ~ {all_t[1]}"),
            build_run_time_card("우유(병포장)", "우유 병포장 생산시간", "")
        ]
    return [
        build_run_time_card("우유(카톤)", "우유 카톤 생산시간", ""),
        build_run_time_card("우유(병포장)", "우유 병포장 생산시간", "")
    ]


def extract_clean_line_time_for_trace(item_title: str, raw_val: str, target_line: str) -> Optional[str]:
    title_ns = re.sub(r"\s+", "", str(item_title or ""))
    val_s = str(raw_val or "").strip()

    if target_line == "우유(카톤)":
        if "병" in title_ns and "카톤" not in title_ns:
            return None
        if "카톤" in title_ns or "카톤" in val_s or "병" in val_s:
            c_card = split_combined_milk_time_entry(val_s)[0]
        else:
            c_card = build_run_time_card("우유(카톤)", "우유 카톤 생산시간", val_s)
        return f"우유(카톤): {c_card['시작~종료']}" if c_card["가동상태"] == "가동" else None

    if target_line == "우유(병포장)":
        if "카톤" in title_ns and "병" not in title_ns:
            return None
        if "카톤" in title_ns or "카톤" in val_s:
            cards = split_combined_milk_time_entry(val_s)
            b_card = cards[1] if len(cards) > 1 else cards[0]
        else:
            b_card = build_run_time_card("우유(병포장)", "우유 병포장 생산시간", val_s)
        return f"우유(병포장): {b_card['시작~종료']}" if b_card["가동상태"] == "가동" else None

    tl_ns = re.sub(r"\s+", "", str(target_line or ""))
    if "호상" in tl_ns and ("병" in title_ns or "카톤" in title_ns) and "호상" not in title_ns:
        return None
    if "병" in tl_ns and ("호상" in title_ns or "카톤" in title_ns) and "병" not in title_ns:
        return None
    if "카톤" in tl_ns and ("호상" in title_ns or "병" in title_ns) and "카톤" not in title_ns:
        return None

    f_card = build_run_time_card(target_line, item_title, val_s)
    if f_card["가동상태"] != "가동":
        return None
    return f"{item_title}: {f_card['시작~종료']}"


def idx_to_col_letter(col_idx: int) -> str:
    res = ""
    n = col_idx + 1
    while n > 0:
        n, rem = divmod(n - 1, 26)
        res = chr(65 + rem) + res
    return res


def is_valid_live_dairy_tab(tab_title: str, base_word: str) -> Tuple[bool, date]:
    t = str(tab_title or "").strip()
    if not t or "(" in t or ")" in t:
        return False, date(1900, 1, 1)
    bad_tokens = ["수정", "복사", "양식", "사본", "백업", "여분", "대기", "예비", "샘플", "_이력DB_"]
    if any(bt in t for bt in bad_tokens):
        return False, date(1900, 1, 1)
    if base_word == "우유" and "발효" in t:
        return False, date(1900, 1, 1)

    today_d = now_kst().date()
    if t == base_word:
        return True, today_d + timedelta(days=1)

    m = re.match(rf"^(\d{{1,2}})[/\.\-](\d{{1,2}})\s*{base_word}$", t)
    if m:
        mm, dd = int(m.group(1)), int(m.group(2))
        yr = today_d.year if mm <= today_d.month + 1 else today_d.year - 1
        try:
            return True, date(yr, mm, dd)
        except ValueError:
            return True, date(today_d.year, 1, 1)

    return False, date(1900, 1, 1)


def format_dairy_display_name(base_name: str, vol_str: str, sub_line: str) -> str:
    p_clean = re.sub(r"\(\s*(?:카톤|병|병포장)?\s*\d+\s*(?:mL|ml|ML|㎖|L|g|kg)\s*\)", "", str(base_name or "")).strip()
    v_clean = str(vol_str or "").replace("㎖", "mL").replace("ml", "mL").strip()
    if not v_clean:
        m_v = extract_volume_ml_token(base_name)
        if m_v:
            v_clean = f"{m_v}mL"
    pack_tag = "병 " if sub_line == "우유(병포장)" else ("카톤 " if sub_line == "우유(카톤)" else "")
    if v_clean:
        return f"{p_clean} ({pack_tag}{v_clean})".replace("( ", "(")
    return p_clean


DAIRY_ROW_HEADER_KEYS = [
    "확인시간", "소비기한", "유통기한", "표시사항", "표기사항",
    "상부접착", "하부접착", "상/하부접착", "실링", "캡속지", "속지접착",
    "4본입", "수축라벨", "외포장", "제품용량", "내용량", "관능", "판정"
]


def parse_dairy_pack_rows(tab_title_raw: str, rows: list, line_label: str):
    if not rows:
        return [], [], "-", {}

    w_date = (tab_title_raw or "-").strip()
    for r in rows[:6]:
        joined = " ".join([str(c).strip() for c in r if str(c).strip()])
        m_dt = re.search(r"202\d[\.\-\s]+\d{1,2}[\.\-\s]+\d{1,2}", joined)
        if m_dt:
            w_date = m_dt.group(0).strip()
            break

    parsed_products = []
    run_times = []
    coord_prod_map: Dict[Tuple[str, str], Tuple[str, str, str, int]] = {}
    current_cat = line_label
    prod_time_sec_idx = -1
    block_seq = 0

    for r_idx, r in enumerate(rows):
        c0_raw = str(r[0]).strip() if len(r) > 0 else ""
        c1_raw = str(r[1]).strip() if len(r) > 1 else ""
        c0 = re.sub(r"\s+", "", c0_raw)
        c1 = re.sub(r"\s+", "", c1_raw)

        if any(k in c0 for k in ["생산시간", "포장생산시간", "포장시간"]) or any(k in c1 for k in ["생산시간", "포장생산시간", "포장시간"]):
            if prod_time_sec_idx < 0:
                prod_time_sec_idx = r_idx

        if (
            c0
            and "공정관리일보" not in c0
            and "포장일보" not in c0
            and c0 not in ["제품명", "품목", "생산시간", "CCP검증모니터링", "특이사항", "최대소비기한"]
            and any(k in c0 for k in ["발효유", "우유", "카톤", "호상", "병"])
        ):
            current_cat = re.sub(r"\s+", " ", c0_raw).strip()

        lbl_col = -1
        if c0 in ["제품명", "품목"]:
            lbl_col = 0
        elif c1 in ["제품명", "품목"]:
            lbl_col = 1

        if lbl_col < 0:
            continue

        block_seq += 1
        is_milk_top_block = (line_label == "우유" and block_seq == 1)
        if line_label == "우유":
            sec_pos_name = f"우유-{'상단' if block_seq == 1 else '하단'}"
        else:
            sec_pos_name = current_cat if current_cat != line_label else f"발효유-{block_seq}"
            # 단산된 '카톤발효유'(딸기·플레인·복숭아·사과·블루베리, 18일) 블록 원천 배제
            if "카톤" in re.sub(r"\s+", "", sec_pos_name):
                continue

        # 발효유 블록 바로 위 행('최대 소비기한 | 제조일로부터 18일 ...')에서 열별 고정 기준일수 추출
        max_days_row = []
        if line_label != "우유" and r_idx >= 1:
            for back_i in (r_idx - 1, r_idx - 2):
                if back_i >= 0:
                    cand_join = re.sub(r"\s+", "", "".join(str(x) for x in rows[back_i]))
                    if "제조일로부터" in cand_join or "최대소비기한" in cand_join:
                        max_days_row = rows[back_i]
                        break

        next_block_r = len(rows)
        for nr in range(r_idx + 1, min(r_idx + 24, len(rows))):
            nc0 = re.sub(r"\s+", "", str(rows[nr][0])) if len(rows[nr]) > 0 else ""
            nc1 = re.sub(r"\s+", "", str(rows[nr][1])) if len(rows[nr]) > 1 else ""
            if nc0 in ["제품명", "품목", "생산시간", "포장시간", "CCP검증모니터링"] or nc1 in ["제품명", "품목", "생산시간", "포장시간", "CCP검증모니터링"]:
                next_block_r = nr
                break

        block_sub_rows = rows[r_idx + 1 : next_block_r]

        last_valid_p_name = ""
        carry_shelf_days = 0
        for col_i in range(lbl_col + 1, len(r)):
            if max_days_row and col_i < len(max_days_row):
                m_days = re.search(r"제조일로부터\s*(\d+)\s*일", str(max_days_row[col_i]))
                if m_days:
                    carry_shelf_days = int(m_days.group(1))

            raw_p_cell = re.sub(r"\s+", " ", str(r[col_i])).strip()
            col_sub_join = "".join(re.sub(r"\s+", "", str(sr[col_i])) for sr in block_sub_rows[:3] if col_i < len(sr))

            if "최대소비기한" in re.sub(r"\s+", "", raw_p_cell) or "최대소비기한" in col_sub_join or "특이사항" in raw_p_cell:
                break
            if re.sub(r"\s+", "", raw_p_cell) in ["제품명", "품목", "-"]:
                last_valid_p_name = ""
                continue

            sub0_val = str(block_sub_rows[0][col_i]).strip() if (block_sub_rows and col_i < len(block_sub_rows[0])) else ""
            has_vol_under = bool(re.search(r"\d+\s*(?:㎖|mL|ml|L|g|kg)", sub0_val))
            col_has_grid_val = any(
                (col_i < len(sr) and str(sr[col_i]).strip() not in ["", "적합", "판정"])
                for sr in block_sub_rows
            )

            if raw_p_cell:
                p_name = raw_p_cell
                last_valid_p_name = raw_p_cell
            elif last_valid_p_name and (has_vol_under or col_has_grid_val):
                p_name = last_valid_p_name
            else:
                continue

            vol_str, chk_time = "", "-"
            exp_list = []
            label_ok, top_ok, bot_ok, cap_ok, vol_ok, sens_ok = "-", "-", "-", "-", "-", "-"
            raw_top_cell, raw_bot_cell = "", ""

            carry_lbl = ""
            for sub_i, sub_r in enumerate(block_sub_rows):
                raw_lbl_c0 = re.sub(r"\s+", "", str(sub_r[0])) if len(sub_r) > 0 else ""
                raw_lbl_c = re.sub(r"\s+", "", str(sub_r[lbl_col])) if len(sub_r) > lbl_col else ""
                cand_lbl = raw_lbl_c or (raw_lbl_c0 if lbl_col == 0 else "")

                if any(hk in cand_lbl for hk in DAIRY_ROW_HEADER_KEYS) or "소비기한기준" in cand_lbl:
                    carry_lbl = cand_lbl
                row_lbl = carry_lbl

                val = str(sub_r[col_i]).strip() if col_i < len(sub_r) else ""
                if "상부접착" in row_lbl and "하부" not in row_lbl:
                    raw_top_cell = val
                elif "하부접착" in row_lbl and "상/" not in row_lbl:
                    raw_bot_cell = val

                if not val:
                    continue

                if sub_i == 0 and re.search(r"\d+\s*(?:㎖|mL|ml|L|g|kg)", val) and not re.search(r"\d{1,2}\.\d{1,2}", val):
                    vol_str = val
                elif "소비기한기준" in row_lbl:
                    continue
                elif "확인시간" in row_lbl:
                    chk_time = val if chk_time == "-" else f"{chk_time} / {val}"
                elif "소비기한" in row_lbl or "유통기한" in row_lbl:
                    if val not in ["00.00", "0.0", "-", "소비기한"] and "제조일로부터" not in val:
                        if val not in exp_list:
                            exp_list.append(val)
                elif "표시사항" in row_lbl or "표기사항" in row_lbl:
                    label_ok = val
                elif "캡속지" in row_lbl or "속지접착" in row_lbl:
                    cap_ok = val
                elif any(k in row_lbl for k in ["상부접착", "상/하부접착", "실링"]):
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

            if line_label == "우유":
                if is_milk_top_block:
                    final_sub_cat = "우유(카톤)"
                else:
                    final_sub_cat = "우유(병포장)" if (raw_top_cell == "-" and raw_bot_cell == "-") else "우유(카톤)"
                item_max_days = 15
            else:
                final_sub_cat = current_cat
                item_max_days = carry_shelf_days

            full_p_name = format_dairy_display_name(p_name, vol_str, final_sub_cat)
            col_let = f"{idx_to_col_letter(col_i)}열"
            coord_prod_map[(sec_pos_name, col_let)] = (full_p_name, final_sub_cat, p_name, item_max_days)

            if parsed_products and parsed_products[-1]["세부유형"] == sec_pos_name and parsed_products[-1]["제품명"] == full_p_name:
                prev_p = parsed_products[-1]
                if is_running:
                    prev_p["가동여부"] = "금일생산"
                    if exp_joined != "-":
                        prev_p["표기 소비기한"] = exp_joined if prev_p["표기 소비기한"] == "-" else f"{prev_p['표기 소비기한']}, {exp_joined}"
                    if chk_time != "-":
                        prev_p["확인시간"] = chk_time if prev_p["확인시간"] == "-" else f"{prev_p['확인시간']} / {chk_time}"
                    for k_f, v_f in [("표시사항", label_ok), ("상부/실링", top_ok), ("하부/라벨", bot_ok), ("캡속지접착", cap_ok), ("제품용량", vol_ok), ("관능(5점법)", sens_ok)]:
                        if prev_p[k_f] in ["-", ""] and v_f not in ["-", ""]:
                            prev_p[k_f] = v_f
                continue

            parsed_products.append({
                "라인구분": final_sub_cat,
                "세부유형": sec_pos_name,
                "열위치": col_let,
                "작성일": w_date,
                "가동여부": "금일생산" if is_running else "미생산(00.00)",
                "제품명": full_p_name,
                "확인시간": chk_time,
                "표기 소비기한": exp_joined,
                "표시사항": label_ok,
                "상부/실링": cap_ok if final_sub_cat == "우유(병포장)" and top_ok == "-" else top_ok,
                "하부/라벨": bot_ok,
                "캡속지접착": cap_ok,
                "제품용량": vol_ok,
                "관능(5점법)": sens_ok,
                "_max_days": item_max_days,
                "_cls_exp": "cell-green" if is_running and exp_joined != "-" else ""
            })

    if prod_time_sec_idx >= 0:
        end_pt_idx = len(rows)
        for nr in range(prod_time_sec_idx + 1, min(prod_time_sec_idx + 14, len(rows))):
            nc = re.sub(r"\s+", "", "".join(str(x) for x in rows[nr][:3]))
            if any(stop_k in nc for stop_k in ["CCP", "살균", "금속검출", "특이사항"]):
                end_pt_idx = nr
                break

        pt_block = rows[prod_time_sec_idx : end_pt_idx]
        ignore_words = {"생산시간", "포장생산시간", "포장시간", "구분", "호기", "시작", "종료", "시작시간", "종료시간"}

        handled_milk_inline = False
        if line_label == "우유":
            flat_cells = []
            for br in pt_block:
                for cell in br:
                    cs = re.sub(r"\s+", " ", str(cell)).strip()
                    if cs and re.sub(r"\s+", "", cs) not in ignore_words and "공정관리일보" not in cs:
                        flat_cells.append(cs)
            flat_pt_txt = " ".join(flat_cells)
            mc_chk = re.search(r"카톤(?:\s*(?:포장|우유|팩|라인))?(?=\s*[:\-~0-9\(]|$)", flat_pt_txt)
            mb_chk = re.search(r"병(?:\s*(?:포장|우유|라인))?(?=\s*[:\-~0-9\(]|$)", flat_pt_txt)
            if mc_chk and mb_chk:
                mid_seg = flat_pt_txt[min(mc_chk.end(), mb_chk.end()) : max(mc_chk.start(), mb_chk.start())]
                if re.search(r"\d{1,2}:\d{2}|~", mid_seg):
                    run_times.extend(split_combined_milk_time_entry(flat_pt_txt))
                    handled_milk_inline = True

        if not handled_milk_inline:
            anchors_by_row: Dict[int, List[Tuple[int, str]]] = {}
            for bi, br in enumerate(pt_block):
                for ci, cell in enumerate(br):
                    cs = re.sub(r"\s+", " ", str(cell)).strip()
                    cs_ns = re.sub(r"\s+", "", cs)
                    if not cs_ns or cs_ns in ignore_words or "공정관리일보" in cs_ns:
                        continue
                    if re.search(r"\d{1,2}:\d{2}", cs_ns) and not any(k in cs_ns for k in ["카톤", "병포장", "병우유", "병발효유", "호상"]):
                        continue
                    if any(mk in cs_ns for mk in ["호기", "카톤", "병포장", "병우유", "병발효유", "호상", "연팩", "로타리", "라인", "팩", "컵"]) or cs_ns == "병":
                        anchors_by_row.setdefault(bi, []).append((ci, cs))

            seen_titles = set()
            for bi, row_anchors in anchors_by_row.items():
                row_anchors.sort(key=lambda x: x[0])
                br = pt_block[bi]
                for idx_a, (ac, albl) in enumerate(row_anchors):
                    next_ac = row_anchors[idx_a + 1][0] if idx_a + 1 < len(row_anchors) else len(br)

                    same_row_chunks = [str(br[sub_c]).strip() for sub_c in range(ac, min(next_ac, len(br))) if str(br[sub_c]).strip()]
                    same_row_txt = " ".join(same_row_chunks)
                    same_row_times = re.findall(r"(?<!\d)\d{1,2}:\d{2}(?!\d)", same_row_txt)

                    use_raw_txt = same_row_txt
                    if len(same_row_times) < 2:
                        below_chunks = []
                        for sub_r_i in range(bi + 1, min(bi + 4, len(pt_block))):
                            if sub_r_i in anchors_by_row:
                                break
                            sbr = pt_block[sub_r_i]
                            for sub_c in range(ac, min(next_ac, len(sbr))):
                                sv = str(sbr[sub_c]).strip()
                                if sv:
                                    below_chunks.append(sv)
                        if below_chunks:
                            use_raw_txt = (same_row_txt + " " + " ".join(below_chunks)).strip()

                    albl_ns = re.sub(r"\s+", "", albl)
                    if line_label == "우유" and ("카톤/병" in albl_ns or ("카톤" in use_raw_txt and "병" in use_raw_txt)):
                        for sp_card in split_combined_milk_time_entry(use_raw_txt):
                            if sp_card["포장호기"] not in seen_titles:
                                seen_titles.add(sp_card["포장호기"])
                                run_times.append(sp_card)
                        continue

                    if line_label == "우유":
                        if "카톤" in albl_ns:
                            norm_ho = "우유 카톤 생산시간"
                            sub_ln = "우유(카톤)"
                        elif "병" in albl_ns:
                            norm_ho = "우유 병포장 생산시간"
                            sub_ln = "우유(병포장)"
                        else:
                            norm_ho = albl
                            sub_ln = "우유"
                    else:
                        clean_lbl = re.sub(r"\d{1,2}:\d{2}.*$", "", albl).strip() or albl
                        norm_ho = clean_lbl if "생산시간" in clean_lbl else f"{clean_lbl} 생산시간"
                        sub_ln = line_label

                    if norm_ho not in seen_titles:
                        seen_titles.add(norm_ho)
                        run_times.append(build_run_time_card(sub_ln, norm_ho, use_raw_txt))

    if line_label == "우유":
        has_c = any("카톤" in r["포장호기"] for r in run_times)
        has_b = any("병" in r["포장호기"] for r in run_times)
        if not has_c:
            run_times.insert(0, build_run_time_card("우유(카톤)", "우유 카톤 생산시간", ""))
        if not has_b:
            run_times.append(build_run_time_card("우유(병포장)", "우유 병포장 생산시간", ""))

    return parsed_products, run_times, w_date, coord_prod_map


def extract_hist_dairy_run_times(dairy_hist_c: list, target_date: str, line_filter: str = "ALL") -> list:
    cards_by_ho: Dict[str, dict] = {}
    order_hos: List[str] = []
    for r in dairy_hist_c:
        if r.get("생산일자") != target_date:
            continue
        ln = str(r.get("라인구분", "")).strip()
        if line_filter != "ALL" and ln not in line_filter and line_filter not in ln:
            continue
        if "생산시간" in str(r.get("기록분류", "")):
            ho_nm = str(r.get("세부항목", "-")).strip()
            raw_v = str(r.get("기록내용_수치", "-")).strip()
            ho_ns = re.sub(r"\s+", "", ho_nm)
            if "카톤/병" in ho_ns or ("우유" in ho_ns and "카톤" in ho_ns and "병" in ho_ns) or ("카톤" in raw_v and "병" in raw_v):
                cand_cards = split_combined_milk_time_entry(raw_v)
            else:
                cand_cards = [build_run_time_card(ln, ho_nm, raw_v)]

            for cd in cand_cards:
                k = cd["포장호기"]
                if k not in cards_by_ho:
                    order_hos.append(k)
                    cards_by_ho[k] = cd
                elif cd["가동상태"] == "가동":
                    cards_by_ho[k] = cd
    return [cards_by_ho[k] for k in order_hos]


def fetch_process_milk_sheet(sheet_urls_map: dict):
    sh_milk, err_milk = open_sheet_isolated(sheet_urls_map, "process_milk")
    if sh_milk is None:
        return [], [], [], [], "-", "-", [], [], [], f"[유제품포장일보] {err_milk}"
    try:
        ws_all = api_call_with_retry(sh_milk.worksheets)
        ws_titles = [w.title for w in ws_all]

        milk_cands = [(dt_v, t) for t in ws_titles for ok, dt_v in [is_valid_live_dairy_tab(t, "우유")] if ok]
        ferm_cands = [(dt_v, t) for t in ws_titles for ok, dt_v in [is_valid_live_dairy_tab(t, "발효유")] if ok]
        milk_cands.sort(key=lambda x: x[0], reverse=True)
        ferm_cands.sort(key=lambda x: x[0], reverse=True)

        t_m = milk_cands[0][1] if milk_cands else None
        t_f = ferm_cands[0][1] if ferm_cands else None

        t_m_tpls = [t for t in ws_titles if "우유" in t and "발효" not in t and "호상" not in t and any(k in t for k in ["수정", "복사"])]
        t_f_tpls = [t for t in ws_titles if ("발효" in t or "호상" in t) and any(k in t for k in ["수정", "복사", "추가"])]

        t_dp = next((t for t in ws_titles if t.strip() == HIST_TABS["dairy_prod"]), None)
        t_dc = next((t for t in ws_titles if t.strip() == HIST_TABS["dairy_ccp"]), None)

        fetch_milk_tabs = list(dict.fromkeys([x for x in ([t_m, t_f] + t_m_tpls[:2] + t_f_tpls[:3] + [t_dp, t_dc]) if x]))
        batch_map = batch_fetch_tabs(sh_milk, fetch_milk_tabs)

        m_rows, m_times, m_dt, coord_m = parse_dairy_pack_rows(t_m, batch_map.get(t_m, []), "우유") if t_m else ([], [], "-", {})
        f_rows, f_times, f_dt, coord_f = parse_dairy_pack_rows(t_f, batch_map.get(t_f, []), "발효유") if t_f else ([], [], "-", {})

        # 구 양식(수정X) -> 신규 복사용/추가 양식 -> 당일 실시간 탭 순으로 덮어써서 최신 기준일수(예: 부산요네뜨 23일) 최우선 적용
        coord_master: Dict[Tuple[str, str], Tuple[str, str, str, int]] = {}
        for tpl_t in sorted(t_m_tpls[:2], key=lambda x: (0 if "수정" in x else 1)):
            _, _, _, c_tpl = parse_dairy_pack_rows(tpl_t, batch_map.get(tpl_t, []), "우유")
            coord_master.update(c_tpl)
        for tpl_t in sorted(t_f_tpls[:3], key=lambda x: (0 if "수정" in x else 1)):
            _, _, _, c_tpl = parse_dairy_pack_rows(tpl_t, batch_map.get(tpl_t, []), "발효유")
            coord_master.update(c_tpl)
        coord_master.update(coord_m)
        coord_master.update(coord_f)

        ferm_days_master: Dict[Tuple[str, str], int] = {}
        for (sec_k, _), (f_nm, f_ln, base_nm, m_days) in coord_master.items():
            if m_days > 0 and ("발효" in f_ln or "호상" in f_ln or "발효" in sec_k or "호상" in sec_k):
                sub_k = _ferm_sub_kind(f_ln, sec_k)
                ferm_days_master[(sub_k, clean_product_for_match(f_nm))] = m_days
                ferm_days_master[(sub_k, clean_product_for_match(base_nm))] = m_days
        for fr in f_rows:
            if fr.get("_max_days", 0) > 0:
                sub_k = _ferm_sub_kind(fr.get("라인구분", ""), fr.get("세부유형", ""))
                ferm_days_master[(sub_k, clean_product_for_match(fr.get("제품명", "")))] = int(fr["_max_days"])

        raw_dp_matrix = batch_map.get(t_dp, []) if t_dp else []
        if t_dp and len(raw_dp_matrix) >= 2:
            def _fix_dairy_db_row(r_vals: list, cmap: dict):
                c_dt = cmap.get("생산일자", -1)
                c_ln = cmap.get("라인구분", -1)
                c_sec = cmap.get("세부유형", -1)
                c_col = cmap.get("열위치", -1)
                c_pnm = cmap.get("제품명", -1)
                c_seal = cmap.get("접착및외관", cmap.get("접착_외관_요약", -1))
                if c_dt >= 0:
                    pd = parse_date_obj(r_vals[c_dt])
                    if pd:
                        r_vals[c_dt] = pd.strftime("%Y-%m-%d")
                if c_pnm < 0 or c_ln < 0:
                    return
                sec_s = r_vals[c_sec] if c_sec >= 0 else ""
                col_s = r_vals[c_col] if c_col >= 0 else ""
                pnm_s = r_vals[c_pnm]
                ln_s = r_vals[c_ln]
                seal_s = r_vals[c_seal] if c_seal >= 0 else ""

                is_vol_only = bool(re.match(r"^\d+(?:\.\d+)?\s*(?:㎖|mL|ml|L|g|kg)?$", pnm_s))
                exact_c = coord_master.get((sec_s, col_s))
                bot_c = coord_master.get(("우유-하단", col_s)) if ("하단" in sec_s or "우유" in ln_s) else None

                if is_vol_only and (exact_c or bot_c):
                    full_nm, mapped_ln, _, _ = exact_c or bot_c
                    r_vals[c_pnm] = full_nm
                    r_vals[c_ln] = mapped_ln
                elif "우유" in ln_s and "발효" not in ln_s and "호상" not in ln_s:
                    true_ln = classify_milk_sub_line(pnm_s, "", "", seal_s, "상단" in sec_s)
                    r_vals[c_ln] = true_ln

            if heal_db_worksheet_if_dirty(sh_milk, t_dp, raw_dp_matrix, _fix_dairy_db_row):
                batch_map[t_dp] = get_values_formatted(api_call_with_retry(sh_milk.worksheet, t_dp))

        dairy_hist_p_raw = rows_to_records(batch_map.get(t_dp, [])) if t_dp else []
        dairy_hist_p = []
        slot_hist_idx: Dict[Tuple[str, str, str], int] = {}

        for dr in dairy_hist_p_raw:
            orig_tab = str(dr.get("원본탭명", "")).strip()
            if orig_tab and any(bad in orig_tab for bad in ["수정", "복사", "양식", "사본", "백업"]):
                continue

            dr_c = dict(dr)
            p_d = parse_date_obj(dr_c.get("생산일자", ""))
            if p_d:
                dr_c["생산일자"] = p_d.strftime("%Y-%m-%d")

            sec_type = str(dr_c.get("세부유형", "")).strip()
            col_pos = str(dr_c.get("열위치", "")).strip()
            raw_p_nm = str(dr_c.get("제품명", "")).strip()
            ln_orig = str(dr_c.get("라인구분", "")).strip()
            seal_raw = str(dr_c.get("접착및외관", dr_c.get("접착_외관_요약", ""))).strip()

            is_vol_only_name = bool(re.match(r"^\d+(?:\.\d+)?\s*(?:㎖|mL|ml|L|g|kg)?$", raw_p_nm))
            bot_coord = coord_master.get(("우유-하단", col_pos)) if ("하단" in sec_type or "우유" in ln_orig) else None
            exact_coord = coord_master.get((sec_type, col_pos))

            if is_vol_only_name and (exact_coord or bot_coord):
                full_nm, mapped_sub_ln, _, _ = exact_coord or bot_coord
                dr_c["제품명"] = full_nm
                dr_c["라인구분"] = mapped_sub_ln
            elif exact_coord and clean_product_for_match(raw_p_nm) == clean_product_for_match(exact_coord[2]):
                dr_c["제품명"] = exact_coord[0]
                dr_c["라인구분"] = exact_coord[1]
            else:
                if "발효" in ln_orig or "호상" in ln_orig or "발효" in sec_type or "호상" in sec_type:
                    dr_c["라인구분"] = sec_type if ("발효" in sec_type or "호상" in sec_type) else ln_orig
                else:
                    is_top_sec = ("상단" in sec_type)
                    dr_c["라인구분"] = classify_milk_sub_line(
                        raw_p_nm,
                        dr_c.get("상부_실링", dr_c.get("상부접착", "")),
                        dr_c.get("하부_외포장", dr_c.get("하부접착", "")),
                        seal_raw,
                        is_top_sec
                    )
                dr_c["제품명"] = format_dairy_display_name(raw_p_nm, "", dr_c.get("라인구분", ""))

            if not dr_c.get("관능검사") and dr_c.get("관능"):
                dr_c["관능검사"] = dr_c.get("관능")
            if not dr_c.get("접착_외관_요약"):
                if seal_raw:
                    dr_c["접착_외관_요약"] = seal_raw
                else:
                    t_v = dr_c.get("상부_실링", "-")
                    b_v = dr_c.get("하부_외포장", "-")
                    dr_c["접착_외관_요약"] = f"상부:{t_v} / 하부:{b_v}"

            sk = (dr_c.get("생산일자", ""), dr_c.get("라인구분", ""), clean_product_for_match(dr_c.get("제품명", "")))
            if sk in slot_hist_idx:
                continue
            slot_hist_idx[sk] = len(dairy_hist_p)
            dairy_hist_p.append(dr_c)

        # 실시간 탭 소비기한·확인시간 수정 시 _이력DB_유제품포장 즉시 Upsert 동기화
        dp_updated_in_place = False
        dp_new_rows = []
        now_saved_s = now_kst().strftime("%Y-%m-%d %H:%M:%S")

        for live_list, live_dt_raw, orig_t_nm in [(m_rows, m_dt, t_m), (f_rows, f_dt, t_f)]:
            if not live_list:
                continue
            live_d_obj = parse_date_obj(live_dt_raw) or now_kst().date()
            live_iso = live_d_obj.strftime("%Y-%m-%d")
            for lr in live_list:
                if "금일생산" not in str(lr.get("가동여부", "")):
                    continue
                exp_live = str(lr.get("표기 소비기한", "-")).strip()
                chk_live = str(lr.get("확인시간", "-")).strip()
                ln_live = str(lr.get("라인구분", "")).strip()
                p_live = str(lr.get("제품명", "")).strip()

                parsed_e_list = parse_exp_dates_against_prod(exp_live, live_d_obj, expected_days=15)
                std_e_str = ", ".join([d.strftime("%Y-%m-%d") for d in parsed_e_list]) if parsed_e_list else ""

                sk = (live_iso, ln_live, clean_product_for_match(p_live))
                if sk in slot_hist_idx:
                    exist_dr = dairy_hist_p[slot_hist_idx[sk]]
                    if exp_live != "-" and str(exist_dr.get("표기소비기한_원문", "")).strip() != exp_live:
                        exist_dr["표기소비기한_원문"] = exp_live
                        exist_dr["표준소비기한_검색용"] = std_e_str
                        exist_dr["저장일시"] = now_saved_s
                        dp_updated_in_place = True
                    if chk_live != "-" and str(exist_dr.get("확인시간", "")).strip() != chk_live:
                        exist_dr["확인시간"] = chk_live
                        dp_updated_in_place = True
                else:
                    is_bot = ln_live == "우유(병포장)"
                    seal_txt = f"캡속지:{lr.get('캡속지접착', '-')}" if is_bot else f"상부:{lr.get('상부/실링', '-')} / 하부:{lr.get('하부/라벨', '-')}"
                    new_dr = {
                        "생산일자": live_iso,
                        "원본탭명": orig_t_nm or "-",
                        "라인구분": ln_live,
                        "세부유형": lr.get("세부유형", ""),
                        "열위치": lr.get("열위치", ""),
                        "제품명": p_live,
                        "Tank번호": "-",
                        "확인시간": chk_live,
                        "표기소비기한_원문": exp_live,
                        "표준소비기한_검색용": std_e_str,
                        "표시사항": lr.get("표시사항", "-"),
                        "접착_외관_요약": seal_txt,
                        "접착및외관": seal_txt,
                        "제품용량": lr.get("제품용량", "-"),
                        "관능검사": lr.get("관능(5점법)", "-"),
                        "관능": lr.get("관능(5점법)", "-"),
                        "판정": "적합",
                        "저장일시": now_saved_s
                    }
                    slot_hist_idx[sk] = len(dairy_hist_p)
                    dairy_hist_p.append(new_dr)
                    dp_new_rows.append(new_dr)

        if t_dp and (dp_updated_in_place or dp_new_rows):
            try:
                dp_matrix = batch_map.get(t_dp, [])
                if dp_matrix and len(dp_matrix) >= 1:
                    dp_hdr = [str(h).strip() for h in dp_matrix[0]]
                    ws_dp = api_call_with_retry(sh_milk.worksheet, t_dp)
                    if dp_updated_in_place:
                        sorted_dp = sorted(dairy_hist_p, key=lambda x: x.get("생산일자", ""), reverse=True)
                        full_dp_payload = [dp_hdr] + [[str(rec.get(c_nm, "")) for c_nm in dp_hdr] for rec in sorted_dp]
                        api_call_with_retry(ws_dp.clear)
                        safe_ws_update(ws_dp, full_dp_payload)
                    elif dp_new_rows:
                        append_dp = [[str(rec.get(c_nm, "")) for c_nm in dp_hdr] for rec in dp_new_rows]
                        api_call_with_retry(ws_dp.append_rows, append_dp, value_input_option="USER_ENTERED")
            except Exception:
                pass

        # DB 저장 데이터 기준 우유(+14일)·발효유(N-1일) 상한 및 직전 재고 역전 전수 검증 후 실시간 행에 투영
        validate_dairy_shelf_life_on_db(dairy_hist_p, ferm_days_master)
        dy_val_lookup = {
            (str(r.get("생산일자", "")).strip(), str(r.get("라인구분", "")).strip(), clean_product_for_match(r.get("제품명", ""))): r
            for r in dairy_hist_p
        }

        for live_list, live_dt_raw in [(m_rows, m_dt), (f_rows, f_dt)]:
            live_iso = (parse_date_obj(live_dt_raw) or now_kst().date()).strftime("%Y-%m-%d")
            for lr in live_list:
                if "금일생산" not in str(lr.get("가동여부", "")):
                    lr["_cls_exp"] = ""
                    lr["_exp_tip"] = ""
                    continue
                lk = (live_iso, str(lr.get("라인구분", "")).strip(), clean_product_for_match(lr.get("제품명", "")))
                matched_dr = dy_val_lookup.get(lk)
                lr["_cls_exp"] = matched_dr.get("_cls_exp", "cell-green") if matched_dr else "cell-green"
                lr["_exp_tip"] = matched_dr.get("_exp_tip", "") if matched_dr else ""
                lr["_exp_alert"] = matched_dr.get("_exp_alert", "") if matched_dr else ""

        dairy_hist_c_raw = rows_to_records(batch_map.get(t_dc, [])) if t_dc else []
        dairy_hist_c = []
        for cr in dairy_hist_c_raw:
            orig_tab = str(cr.get("원본탭명", "")).strip()
            if orig_tab and any(bad in orig_tab for bad in ["수정", "복사", "양식", "사본", "백업"]):
                continue
            cr_c = dict(cr)
            c_d = parse_date_obj(cr_c.get("생산일자", ""))
            if c_d:
                cr_c["생산일자"] = c_d.strftime("%Y-%m-%d")
            dairy_hist_c.append(cr_c)

        m_date_obj = parse_date_obj(m_dt)
        if m_date_obj and dairy_hist_c and all(r.get("가동상태") == "비가동" for r in m_times):
            hist_m_t = extract_hist_dairy_run_times(dairy_hist_c, m_date_obj.strftime("%Y-%m-%d"), "우유")
            if any(r.get("가동상태") == "가동" for r in hist_m_t):
                m_times = hist_m_t

        f_date_obj = parse_date_obj(f_dt)
        if not f_times and f_date_obj and dairy_hist_c:
            f_times = extract_hist_dairy_run_times(dairy_hist_c, f_date_obj.strftime("%Y-%m-%d"), "발효유")

        dairy_prod_names = []
        for full_nm, _, _, _ in coord_master.values():
            if full_nm and full_nm != "-" and full_nm not in dairy_prod_names:
                dairy_prod_names.append(full_nm)

        dairy_hist_p.sort(key=lambda x: x.get("생산일자", ""), reverse=True)
        return m_rows, f_rows, m_times, f_times, m_dt, f_dt, dairy_hist_p, dairy_hist_c, dairy_prod_names, None
    except Exception as e:
        return [], [], [], [], "-", "-", [], [], [], f"[유제품포장일보] {e}"

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


def extract_soy_inc_days(inc_text: Any) -> int:
    """배양구분/배양검사 문자열(예: '5일', '7일', '5일 (40 ± 5 ℃)')에서 배양일수만 정확히 추출"""
    s = str(inc_text or "").strip()
    m = re.search(r"(\d+)\s*일", s)
    if m:
        d = int(m.group(1))
        if 1 <= d <= 30:
            return d
    m_num = re.match(r"^(\d+)$", s)
    if m_num:
        d = int(m_num.group(1))
        if 1 <= d <= 30:
            return d
    if "7" in s:
        return 7
    return 5


def parse_soy_tab_date(tab_title: Any, ref_date: Optional[date] = None) -> Optional[date]:
    """두유 배양검사 하단 탭 이름(예: '11/19', '11.19', '11. 19')을 개봉·검사일(date)로 변환"""
    s = str(tab_title or "").strip().rstrip(".")
    if not s:
        return None
    m_ymd = re.match(r"^(20\d{2}|\d{2})[/\.\-\s]+(\d{1,2})[/\.\-\s]+(\d{1,2})(?:\D.*)?$", s)
    if m_ymd:
        yr_raw, mm, dd = int(m_ymd.group(1)), int(m_ymd.group(2)), int(m_ymd.group(3))
        yr = 2000 + yr_raw if yr_raw < 100 else yr_raw
        try:
            return date(yr, mm, dd)
        except ValueError:
            return None

    m_md = re.match(r"^(\d{1,2})[/\.\-\s]+(\d{1,2})(?:\s*\([^)]*\))?$", s)
    if m_md:
        mm, dd = int(m_md.group(1)), int(m_md.group(2))
        today_d = now_kst().date()
        if ref_date and isinstance(ref_date, date) and 2020 <= ref_date.year <= 2035:
            yr = ref_date.year
            if mm == 12 and ref_date.month == 1:
                yr -= 1
        else:
            yr = today_d.year if mm <= today_d.month + 1 else today_d.year - 1
        try:
            return date(yr, mm, dd)
        except ValueError:
            return None
    return None


def resolve_soy_realtime_dates(tab_title: str, raw_prod_dt: str, raw_insp_dt: str, inc_days: str):
    """
    시트 셀 내부의 '=TODAY()' 수식 날짜 왜곡 원천 차단:
    무조건 [탭 이름 날짜 = 검사일(개봉일)], [탭 이름 날짜 - 배양일(5일/7일) = 생산일자]로 산출
    """
    tab_d = parse_soy_tab_date(tab_title)
    if tab_d is None:
        return raw_prod_dt or "-", raw_insp_dt or "-"

    offset = extract_soy_inc_days(inc_days)
    true_prod = (tab_d - timedelta(days=offset)).strftime("%Y-%m-%d")
    true_insp = tab_d.strftime("%Y-%m-%d")
    return true_prod, true_insp


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

    note_map = {}
    for r_i in range(68, len(rows)):
        line_txt = g(r_i, 0)
        if "(주" not in line_txt:
            continue
        for m in re.finditer(r"\(주\s*(\d+)\)\s*([^(]+)", line_txt):
            note_map[f"주{m.group(1)}"] = re.sub(r"\s+", " ", m.group(2)).strip()

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

    for c_idx in carton_cols:
        p_name = re.sub(r"\s+", " ", g(5, c_idx)).strip()
        if not p_name:
            continue
        tank_s = g(7, c_idx) or "-"
        judge_s = g(8, c_idx) or "적합"
        if "부적합" in judge_s:
            has_fail = True

        alias_pool, _ = expand_micro_alias_pool(p_name, "", note_map)
        active_lines, nonzero_notes = [], []
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
                    "연관품목_주석포함": alias_pool,
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
            "판정": judge_s if has_actual_input else "작성전(빈양식)"
        })

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
        alias_pool, _ = expand_micro_alias_pool(cur_b_prod, "", note_map)
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
                "연관품목_주석포함": alias_pool,
                "호기_용량": vol_lbl,
                "세부라인": "-",
                "Storage_Tank": cur_b_tank,
                "대장균군": e_v,
                "세균수": g_v,
                "효모_곰팡이": "-",
                "유산균": "-",
                "판정": "적합"
            })

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
        alias_pool, _ = expand_micro_alias_pool(j_txt, "", note_map)
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
                "연관품목_주석포함": alias_pool,
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


def fmt_mmdd_short(iso_str: str) -> str:
    s = str(iso_str or "").strip()
    if len(s) >= 10 and s[4] == "-":
        return s[5:10].replace("-", "/")
    return s


def fetch_process_micro_sheet(sheet_urls_map: dict):
    empty_micro = {
        "has_sheet": False,
        "auto_status": "출고적합",
        "auto_note": "최근 판정분 우유 · 발효유 제품 특이사항 없음",
        "latest_judge_date": "",
        "live_tabs": {},
        "default_live_tab": "",
        "hist_rows": [],
        "total_db_cnt": 0
    }
    sh, err = open_sheet_isolated(sheet_urls_map, "process_micro")
    if sh is None:
        return empty_micro, None

    try:
        ws_all = api_call_with_retry(sh.worksheets)
        ws_titles = [w.title for w in ws_all]

        today_d = now_kst().date()
        weekdays_kr = ["월", "화", "수", "목", "금", "토", "일"]
        today_day_kr = weekdays_kr[today_d.weekday()]

        target_live = [t for t in MICRO_LIVE_TABS if t in ws_titles]
        if not target_live and today_day_kr in ws_titles:
            target_live = [today_day_kr]

        t_db = next((t for t in ws_titles if t.strip() == HIST_TABS["micro_db"]), None)
        batch_map = batch_fetch_tabs(sh, target_live + ([t_db] if t_db else []), "FORMATTED_VALUE")

        # [시트 원본 자동 수리] _이력DB_미생물에 잘못 들어간 주1/주2 연관품목 텍스트 및 날짜 포맷 영구 교정
        raw_db_matrix = batch_map.get(t_db, []) if t_db else []
        if t_db and len(raw_db_matrix) >= 2:
            def _fix_micro_row(r_vals: list, cmap: dict):
                c_j = cmap.get("판정일자", -1)
                c_i = cmap.get("검사일자_생산일", -1)
                c_rep = cmap.get("대표품목명", -1)
                c_ali = cmap.get("연관품목_주석포함", -1)
                if c_j >= 0:
                    jd = parse_date_obj(r_vals[c_j])
                    if jd:
                        r_vals[c_j] = jd.strftime("%Y-%m-%d")
                if c_i >= 0:
                    id_obj = parse_date_obj(r_vals[c_i])
                    if id_obj:
                        r_vals[c_i] = id_obj.strftime("%Y-%m-%d")
                if c_rep >= 0 and c_ali >= 0:
                    # 이미 저장된 당일 주석 스냅샷은 영구 보존하고, 아예 비어 있는 초기 행만 보충
                    if not str(r_vals[c_ali]).strip() or str(r_vals[c_ali]).strip() == "-":
                        clean_pool, _ = expand_micro_alias_pool(r_vals[c_rep], "")
                        r_vals[c_ali] = clean_pool

            if heal_db_worksheet_if_dirty(sh, t_db, raw_db_matrix, _fix_micro_row):
                batch_map[t_db] = get_values_formatted(api_call_with_retry(sh.worksheet, t_db))

        live_parsed_map = {}
        for lt in target_live:
            parsed_l = parse_micro_live_sheet(lt, batch_map.get(lt, []))
            if parsed_l:
                live_parsed_map[lt] = parsed_l

        db_records_raw = rows_to_records(batch_map.get(t_db, [])) if t_db else []
        db_records = []
        for r in db_records_raw:
            j_d = parse_date_obj(r.get("판정일자", ""))
            i_d = parse_date_obj(r.get("검사일자_생산일", ""))
            rc = dict(r)
            if j_d:
                rc["판정일자"] = j_d.strftime("%Y-%m-%d")
            if i_d:
                rc["검사일자_생산일"] = i_d.strftime("%Y-%m-%d")
            db_records.append(rc)

        existing_judge_dates = {r.get("판정일자", "") for r in db_records if r.get("판정일자")}
        merged_hist = list(db_records)
        for lp in live_parsed_map.values():
            if lp["has_actual_input"] and lp["judge_date"] not in existing_judge_dates:
                merged_hist.extend(lp["flat_records"])
                existing_judge_dates.add(lp["judge_date"])
        merged_hist.sort(key=lambda x: (x.get("판정일자", ""), x.get("대분류", "")), reverse=True)

        filled_tabs = [lp for lp in live_parsed_map.values() if lp["has_actual_input"]]
        if filled_tabs:
            filled_tabs.sort(key=lambda x: x["judge_date"], reverse=True)
            def_tab = filled_tabs[0]["tab_title"]
        else:
            def_tab = "일반 일보" if "일반 일보" in live_parsed_map else ("일보작성" if "일보작성" in live_parsed_map else (list(live_parsed_map.keys())[0] if live_parsed_map else ""))

        latest_j_iso = ""
        if merged_hist:
            latest_j_iso = merged_hist[0].get("판정일자", "")
            latest_batch = [r for r in merged_hist if r.get("판정일자") == latest_j_iso]
            has_any_fail = any("부적합" in str(r.get("판정", "")) for r in latest_batch)

            milk_dates = sorted(list({
                r.get("검사일자_생산일", "") for r in latest_batch
                if "발효유" not in str(r.get("대분류", "")) and r.get("검사일자_생산일")
            }), reverse=True)
            ferm_dates = sorted(list({
                r.get("검사일자_생산일", "") for r in latest_batch
                if "발효유" in str(r.get("대분류", "")) and r.get("검사일자_생산일")
            }), reverse=True)

            j_mmdd = fmt_mmdd_short(latest_j_iso)
            m_mmdd = fmt_mmdd_short(milk_dates[0]) if milk_dates else ""
            f_mmdd = fmt_mmdd_short(ferm_dates[0]) if ferm_dates else ""

            if has_any_fail:
                auto_st = "재검·출고보류"
                auto_nt = f"최근 판정({j_mmdd}) 부적합 발생 — 확인 필요"
            else:
                auto_st = "출고적합"
                if m_mmdd and f_mmdd:
                    auto_nt = f"우유({m_mmdd}) · 발효유({f_mmdd}) 제품 특이사항 없음"
                elif m_mmdd:
                    auto_nt = f"우유({m_mmdd}) 생산 제품 특이사항 없음"
                else:
                    auto_nt = f"최근 판정({j_mmdd}) 전 품목 특이사항 없음"
        else:
            ref_lp = live_parsed_map.get(def_tab)
            auto_st = "출고적합"
            if ref_lp:
                m_mmdd = fmt_mmdd_short(ref_lp["milk_insp_date"])
                f_mmdd = fmt_mmdd_short(ref_lp["ferm_insp_date"])
                auto_nt = f"우유({m_mmdd}) · 발효유({f_mmdd}) 제품 특이사항 없음"
            else:
                auto_nt = "최근 판정분 우유 · 발효유 제품 특이사항 없음"

        clean_live_tabs = {}
        for k, v in live_parsed_map.items():
            vc = dict(v)
            vc.pop("flat_records", None)
            clean_live_tabs[k] = vc

        return {
            "has_sheet": True,
            "auto_status": auto_st,
            "auto_note": auto_nt,
            "latest_judge_date": latest_j_iso,
            "live_tabs": clean_live_tabs,
            "default_live_tab": def_tab,
            "hist_rows": merged_hist,
            "total_db_cnt": len(merged_hist)
        }, None
    except Exception as e:
        return empty_micro, f"[미생물일보] {e}"

def build_rt_view_from_db_records(dt_iso: str, p_list: list, c_list: list) -> dict:
    """시트 탭이 삭제되고 DB에만 남은 과거 날짜도 금일·전일 탭과 동일한 3단 요약 화면으로 변환"""
    tab_lbl = p_list[0].get("원본탭명", dt_iso[5:].replace("-", "/")) if p_list else dt_iso[5:].replace("-", "/")
    ster_records = []
    ccp3b_map: Dict[Tuple[str, str], dict] = {}
    ho_ster_sum, ho_rem = {}, {}
    special_notes = []

    for c in c_list:
        rcat = str(c.get("기록분류", "")).strip()
        eq = str(c.get("설비_호기", c.get("포장호기", "-"))).strip()
        sub = str(c.get("세부항목", "-")).strip()
        val = str(c.get("기록내용_수치", "-")).strip()
        rem = str(c.get("특이사항", "")).strip()

        if rem and rem not in ["-", "없음", "이상없음", "적합"] and rem not in special_notes:
            special_notes.append(f"[{eq}] {rem}")

        if "멸균" in rcat and "3B" not in rcat and "포장살균" not in rcat:
            m_pack = re.search(r"\(([A-F]호기)\)", eq)
            if m_pack:
                ho_ster_sum[m_pack.group(1)] = f"{eq} ({val})"
                if rem and rem not in ["-", "없음", "이상없음"]:
                    ho_rem[m_pack.group(1)] = rem
            ster_records.append({
                "멸균기_포장": eq,
                "매칭 제품명": sub,
                "확인시간": val if re.search(r"\d{1,2}:\d{2}", val) else "-",
                "멸균온도": val if "℃" in val else "-",
                "균질압": "-",
                "멸균시간": "-",
                "유량_냉각": "-",
                "여과망_서명": "적합",
                "특이사항": rem or "이상없음",
                "_c_time": "cell-green" if val != "-" else "cell-gray",
                "_c_temp": "cell-green",
                "_c_press": "",
                "_c_hold": "",
                "_c_flt": "cell-green",
                "_c_rem": "cell-yellow" if (rem and rem not in ["-", "없음", "이상없음"]) else ""
            })
        elif "3B" in rcat or "포장살균" in rcat:
            key3 = (sub, eq)
            rec3 = ccp3b_map.setdefault(key3, {
                "항목": sub, "관리기준": "-", "적용설비": eq,
                "A호기": "-", "B호기": "-", "C호기": "-", "D호기": "-", "E호기": "-", "F호기": "-",
                "_cls_map": {}
            })
            for h_nm in ["A호기", "B호기", "C호기", "D호기", "E호기", "F호기"]:
                if c.get(h_nm) and str(c.get(h_nm)).strip() not in ["", "-"]:
                    rec3[h_nm] = str(c.get(h_nm)).strip()
                    rec3["_cls_map"][h_nm] = "cell-green"
            for m_h in re.finditer(r"([A-F]호기)\s*:\s*([^/]+)", val):
                rec3[m_h.group(1)] = m_h.group(2).strip()
                rec3["_cls_map"][m_h.group(1)] = "cell-green"

    compact_rows = []
    seen_cp = set()
    for p in p_list:
        ho = str(p.get("포장호기", "-")).strip()
        pnm = str(p.get("제품명", "-")).strip()
        exp_v = str(p.get("표기소비기한_원문", "-")).strip()
        chk_t = str(p.get("확인시간", "-")).strip()
        ck = (ho, clean_product_for_match(pnm), re.sub(r"\s+", "", exp_v))
        if ck in seen_cp:
            continue
        seen_cp.add(ck)
        rem_t = ho_rem.get(ho, "이상없음")
        compact_rows.append({
            "포장호기": ho,
            "생산 제품명": pnm,
            "소비기한": exp_v,
            "확인시간": chk_t,
            "멸균기·CCP 요약": ho_ster_sum.get(ho, "이력 확인완료"),
            "특이사항": rem_t,
            "_cls_status": "cell-green" if chk_t not in ["", "-"] else "cell-gray",
            "_cls_rem": "cell-yellow" if rem_t != "이상없음" else "cell-green"
        })

    return {
        "tab_title": tab_lbl,
        "prod_date": dt_iso,
        "running_hos": sorted(list({r["포장호기"] for r in compact_rows if r["포장호기"] != "-"})),
        "lines_records": [],
        "compact_rows": compact_rows,
        "ccp3b_records": list(ccp3b_map.values()),
        "ster_records": ster_records,
        "ster_special_notes": special_notes
    }
def fetch_process_rt_sheet(sheet_urls_map: dict):
    sh_rt, err_rt = open_sheet_isolated(sheet_urls_map, "process_rt")
    if sh_rt is None:
        return None, None, [], [], [], f"[상온공정일보] {err_rt}"
    try:
        ws_all = api_call_with_retry(sh_rt.worksheets)
        ws_titles = [w.title for w in ws_all]
        t_today, t_yest, ordered_rt_titles = select_date_tab_titles(ws_titles)

        recent_date_tabs = ordered_rt_titles
        t_hp = next((t for t in ws_titles if t.strip() == HIST_TABS["rt_prod"]), None)
        t_hc = next((t for t in ws_titles if t.strip() == HIST_TABS["rt_ccp"]), None)
        t_fsk = next((t for t in ws_titles if re.sub(r"\s+", "", t) == "제품명식품안전나라" or "식품안전나라" in t), None)
        t_std = next((t for t in ws_titles if "기준" in t or "복사" in t), None)

        fetch_targets = list(dict.fromkeys([t for t in ([t_today, t_yest] + recent_date_tabs + [t_hp, t_hc, t_fsk, t_std]) if t]))
        batch_map = batch_fetch_tabs(sh_rt, fetch_targets)

        rt_spec_map: Dict[Any, dict] = {}
        if t_fsk:
            extract_rt_shelf_specs_from_rows(batch_map.get(t_fsk, []), rt_spec_map)
        if t_std:
            extract_rt_shelf_specs_from_rows(batch_map.get(t_std, []), rt_spec_map)
        for ref_t in [t_today, t_yest]:
            if ref_t and batch_map.get(ref_t):
                extract_rt_shelf_specs_from_rows(batch_map[ref_t][30:], rt_spec_map)

        rt_today_parsed = parse_rt_process_rows(t_today, batch_map.get(t_today, [])) if t_today else None
        rt_yest_parsed = parse_rt_process_rows(t_yest, batch_map.get(t_yest, [])) if t_yest else None
        for p_obj in [rt_today_parsed, rt_yest_parsed]:
            if p_obj and p_obj.get("spec_map"):
                rt_spec_map.update(p_obj["spec_map"])

        def _fix_rt_db_p_row(r_vals: list, cmap: dict):
            c_dt = cmap.get("생산일자", -1)
            c_tb = cmap.get("원본탭명", -1)
            c_ho = cmap.get("포장호기", -1)
            c_pnm = cmap.get("제품명", -1)
            c_exp = cmap.get("표기소비기한_원문", -1)
            c_std = cmap.get("표준소비기한_검색용", -1)
            c_sav = cmap.get("저장일시", -1)

            if c_dt >= 0 and c_tb >= 0:
                pd = parse_date_obj(r_vals[c_dt])
                true_iso = tab_title_to_iso_date(r_vals[c_tb], pd)
                if true_iso and r_vals[c_dt] != true_iso:
                    r_vals[c_dt] = true_iso

            if c_dt >= 0 and c_pnm >= 0 and c_exp >= 0:
                ho_v = r_vals[c_ho] if c_ho >= 0 else ""
                sav_s = r_vals[c_sav] if c_sav >= 0 else ""
                fixed_exp, fixed_std = fix_rt_today_shifted_exp(
                    r_vals[c_exp], r_vals[c_dt], ho_v, r_vals[c_pnm], rt_spec_map, sav_s
                )
                if fixed_exp and fixed_exp != r_vals[c_exp]:
                    r_vals[c_exp] = fixed_exp
                    if c_std >= 0 and fixed_std:
                        r_vals[c_std] = fixed_std

        def _fix_rt_db_c_row(r_vals: list, cmap: dict):
            c_dt = cmap.get("생산일자", -1)
            c_tb = cmap.get("원본탭명", -1)
            if c_dt >= 0 and c_tb >= 0:
                pd = parse_date_obj(r_vals[c_dt])
                true_iso = tab_title_to_iso_date(r_vals[c_tb], pd)
                if true_iso and r_vals[c_dt] != true_iso:
                    r_vals[c_dt] = true_iso

        if t_hp and len(batch_map.get(t_hp, [])) >= 2:
            if heal_db_worksheet_if_dirty(sh_rt, t_hp, batch_map[t_hp], _fix_rt_db_p_row):
                batch_map[t_hp] = get_values_formatted(api_call_with_retry(sh_rt.worksheet, t_hp))
        if t_hc and len(batch_map.get(t_hc, [])) >= 2:
            if heal_db_worksheet_if_dirty(sh_rt, t_hc, batch_map[t_hc], _fix_rt_db_c_row):
                batch_map[t_hc] = get_values_formatted(api_call_with_retry(sh_rt.worksheet, t_hc))

        rt_hist_p = []
        slot_index_map: Dict[Tuple[str, str, str], int] = {}
        db_p_dates_with_items = set()
        for r in (rows_to_records(batch_map.get(t_hp, [])) if t_hp else []):
            rc = dict(r)
            pd = parse_date_obj(rc.get("생산일자", ""))
            true_iso = tab_title_to_iso_date(rc.get("원본탭명", ""), pd)
            if true_iso:
                rc["생산일자"] = true_iso
            elif pd:
                rc["생산일자"] = pd.strftime("%Y-%m-%d")

            p_nm_val = str(rc.get("제품명", "")).strip()
            if not p_nm_val or p_nm_val in ["-", "(비가동)"]:
                continue

            ho_val = str(rc.get("포장호기", "")).strip()
            fixed_e, fixed_std = fix_rt_today_shifted_exp(
                rc.get("표기소비기한_원문", ""),
                rc.get("생산일자", ""),
                ho_val,
                p_nm_val,
                rt_spec_map,
                rc.get("저장일시", "")
            )
            if fixed_e:
                rc["표기소비기한_원문"] = fixed_e
            if fixed_std:
                rc["표준소비기한_검색용"] = fixed_std

            slot_k = (rc.get("생산일자", ""), ho_val, clean_product_for_match(p_nm_val))
            if slot_k in slot_index_map:
                continue
            slot_index_map[slot_k] = len(rt_hist_p)
            if rc.get("생산일자"):
                db_p_dates_with_items.add(rc["생산일자"])
            rt_hist_p.append(rc)

        rt_hist_c = []
        db_c_dates = set()
        for c in (rows_to_records(batch_map.get(t_hc, [])) if t_hc else []):
            cc = dict(c)
            cd = parse_date_obj(cc.get("생산일자", ""))
            true_iso = tab_title_to_iso_date(cc.get("원본탭명", ""), cd)
            if true_iso:
                cc["생산일자"] = true_iso
            elif cd:
                cc["생산일자"] = cd.strftime("%Y-%m-%d")
            if cc.get("생산일자"):
                db_c_dates.add(cc["생산일자"])
            rt_hist_c.append(cc)

        rt_date_views: Dict[str, dict] = {}
        missing_db_p_rows = []
        missing_db_c_rows = []
        rt_p_updated_in_place = False
        now_saved_str = now_kst().strftime("%Y-%m-%d %H:%M:%S")
        live_upsert_tabs = {x for x in (t_today, t_yest) if x}

        for raw_dt_tab in recent_date_tabs:
            try:
                parsed_t = parse_rt_process_rows(raw_dt_tab, batch_map.get(raw_dt_tab, []))
                if not parsed_t:
                    continue
                if parsed_t.get("spec_map"):
                    for sk_k, sk_v in parsed_t["spec_map"].items():
                        cur_sp = rt_spec_map.setdefault(sk_k, {"mfg": "", "ops": "", "exp_ops": ""})
                        for f_k in ("mfg", "ops", "exp_ops"):
                            if sk_v.get(f_k) and not cur_sp.get(f_k):
                                cur_sp[f_k] = sk_v[f_k]

                dt_iso = tab_title_to_iso_date(parsed_t["tab_title"]) or parsed_t["tab_title"]
                parsed_t["prod_date"] = dt_iso
                rt_date_views[dt_iso] = parsed_t
                allow_upsert = (raw_dt_tab in live_upsert_tabs)

                for lr in parsed_t.get("lines_records", []):
                    p_nm = str(lr.get("생산 제품명", "")).strip()
                    if not p_nm or p_nm == "(비가동)":
                        continue
                    ho_cur = str(lr.get("포장호기", "-")).strip()
                    raw_exp_cell = str(lr.get("소비기한", "-")).strip()
                    chk_raw = str(lr.get("확인시간", "-")).strip()

                    exp_clean, std_iso_str = fix_rt_today_shifted_exp(
                        raw_exp_cell, dt_iso, ho_cur, p_nm, rt_spec_map, now_saved_str
                    )
                    if not exp_clean:
                        exp_clean = "-"

                    slot_k = (dt_iso, ho_cur, clean_product_for_match(p_nm))
                    if slot_k in slot_index_map:
                        if allow_upsert:
                            exist_rec = rt_hist_p[slot_index_map[slot_k]]
                            if exp_clean != "-" and str(exist_rec.get("표기소비기한_원문", "")).strip() != exp_clean:
                                exist_rec["표기소비기한_원문"] = exp_clean
                                exist_rec["표준소비기한_검색용"] = std_iso_str
                                exist_rec["저장일시"] = now_saved_str
                                rt_p_updated_in_place = True
                            if chk_raw not in ("", "-", "미기입(대기)") and str(exist_rec.get("확인시간", "")).strip() != chk_raw:
                                exist_rec["확인시간"] = chk_raw
                                rt_p_updated_in_place = True
                    else:
                        new_p_rec = {
                            "생산일자": dt_iso,
                            "원본탭명": parsed_t["tab_title"],
                            "포장호기": ho_cur,
                            "제품명": p_nm,
                            "표기소비기한_원문": exp_clean,
                            "표준소비기한_검색용": std_iso_str,
                            "확인시간": chk_raw,
                            "저장일시": now_saved_str
                        }
                        slot_index_map[slot_k] = len(rt_hist_p)
                        rt_hist_p.append(new_p_rec)
                        if dt_iso not in db_p_dates_with_items:
                            missing_db_p_rows.append(new_p_rec)

                if dt_iso not in db_c_dates:
                    for sr in parsed_t.get("ster_records", []):
                        if sr.get("확인시간") == "기록대기":
                            continue
                        new_c_rec = {
                            "생산일자": dt_iso,
                            "원본탭명": parsed_t["tab_title"],
                            "기록분류": "CCP-1B/2P(멸균)",
                            "설비_호기": sr.get("멸균기_포장", "-"),
                            "세부항목": sr.get("매칭 제품명", "-"),
                            "기록내용_수치": f"{sr.get('확인시간', '-')} / {sr.get('멸균온도', '-')}",
                            "특이사항": sr.get("특이사항", "이상없음"),
                            "저장일시": now_saved_str
                        }
                        rt_hist_c.append(new_c_rec)
                        missing_db_c_rows.append(new_c_rec)

                    for c3 in parsed_t.get("ccp3b_records", []):
                        ho_kv = " / ".join([f"{h}:{c3.get(h, '-')}" for h in ["A호기", "B호기", "C호기", "D호기", "E호기", "F호기"] if c3.get(h, "-") not in ("", "-")])
                        if not ho_kv:
                            continue
                        new_3b_rec = {
                            "생산일자": dt_iso,
                            "원본탭명": parsed_t["tab_title"],
                            "기록분류": "CCP-3B(무균포장살균)",
                            "설비_호기": c3.get("적용설비", "-"),
                            "세부항목": f"{c3.get('항목', '')} ({c3.get('관리기준', '-')})",
                            "기록내용_수치": ho_kv,
                            "특이사항": "적합",
                            "저장일시": now_saved_str
                        }
                        rt_hist_c.append(new_3b_rec)
                        missing_db_c_rows.append(new_3b_rec)
            except Exception:
                continue

        if t_hp and (rt_p_updated_in_place or missing_db_p_rows):
            try:
                hp_matrix = batch_map.get(t_hp, [])
                hp_hdr = [str(h).strip() for h in hp_matrix[0]] if hp_matrix else [
                    "생산일자", "원본탭명", "포장호기", "제품명", "표기소비기한_원문", "표준소비기한_검색용", "확인시간", "저장일시"
                ]
                ws_hp = api_call_with_retry(sh_rt.worksheet, t_hp)
                if rt_p_updated_in_place:
                    sorted_for_save = sorted(rt_hist_p, key=lambda x: x.get("생산일자", ""), reverse=True)
                    full_payload = [hp_hdr] + [[str(rec.get(c_nm, "")) for c_nm in hp_hdr] for rec in sorted_for_save]
                    api_call_with_retry(ws_hp.clear)
                    safe_ws_update(ws_hp, full_payload)
                elif missing_db_p_rows:
                    append_p = [[str(rec.get(col_nm, "")) for col_nm in hp_hdr] for rec in missing_db_p_rows]
                    api_call_with_retry(ws_hp.append_rows, append_p, value_input_option="USER_ENTERED")
            except Exception:
                pass

        if t_hc and missing_db_c_rows:
            try:
                hc_matrix = batch_map.get(t_hc, [])
                if hc_matrix:
                    hc_hdr = [str(h).strip() for h in hc_matrix[0]]
                    ws_hc = api_call_with_retry(sh_rt.worksheet, t_hc)
                    append_c = [[str(rec.get(col_nm, "")) for col_nm in hc_hdr] for rec in missing_db_c_rows]
                    api_call_with_retry(ws_hc.append_rows, append_c, value_input_option="USER_ENTERED")
            except Exception:
                pass

        rt_alerts_by_dt = validate_rt_shelf_life_on_db(rt_hist_p, rt_spec_map)
        exp_val_lookup: Dict[Tuple[str, str, str], dict] = {}
        for r in rt_hist_p:
            ek = (str(r.get("생산일자", "")).strip(), str(r.get("포장호기", "")).strip(), clean_product_for_match(r.get("제품명", "")))
            exp_val_lookup[ek] = r

        all_db_dates = sorted(list({r.get("생산일자", "") for r in rt_hist_p if r.get("생산일자")}), reverse=True)
        for db_dt in all_db_dates:
            if db_dt not in rt_date_views:
                p_sub = [r for r in rt_hist_p if r.get("생산일자") == db_dt]
                c_sub = [c for c in rt_hist_c if c.get("생산일자") == db_dt]
                if p_sub or c_sub:
                    rt_date_views[db_dt] = build_rt_view_from_db_records(db_dt, p_sub, c_sub)

        def _attach_rt_exp_status(view_obj: Optional[dict]):
            if not view_obj:
                return
            v_dt = str(view_obj.get("prod_date", "")).strip()
            view_obj["exp_alerts"] = rt_alerts_by_dt.get(v_dt, [])
            for cp in view_obj.get("compact_rows", []):
                lk = (v_dt, str(cp.get("포장호기", "")).strip(), clean_product_for_match(cp.get("생산 제품명", "")))
                matched_r = exp_val_lookup.get(lk)
                if matched_r:
                    if matched_r.get("표기소비기한_원문"):
                        cp["소비기한"] = matched_r["표기소비기한_원문"]
                    cp["_cls_exp"] = matched_r.get("_cls_exp", "cell-green")
                    cp["_exp_tip"] = matched_r.get("_exp_tip", "")
                else:
                    cp["_cls_exp"] = "cell-green"
            for lr in view_obj.get("lines_records", []):
                if lr.get("생산 제품명") == "(비가동)":
                    continue
                lk = (v_dt, str(lr.get("포장호기", "")).strip(), clean_product_for_match(lr.get("생산 제품명", "")))
                matched_r = exp_val_lookup.get(lk)
                if matched_r:
                    if matched_r.get("표기소비기한_원문"):
                        lr["소비기한"] = matched_r["표기소비기한_원문"]
                    lr["_cls_exp"] = matched_r.get("_cls_exp", "cell-green")
                    lr["_exp_tip"] = matched_r.get("_exp_tip", "")
                    if matched_r.get("_cls_exp") == "cell-red":
                        lr["_cls"] = "cell-red"

        for dv_obj in rt_date_views.values():
            _attach_rt_exp_status(dv_obj)
        _attach_rt_exp_status(rt_today_parsed)
        _attach_rt_exp_status(rt_yest_parsed)

        if rt_today_parsed is None:
            rt_today_parsed = {
                "tab_title": "-", "prod_date": "-", "running_hos": [],
                "lines_records": [], "compact_rows": [], "ccp3b_records": [],
                "ster_records": [], "ster_special_notes": [], "exp_alerts": []
            }
        rt_today_parsed["date_views"] = rt_date_views

        rt_hist_p.sort(key=lambda x: x.get("생산일자", ""), reverse=True)
        rt_hist_c.sort(key=lambda x: x.get("생산일자", ""), reverse=True)

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


def fetch_process_soy_sheet(sheet_urls_map: dict):
    sh_soy, err_soy = open_sheet_isolated(sheet_urls_map, "process_soy")
    if sh_soy is None:
        return "-", {}, [], [], [], f"[두유배양일보] {err_soy}"
    try:
        ws_all = api_call_with_retry(sh_soy.worksheets)
        ws_titles = [w.title for w in ws_all]

        today_d = now_kst().date()
        soy_dated_tabs = []
        for raw_t in ws_titles:
            t_s = raw_t.strip()
            if t_s.startswith("_이력DB_") or any(ex in t_s for ex in ["양식", "복사", "백업", "사본", "대기", "예비", "샘플", "수정"]):
                continue
            td = parse_soy_tab_date(t_s)
            if td:
                soy_dated_tabs.append((td, raw_t))

        past_or_today = sorted([x for x in soy_dated_tabs if x[0] <= today_d], key=lambda x: x[0], reverse=True)
        future_tabs = sorted([x for x in soy_dated_tabs if x[0] > today_d], key=lambda x: x[0], reverse=True)
        ordered_soy_titles = [t for _, t in (past_or_today + future_tabs)]

        top_live_titles = ordered_soy_titles[:12]
        t_hist_soy = next((t for t in ws_titles if t.strip() == HIST_TABS["soy_inc"]), None)

        batch_map = batch_fetch_tabs(sh_soy, top_live_titles + ([t_hist_soy] if t_hist_soy else []))

        # [시트 원본 자동 수리] _이력DB_두유배양의 생산일자를 무조건 [원본탭명 날짜 - 배양일수]로 시트 원본에 영구 교정
        raw_soy_db = batch_map.get(t_hist_soy, []) if t_hist_soy else []
        if t_hist_soy and len(raw_soy_db) >= 2:
            def _fix_soy_db_row(r_vals: list, cmap: dict):
                c_prod = cmap.get("생산일자", -1)
                c_insp = cmap.get("검사일자", -1)
                c_tab = cmap.get("원본탭명", cmap.get("시트탭", cmap.get("탭명", -1)))
                c_inc = cmap.get("배양구분", cmap.get("배양검사", cmap.get("배양일수", -1)))
                c_sav = cmap.get("저장일시", cmap.get("백업일시", -1))
                if c_prod < 0 or c_tab < 0:
                    return
                inc_txt = r_vals[c_inc] if c_inc >= 0 else ""
                offset = extract_soy_inc_days(inc_txt)
                ref_d = (
                    (parse_date_obj(r_vals[c_sav]) if c_sav >= 0 else None)
                    or (parse_date_obj(r_vals[c_insp]) if c_insp >= 0 else None)
                    or parse_date_obj(r_vals[c_prod])
                )
                tab_d = parse_soy_tab_date(r_vals[c_tab], ref_d)
                if tab_d:
                    true_prod = (tab_d - timedelta(days=offset)).strftime("%Y-%m-%d")
                    true_insp = tab_d.strftime("%Y-%m-%d")
                    if r_vals[c_prod] != true_prod:
                        r_vals[c_prod] = true_prod
                    if c_insp >= 0 and r_vals[c_insp] != true_insp:
                        r_vals[c_insp] = true_insp

            if heal_db_worksheet_if_dirty(sh_soy, t_hist_soy, raw_soy_db, _fix_soy_db_row):
                batch_map[t_hist_soy] = get_values_formatted(api_call_with_retry(sh_soy.worksheet, t_hist_soy))

        soy_tabs_parsed = {}
        default_tab_title = "-"

        for raw_t in ordered_soy_titles[:5]:
            t_title = raw_t.strip()
            b_list, h_list = parse_soy_incubation_rows(raw_t, batch_map.get(raw_t, []))
            if b_list:
                soy_tabs_parsed[t_title] = {"blocks": b_list, "hourly": h_list}
                if default_tab_title == "-":
                    default_tab_title = t_title

        if default_tab_title == "-" and ordered_soy_titles:
            first_raw = ordered_soy_titles[0]
            default_tab_title = first_raw.strip()
            b_list, h_list = parse_soy_incubation_rows(first_raw, batch_map.get(first_raw, []))
            soy_tabs_parsed[default_tab_title] = {"blocks": b_list, "hourly": h_list}

        def_pack = soy_tabs_parsed.get(default_tab_title, {"blocks": [], "hourly": []})

        soy_hist = []
        seen_soy_keys = set()

        for raw_t in top_live_titles:
            t_clean = raw_t.strip()
            if t_clean in soy_tabs_parsed:
                b_list = soy_tabs_parsed[t_clean]["blocks"]
            else:
                b_list, _ = parse_soy_incubation_rows(raw_t, batch_map.get(raw_t, []))
            for blk in b_list:
                p_nm = str(blk.get("품목명", "")).strip()
                if not p_nm or p_nm == "-":
                    continue
                prod_iso = blk.get("생산일", "-")
                insp_iso = blk.get("검사일(개봉일)", "-")
                ho_nm = str(blk.get("포장기", "-")).strip()
                sk = (prod_iso, ho_nm, clean_product_for_match(p_nm))
                if sk in seen_soy_keys:
                    continue
                seen_soy_keys.add(sk)
                exp_raw = str(blk.get("소비기한", "-")).strip()
                first_exp = exp_raw.split("/")[0].strip() if exp_raw else ""
                exp_d = parse_date_obj(first_exp)
                soy_hist.append({
                    "생산일자": prod_iso,
                    "원본탭명": t_clean,
                    "포장기": ho_nm,
                    "배양구분": blk.get("배양구분", "-"),
                    "품목명": p_nm,
                    "표기소비기한_원문": exp_raw,
                    "표준소비기한_검색용": exp_d.strftime("%Y-%m-%d") if exp_d else "",
                    "검사일자": insp_iso,
                    "생산시간": blk.get("생산 시작~종료", "-"),
                    "총검사수량": blk.get("총 검사수량", "0"),
                    "오염수량": blk.get("오염수량", "-"),
                    "검사자": blk.get("검사자", "-"),
                    "판정": blk.get("판정", "검사전"),
                    "특이사항": blk.get("특이사항", "없음")
                })

        soy_hist_raw = rows_to_records(batch_map.get(t_hist_soy, [])) if t_hist_soy else []
        for sr in soy_hist_raw:
            sc = dict(sr)
            raw_tab = sc.get("원본탭명") or sc.get("시트탭") or sc.get("탭명") or ""
            inc_str = sc.get("배양구분") or sc.get("배양검사") or sc.get("배양일수") or sc.get("배양일") or ""
            offset = extract_soy_inc_days(inc_str)

            ref_d = (
                parse_date_obj(sc.get("저장일시", ""))
                or parse_date_obj(sc.get("백업일시", ""))
                or parse_date_obj(sc.get("검사일자", ""))
                or parse_date_obj(sc.get("생산일자", ""))
            )
            tab_d = parse_soy_tab_date(raw_tab, ref_d)

            if tab_d:
                sc["검사일자"] = tab_d.strftime("%Y-%m-%d")
                sc["생산일자"] = (tab_d - timedelta(days=offset)).strftime("%Y-%m-%d")
            else:
                insp_d = parse_date_obj(sc.get("검사일자", ""))
                prod_d = parse_date_obj(sc.get("생산일자", ""))
                if insp_d:
                    sc["검사일자"] = insp_d.strftime("%Y-%m-%d")
                    sc["생산일자"] = (insp_d - timedelta(days=offset)).strftime("%Y-%m-%d")
                elif prod_d:
                    sc["생산일자"] = prod_d.strftime("%Y-%m-%d")

            sk = (
                sc.get("생산일자", ""),
                str(sc.get("포장기", "")).strip(),
                clean_product_for_match(sc.get("품목명", ""))
            )
            if sk in seen_soy_keys:
                continue
            seen_soy_keys.add(sk)
            soy_hist.append(sc)

        soy_hist.sort(key=lambda x: (x.get("생산일자", ""), x.get("포장기", "")), reverse=True)
        return default_tab_title, soy_tabs_parsed, def_pack["blocks"], def_pack["hourly"], soy_hist, None
    except Exception as e:
        return "-", {}, [], [], [], f"[두유배양일보] {e}"
# ============================ [api/index.py - PART 2 / 4 끝] ============================
# ==============================================================================
# [api/index.py - PART 3 / 4]
# 협력업체·심사점수 정밀 파서 · 법규 1~2줄 요약 [_법규요약DB] · 표시사항 [특이사항] 탭 연동
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
            sc_s = rec["점수"]
            master_records.append({
                "NO": i,
                "구분": rec["구분"],
                "업체명": rec["업체명"],
                "품목": rec["품목"],
                "점수": sc_s,
                "결과": rec["결과"],
                "비고": rec["비고"]
            })
            if rec["결과"] == "등급 외":
                w_cnt = rec["_weak_cnt"]
                under_comp_rows.append({
                    "업체명": rec["업체명"],
                    "점수": f"{sc_s}점" if sc_s != "미제출" else "미제출",
                    "미비 항목": f"{w_cnt}개" if sc_s != "미제출" else "미제출",
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

        sub_done = tot_sup - cnt_unsub
        grade_sum = [
            {"등급": "승인", "점수": "85-100점", "업체수": cnt_ok, "조치": "승인"},
            {"등급": "지도", "점수": "70-84점", "업체수": cnt_guide, "조치": "업체별 개선사항 피드백"},
            {"등급": "등급 외", "점수": "70점미만, 미제출", "업체수": cnt_out, "조치": "복수거래, 거래중지 등 검토"},
            {"등급": "합계", "점수": "-", "업체수": tot_sup, "조치": f"제출 {sub_done}개사 / 미제출 {cnt_unsub}개사"}
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

    hdr_idx = -1
    hdr0_ns = []
    for idx, cand_r in enumerate(rows[:6]):
        cand_ns = [re.sub(r"\s+", "", str(c)) for c in cand_r]
        if "심사명" in cand_ns or "고유ID" in cand_ns or ("구분" in cand_ns and any("일자" in x or "날짜" in x for x in cand_ns)):
            hdr_idx = idx
            hdr0_ns = cand_ns
            break

    if hdr_idx >= 0:
        col_uid = next((i for i, h in enumerate(hdr0_ns) if "ID" in h.upper() or "고유" in h), -1)
        col_tp = next((i for i, h in enumerate(hdr0_ns) if "구분" in h or "분류" in h), -1)
        col_dt = next((i for i, h in enumerate(hdr0_ns) if "일자" in h or "날짜" in h or "일시" in h), -1)
        col_nm = next((i for i, h in enumerate(hdr0_ns) if "심사명" in h or "업체" in h or "내용" in h or "제목" in h), -1)
        col_st = next((i for i, h in enumerate(hdr0_ns) if "진행" in h or "현황" in h or "상태" in h), -1)
        col_sc = next((i for i, h in enumerate(hdr0_ns) if "점수" in h), -1)
        col_mm = next((i for i, h in enumerate(hdr0_ns) if "비고" in h or "메모" in h), -1)
        col_ts = next((i for i, h in enumerate(hdr0_ns) if "수정" in h), -1)

        out = []
        for r_idx, r in enumerate(rows[hdr_idx + 1:], start=hdr_idx + 2):
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
        out.sort(key=lambda x: x.get("심사일자", ""), reverse=True)
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
        out_dual.sort(key=lambda x: x.get("심사일자", ""), reverse=True)
    return out_dual


def fetch_sup_and_audit_kpi_sheet(sheet_urls_map: dict):
    """6번 협력업체 목표치(88.3%↑)가 가중치 배점(60%)과 혼동되지 않도록 정밀 고정"""
    default_sup_kpi = {
        "has_sheet": False,
        "target_rate": 88.3,
        "actual_rate": 0.0,
        "level_str": "-",
        "signal": "gray",
        "criteria": KPI_LEVEL_CRITERIA["supplier_exec"],
        "weight_rows": [],
        "visit_rows": [],
        "visit_counts": {"전체": 0, "연세유업": 0, "연세생건": 0, "이롬": 0}
    }
    default_aud_kpi = {
        "has_sheet": False,
        "score_target": 94.28,
        "score_actual": 0.0,
        "score_level": "-",
        "imp_target": 94.5,
        "imp_actual": 0.0,
        "imp_level": "-",
        "imp_cnt_text": "",
        "signal": "gray",
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

                # 목표치는 반드시 80~99% 범위의 '이상 / ↑' 표기만 인식 (배점 20%, 40%, 60% 오인식 완전 차단)
                if r_i <= 5:
                    m_t = re.search(r"(8\d(?:\.\d+)?|9\d(?:\.\d+)?)\s*%\s*(?:이상|↑)", row_join)
                    if m_t:
                        sup_kpi["target_rate"] = float(m_t.group(1))

                if "현재점수" in "".join(r_ns):
                    for m_a in re.finditer(r"(\d{2,3}(?:\.\d+)?)\s*%", row_join):
                        cand_v = float(m_a.group(1))
                        if cand_v not in (20.0, 40.0, 60.0):
                            sup_kpi["actual_rate"] = cand_v
                            break

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

            sup_kpi["level_str"] = eval_kpi_level(sup_kpi["actual_rate"], "supplier_exec") if sup_kpi["actual_rate"] > 0 else "-"
            sup_kpi["weight_rows"] = weight_rows
            sup_kpi["visit_rows"] = visit_rows
            sup_kpi["visit_counts"] = {
                "전체": len(visit_rows),
                "연세유업": sum(1 for r in visit_rows if r["계열사 구분"] == "연세유업"),
                "연세생건": sum(1 for r in visit_rows if r["계열사 구분"] == "연세생건"),
                "이롬": sum(1 for r in visit_rows if r["계열사 구분"] == "이롬")
            }
            sup_kpi["signal"] = "green" if sup_kpi["actual_rate"] >= sup_kpi["target_rate"] else "red"

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
                        m_imp_t = re.search(r"(9\d(?:\.\d+)?)\s*%\s*↑", cell_s)
                        if m_imp_t:
                            aud_kpi["imp_target"] = float(m_imp_t.group(1))
                    for cell_s in r_pad[5:10]:
                        m_sc_t = re.search(r"(9\d(?:\.\d+)?)\s*점\s*↑", cell_s)
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
                    r6_s = r_pad[6]
                    r7_s = r_pad[7]
                    r8_s = r_pad[8]
                    pct_int = int(round(to_float(r7_s, 0) * 100))
                    formula_rows.append({
                        "구분": r_pad[5],
                        "기준수치": f"{r6_s}점" if r_ns[5] == "점수평균" else f"대상 {r6_s}건",
                        "적용배율": f"{pct_int}%" if to_float(r7_s, 0) <= 1.0 else r7_s,
                        "환산점수": f"{calc_num:.2f}점" if calc_num > 0 else r8_s
                    })
                elif r_i < 12 and r_ns[5] == "" and r_pad[6].isdigit() and formula_rows and formula_rows[-1]["구분"] == "통과율":
                    prev_std = formula_rows[-1]["기준수치"]
                    r6_s = r_pad[6]
                    formula_rows[-1]["기준수치"] = f"통과 {r6_s}건 / {prev_std}"

                if hist_hdr_idx < 0 and r_ns[0] in ["심사일", "일자"] and ("심사업체" in r_ns[1] or "업체" in r_ns[1] or "기관" in r_ns[1]):
                    hist_hdr_idx = r_i

            if aud_kpi["score_actual"] <= 0 and formula_rows:
                sum_f = sum(to_float(fr.get("환산점수", 0), 0.0) for fr in formula_rows)
                if sum_f > 0:
                    aud_kpi["score_actual"] = round(sum_f, 2)

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
                if aud_kpi["imp_actual"] <= 0:
                    aud_kpi["imp_actual"] = round((tot_imp_sum / tot_iss_sum) * 100, 2)

            aud_kpi["score_level"] = eval_kpi_level(aud_kpi["score_actual"], "audit_score") if aud_kpi["score_actual"] > 0 else "-"
            aud_kpi["imp_level"] = eval_kpi_level(aud_kpi["imp_actual"], "audit_imp") if aud_kpi["imp_actual"] > 0 else "-"
            aud_kpi["formula_rows"] = formula_rows
            aud_kpi["history_rows"] = history_rows
            aud_kpi["signal"] = "green" if aud_kpi["score_actual"] >= aud_kpi["score_target"] else "red"

        sched_from_sak = parse_schedule_tab_rows(batch_map.get(t_sched, [])) if t_sched is not None else None
        return sup_kpi, aud_kpi, sched_from_sak, None
    except Exception as e:
        return default_sup_kpi, default_aud_kpi, None, f"[협력업체, 심사 점수] {e}"


# ==============================================================================
# 6. [5] '법규정보' 시트 — 1~2줄 압축 요약 + [_법규요약DB] 영구 캐싱 + [대시보드] 동기화
# ==============================================================================
def make_law_id(tab_name: str, law_name: str, enf_raw: str, notice_raw: str) -> str:
    ln = re.sub(r"\s+", "", str(law_name or ""))
    en = re.sub(r"\s+", "", str(enf_raw or ""))
    nt = re.sub(r"\s+", "", str(notice_raw or ""))
    raw_key = f"{tab_name.strip()}|{ln}|{en}|{nt}"
    return hashlib.md5(raw_key.encode("utf-8")).hexdigest()[:10]


def is_broken_or_overlong_summary(s: str, full_content: str = "") -> bool:
    if not s:
        return True
    t = s.strip()
    if len(t) <= 8 or len(t) > 155:
        return True
    ns = re.sub(r"\s+", "", t)
    if ns in ["개정이유", "1.개정이유", "주요내용", "2.주요내용"] or ns.startswith("개정이유·") or ns.startswith("개정이유:") or ns.startswith("1.개정이유"):
        return True
    # 기호로 끝나거나 조사가 잘린 채 끝나는 경우 불완전 요약으로 판정
    if t[-1] in ("·", ",", "/", ":", "-", "(", "「", "『", "‘", "'"):
        return True
    if re.search(r"\s+(?:및|또는|등을|등을|관한|따른|위한|하여|에서|으로|에게|부터)$", t):
        return True
    last_tok = t.split()[-1].rstrip(".,)")
    if len(last_tok) >= 1 and re.search(r"(?:을|를|이|가|은|는|의|에|로)$", last_tok):
        if not re.search(r"(?:강화|완화|신설|개정|추가|확대|축소|제외|포함|명확화|구체화|허용|금지|의무화|합리화|정비|개선|마련|적용|시행|폐지|조정|변경|확인|준수|관리|표시|고시|처분|기준|대상|시설|절차|방법|요건)$", last_tok):
            return True
    # 이전 56자 강제 슬라이싱으로 단어 중간이 잘린 경우(예: '식품제' -> 원문에는 '식품제조') 자동 감지
    if full_content and len(last_tok) >= 2:
        idx = full_content.find(last_tok)
        if idx != -1 and (idx + len(last_tok)) < len(full_content):
            nxt_ch = full_content[idx + len(last_tok)]
            if re.match(r"[가-힣A-Za-z0-9]", nxt_ch) and nxt_ch not in ("함", "됨"):
                return True
    return False


def smart_trim_korean_clause(text: str, limit: int = 105) -> str:
    """
    1) 길이는 무조건 limit(기본 105자, 1~2줄) 이하로 엄격 제한
    2) 단어 중간 절단 원천 금지
    3) limit 초과 장문일 때 한국어 법령의 맨 끝 핵심 결론(확대·신설·허용·완화 등)을 보존
    """
    s = re.sub(r"\s+", " ", str(text or "")).strip(" ,./·:-")
    if len(s) <= limit:
        return s

    s_compact = re.sub(
        r"([가-힣A-Za-z0-9]+)\s*,\s*([가-힣A-Za-z0-9]+)\s*,\s*[가-힣A-Za-z0-9,\s·]+?(?:및|또는)\s*[가-힣A-Za-z0-9\s·]+?(?=\s*(?:을|를|의|에| 등| 원재료| 포함| 제외| 추가| 삭제))",
        r"\1·\2 등",
        s
    )
    if len(s_compact) <= limit:
        return s_compact.strip(" ,./·:-")
    s = s_compact

    tail_conclusion = ""
    m_tail = re.search(
        r"\s+([가-힣A-Za-z0-9·\(\)]+\s+(?:표시대상\s*)?(?:강화|완화|신설|개정|추가|확대|축소|제외|포함|명확화|구체화|허용|금지|의무화|면제|합리화|정비|개선|변경|조정|폐지|마련|적용|시행|처분기준\s*신설|처분기준\s*명확화))$",
        s
    )
    if m_tail and len(m_tail.group(1)) <= 28:
        tail_conclusion = m_tail.group(1).strip()

    if tail_conclusion and limit >= (len(tail_conclusion) + 25):
        head_budget = limit - len(tail_conclusion) - 3
        cut_h = s[:head_budget + 1]
        pos_h = max(cut_h.rfind(", "), cut_h.rfind(" "))
        head_part = cut_h[:pos_h] if pos_h >= int(head_budget * 0.5) else cut_h[:head_budget]
        head_part = head_part.strip(" ,./·:-(")
        head_part = re.sub(r"\s+(?:및|또는|관한|따른|위한|하여|에서|으로|대하여)$", "", head_part).strip(" ,./·:-")
        if head_part.count("(") > head_part.count(")"):
            head_part = head_part[:head_part.rfind("(")].strip(" ,./·:-")
        return f"{head_part} → {tail_conclusion}"

    cut = s[:limit + 1]
    best_pos = -1
    for sep in [" / ", ", ", " "]:
        p = cut.rfind(sep)
        if p >= int(limit * 0.55):
            best_pos = p
            break
    if best_pos == -1:
        sp = s.find(" ", limit)
        trimmed = s[:sp] if sp != -1 else s[:limit]
    else:
        trimmed = cut[:best_pos]

    trimmed = trimmed.strip(" ,./·:-(")
    trimmed = re.sub(r"\s+(?:및|또는|관한|따른|위한|하여|에서|으로|대하여)$", "", trimmed).strip(" ,./·:-")
    trimmed = re.sub(r"(?<=[가-힣]{2})(?:을|를|은|는)$", "", trimmed)
    if trimmed.count("(") > trimmed.count(")"):
        trimmed = trimmed[:trimmed.rfind("(")].strip(" ,./·:-")
    return trimmed


def compress_legal_clause_line(clause: str) -> str:
    s = str(clause or "").strip()
    s = re.sub(r"\(\s*(?:안\s*)?제\d+조[^)]*\)", "", s)
    s = re.sub(r"\(\s*(?:안\s*)?별표\s*\d+[^)]*\)", "", s)
    s = re.sub(r"「[^」]+」(?:\s*제\d+조[^\s,]*)?\s*에\s*따른\s*", "", s)
    s = re.sub(r"의사,\s*치과의사,\s*한의사[^,]*대학교수\s*또는\s*그\s*밖의\s*사람이", "의료인·전문가가", s)
    s = re.sub(r"인공지능시스템을\s*이용하여\s*생성한\s*실제와\s*구분하기\s*어려운\s*가상의\s*음향,\s*이미지\s*또는\s*영상\s*등을\s*활용한\s*광고로서", "AI 가상 생성물 활용", s)
    s = re.sub(r"유병률,\s*중증도,\s*국제기준과의\s*조화\s*등을\s*종합적으로\s*고려하여\s*", "", s)
    s = re.sub(r"허가받은\s*축산물\s*작업장\s*외의\s*장소에서\s*축산물의\s*가공\s*[·,]\s*포장\s*또는\s*보관이\s*가능한\s*식품\s*제조\s*시설을\s*", "작업장 외 축산물 가공·보관 시설을 ", s)
    s = re.sub(r"한식간장,\s*양조간장,\s*산분해간장,\s*효소분해간장,\s*혼합간장\s*및\s*간장을\s*원재료로\s*하여\s*제조\s*[·,]\s*가공한\s*", "간장류(한식·양조·혼합간장 등) 및 이를 원재료로 한 ", s)
    s = re.sub(r"에\s*대한\s*처분\s*기준을\s*명확화(?:함)?", " 처분기준 명확화", s)
    s = re.sub(r"에\s*대한\s*처분\s*기준(?:을)?\s*신설(?:함)?", " 처분기준 신설", s)
    s = re.sub(r"할\s*수\s*있도록\s*(?:함|개정함?|완화함?|신설함?)", " 허용", s)
    s = re.sub(r"(?:을|를)\s*할\s*수\s*있게\s*(?:함|개정함?)", " 허용", s)
    s = re.sub(r"하도록\s*(?:규정함|개정함|명시함|함)", "", s)
    s = re.sub(r"(?:을|를)\s*구체적으로\s*명시(?:함)?", " 구체화", s)
    s = re.sub(r"(?:으로|로)\s*정함\.?$", "", s)
    s = re.sub(r"(?:함|됨)\.?$", "", s)
    s = re.sub(r"\s+", " ", s).strip(" ,./·:-")
    return s


def extract_concise_law_summary(law_name: str, content_str: str, max_len: int = 112) -> str:
    raw = str(content_str or "").strip()
    t = str(law_name or "").strip()
    if not raw or raw == "-":
        return f"{t} 개정 세부기준 확인"

    m_main = re.search(
        r"(?:^|\n)\s*(?:2\.|Ⅱ\.|[2②]|[\[【\(])?\s*(?:주요\s*내용|개정\s*내용|주요\s*골자)\s*[\]】\)]?\s*[:\-]?",
        raw
    )
    if m_main:
        target_body = raw[m_main.end():]
    else:
        target_body = re.sub(r"^\s*(?:1\.|[1①]|[\[【\(])?\s*개정\s*이유\s*[\]】\)]?\s*[:\-]?\s*", "", raw)

    m_stop = re.search(
        r"(?:^|\n)\s*(?:3\.|4\.|Ⅲ\.|[3③]|[\[【\(])?\s*(?:의견\s*제출|참고\s*사항|기타\s*사항|부\s*칙|향후\s*일정)",
        target_body
    )
    if m_stop:
        target_body = target_body[:m_stop.start()]

    raw_lines = [re.sub(r"\s+", " ", ln).strip() for ln in target_body.splitlines() if ln.strip()]
    clauses = []
    quoted_keywords = []

    for qm in re.findall(r"['‘’「]([^'‘’」]{1,18})['‘’」]", target_body):
        qc = qm.strip()
        if qc and "법" not in qc and "규칙" not in qc and "기준" not in qc and qc not in quoted_keywords:
            quoted_keywords.append(qc)

    for ln in raw_lines:
        ns_ln = re.sub(r"\s+", "", ln)
        if ns_ln in ["개정이유", "1.개정이유", "주요내용", "2.주요내용", "개정내용"]:
            continue
        if ln.startswith("*") or ln.startswith("※"):
            continue
        ln_clean = re.sub(r"^(?:[가-하]\.|[0-9]+\)|[0-9]+\.|[①-⑩]|[-•▪▫▶▸ㅇ])\s*", "", ln).strip(" :-")
        comp = compress_legal_clause_line(ln_clean)
        if len(comp) >= 4 and comp not in clauses:
            clauses.append(comp)

    if not clauses:
        fallback = smart_trim_korean_clause(compress_legal_clause_line(target_body), max_len)
        return fallback if fallback else f"{t} 세부 개정내용 확인"

    # 단일 핵심 조항이 길면 1개 문장을 온전히 보존(최대 105자 어절단위), 짧은 조항이면 2개 결합
    if len(clauses[0]) >= 58:
        summary_out = smart_trim_korean_clause(clauses[0], max_len)
    else:
        c1 = smart_trim_korean_clause(clauses[0], 64)
        if len(clauses) >= 2:
            remain = max_len - len(c1) - 3
            if remain >= 22:
                c2 = smart_trim_korean_clause(clauses[1], remain)
                summary_out = f"{c1} / {c2}" if c2 else c1
            else:
                summary_out = c1
        else:
            summary_out = c1

    missing_q = [q for q in quoted_keywords[:4] if q not in summary_out]
    if missing_q and len(summary_out) <= max_len - 18:
        q_str = "·".join(missing_q[:3])
        summary_out = f"{summary_out} ({q_str} 등)"

    return summary_out


def generate_gemini_law_summary_single(law_name: str, content_str: str) -> Optional[str]:
    api_key = os.environ.get("GEMINI_API_KEY", "").strip()
    if not api_key:
        return None

    prompt = (
        "너는 식품회사 품질안전팀 법규 담당자야. 아래 법령 개정안 원문에서 '개정 이유'와 법조문 번호·수식어는 모두 빼고, "
        "'주요 내용'에서 신설·변경되는 핵심 대상 품목·성분·표시기준만 딱 1~2줄 개조식 명사형(공백 포함 65~90자 이내, "
        "줄바꿈 없이 ' / '로 구분)으로 초간결하게 요약해줘.\n\n"
        f"[법규명] {law_name}\n[원문]\n{content_str}"
    )
    for endpoint in [
        f"https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash:generateContent?key={api_key}",
        f"https://aiplatform.googleapis.com/v1/publishers/google/models/gemini-2.5-flash:generateContent?key={api_key}"
    ]:
        try:
            resp = requests.post(
                endpoint,
                json={"contents": [{"role": "user", "parts": [{"text": prompt}]}]},
                timeout=9
            )
            if resp.status_code == 200:
                cands = resp.json().get("candidates", [])
                if cands:
                    parts = cands[0].get("content", {}).get("parts", [])
                    if parts and parts[0].get("text"):
                        txt = re.sub(r"\s+", " ", parts[0]["text"]).strip().lstrip("-•* ")
                        if 10 <= len(txt) <= 135:
                            return txt
        except Exception:
            continue
    return None


def compute_obsolescence_prep_tag(d_day: Optional[int], is_notice: bool, prep_status: str) -> Tuple[str, str]:
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
    months_left = max(1, round((d_day - 365) / 30))
    return f"여유 ({months_left}개월 뒤 착수)", "badge-neutral"


def parse_law_summary_db_tab(rows: list) -> Dict[str, dict]:
    if not rows or len(rows) < 2:
        return {}
    hdr_ns = [re.sub(r"\s+", "", str(x)) for x in rows[0]]
    c_id = next((i for i, h in enumerate(hdr_ns) if "ID" in h.upper()), 0)
    c_sum = next((i for i, h in enumerate(hdr_ns) if "요약" in h), 4)
    c_mth = next((i for i, h in enumerate(hdr_ns) if "방식" in h), 5)
    c_ts = next((i for i, h in enumerate(hdr_ns) if "수정" in h or "일시" in h), 6)

    out: Dict[str, dict] = {}
    for r in rows[1:]:
        if not any(str(x).strip() for x in r):
            continue
        lid = str(r[c_id]).strip() if c_id < len(r) else ""
        summ = str(r[c_sum]).strip() if c_sum < len(r) else ""
        mth = str(r[c_mth]).strip() if c_mth < len(r) else "auto_2line"
        ts = str(r[c_ts]).strip() if c_ts < len(r) else ""
        if lid and summ:
            out[lid] = {"summary": summ, "method": mth, "updated_at": ts}
    return out


def sync_law_summary_db_worksheet(sh, all_laws: list, summary_db_map: Dict[str, dict]):
    ws_all = api_call_with_retry(sh.worksheets)
    ws_sdb = next((w for w in ws_all if w.title.strip() == LAW_SUMMARY_DB_TAB), None)
    if ws_sdb is None:
        ws_sdb = api_call_with_retry(sh.add_worksheet, title=LAW_SUMMARY_DB_TAB, rows=200, cols=8)

    now_s = now_kst().strftime("%Y-%m-%d %H:%M:%S")
    payload = [LAW_SUMMARY_DB_COLS]
    seen_ids = set()

    for it in all_laws:
        lid = it["law_id"]
        if lid in seen_ids:
            continue
        seen_ids.add(lid)
        entry = summary_db_map.get(lid, {})
        payload.append([
            str(lid),
            str(it.get("탭구분", "")),
            str(it.get("시행일", "-")),
            str(it.get("법규명", "")),
            str(entry.get("summary") or it.get("자동조치요약", "")),
            str(entry.get("method", "auto_2line")),
            str(entry.get("updated_at") or now_s)
        ])

    api_call_with_retry(ws_sdb.clear)
    safe_ws_update(ws_sdb, payload)


def parse_law_dashboard_tab(rows: list) -> Tuple[bool, Dict[str, dict]]:
    if not rows or len(rows) < 2:
        return False, {}

    hdr_idx = -1
    hdr_ns = []
    for idx, r in enumerate(rows[:5]):
        cand = [re.sub(r"\s+", "", str(x)) for x in r]
        if any(k in "".join(cand) for k in ["법규ID", "법규명", "법령명"]):
            hdr_idx = idx
            hdr_ns = cand
            break
    if hdr_idx < 0:
        return False, {}

    c_id = next((i for i, h in enumerate(hdr_ns) if "ID" in h.upper()), -1)
    c_nm = next((i for i, h in enumerate(hdr_ns) if "법규명" in h or "법령명" in h or "고시명" in h), -1)
    c_prep = next((i for i, h in enumerate(hdr_ns) if "준비" in h or "상태" in h), -1)
    c_sum = next((i for i, h in enumerate(hdr_ns) if "요약" in h or "내용" in h), -1)

    pinned_map: Dict[str, dict] = {}
    for r in rows[hdr_idx + 1:]:
        if not any(str(x).strip() for x in r):
            continue
        lid = str(r[c_id]).strip() if (0 <= c_id < len(r)) else ""
        nm = str(r[c_nm]).strip() if (0 <= c_nm < len(r)) else ""
        if not lid and not nm:
            continue
        prep = str(r[c_prep]).strip() if (0 <= c_prep < len(r)) else "검토전"
        if prep not in LAW_PREP_LIST:
            prep = "검토전"
        summ = str(r[c_sum]).strip() if (0 <= c_sum < len(r)) else ""
        entry = {"prep": prep, "summary": summ, "law_name": nm}
        if lid:
            pinned_map[lid] = entry
        norm_nm = re.sub(r"\s+", "", nm)
        if norm_nm:
            pinned_map[norm_nm] = entry

    return (len(pinned_map) > 0), pinned_map


def sync_law_dashboard_worksheet(sh, all_laws_enriched: list):
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
        return [], False, {}, {}, f"[법규정보] {err}"

    try:
        ws_all = api_call_with_retry(sh.worksheets)
        ws_titles = [w.title for w in ws_all]
        t_dash = next((t for t in ws_titles if t.strip() == LAW_DASH_TAB), None)
        t_sdb = next((t for t in ws_titles if t.strip() == LAW_SUMMARY_DB_TAB), None)

        exclude_sys = {t for t in [t_dash, t_sdb] if t}
        target_tabs = [t for t in ws_titles if t not in exclude_sys and any(yr in t for yr in ["2026", "2025", "2027"])]
        if not target_tabs:
            target_tabs = [t for t in ws_titles if t not in exclude_sys][:4]

        fetch_list = target_tabs + ([t_dash] if t_dash else []) + ([t_sdb] if t_sdb else [])
        batch_map = batch_fetch_tabs(sh, fetch_list, "FORMATTED_VALUE")

        dash_initialized, dash_tab_map = parse_law_dashboard_tab(batch_map.get(t_dash, [])) if t_dash else (False, {})
        summary_db_map = parse_law_summary_db_tab(batch_map.get(t_sdb, [])) if t_sdb else {}

        today_d = now_kst().date()
        now_s = now_kst().strftime("%Y-%m-%d %H:%M:%S")
        all_laws = []
        sdb_dirty = (t_sdb is None)
        missing_laws_count = 0

        raw_parsed_items = []
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

                lid = make_law_id(t_name, law_nm, enf_raw, notice_raw)
                if lid not in summary_db_map:
                    missing_laws_count += 1

                raw_parsed_items.append((t_name, tab_has_notice, last_ministry, law_nm, content_full, notice_raw, enf_raw, rel_raw, lid))

        # 서버 타임아웃 없이 한 번 동기화할 때마다 최대 4건씩 Gemini AI 요약을 생성하여 _법규요약DB에 영구 축적
        ai_budget = 0
        ai_used = 0

        for (t_name, tab_has_notice, last_ministry, law_nm, content_full, notice_raw, enf_raw, rel_raw, lid) in raw_parsed_items:
            cached_s_obj = summary_db_map.get(lid)
            if cached_s_obj and cached_s_obj.get("summary"):
                saved_s = cached_s_obj["summary"]
                saved_m = cached_s_obj.get("method", "auto_2line")
                if saved_m == "manual" or (not is_broken_or_overlong_summary(saved_s, content_full)):
                    short_sum = saved_s
                else:
                    ai_txt = generate_gemini_law_summary_single(law_nm, content_full) if ai_used < ai_budget else None
                    if ai_txt and not is_broken_or_overlong_summary(ai_txt):
                        ai_used += 1
                        short_sum = ai_txt
                        summary_db_map[lid] = {"summary": short_sum, "method": "gemini", "updated_at": now_s}
                    else:
                        short_sum = extract_concise_law_summary(law_nm, content_full, max_len=105)
                        summary_db_map[lid] = {"summary": short_sum, "method": "auto_2line", "updated_at": now_s}
                    sdb_dirty = True
            else:
                ai_txt = generate_gemini_law_summary_single(law_nm, content_full) if ai_used < ai_budget else None
                if ai_txt and not is_broken_or_overlong_summary(ai_txt):
                    ai_used += 1
                    short_sum = ai_txt
                    summary_db_map[lid] = {"summary": short_sum, "method": "gemini", "updated_at": now_s}
                else:
                    short_sum = extract_concise_law_summary(law_nm, content_full, max_len=105)
                    summary_db_map[lid] = {"summary": short_sum, "method": "auto_2line", "updated_at": now_s}
                sdb_dirty = True

            is_notice_item = tab_has_notice or any(k in law_nm for k in ["예고", "개정안", "개정령(안)", "고시(안)"])
            stage_type = "예고" if is_notice_item else "고시"

            enf_d = parse_date_obj(enf_raw)
            d_day = (enf_d - today_d).days if enf_d else None
            enf_iso = enf_d.strftime("%Y-%m-%d") if enf_d else (enf_raw or "미정")

            is_qs_dept = ("품질안전" in rel_raw) or ("식품안전" in rel_raw) or ("품질관리" in rel_raw) or ("전부문" in rel_raw) or ("전사" in rel_raw)
            auto_default_show = (not tab_has_notice) and is_qs_dept and (d_day is not None and d_day >= -14)

            all_laws.append({
                "law_id": lid,
                "탭구분": t_name.strip(),
                "법규단계": stage_type,
                "예고여부": is_notice_item,
                "주무부처": last_ministry or "-",
                "법규명": law_nm,
                "자동조치요약": short_sum,
                "원문내용": content_full or "-",
                "고시_예고일": notice_raw or "-",
                "시행일": enf_iso,
                "시행일_원문": enf_raw or "-",
                "d_day": d_day,
                "당사관련부서": rel_raw or "-",
                "품질안전해당": is_qs_dept,
                "auto_default": auto_default_show
            })

        if sdb_dirty and all_laws:
            try:
                sync_law_summary_db_worksheet(sh, all_laws, summary_db_map)
            except Exception:
                pass

        needs_dash_resync = (t_dash and not dash_initialized and bool(all_laws))
        if dash_initialized and dash_tab_map:
            for _, v_obj in dash_tab_map.items():
                if is_broken_or_overlong_summary(v_obj.get("summary", "")):
                    v_obj["summary"] = ""
                    needs_dash_resync = True

        if t_dash and needs_dash_resync and all_laws:
            repaired_bundle = apply_law_preferences(all_laws, dash_initialized, dash_tab_map, {})
            try:
                sync_law_dashboard_worksheet(sh, repaired_bundle["all_laws"])
                dash_initialized = True
                dash_tab_map = {
                    x["law_id"]: {"prep": x["prep_status"], "summary": x["실무조치요약"], "law_name": x["법규명"]}
                    for x in repaired_bundle["dashboard_laws"]
                }
            except Exception:
                pass

        return all_laws, dash_initialized, dash_tab_map, summary_db_map, None
    except Exception as e:
        return [], False, {}, {}, f"[법규정보] {e}"


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

        if is_broken_or_overlong_summary(custom_sum):
            clean_custom = ""
        else:
            clean_custom = str(custom_sum).strip()

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
            abs_d = abs(d_val)
            d_label, d_cls, sort_key = f"시행+{abs_d}일", "badge-ok", 50000 + abs_d

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
    explicitly_hidden_all = bool(legacy_prefs) and all(v.get("mode") == "Hide" for v in legacy_prefs.values())
    if not dashboard_list and enriched and not explicitly_hidden_all:
        fallback_cands = [x for x in enriched if not x.get("예고여부") and (x.get("d_day") is None or x["d_day"] >= -30)]
        if not fallback_cands:
            fallback_cands = list(enriched)
        fallback_cands.sort(key=lambda x: x["_sort_k"])
        for fc in fallback_cands[:6]:
            fc["show_on_dashboard"] = True
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
# 7. [6] '클레임, 부적합 이력' · [7] 보조 설정 DB · [8] '표시사항 현황'([특이사항] 탭 포함)
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
                    if k_key in DEFAULT_MANUAL_KPIS and k_key not in ("kpi_11", "kpi_5"):
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

        loaded_lbl_memo_fallback = None
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
                        if b_key == "label_memo":
                            loaded_lbl_memo_fallback = b_val
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

        return legacy_sched, saved_manual, loaded_lbl_memo_fallback, loaded_micro, loaded_law_prefs, loaded_label_pins, None
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
    dept_s = row.get("요청부서", "")
    title_s = row.get("제목", "")
    raw = f"{dept_s}|{title_s}"
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


def parse_label_special_tab_rows(rows: list) -> str:
    if not rows:
        return ""
    r0_ns = [re.sub(r"\s+", "", str(x)) for x in rows[0]]
    is_std_header = len(r0_ns) >= 1 and r0_ns[0] in ["특이사항내용", "특이사항", "내용", "메모"]
    target_rows = rows[1:] if is_std_header else rows

    lines = []
    for r in target_rows:
        if not r:
            continue
        if is_std_header:
            c0 = str(r[0]).strip()
            if c0:
                lines.append(c0)
        else:
            cells = [str(x).strip() for x in r if str(x).strip() and not re.match(r"^20\d{2}-\d{2}-\d{2}\s+\d{2}:\d{2}", str(x).strip())]
            if cells:
                lines.append(" ".join(cells))
    return "\n".join(lines).strip()


def save_label_special_tab_memo(sheet_urls_map: dict, memo_text: str):
    sh, err = open_sheet_isolated(sheet_urls_map, "label_status")
    if sh is None:
        raise HTTPException(status_code=500, detail=f"'표시사항 현황' 시트를 열 수 없습니다: {err}")

    ws_all = api_call_with_retry(sh.worksheets)
    ws_sp = next((w for w in ws_all if w.title.strip() == LABEL_SPECIAL_TAB), None)
    if ws_sp is None:
        ws_sp = api_call_with_retry(sh.add_worksheet, title=LABEL_SPECIAL_TAB, rows=50, cols=4)

    now_s = now_kst().strftime("%Y-%m-%d %H:%M:%S")
    clean_lines = [ln.strip() for ln in str(memo_text or "").splitlines() if ln.strip()]
    payload = [["특이사항내용", "수정일시"]]
    for ln in clean_lines:
        payload.append([ln, now_s])

    api_call_with_retry(ws_sp.clear)
    safe_ws_update(ws_sp, payload)


def fetch_label_status_sheet(sheet_urls_map: dict):
    sh, err = open_sheet_isolated(sheet_urls_map, "label_status")
    if sh is None:
        return [], None, err
    try:
        ws_all = api_call_with_retry(sh.worksheets)
        ws_titles = [w.title for w in ws_all]
        t_main = next((t for t in ws_titles if t.strip() != LABEL_SPECIAL_TAB), ws_titles[0] if ws_titles else None)
        t_sp = next((t for t in ws_titles if t.strip() == LABEL_SPECIAL_TAB), None)

        batch_map = batch_fetch_tabs(sh, [t_main] + ([t_sp] if t_sp else []), "UNFORMATTED_VALUE")
        rows = batch_map.get(t_main, []) if t_main else []
        sp_memo = parse_label_special_tab_rows(batch_map.get(t_sp, [])) if t_sp is not None else None

        if len(rows) < 2:
            return [], sp_memo, "표시사항 현황 시트에 데이터가 없습니다."

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
        return data, sp_memo, None
    except Exception as e:
        return [], None, str(e)


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
# ============================ [api/index.py - PART 3 / 4 끝] ============================
# ==============================================================================
# [api/index.py - PART 4 / 4]
# 통합 번들 엔진 · 냉장/상온 분리 LOT 역추적(미생물 누적+배양예정 매칭) · 양방향 저장 API
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
        gubun_s = nr.get("구분", "방문심사")
        nm_s = nr["심사명"]
        up_banner = f"[{gubun_s} · {d_tag}] {nm_s} ({dt_s})"
        up_type = gubun_s
    else:
        nr = sorted(sched_list, key=lambda x: x.get("심사일자", ""), reverse=True)[0]
        dt_s = nr["심사일자"][5:] if len(nr["심사일자"]) >= 10 else nr["심사일자"]
        gubun_s = nr.get("구분", "방문심사")
        nm_s = nr["심사명"]
        up_banner = f"[최근 일정 · {gubun_s}] {nm_s} ({dt_s})"
        up_type = gubun_s

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
        v_dt = vr["심사일자"][5:]
        v_prefix = f"금일 방문심사 ({v_dt})"
    elif v_inprog:
        vr = v_inprog[0][2]
        v_dt = vr["심사일자"][5:]
        v_prefix = f"진행중 방문심사 ({v_dt})"
    elif v_future:
        _, dl, vr = v_future[0]
        v_dt = vr["심사일자"][5:]
        v_prefix = f"차기 방문심사 D-{dl} ({v_dt})"
    else:
        vr = sorted([x[2] for x in visit_only], key=lambda x: x.get("심사일자", ""), reverse=True)[0]
        v_dt = vr["심사일자"][5:]
        v_prefix = f"최근 방문심사 ({v_dt})"

    st_nm = vr["진행현황"] if vr["진행현황"] in AUDIT_STATUS_CONFIG else "심사준비중"
    cfg = AUDIT_STATUS_CONFIG[st_nm]
    v_nm = vr["심사명"]
    return {
        "has_data": True,
        "upcoming_banner": up_banner,
        "upcoming_type": up_type,
        "has_visit_gauge": True,
        "visit_title": f"[{v_prefix}] {v_nm}",
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

    cur_kst_m = now_kst().month
    r_25 = [r for r in valid_rows if r.get("_yr_num") == 2025]
    r_26 = [r for r in valid_rows if r.get("_yr_num") == 2026]
    completed_26_cnt = sum(1 for r in completed_rows if r.get("_yr_num") == 2026)

    max_m_26 = max([int(r.get("_mo_num", 0)) for r in r_26 if int(r.get("_mo_num", 0)) > 0] or [cur_kst_m])
    disp_cur_m = cur_kst_m if any(int(r.get("_mo_num", 0)) == cur_kst_m for r in r_26) else max_m_26
    cnt_26_cur_m = sum(1 for r in r_26 if int(r.get("_mo_num", 0)) == disp_cur_m)
    cnt_25_cur_m = sum(1 for r in r_25 if int(r.get("_mo_num", 0)) == disp_cur_m)

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
        top_parts = []
        for x in rev_dept_breakdown[:4]:
            d_k = x["dept"]
            c_v = x["cnt"]
            top_parts.append(f"{d_k} {c_v}")
        rest_cnt = sum(x["cnt"] for x in rev_dept_breakdown[4:])
        if rest_cnt > 0:
            top_parts.append(f"외 {rest_cnt}")
        rev_summary_line = " · ".join(top_parts)
    else:
        rev_summary_line = "현재 검토 대기 품목 없음"

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
        "cur_month": disp_cur_m,
        "cnt_26_cur_m": cnt_26_cur_m,
        "cnt_25_cur_m": cnt_25_cur_m,
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
            legacy_sched, saved_manual_kpis, saved_lbl_memo_fb, saved_micro, saved_law_prefs, saved_lbl_pins, err_audit = resolve_with_last_good("audit", f_audit.result(), 6)
            lbl_rows, lbl_sp_memo, err_lbl = resolve_with_last_good("lbl", f_lbl.result(), 2)
            raw_sub, raw_m_cnt, err_raw = resolve_with_last_good("raw", f_raw.result(), 2)
            raw_laws, dash_init_flag, dash_tab_map, law_sdb_map, err_law = resolve_with_last_good("law", f_law.result(), 4)
            sup_kpi_sheet, aud_kpi_sheet, sak_sched, err_sak = resolve_with_last_good("sak", f_sak.result(), 3)
            claim_nc_data, err_cnh = resolve_with_last_good("cnh", f_cnh.result(), 1)

        audit_sched = sak_sched if (sak_sched is not None and len(sak_sched) > 0) else (legacy_sched if legacy_sched else [])
        if not audit_sched:
            fallback_scheds = []
            for idx_h, hr in enumerate((aud_kpi_sheet or {}).get("history_rows", [])[:10]):
                d_obj = parse_date_obj(hr.get("심사일", ""))
                dt_s = d_obj.strftime("%Y-%m-%d") if d_obj else str(hr.get("심사일", "-"))
                fallback_scheds.append({
                    "고유ID": f"aud_{idx_h}",
                    "구분": "방문심사",
                    "심사일자": dt_s,
                    "심사명": hr.get("심사업체", "-"),
                    "진행현황": "심사종료",
                    "점수": hr.get("점수", ""),
                    "비고": hr.get("평가유형(H열)", ""),
                    "수정일시": ""
                })
            for idx_v, vr in enumerate((sup_kpi_sheet or {}).get("visit_rows", [])[:10]):
                d_obj = parse_date_obj(vr.get("점검일", ""))
                dt_s = d_obj.strftime("%Y-%m-%d") if d_obj else str(vr.get("점검일", "-"))
                fallback_scheds.append({
                    "고유ID": f"sup_{idx_v}",
                    "구분": "협력업체 방문",
                    "심사일자": dt_s,
                    "심사명": f"[{vr.get('계열사 구분', '')}] {vr.get('업체명', '-')}",
                    "진행현황": "심사종료",
                    "점수": "",
                    "비고": vr.get("비고", ""),
                    "수정일시": ""
                })
            fallback_scheds.sort(key=lambda x: x.get("심사일자", ""), reverse=True)
            audit_sched = fallback_scheds

        dairy_only_prods, rt_only_prods, master_prods = [], [], []
        seen_dairy, seen_rt, seen_all = set(), set(), set()

        def make_prod_dedup_key(s: str) -> str:
            vol_tok = extract_volume_ml_token(s)
            ns = re.sub(r"\s+", "", s).lower()
            pk = "B" if "병" in ns else ("C" if "카톤" in ns else "")
            return f"{clean_product_for_match(s)}|{vol_tok}|{pk}"

        def push_prod(target_list: list, seen_set: set, p_str: str):
            ps = re.sub(r"\(주\s*\d+\)", "", str(p_str or "")).strip()
            if not ps or ps in ["-", "(비가동)", "(제품명 미기입)"]:
                return
            if re.match(r"^\d+(?:\.\d+)?\s*(?:㎖|mL|ml|L|g|kg)?$", ps):
                return
            nk = make_prod_dedup_key(ps)
            if len(clean_product_for_match(ps)) >= 2 and nk not in seen_set:
                seen_set.add(nk)
                target_list.append(ps)

        for dp in dairy_prods:
            push_prod(dairy_only_prods, seen_dairy, dp)
            push_prod(master_prods, seen_all, dp)
        for r in dy_hp:
            push_prod(dairy_only_prods, seen_dairy, r.get("제품명", ""))
            push_prod(master_prods, seen_all, r.get("제품명", ""))

        for fp in fsk_prods:
            push_prod(rt_only_prods, seen_rt, fp)
            push_prod(master_prods, seen_all, fp)
        for r in rt_hp:
            push_prod(rt_only_prods, seen_rt, r.get("제품명", ""))
            push_prod(master_prods, seen_all, r.get("제품명", ""))

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
        sig_cost = "green" if cost_dept_sum["actual_uk"] >= cost_targets["부문"] else ("red" if cost_rows else "gray")

        claim_target_ppm = 1.12
        avail_claim_years = sorted(list({r["연도"] for r in claim_cat}), reverse=True)
        active_claim_yr = claim_trend[0].get("당해_연도", "2026년") if claim_trend else (avail_claim_years[0] if avail_claim_years else "2026년")

        year_totals_map = {}
        div_claims = {}
        for yr in avail_claim_years:
            m_yn = re.search(r"(\d{4})", yr)
            prv_cand = f"{int(m_yn.group(1)) - 1}년" if m_yn else ""
            prv_yr = prv_cand if prv_cand in avail_claim_years else next((y for y in avail_claim_years if y < yr), "")
            year_totals_map[yr] = build_year_total_claim_pack(claim_cat, yr, prv_yr)
            div_claims[yr] = {
                "dairy": build_division_claim_summary(claim_cat, yr, DAIRY_CATEGORIES, prv_yr),
                "rt": build_division_claim_summary(claim_cat, yr, RT_CATEGORIES, prv_yr)
            }

        cur_cl_pack = year_totals_map.get(active_claim_yr, {})
        row_cur_sum = next((r for r in claim_cat if r["연도"] == active_claim_yr and r["유형"] == "합계"), None)
        claim_actual_ppm = float(cur_cl_pack.get("total_ppm", 0.0))
        claim_total_cnt = int(cur_cl_pack.get("total_cnt", 0))
        cl_act_l = cur_cl_pack.get("act_line", [None] * 12)
        cl_fc_l = cur_cl_pack.get("fc_line", [None] * 12)
        cl_full_l = cur_cl_pack.get("full_line", [0.0] * 12)
        cl_slope = cur_cl_pack.get("slope", 0.0)
        cl_m_cnt = cur_cl_pack.get("recorded_months", 0)
        cl_last_i = cur_cl_pack.get("last_idx", -1)
        claim_feb_pred = cur_cl_pack.get("feb_pred", 0.0)
        claim_cum_pred = cur_cl_pack.get("cum_pred_ppm", claim_actual_ppm)
        sig_claim = "gray" if not row_cur_sum else ("red" if claim_actual_ppm > claim_target_ppm else "green")

        rt_t_lines = rt_today["lines_records"] if rt_today else []
        rt_running_hos = rt_today["running_hos"] if rt_today else []
        active_rt_cnt = sum(1 for r in rt_t_lines if r["생산 제품명"] != "(비가동)")
        wait_rt_cnt = sum(1 for r in rt_t_lines if r["_cls"] == "cell-yellow")
        red_rt_cnt = sum(1 for r in rt_t_lines if r["_cls"] == "cell-red") + (
            sum(1 for sr in rt_today["ster_records"] if "cell-red" in (sr["_c_temp"], sr["_c_press"], sr["_c_hold"], sr["_c_flt"])) if rt_today else 0
        )

        milk_carton_pack = [r for r in milk_pack if r.get("라인구분") != "우유(병포장)"]
        milk_bottle_pack = [r for r in milk_pack if r.get("라인구분") == "우유(병포장)"]

        active_milk_carton_cnt = sum(1 for r in milk_carton_pack if "금일생산" in str(r.get("가동여부", "")))
        active_milk_bottle_cnt = sum(1 for r in milk_bottle_pack if "금일생산" in str(r.get("가동여부", "")))
        active_milk_cnt = active_milk_carton_cnt + active_milk_bottle_cnt
        active_ferm_cnt = sum(1 for r in ferm_pack if "금일생산" in str(r.get("가동여부", "")))
        active_dairy_cnt = active_milk_cnt + active_ferm_cnt

        # 우유 탭 또는 발효유 탭 중 하나라도 마감 후 삭제·초기화되면 당일(또는 최근 마감일) 이력 DB에서 자동 복원
        if (active_milk_cnt == 0 or active_ferm_cnt == 0) and dy_hp:
            today_dy = [r for r in dy_hp if r.get("생산일자") == today_str]
            if not today_dy and active_dairy_cnt == 0:
                latest_dy_dt = max((r.get("생산일자", "") for r in dy_hp), default="")
                if latest_dy_dt == (cur_kst - timedelta(days=1)).strftime("%Y-%m-%d"):
                    today_dy = [r for r in dy_hp if r.get("생산일자") == latest_dy_dt]
            if today_dy:
                if active_milk_cnt == 0:
                    active_milk_carton_cnt = sum(1 for r in today_dy if r.get("라인구분") == "우유(카톤)")
                    active_milk_bottle_cnt = sum(1 for r in today_dy if r.get("라인구분") == "우유(병포장)")
                    active_milk_cnt = active_milk_carton_cnt + active_milk_bottle_cnt
                if active_ferm_cnt == 0:
                    active_ferm_cnt = sum(1 for r in today_dy if "발효" in str(r.get("라인구분", "")) or "호상" in str(r.get("라인구분", "")))
                active_dairy_cnt = active_milk_cnt + active_ferm_cnt

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
        final_sup_lv = sup_kpi_sheet.get("level_str") if sup_kpi_sheet.get("has_sheet") else (eval_kpi_level(final_sup_rate, "supplier_exec") if final_sup_rate > 0 else "-")
        sig_supplier = "gray" if final_sup_rate <= 0 else ("green" if final_sup_rate >= final_sup_target else "red")

        merged_manual = {k: {**v, "notes": list(v["notes"])} for k, v in DEFAULT_MANUAL_KPIS.items()}
        for mk_k, mk_val in saved_manual_kpis.items():
            if mk_k in merged_manual and mk_k not in ("kpi_11", "kpi_5"):
                merged_manual[mk_k].update(mk_val)

        if sup_kpi_sheet.get("has_sheet") or final_sup_rate > 0:
            vc = sup_kpi_sheet.get("visit_counts", {})
            v_all = vc.get("전체", 0)
            v_y = vc.get("연세유업", 0)
            v_s = vc.get("연세생건", 0)
            v_e = vc.get("이롬", 0)
            merged_manual["kpi_11"].update({
                "target_text": f"실행률 {final_sup_target}% 이상",
                "actual_text": f"{final_sup_rate:.2f}% ({final_sup_lv})",
                "compact_val": f"{final_sup_rate:.1f}%",
                "sub_badge": f"방문점검 {v_all}개사(유업{v_y}·생건{v_s}·이롬{v_e})" if sup_kpi_sheet.get("has_sheet") else f"서류제출 {sub_sup_cnt}/{total_sup_cnt}개사",
                "target_num": final_sup_target,
                "actual_num": round(final_sup_rate, 2),
                "signal": sig_supplier
            })

        if aud_kpi_sheet.get("has_sheet"):
            sc_t = aud_kpi_sheet["score_target"]
            sc_a = aud_kpi_sheet["score_actual"]
            sc_lv = aud_kpi_sheet["score_level"]
            h_cnt = len(aud_kpi_sheet.get("history_rows", []))
            merged_manual["kpi_5"].update({
                "target_text": f"{sc_t}점 이상",
                "actual_text": f"{sc_a}점 ({sc_lv})" if sc_a > 0 else "수검 데이터 집계중",
                "compact_val": f"{sc_a}점" if sc_a > 0 else "-",
                "sub_badge": f"수검 {h_cnt}건 · 클릭 시 심사점수 상세",
                "target_num": sc_t,
                "actual_num": round(sc_a, 2),
                "signal": aud_kpi_sheet["signal"]
            })

        if lbl_sp_memo is not None:
            label_memo_final = lbl_sp_memo.strip()
        elif saved_lbl_memo_fb is not None:
            label_memo_final = saved_lbl_memo_fb.strip()
        else:
            label_memo_final = DEFAULT_LABEL_MEMO

        # 미생물 출고 판정: '미생물 일보' 구글 시트가 연결되어 있으면 항상 시트 판정 결과를 최우선 반영
        raw_m_pref = saved_micro if isinstance(saved_micro, dict) else dict(DEFAULT_MICRO_STATUS)
        if micro_data.get("has_sheet"):
            micro_final = {
                "mode": "자동연동",
                "status": micro_data["auto_status"],
                "note": micro_data["auto_note"],
                "updated_at": f"미생물 일보 시트 연동 ({micro_data.get('latest_judge_date') or today_str})"
            }
        else:
            micro_final = {
                "mode": raw_m_pref.get("mode", "자동연동"),
                "status": raw_m_pref.get("status", "출고적합"),
                "note": raw_m_pref.get("note") or "최근 판정분 우유 · 발효유 제품 특이사항 없음",
                "updated_at": raw_m_pref.get("updated_at", "")
            }

        law_bundle = apply_law_preferences(raw_laws, dash_init_flag, dash_tab_map or {}, saved_law_prefs or {})
        label_analytics = build_label_analytics(lbl_rows, saved_lbl_pins or [])

        # 원자재 표시사항 DB: '해당없음(안전)' 배제 후 실제 신규 4종(들깨·참깨·캐슈넛·아몬드) 검출 품목만 정밀 집계
        sub_raw_cnt = len(raw_sub)
        verified_raw_cnt = 0
        allergy_raw_cnt = 0
        new_allergen_stats = {"들깨": 0, "참깨": 0, "캐슈넛": 0, "아몬드": 0}
        enriched_raw_rows = []

        for r in raw_sub:
            rc = dict(r)
            v_st = str(rc.get("검증상태", "")).strip()
            e_alg = str(rc.get("기존알레르기", "")).strip()
            n_alg = str(rc.get("신규알레르기", "")).strip()
            ing_txt = str(rc.get("원재료명(한글표시사항)", rc.get("원재료명", ""))).strip()

            if re.search(r"완료|적합", v_st) or (not v_st and (e_alg or n_alg)):
                verified_raw_cnt += 1

            is_safe_txt = (not n_alg) or any(sk in n_alg for sk in ["해당없음", "안전", "없음", "무", "-"])
            detected_4 = []
            check_pool = ("" if is_safe_txt else n_alg) + " " + ing_txt
            if "들깨" in check_pool:
                detected_4.append("들깨")
            if "참깨" in check_pool or "흑임자" in check_pool or "검은깨" in check_pool or "통깨" in check_pool:
                detected_4.append("참깨")
            if "캐슈넛" in check_pool or "캐슈너트" in check_pool:
                detected_4.append("캐슈넛")
            if "아몬드" in check_pool:
                detected_4.append("아몬드")

            has_new_alg = (not is_safe_txt) or (len(detected_4) > 0)
            if has_new_alg:
                allergy_raw_cnt += 1
                for d_k in detected_4:
                    new_allergen_stats[d_k] += 1
                if is_safe_txt and detected_4:
                    rc["_신규알레르기_표시"] = ", ".join(detected_4) + " (원재료명 검출)"
                else:
                    rc["_신규알레르기_표시"] = n_alg
            else:
                rc["_신규알레르기_표시"] = n_alg if n_alg else "해당없음 (안전)"

            rc["_has_new_allergy"] = has_new_alg
            rc["_detected_list"] = detected_4
            enriched_raw_rows.append(rc)

        dairy_hist_dates = sorted(list({r.get("생산일자", "") for r in (dy_hp + dy_hc) if r.get("생산일자")}), reverse=True)
        dairy_hist_times_map = {
            dt_k: extract_hist_dairy_run_times(dy_hc, dt_k, "ALL")
            for dt_k in dairy_hist_dates
        }

        dairy_exp_alerts_map: Dict[str, List[dict]] = {}
        for dr_it in dy_hp:
            if dr_it.get("_exp_alert") and dr_it.get("생산일자"):
                dt_a = dr_it["생산일자"]
                lv_a = "red" if dr_it.get("_cls_exp") == "cell-red" else "yellow"
                entry_a = {"level": lv_a, "msg": dr_it["_exp_alert"]}
                dairy_exp_alerts_map.setdefault(dt_a, [])
                if entry_a not in dairy_exp_alerts_map[dt_a]:
                    dairy_exp_alerts_map[dt_a].append(entry_a)

        yest_iso_str = yest_dt.strftime("%Y-%m-%d")
        active_exp_alerts: List[dict] = []
        for al in ((rt_today or {}).get("exp_alerts", []) + (rt_yest or {}).get("exp_alerts", [])):
            if al not in active_exp_alerts:
                active_exp_alerts.append(al)
        latest_dy_iso = dairy_hist_dates[0] if dairy_hist_dates else today_str
        for d_chk in list(dict.fromkeys([today_str, yest_iso_str, latest_dy_iso])):
            if d_chk in (today_str, yest_iso_str):
                for al in dairy_exp_alerts_map.get(d_chk, []):
                    if al not in active_exp_alerts:
                        active_exp_alerts.append(al)

        exp_red_cnt = sum(1 for a in active_exp_alerts if a.get("level") == "red")
        exp_warn_cnt = sum(1 for a in active_exp_alerts if a.get("level") == "yellow")

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
        w_kr = weekdays_kr[done_kst.weekday()]
        kst_korean_full = f"{done_kst.year}년 {done_kst.month}월 {done_kst.day}일 ({w_kr})"

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
                "active_year": active_claim_yr,
                "target_ppm": claim_target_ppm,
                "actual_ppm": claim_actual_ppm,
                "total_cnt": claim_total_cnt,
                "slope": cl_slope,
                "recorded_months": cl_m_cnt,
                "feb_pred": claim_feb_pred,
                "cum_pred_ppm": claim_cum_pred,
                "last_idx": cl_last_i,
                "act_line": cl_act_l,
                "fc_line": cl_fc_l,
                "full_line": cl_full_l,
                "signal": sig_claim,
                "years": avail_claim_years,
                "year_totals": year_totals_map,
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
                    "active_milk_carton_cnt": active_milk_carton_cnt,
                    "active_milk_bottle_cnt": active_milk_bottle_cnt,
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
                    "yest_notes": rt_yest["ster_special_notes"] if rt_yest else [],
                    "exp_red_cnt": exp_red_cnt,
                    "exp_warn_cnt": exp_warn_cnt,
                    "exp_alerts": active_exp_alerts
                },
                "rt_today": rt_today,
                "rt_yest": rt_yest,
                "milk_date": milk_dt,
                "ferm_date": ferm_dt,
                "milk_pack": milk_pack,
                "milk_carton_pack": milk_carton_pack,
                "milk_bottle_pack": milk_bottle_pack,
                "ferm_pack": ferm_pack,
                "milk_run_times": milk_times,
                "ferm_run_times": ferm_times,
                "dairy_hist_times_map": dairy_hist_times_map,
                "dairy_exp_alerts_map": dairy_exp_alerts_map,
                "soy_default_tab": soy_def_tab,
                "soy_tabs_map": soy_map,
                "micro": micro_data,
                "rt_hist_p": rt_hp,
                "rt_hist_c": rt_hc,
                "dairy_hist_p": dy_hp,
                "dairy_hist_c": dy_hc,
                "soy_hist": soy_hist,
                "fsk_products_cnt": len(fsk_prods),
                "dairy_products": dairy_only_prods,
                "rt_products": rt_only_prods,
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
                "master_cnt": raw_m_cnt if raw_m_cnt > 0 else sub_raw_cnt,
                "sub_cnt": sub_raw_cnt,
                "verified_cnt": verified_raw_cnt,
                "allergy_cnt": allergy_raw_cnt,
                "allergen_breakdown": new_allergen_stats,
                "rows": enriched_raw_rows
            }
        }

        _BUNDLE_CACHE["ts"] = time.time() - (CACHE_TTL_SEC - 15.0 if errors else 0.0)
        _BUNDLE_CACHE["date_key"] = today_str
        _BUNDLE_CACHE["data"] = bundle
        return bundle


# ==============================================================================
# 9. 정적 파일 · 냉장/상온 분리 LOT 역추적 · 시트 직통 양방향 저장 API 엔드포인트
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
    summary_method: Optional[str] = "manual"


class LawAiReqPayload(BaseModel):
    law_id: str


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

# ==============================================================================
# '원가절감 현황' 시트 내 [로그인] 탭 연동 인증 · 비밀번호 변경 · 관리자 계정관리 엔진
# ==============================================================================
LOGIN_TAB_NAME = "로그인"
AUTH_USER_COLS = ["사번", "성명", "직급", "소속_직책", "비밀번호", "권한", "상태", "최근접속일시", "비번변경일시"]

INITIAL_AUTH_USERS = [
    {"사번": "19930503", "성명": "오재필", "직급": "부장", "소속_직책": "품질안전부문 부문장", "비밀번호": "000", "권한": "실무자", "상태": "정상"},
    {"사번": "20120102", "성명": "김상원", "직급": "부장", "소속_직책": "식품안전팀 팀장", "비밀번호": "000", "권한": "실무자", "상태": "정상"},
    {"사번": "20081101", "성명": "조준희", "직급": "과장", "소속_직책": "식품안전팀 팀원", "비밀번호": "000", "권한": "실무자", "상태": "정상"},
    {"사번": "20150902", "성명": "박병규", "직급": "대리", "소속_직책": "식품안전팀 팀원", "비밀번호": "000", "권한": "실무자", "상태": "정상"},
    {"사번": "20190901", "성명": "김재호", "직급": "주임", "소속_직책": "식품안전팀 팀원", "비밀번호": "000", "권한": "실무자", "상태": "정상"},
    {"사번": "20220403", "성명": "곽정혁", "직급": "주임", "소속_직책": "식품안전팀 팀원", "비밀번호": "000", "권한": "관리자", "상태": "정상"},
    {"사번": "20251101", "성명": "김한조", "직급": "차장", "소속_직책": "품질관리팀 팀장", "비밀번호": "000", "권한": "실무자", "상태": "정상"},
    {"사번": "19930401", "성명": "성미희", "직급": "차장", "소속_직책": "품질관리팀 팀원", "비밀번호": "000", "권한": "실무자", "상태": "정상"},
    {"사번": "20060102", "성명": "한의진", "직급": "과장", "소속_직책": "품질관리팀 팀원", "비밀번호": "000", "권한": "실무자", "상태": "정상"},
    {"사번": "19970402", "성명": "서민우", "직급": "대리", "소속_직책": "품질관리팀 팀원", "비밀번호": "000", "권한": "실무자", "상태": "정상"},
    {"사번": "20060601", "성명": "곽희산", "직급": "대리", "소속_직책": "품질관리팀 팀원", "비밀번호": "000", "권한": "실무자", "상태": "정상"},
    {"사번": "20120701", "성명": "이승재", "직급": "대리", "소속_직책": "품질관리팀 팀원", "비밀번호": "000", "권한": "실무자", "상태": "정상"},
    {"사번": "20251104", "성명": "윤주한", "직급": "사원", "소속_직책": "품질관리팀 팀원", "비밀번호": "000", "권한": "실무자", "상태": "정상"},
    {"사번": "20220101", "성명": "권석민", "직급": "사원", "소속_직책": "품질관리팀 팀원", "비밀번호": "000", "권한": "실무자", "상태": "정상"},
    {"사번": "20221202", "성명": "장영진", "직급": "사원", "소속_직책": "품질관리팀 팀원", "비밀번호": "000", "권한": "실무자", "상태": "정상"},
    {"사번": "20240202", "성명": "임경현", "직급": "사원", "소속_직책": "품질관리팀 팀원", "비밀번호": "000", "권한": "실무자", "상태": "정상"},
    {"사번": "20241102", "성명": "조유건", "직급": "사원", "소속_직책": "품질관리팀 팀원", "비밀번호": "000", "권한": "실무자", "상태": "정상"},
    {"사번": "20250908", "성명": "전찬우", "직급": "사원", "소속_직책": "품질관리팀", "비밀번호": "000", "권한": "실무자", "상태": "정상"},
    {"사번": "20260401", "성명": "이지민", "직급": "사원", "소속_직책": "품질관리팀 팀원", "비밀번호": "000", "권한": "실무자", "상태": "정상"},
    {"사번": "20260609", "성명": "김유진", "직급": "사원", "소속_직책": "품질관리팀 팀원", "비밀번호": "000", "권한": "실무자", "상태": "정상"},
    {"사번": "20260610", "성명": "유지운", "직급": "사원", "소속_직책": "품질관리팀 팀원", "비밀번호": "000", "권한": "실무자", "상태": "정상"}
]

_AUTH_CACHE_LOCK = threading.Lock()
_AUTH_CACHE: Dict[str, Any] = {"ts": 0.0, "sheet_ok": False, "users": []}
AUTH_MICRO_TTL = 15.0


def _get_auth_secret() -> bytes:
    env_k = os.environ.get("AUTH_SECRET_KEY", "").strip()
    if env_k:
        return env_k.encode("utf-8")
    seed = os.environ.get("GOOGLE_CREDENTIALS_JSON", "qsafety-yonsei-2026")
    return hashlib.sha256((seed + "|qsafety-login-v2").encode("utf-8")).hexdigest().encode("utf-8")


def _normalize_pw_str(raw_pw: Any) -> str:
    """구글 시트에서 '000'이 숫자 '0'으로 자동 변환된 경우에도 '000'으로 정상 복원"""
    s = str(raw_pw if raw_pw is not None else "").strip()
    if s.startswith("'"):
        s = s[1:]
    if s in ("", "0"):
        return "000"
    return s


def _format_cell_keep_leading_zero(val: str) -> str:
    """구글 시트 USER_ENTERED 저장 시 0으로 시작하는 비밀번호/사번(예: 000, 0123) 앞자리 0 보존"""
    s = str(val or "").strip()
    if s and s.isdigit() and s.startswith("0"):
        return "'" + s
    return s


def _get_or_create_cost_login_ws():
    sheet_urls_map = get_sheet_urls_map()
    sh, err = open_sheet_isolated(sheet_urls_map, "cost")
    if sh is None:
        return None, err
    ws_all = api_call_with_retry(sh.worksheets)
    ws_login = next((w for w in ws_all if w.title.strip() == LOGIN_TAB_NAME), None)
    if ws_login is None:
        ws_login = api_call_with_retry(sh.add_worksheet, title=LOGIN_TAB_NAME, rows=100, cols=10)
    return ws_login, None


def _build_login_sheet_payload(users_list: List[dict]) -> list:
    payload = [AUTH_USER_COLS]
    for u in users_list:
        pw_clean = _normalize_pw_str(u.get("비밀번호", "000"))
        payload.append([
            _format_cell_keep_leading_zero(u.get("사번", "")),
            str(u.get("성명", "")).strip(),
            str(u.get("직급", "")).strip(),
            str(u.get("소속_직책", "")).strip(),
            _format_cell_keep_leading_zero(pw_clean),
            str(u.get("권한", "실무자")).strip(),
            str(u.get("상태", "정상")).strip(),
            str(u.get("최근접속일시", "")).strip(),
            str(u.get("비번변경일시", "")).strip()
        ])
    return payload


def create_signed_token(emp_no: str, days: int = 14) -> str:
    exp_ts = int(time.time()) + (days * 86400)
    msg = f"{emp_no}.{exp_ts}"
    sig = hmac.new(_get_auth_secret(), msg.encode("utf-8"), hashlib.sha256).hexdigest()[:32]
    return f"{msg}.{sig}"


def verify_signed_token_str(token_str: str) -> Optional[str]:
    s = str(token_str or "").strip()
    parts = s.split(".")
    if len(parts) != 3:
        return None
    emp_no, exp_s, sig = parts
    if not emp_no or not exp_s.isdigit():
        return None
    if int(exp_s) < int(time.time()):
        return None
    expected = hmac.new(_get_auth_secret(), f"{emp_no}.{exp_s}".encode("utf-8"), hashlib.sha256).hexdigest()[:32]
    if not hmac.compare_digest(sig, expected):
        return None
    return emp_no


def load_auth_users_live(force: bool = False) -> Tuple[List[dict], bool]:
    now_t = time.time()
    if not force and _AUTH_CACHE["users"] and (now_t - _AUTH_CACHE["ts"]) < AUTH_MICRO_TTL:
        return _AUTH_CACHE["users"], _AUTH_CACHE["sheet_ok"]

    with _AUTH_CACHE_LOCK:
        if not force and _AUTH_CACHE["users"] and (time.time() - _AUTH_CACHE["ts"]) < AUTH_MICRO_TTL:
            return _AUTH_CACHE["users"], _AUTH_CACHE["sheet_ok"]

        ws_login, _ = _get_or_create_cost_login_ws()
        if ws_login is not None:
            try:
                rows = get_values_formatted(ws_login)
                if len(rows) < 2:
                    seed_users = [{**u, "최근접속일시": "", "비번변경일시": ""} for u in INITIAL_AUTH_USERS]
                    safe_ws_update(ws_login, _build_login_sheet_payload(seed_users))
                    _AUTH_CACHE["users"] = seed_users
                    _AUTH_CACHE["sheet_ok"] = True
                    _AUTH_CACHE["ts"] = time.time()
                    return seed_users, True

                recs = rows_to_records(rows)
                users = []
                for r in recs:
                    emp = re.sub(r"\s+", "", str(r.get("사번", ""))).lstrip("'")
                    nm = str(r.get("성명", "")).strip()
                    if not emp or not nm:
                        continue
                    users.append({
                        "사번": emp,
                        "성명": nm,
                        "직급": str(r.get("직급", "")).strip(),
                        "소속_직책": str(r.get("소속_직책", "")).strip(),
                        "비밀번호": _normalize_pw_str(r.get("비밀번호", "000")),
                        "권한": "관리자" if (str(r.get("권한", "")).strip() == "관리자" or emp == "20220403") else "실무자",
                        "상태": str(r.get("상태", "정상")).strip() or "정상",
                        "최근접속일시": str(r.get("최근접속일시", "")).strip(),
                        "비번변경일시": str(r.get("비번변경일시", "")).strip()
                    })
                if users:
                    _AUTH_CACHE["users"] = users
                    _AUTH_CACHE["sheet_ok"] = True
                    _AUTH_CACHE["ts"] = time.time()
                    return users, True
            except Exception:
                pass

        if not _AUTH_CACHE["users"]:
            _AUTH_CACHE["users"] = [{**u, "최근접속일시": "", "비번변경일시": ""} for u in INITIAL_AUTH_USERS]
            _AUTH_CACHE["sheet_ok"] = False
            _AUTH_CACHE["ts"] = time.time()
        return _AUTH_CACHE["users"], _AUTH_CACHE["sheet_ok"]


def save_auth_users_to_sheet(users_list: List[dict]) -> bool:
    _AUTH_CACHE["users"] = users_list
    _AUTH_CACHE["ts"] = time.time()
    ws_login, _ = _get_or_create_cost_login_ws()
    if ws_login is None:
        _AUTH_CACHE["sheet_ok"] = False
        return False
    try:
        api_call_with_retry(ws_login.clear)
        safe_ws_update(ws_login, _build_login_sheet_payload(users_list))
        _AUTH_CACHE["sheet_ok"] = True
        return True
    except Exception:
        return False


def _public_user_info(u: dict) -> dict:
    return {
        "사번": u.get("사번", ""),
        "성명": u.get("성명", ""),
        "직급": u.get("직급", ""),
        "소속_직책": u.get("소속_직책", ""),
        "권한": u.get("권한", "실무자"),
        "상태": u.get("상태", "정상"),
        "초기비번여부": (_normalize_pw_str(u.get("비밀번호", "000")) == "000"),
        "최근접속일시": u.get("최근접속일시", ""),
        "비번변경일시": u.get("비번변경일시", "")
    }


def require_auth_user(x_qsafety_token: Optional[str] = Header(default=None)) -> dict:
    emp_no = verify_signed_token_str(x_qsafety_token or "")
    if not emp_no:
        raise HTTPException(status_code=401, detail="로그인이 필요하거나 세션이 만료되었습니다.")
    users, _ = load_auth_users_live(force=False)
    matched = next((u for u in users if u["사번"] == emp_no), None)
    if not matched or matched.get("상태") != "정상":
        raise HTTPException(status_code=401, detail="접속 권한이 없거나 승인 대기·차단된 사번입니다.")
    return matched


class AuthLoginPayload(BaseModel):
    emp_no: str
    password: str
    remember: bool = True


class AuthChangePwPayload(BaseModel):
    current_pw: str
    new_pw: str


class AuthRequestPayload(BaseModel):
    name: str
    emp_no: str
    rank: Optional[str] = ""
    dept: Optional[str] = ""


class AuthManagePayload(BaseModel):
    action: str  # approve | block | activate | reset_pw | delete | add
    emp_no: str
    name: Optional[str] = ""
    rank: Optional[str] = ""
    dept: Optional[str] = ""
    role: Optional[str] = "실무자"


@app.post("/api/auth/login")
def api_auth_login(body: AuthLoginPayload):
    in_emp = re.sub(r"\s+", "", str(body.emp_no or ""))
    in_pw = str(body.password if body.password is not None else "").strip()
    if not in_emp or not in_pw:
        raise HTTPException(status_code=400, detail="사번과 비밀번호를 모두 입력해 주세요.")

    users, _ = load_auth_users_live(force=True)
    idx_u, user = next(((i, u) for i, u in enumerate(users) if u["사번"] == in_emp), (-1, None))
    if not user:
        raise HTTPException(status_code=401, detail="등록되지 않은 사번입니다. 우측 [신규 계정 신청] 탭을 이용해 주세요.")

    saved_pw = _normalize_pw_str(user.get("비밀번호", "000"))
    pw_ok = (in_pw == saved_pw) or (saved_pw == "000" and in_pw == "000")
    if not pw_ok:
        raise HTTPException(status_code=401, detail="비밀번호가 일치하지 않습니다. (초기 비밀번호: 000)")

    if user.get("상태") == "승인대기":
        raise HTTPException(status_code=403, detail="현재 관리자(곽정혁 주임) 승인 대기 중인 사번입니다.")
    if user.get("상태") != "정상":
        raise HTTPException(status_code=403, detail="접속이 제한된 계정입니다. 관리자에게 문의해 주세요.")

    now_hm = now_kst().strftime("%Y-%m-%d %H:%M")
    user["최근접속일시"] = now_hm
    _AUTH_CACHE["ts"] = time.time()

    # 전체 시트 초기화(clear) 대신 해당 행의 H열(최근접속일시) 1칸만 가볍게 업데이트하여 로그인 속도 극대화
    def _bg_stamp_login(row_no: int, ts_val: str):
        try:
            ws_l, _ = _get_or_create_cost_login_ws()
            if ws_l is not None:
                ws_l.update_acell(f"H{row_no}", ts_val)
        except Exception:
            pass

    if idx_u >= 0:
        threading.Thread(target=_bg_stamp_login, args=(idx_u + 2, now_hm), daemon=True).start()

    tok = create_signed_token(in_emp, days=(14 if body.remember else 1))
    pending_cnt = sum(1 for x in users if x.get("상태") == "승인대기") if user["권한"] == "관리자" else 0
    return {"ok": True, "token": tok, "user": _public_user_info(user), "pending_cnt": pending_cnt}


@app.post("/api/auth/change-password")
def api_auth_change_password(body: AuthChangePwPayload, cur_user: dict = Depends(require_auth_user)):
    cur_in = str(body.current_pw or "").strip()
    new_in = str(body.new_pw or "").strip()
    if not cur_in or not new_in:
        raise HTTPException(status_code=400, detail="현재 비밀번호와 새 비밀번호를 모두 입력해 주세요.")
    if len(new_in) < 3:
        raise HTTPException(status_code=400, detail="새 비밀번호는 3자리 이상으로 입력해 주세요.")

    users, _ = load_auth_users_live(force=True)
    target = next((u for u in users if u["사번"] == cur_user["사번"]), None)
    if not target:
        raise HTTPException(status_code=404, detail="계정 정보를 찾을 수 없습니다.")

    saved_pw = _normalize_pw_str(target.get("비밀번호", "000"))
    if cur_in != saved_pw:
        raise HTTPException(status_code=400, detail="현재 비밀번호가 일치하지 않습니다.")

    target["비밀번호"] = new_in
    target["비번변경일시"] = now_kst().strftime("%Y-%m-%d %H:%M")
    sheet_saved = save_auth_users_to_sheet(users)
    if not sheet_saved:
        raise HTTPException(status_code=500, detail="시트 저장 중 오류가 발생했습니다. 잠시 후 다시 시도해 주세요.")

    return {
        "ok": True,
        "message": "'원가절감 현황' 시트의 [로그인] 탭에 새 비밀번호가 저장되었습니다.",
        "user": _public_user_info(target)
    }


@app.post("/api/auth/request-access")
def api_auth_request_access(body: AuthRequestPayload):
    in_emp = re.sub(r"\s+", "", str(body.emp_no or ""))
    in_nm = str(body.name or "").strip()
    if not in_emp or not in_nm:
        raise HTTPException(status_code=400, detail="성명과 사번은 필수 입력 항목입니다.")

    users, _ = load_auth_users_live(force=True)
    exist = next((u for u in users if u["사번"] == in_emp), None)
    if exist:
        if exist.get("상태") == "정상":
            raise HTTPException(status_code=400, detail="이미 등록된 사번입니다. 초기 비밀번호(000)로 로그인해 주세요.")
        raise HTTPException(status_code=400, detail=f"이미 등록된 사번입니다 (현재 상태: {exist.get('상태')}).")

    users.append({
        "사번": in_emp,
        "성명": in_nm,
        "직급": str(body.rank or "").strip() or "-",
        "소속_직책": str(body.dept or "").strip() or "-",
        "비밀번호": "000",
        "권한": "실무자",
        "상태": "승인대기",
        "최근접속일시": "",
        "비번변경일시": ""
    })
    save_auth_users_to_sheet(users)
    return {"ok": True, "message": "계정 신청이 완료되었습니다. 관리자 승인 후 초기 비밀번호(000)로 접속 가능합니다."}


@app.get("/api/auth/me")
def api_auth_me(cur_user: dict = Depends(require_auth_user)):
    users, sheet_ok = load_auth_users_live(force=False)
    pending_cnt = sum(1 for x in users if x.get("상태") == "승인대기") if cur_user["권한"] == "관리자" else 0
    return {"ok": True, "user": _public_user_info(cur_user), "pending_cnt": pending_cnt, "sheet_ok": sheet_ok}


@app.get("/api/auth/users")
def api_auth_list_users(cur_user: dict = Depends(require_auth_user)):
    if cur_user.get("권한") != "관리자":
        raise HTTPException(status_code=403, detail="관리자만 접근할 수 있습니다.")
    users, sheet_ok = load_auth_users_live(force=True)
    return {"ok": True, "users": [_public_user_info(u) for u in users], "sheet_ok": sheet_ok}


@app.post("/api/auth/manage")
def api_auth_manage_user(body: AuthManagePayload, cur_user: dict = Depends(require_auth_user)):
    if cur_user.get("권한") != "관리자":
        raise HTTPException(status_code=403, detail="관리자만 계정을 변경할 수 있습니다.")

    target_emp = re.sub(r"\s+", "", str(body.emp_no or ""))
    if not target_emp:
        raise HTTPException(status_code=400, detail="사번을 확인해 주세요.")
    if target_emp == cur_user["사번"] and body.action in ("block", "delete"):
        raise HTTPException(status_code=400, detail="본인 관리자 계정은 차단하거나 삭제할 수 없습니다.")

    users, _ = load_auth_users_live(force=True)
    msg = "계정 변경 사항이 [로그인] 탭에 즉시 반영되었습니다."

    if body.action == "add":
        if any(u["사번"] == target_emp for u in users):
            raise HTTPException(status_code=400, detail="이미 등록된 사번입니다.")
        if not (body.name or "").strip():
            raise HTTPException(status_code=400, detail="성명을 입력해 주세요.")
        users.append({
            "사번": target_emp,
            "성명": body.name.strip(),
            "직급": (body.rank or "").strip() or "-",
            "소속_직책": (body.dept or "").strip() or "-",
            "비밀번호": "000",
            "권한": "관리자" if body.role == "관리자" else "실무자",
            "상태": "정상",
            "최근접속일시": "",
            "비번변경일시": ""
        })
        msg = f"{body.name.strip()}({target_emp}) 계정이 초기 비밀번호(000)로 등록되었습니다."
    elif body.action in ("approve", "activate"):
        for u in users:
            if u["사번"] == target_emp:
                u["상태"] = "정상"
    elif body.action == "block":
        for u in users:
            if u["사번"] == target_emp:
                u["상태"] = "차단"
    elif body.action == "reset_pw":
        for u in users:
            if u["사번"] == target_emp:
                u["비밀번호"] = "000"
                u["비번변경일시"] = now_kst().strftime("%Y-%m-%d %H:%M") + " (초기화)"
        msg = f"사번 {target_emp}의 비밀번호가 '000'으로 초기화되었습니다."
    elif body.action == "delete":
        users = [u for u in users if u["사번"] != target_emp]

    sheet_saved = save_auth_users_to_sheet(users)
    return {
        "ok": True,
        "sheet_ok": sheet_saved,
        "users": [_public_user_info(u) for u in users],
        "pending_cnt": sum(1 for x in users if x.get("상태") == "승인대기"),
        "message": msg
    }

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


from fastapi.responses import JSONResponse


def _sanitize_json_payload(obj: Any) -> Any:
    if obj is None or isinstance(obj, (str, int, bool)):
        return obj
    if isinstance(obj, float):
        if obj != obj or obj in (float("inf"), float("-inf")):
            return 0.0
        return obj
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    if isinstance(obj, dict):
        out = {}
        for k, v in obj.items():
            str_k = "|".join(str(x) for x in k) if isinstance(k, tuple) else str(k)
            out[str_k] = _sanitize_json_payload(v)
        return out
    if isinstance(obj, (list, tuple, set)):
        return [_sanitize_json_payload(x) for x in obj]
    return str(obj)


@app.get("/api/bundle")
def api_get_bundle(force: int = 0, _u: dict = Depends(require_auth_user)):
    try:
        raw_bundle = build_full_dashboard_bundle(force_refresh=(force == 1))
        return JSONResponse(content=_sanitize_json_payload(raw_bundle))
    except Exception as e:
        import traceback
        tb_tail = " / ".join(traceback.format_exc().splitlines()[-3:])
        return JSONResponse(
            status_code=200,
            content={
                "ok": False,
                "loaded_at": now_kst().strftime("%Y. %m. %d %H:%M:%S (KST)"),
                "loaded_date": now_kst().strftime("%Y-%m-%d"),
                "errors": [f"백엔드 처리 오류({type(e).__name__}): {e} ({tb_tail})"]
            }
        )


@app.get("/api/lot-trace")
def api_lot_trace(product: str = "", exp: str = "", kw: str = "", scope: str = "ALL", _u: dict = Depends(require_auth_user)):
    """
    - scope='DAIRY' : 냉장 전용 역추적 (우유[카톤·병 분리]·발효유 포장일보 + 미생물 검사일보[주1~12 매칭])
    - scope='RT'    : 상온 전용 역추적 (상온 공정·멸균 CCP + 두유 배양검사 일보)
    - 품목만 선택 조회 시: 날짜 교집합에 막히지 않고 해당 품목의 누적 미생물 이력 전체 반환 + 최근 배양중 생산분 판정예정 안내
    """
    bundle = build_full_dashboard_bundle(force_refresh=False)
    if not bundle.get("ok"):
        raise HTTPException(status_code=500, detail="시트 데이터가 아직 로드되지 않았습니다.")

    scope_u = str(scope or "ALL").strip().upper()
    run_dairy = scope_u in ("ALL", "DAIRY")
    run_rt = scope_u in ("ALL", "RT")

    p_data = bundle["process"]
    rt_hp = p_data["rt_hist_p"]
    rt_hc = p_data["rt_hist_c"]
    soy_h = p_data["soy_hist"]
    dy_hp = p_data["dairy_hist_p"]
    dy_hc = p_data["dairy_hist_c"]
    micro_h = p_data.get("micro", {}).get("hist_rows", [])

    exp_q = parse_exp_query_info(exp)
    is_exp_empty = exp_q.get("empty", True)
    today_d = now_kst().date()

    matched_rt, matched_rt_ccp, matched_rt_ccp3b, matched_soy = [], [], [], []
    if run_rt:
        rt_date_views = (p_data.get("rt_today") or {}).get("date_views") or {}

        matched_rt_raw = [
            r for r in rt_hp
            if is_product_match(r.get("제품명", ""), product, kw)
            and is_exp_match(r.get("표기소비기한_원문", ""), r.get("표준소비기한_검색용", ""), exp_q)
        ]
        matched_rt_raw.sort(key=lambda x: x.get("생산일자", ""), reverse=True)

        # ① 생산·포장 매칭 행에 원본 탭의 확인시간 · 멸균요약 · 색상 클래스 보강
        for r in matched_rt_raw:
            rc = dict(r)
            dt_s = str(rc.get("생산일자", "")).strip()
            ho_s = str(rc.get("포장호기", "")).strip()
            dv = rt_date_views.get(dt_s)
            if dv:
                for cp in dv.get("compact_rows", []):
                    if cp.get("포장호기") == ho_s and is_product_match(cp.get("생산 제품명", ""), rc.get("제품명", ""), ""):
                        if str(rc.get("확인시간", "")).strip() in ("", "-", "미기입(대기)") and cp.get("확인시간"):
                            rc["확인시간"] = cp["확인시간"]
                        rc["멸균요약"] = cp.get("멸균기·CCP 요약", "-")
                        rc["특이사항"] = cp.get("특이사항", "이상없음")
                        rc["_cls_time"] = cp.get("_cls_status", "cell-green")
                        rc["_cls_rem"] = cp.get("_cls_rem", "cell-green")
                        break
            if "_cls_time" not in rc:
                chk_v = str(rc.get("확인시간", "")).strip()
                rc["_cls_time"] = "cell-green" if re.search(r"\d{1,2}:\d{2}", chk_v) else "cell-yellow"
                rc["멸균요약"] = rc.get("멸균요약", "-")
                rc["특이사항"] = rc.get("특이사항", "이상없음")
                rc["_cls_rem"] = "cell-green"
            matched_rt.append(rc)

        rt_keys = [(str(r.get("생산일자", "")).strip(), str(r.get("포장호기", "")).strip(), str(r.get("제품명", "")).strip()) for r in matched_rt]
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

        # ② 당일 상온 UHT 멸균기 CCP & CCP-3B 무균포장기 기록 (해당 포장호기 정밀 매칭 + 적합 색상 포함)
        date_hos_map: Dict[str, List[str]] = {}
        for r in matched_rt:
            d_k = str(r.get("생산일자", "")).strip()
            h_k = str(r.get("포장호기", "")).strip()
            if d_k:
                date_hos_map.setdefault(d_k, [])
                if h_k and h_k not in date_hos_map[d_k]:
                    date_hos_map[d_k].append(h_k)

        for dt_s in sorted(date_hos_map.keys(), reverse=True):
            target_hos = date_hos_map[dt_s]
            dv = rt_date_views.get(dt_s)
            ster_added_for_dt = False

            if dv and dv.get("ster_records"):
                all_ster = dv["ster_records"]
                filtered_ster = [
                    sr for sr in all_ster
                    if not target_hos or any(f"({h})" in str(sr.get("멸균기_포장", "")) or h in str(sr.get("멸균기_포장", "")) for h in target_hos)
                ]
                use_ster = filtered_ster if filtered_ster else all_ster
                for sr in use_ster:
                    sr_copy = dict(sr)
                    sr_copy["생산일자"] = dt_s
                    matched_rt_ccp.append(sr_copy)
                    ster_added_for_dt = True

                for c3 in dv.get("ccp3b_records", []):
                    c3_copy = dict(c3)
                    c3_copy["생산일자"] = dt_s
                    c3_copy["_target_hos"] = target_hos
                    matched_rt_ccp3b.append(c3_copy)

            # 시트 탭이 삭제되어 _이력DB_상온CCP에만 남은 과거 데이터 폴백 파싱 (멸균기 + CCP-3B 포장기 동시 복원)
            if not ster_added_for_dt:
                day_c_rows = [c for c in rt_hc if str(c.get("생산일자", "")).strip() == dt_s]
                ccp3b_fb_Added = False
                if dv and dv.get("ccp3b_records") and not any(x.get("생산일자") == dt_s for x in matched_rt_ccp3b):
                    for c3 in dv["ccp3b_records"]:
                        c3_copy = dict(c3)
                        c3_copy["생산일자"] = dt_s
                        c3_copy["_target_hos"] = target_hos
                        matched_rt_ccp3b.append(c3_copy)
                        ccp3b_fb_Added = True

                for c in day_c_rows:
                    rcat_s = str(c.get("기록분류", "")).strip()
                    # CCP-3B 무균포장기 행인 경우 포장기 전용 배열로 복원
                    if ("3B" in rcat_s or "포장" in rcat_s) and not ccp3b_fb_Added:
                        sub_it = str(c.get("세부항목") or c.get("항목") or "과산화수소·살균온도").strip()
                        eq_3b = str(c.get("설비_호기") or c.get("적용설비") or "-").strip()
                        raw_3b_v = str(c.get("기록내용_수치") or "").strip()
                        rec_fb = {
                            "생산일자": dt_s,
                            "항목": sub_it,
                            "관리기준": str(c.get("관리기준") or "-").strip(),
                            "적용설비": eq_3b,
                            "A호기": "-", "B호기": "-", "C호기": "-", "D호기": "-", "E호기": "-", "F호기": "-",
                            "_cls_map": {},
                            "_target_hos": target_hos
                        }
                        any_3b = False
                        for h_nm in ["A호기", "B호기", "C호기", "D호기", "E호기", "F호기"]:
                            hv = str(c.get(h_nm) or "").strip()
                            if hv and hv != "-":
                                rec_fb[h_nm] = hv
                                rec_fb["_cls_map"][h_nm] = eval_ccp3b_cell(hv, rec_fb["관리기준"])
                                any_3b = True
                        for m_h in re.finditer(r"([A-F]호기)\s*[:=]\s*([^/,;]+)", raw_3b_v):
                            hk, hval = m_h.group(1), m_h.group(2).strip()
                            rec_fb[hk] = hval
                            rec_fb["_cls_map"][hk] = eval_ccp3b_cell(hval, rec_fb["관리기준"])
                            any_3b = True
                        if not any_3b and raw_3b_v and raw_3b_v != "-" and target_hos:
                            rec_fb[target_hos[0]] = raw_3b_v
                            rec_fb["_cls_map"][target_hos[0]] = "cell-green"
                            any_3b = True
                        if any_3b:
                            matched_rt_ccp3b.append(rec_fb)
                        continue

                    eq_v = (
                        c.get("설비_호기") or c.get("멸균기_포장") or c.get("멸균기")
                        or c.get("포장호기") or c.get("호기") or "-"
                    )
                    if target_hos and eq_v != "-" and not any(h in str(eq_v) for h in target_hos):
                        continue

                    prod_v = c.get("매칭 제품명") or c.get("세부항목") or c.get("제품명") or c.get("항목") or "-"
                    time_v = c.get("확인시간") or c.get("모니터링시간") or c.get("시간") or "-"
                    temp_v = c.get("멸균온도") or c.get("온도") or "-"
                    press_v = c.get("균질압") or c.get("압력") or "-"
                    hold_v = c.get("멸균시간") or c.get("유지시간") or "-"
                    flow_v = c.get("유량_냉각") or (
                        f"{c.get('유량', '-')} / {c.get('냉각온도', c.get('냉각', '-'))}"
                        if (c.get("유량") or c.get("냉각온도") or c.get("냉각")) else "-"
                    )
                    flt_v = c.get("여과망_서명") or c.get("여과망") or c.get("판정") or "-"
                    rem_v = str(c.get("특이사항", "")).strip() or "없음"

                    raw_val_col = str(c.get("기록내용_수치") or c.get("실측치") or c.get("측정값") or "").strip()
                    if raw_val_col and temp_v == "-":
                        m_tm = re.search(r"(\d{1,2}:\d{2})", raw_val_col)
                        m_tp = re.search(r"(\d{2,3}(?:\.\d+)?\s*℃[^/]*)", raw_val_col)
                        m_pr = re.search(r"(\d{2,3}(?:\.\d+)?\s*bar)", raw_val_col, re.IGNORECASE)
                        m_hd = re.search(r"(\d+(?:\.\d+)?\s*초)", raw_val_col)
                        if m_tm and time_v == "-":
                            time_v = m_tm.group(1)
                        temp_v = m_tp.group(1).strip() if m_tp else raw_val_col
                        if m_pr and press_v == "-":
                            press_v = m_pr.group(1)
                        if m_hd and hold_v == "-":
                            hold_v = m_hd.group(1)

                    if all(str(x).strip() in ("", "-") for x in [eq_v, prod_v, time_v, temp_v, press_v, hold_v]) and rem_v in ("없음", "-"):
                        continue

                    matched_rt_ccp.append({
                        "생산일자": dt_s,
                        "멸균기_포장": eq_v,
                        "매칭 제품명": prod_v,
                        "확인시간": time_v,
                        "멸균온도": temp_v,
                        "균질압": press_v,
                        "멸균시간": hold_v,
                        "유량_냉각": flow_v,
                        "여과망_서명": flt_v if flt_v != "-" else "적합",
                        "특이사항": rem_v,
                        "_c_time": "cell-green" if re.search(r"\d{1,2}:\d{2}", str(time_v)) else ("cell-yellow" if time_v != "-" else ""),
                        "_c_temp": "cell-green" if temp_v not in ("", "-") else "",
                        "_c_press": "cell-green" if press_v not in ("", "-") else "",
                        "_c_hold": "cell-green" if hold_v not in ("", "-") else "",
                        "_c_flt": "cell-red" if "부적합" in str(flt_v) else "cell-green",
                        "_c_rem": "cell-yellow" if rem_v not in ("없음", "이상없음", "-") else ""
                    })

    matched_dy, matched_micro = [], []
    if run_dairy:
        dy_prod_dates_info: Dict[str, Tuple[str, str]] = {}
        dy_prod_names_matched = []

        for dr in dy_hp:
            if is_product_match(dr.get("제품명", ""), product, kw, record_line_hint=dr.get("라인구분", "")) and is_exp_match(dr.get("표기소비기한_원문", ""), dr.get("표준소비기한_검색용", ""), exp_q):
                d_dt = str(dr.get("생산일자", "")).strip()
                d_ln = str(dr.get("라인구분", "")).strip()
                base_ln = "발효유" if "발효" in d_ln else "우유"
                p_nm = str(dr.get("제품명", "")).strip()

                if d_dt and d_dt not in dy_prod_dates_info:
                    dy_prod_dates_info[d_dt] = (p_nm, base_ln)
                if p_nm and p_nm not in dy_prod_names_matched:
                    dy_prod_names_matched.append(p_nm)

                time_lines, ccp_lines, note_lines = [], [], []
                for cr in dy_hc:
                    cr_ln = str(cr.get("라인구분", "")).strip()
                    if str(cr.get("생산일자", "")).strip() == d_dt and (base_ln in cr_ln or cr_ln in d_ln):
                        rcat = str(cr.get("기록분류", ""))
                        if "생산시간" in rcat:
                            s_item = cr.get("세부항목", "")
                            s_val = cr.get("기록내용_수치", "-")
                            clean_t_line = extract_clean_line_time_for_trace(s_item, s_val, d_ln)
                            if clean_t_line and clean_t_line not in time_lines:
                                time_lines.append(clean_t_line)
                        elif "CCP" in rcat:
                            s_item = cr.get("세부항목", "CCP")
                            s_val = cr.get("기록내용_수치", "-")
                            ccp_lines.append(f"{s_item}: {s_val}")
                        elif "특이사항" in rcat:
                            rv = str(cr.get("특이사항", "이상없음")).strip()
                            if rv and rv not in ["이상없음", "-"]:
                                note_lines.append(rv)

                dr_copy = dict(dr)
                dr_copy["포장기_시작~종료시간"] = " / ".join(time_lines) if time_lines else "-"
                dr_copy["살균기온도_균질압_CCP"] = " / ".join(ccp_lines) if ccp_lines else "-"
                dr_copy["현장특이사항"] = " / ".join(note_lines) if note_lines else "이상없음"
                dr_copy["접착_실링_관능"] = f"{dr.get('접착_외관_요약', '-')} / 관능:{dr.get('관능검사', dr.get('관능', '-'))}"
                matched_dy.append(dr_copy)
        matched_dy.sort(key=lambda x: x.get("생산일자", ""), reverse=True)

        has_prod_filter = bool((product and product != "ALL") or (kw and kw.strip()))
        judged_insp_dates = set()

        for mr in micro_h:
            rep_nm = mr.get("대표품목명", "")
            ho_vol_s = str(mr.get("호기_용량", "")).strip()
            ho_suffix = f" {ho_vol_s}" if ho_vol_s and ho_vol_s != "-" else ""
            full_alias_pool, ju_badge = expand_micro_alias_pool(f"{rep_nm}{ho_suffix}", mr.get("연관품목_주석포함", ""))
            m_insp_dt = str(mr.get("검사일자_생산일", "")).strip()

            if not is_exp_empty:
                if not dy_prod_dates_info or m_insp_dt not in dy_prod_dates_info:
                    continue

            m_cat_hint = mr.get("대분류", "")
            matched_target_label = ""
            for dp_nm in dy_prod_names_matched:
                if is_product_match(full_alias_pool, dp_nm, "", allow_no_vol_record=True, record_line_hint=m_cat_hint):
                    matched_target_label = dp_nm
                    break
            if not matched_target_label and has_prod_filter and is_product_match(full_alias_pool, product, kw, allow_no_vol_record=True, record_line_hint=m_cat_hint):
                matched_target_label = product if (product and product != "ALL") else kw
            if not matched_target_label and not has_prod_filter and m_insp_dt in dy_prod_dates_info:
                matched_target_label = rep_nm

            if not matched_target_label:
                continue

            judged_insp_dates.add(m_insp_dt)
            matched_micro.append({
                "생산·검사일": m_insp_dt,
                "판정일자": mr.get("판정일자", "-"),
                "대분류": mr.get("대분류", "-"),
                "매칭 포장품목": f"{matched_target_label} {ju_badge}".strip(),
                "미생물일보 표기명": rep_nm,
                "Storage Tank": mr.get("Storage_Tank", "-"),
                "호기/용량": ho_vol_s or "-",
                "세부라인": mr.get("세부라인", "-"),
                "대장균군": mr.get("대장균군", "-"),
                "세균수": mr.get("세균수", "-"),
                "효모·곰팡이": mr.get("효모_곰팡이", "-"),
                "유산균": mr.get("유산균", "-"),
                "판정": mr.get("판정", "적합")
            })

        for p_dt_str, (p_nm_val, base_ln_val) in sorted(dy_prod_dates_info.items(), reverse=True)[:4]:
            if p_dt_str in judged_insp_dates:
                continue
            p_d_obj = parse_date_obj(p_dt_str)
            if not p_d_obj:
                continue
            inc_days = 5 if base_ln_val == "발효유" else 2
            exp_judge_d = p_d_obj + timedelta(days=inc_days)
            days_ago = (today_d - p_d_obj).days
            if days_ago <= 6:
                _, ju_b = expand_micro_alias_pool(p_nm_val, "")
                st_label = "배양중 (판정예정)" if exp_judge_d >= today_d else "판정입력대기"
                exp_j_iso = exp_judge_d.strftime("%Y-%m-%d")
                matched_micro.append({
                    "생산·검사일": p_dt_str,
                    "판정일자": f"예정 {exp_j_iso}",
                    "대분류": "3. 발효유·호상" if base_ln_val == "발효유" else "1. 우유(카톤·병)",
                    "매칭 포장품목": f"{p_nm_val} {ju_b}".strip(),
                    "미생물일보 표기명": "배양 진행중 (미생물 일보 작성 시 자동연동)",
                    "Storage Tank": "-",
                    "호기/용량": "-",
                    "세부라인": "-",
                    "대장균군": "-",
                    "세균수": "-",
                    "효모·곰팡이": "-",
                    "유산균": "-",
                    "판정": st_label
                })

        matched_micro.sort(key=lambda x: x.get("생산·검사일", ""), reverse=True)

    dairy_sum = len(matched_dy) + len(matched_micro)
    rt_sum = len(matched_rt) + len(matched_soy)
    if scope_u in ("DAIRY", "RT"):
        rec_scope = scope_u
    else:
        rec_scope = "DAIRY" if dairy_sum >= rt_sum and dairy_sum > 0 else ("RT" if rt_sum > 0 else "DAIRY")

    return {
        "ok": True,
        "scope": scope_u,
        "recommended_scope": rec_scope,
        "counts": {
            "rt": len(matched_rt),
            "soy": len(matched_soy),
            "dairy": len(matched_dy),
            "micro": len(matched_micro)
        },
        "matched_rt": matched_rt,
        "matched_rt_ccp": matched_rt_ccp,
        "matched_rt_ccp3b": matched_rt_ccp3b,
        "matched_soy": matched_soy,
        "matched_dairy": matched_dy,
        "matched_micro": matched_micro[:120]
    }


@app.post("/api/law-ai-summary")
def api_generate_law_ai_summary(body: LawAiReqPayload, _u: dict = Depends(require_auth_user)):
    bundle = build_full_dashboard_bundle(force_refresh=False)
    all_laws = bundle.get("law", {}).get("all_laws", [])
    target = next((x for x in all_laws if x.get("law_id") == body.law_id), None)
    if not target:
        raise HTTPException(status_code=404, detail="법규 항목을 찾을 수 없습니다.")

    ai_txt = generate_gemini_law_summary_single(target.get("법규명", ""), target.get("원문내용", ""))
    if ai_txt:
        return {"ok": True, "source": "gemini", "summary": ai_txt}
    return {"ok": True, "source": "rule_engine", "summary": extract_concise_law_summary(target.get("법규명", ""), target.get("원문내용", ""))}


@app.post("/api/audit-schedule")
def api_save_audit_schedule(body: AuditSchedulePayload, _u: dict = Depends(require_auth_user)):
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

    cur_list.sort(key=lambda x: x.get("심사일자", ""), reverse=True)
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
def api_save_special_boards(body: SpecialBoardPayload, _u: dict = Depends(require_auth_user)):
    sheet_urls_map = get_sheet_urls_map()

    current_lbl_memo = ""
    if _BUNDLE_CACHE["data"] and _BUNDLE_CACHE["data"].get("special_boards"):
        current_lbl_memo = _BUNDLE_CACHE["data"]["special_boards"].get("label_memo", "")

    if body.label_memo is not None:
        clean_memo = str(body.label_memo).strip()
        save_label_special_tab_memo(sheet_urls_map, clean_memo)
        current_lbl_memo = clean_memo
        if "lbl" in _LAST_GOOD_SHEETS:
            prev_rows, _, prev_err = _LAST_GOOD_SHEETS["lbl"]
            _LAST_GOOD_SHEETS["lbl"] = (prev_rows, clean_memo, prev_err)

    eff_micro = None
    if body.micro_status is not None:
        def _upd_m(b_map: dict, ts: str):
            mode_v = str(body.micro_status.get("mode", "수동지정")).strip()
            st_v = str(body.micro_status.get("status", "출고적합")).strip()
            if st_v == "자동연동" or mode_v == "자동연동":
                m_obj = {"mode": "자동연동", "status": "출고적합", "note": "", "updated_at": ts}
            else:
                if st_v not in ["출고적합", "검사중", "재검·출고보류"]:
                    st_v = "출고적합"
                m_obj = {
                    "mode": "수동지정",
                    "status": st_v,
                    "note": str(body.micro_status.get("note", "")).strip(),
                    "updated_at": ts
                }
            b_map["micro_status_json"] = json.dumps(m_obj, ensure_ascii=False)

        b_map, _ = _read_and_write_special_board(_upd_m)
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
    else:
        if _BUNDLE_CACHE["data"] and _BUNDLE_CACHE["data"].get("special_boards"):
            eff_micro = _BUNDLE_CACHE["data"]["special_boards"].get("micro_status", dict(DEFAULT_MICRO_STATUS))
        else:
            eff_micro = dict(DEFAULT_MICRO_STATUS)

    new_sp = {
        "label_memo": current_lbl_memo,
        "micro_status": eff_micro
    }
    if _BUNDLE_CACHE["data"] is not None:
        _BUNDLE_CACHE["data"]["special_boards"] = new_sp

    msg = "'표시사항 현황' 시트의 [특이사항] 탭에 저장되었습니다." if body.label_memo is not None else "변경사항이 저장되었습니다."
    return {"ok": True, "message": msg, "special_boards": new_sp}


@app.post("/api/law-pref")
def api_save_law_pref(body: LawPrefPayload, _u: dict = Depends(require_auth_user)):
    sheet_urls_map = get_sheet_urls_map()
    sh_law, err_law = open_sheet_isolated(sheet_urls_map, "law_info")
    if sh_law is None:
        raise HTTPException(status_code=500, detail=f"'법규정보' 시트를 열 수 없습니다: {err_law}")

    if "law" in _LAST_GOOD_SHEETS and _LAST_GOOD_SHEETS["law"][0]:
        raw_laws_cur, dash_init_cur, dash_map_cur, sdb_map_cur, _ = _LAST_GOOD_SHEETS["law"]
        dash_map_cur = dict(dash_map_cur or {})
        sdb_map_cur = dict(sdb_map_cur or {})
    else:
        raw_laws_cur, dash_init_cur, dash_map_cur, sdb_map_cur, _ = fetch_law_info_sheet(sheet_urls_map)
        dash_map_cur = dict(dash_map_cur or {})
        sdb_map_cur = dict(sdb_map_cur or {})

    now_s = now_kst().strftime("%Y-%m-%d %H:%M:%S")
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

    sdb_updated = False
    if body.summary is not None and body.summary.strip():
        clean_s = body.summary.strip()
        mth = body.summary_method if body.summary_method in ("manual", "gemini") else "manual"
        sdb_map_cur[body.law_id] = {"summary": clean_s, "method": mth, "updated_at": now_s}
        if target_law:
            target_law["자동조치요약"] = clean_s
        sdb_updated = True

    existing_entry = dash_map_cur.get(body.law_id) or (dash_map_cur.get(norm_nm) if norm_nm else None)
    if norm_nm and norm_nm in dash_map_cur and norm_nm != body.law_id:
        dash_map_cur.pop(norm_nm, None)

    if body.mode == "Hide":
        dash_map_cur.pop(body.law_id, None)
    elif body.mode == "Pin":
        cur_p = (existing_entry.get("prep") if existing_entry else None) or "검토전"
        cur_s = (existing_entry.get("summary") if existing_entry else "")
        if is_broken_or_overlong_summary(cur_s):
            cur_s = ""
        if body.summary is not None and body.summary.strip():
            final_s = body.summary.strip()
        elif cur_s:
            final_s = cur_s
        else:
            final_s = target_law["자동조치요약"] if target_law else ""

        dash_map_cur[body.law_id] = {
            "prep": body.prep if body.prep in LAW_PREP_LIST else cur_p,
            "summary": final_s,
            "law_name": target_law["법규명"] if target_law else ""
        }
    else:
        if existing_entry is not None:
            if body.prep in LAW_PREP_LIST:
                existing_entry["prep"] = body.prep
            if body.summary is not None and body.summary.strip():
                existing_entry["summary"] = body.summary.strip()
            dash_map_cur[body.law_id] = existing_entry

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

    if sdb_updated and raw_laws_cur:
        try:
            sync_law_summary_db_worksheet(sh_law, raw_laws_cur, sdb_map_cur)
        except Exception:
            pass

    new_law_bundle = apply_law_preferences(raw_laws_cur, True, dash_map_cur, aux_prefs)
    sync_law_dashboard_worksheet(sh_law, new_law_bundle["all_laws"])

    _LAST_GOOD_SHEETS["law"] = (raw_laws_cur, True, dash_map_cur, sdb_map_cur, None)
    if _BUNDLE_CACHE["data"] is not None:
        _BUNDLE_CACHE["data"]["law"] = new_law_bundle

    return {
        "ok": True,
        "message": "'법규정보' 시트의 [_법규요약DB] 및 [대시보드] 탭에 저장되었습니다.",
        "law": new_law_bundle
    }


@app.post("/api/label-pin")
def api_save_label_pin(body: LabelPinPayload, _u: dict = Depends(require_auth_user)):
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
        lbl_rows_cached = _LAST_GOOD_SHEETS.get("lbl", ([], None, None))[0]
        if lbl_rows_cached:
            new_lbl_analytics = build_label_analytics(lbl_rows_cached, new_pins)
            _BUNDLE_CACHE["data"]["label_status"] = new_lbl_analytics

    return {"ok": True, "message": "대표 표시사항 노출 설정이 저장되었습니다.", "label_status": new_lbl_analytics}


@app.post("/api/claim-nc-history")
def api_save_claim_nc_history(body: ClaimNcHistPayload, _u: dict = Depends(require_auth_user)):
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
def api_save_manual_kpi(body: ManualKpiPayload, _u: dict = Depends(require_auth_user)):
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
        if k in ("kpi_11", "kpi_5"):
            continue
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
def api_generate_ai_briefing(_u: dict = Depends(require_auth_user)):
    bundle = build_full_dashboard_bundle(force_refresh=False)
    if not bundle.get("ok"):
        raise HTTPException(status_code=500, detail="데이터가 준비되지 않았습니다.")

    pr = bundle["process"]
    ps = pr["summary"]
    lb = bundle["label_status"]
    rw = bundle["raw_db"]
    au = bundle["audit"]
    sp = bundle["supplier"]
    lw = bundle["law"]
    cnh = bundle["claim_nc_hist"]
    cs = bundle["cost"]
    cl = bundle["claim"]
    mc = bundle.get("special_boards", {}).get("micro_status", DEFAULT_MICRO_STATUS)

    # 1) [공정 · 배양 · 출고 판정] 단순 건수 나열 대신 실제 미입력 호기명·이탈·특이사항 핀셋 추출
    rt_today_obj = pr.get("rt_today") or {}
    rt_wait_hos = [
        f"{r['포장호기']}({r['생산 제품명']})"
        for r in rt_today_obj.get("lines_records", [])
        if r.get("_cls") == "cell-yellow"
    ]
    rt_err_hos = [
        f"{r['포장호기']}({r['생산 제품명']})"
        for r in rt_today_obj.get("lines_records", [])
        if r.get("_cls") == "cell-red"
    ]
    soy_def = ps.get("soy_default_tab", "-")
    soy_blocks = pr.get("soy_tabs_map", {}).get(soy_def, {}).get("blocks", [])
    soy_wait_hos = [
        f"{b['포장기']}({b['품목명']})"
        for b in soy_blocks
        if not b.get("검사완료여부")
    ]

    sec1_items = []
    exp_alerts_live = ps.get("exp_alerts", [])
    if exp_alerts_live:
        for ea in exp_alerts_live[:3]:
            sec1_items.append(f"소비기한 검증 확인 필요 : {ea.get('msg', '')}")
    if rt_err_hos or ps.get("red_rt_cnt", 0) > 0:
        sec1_items.append(f"상온 공정 기준·기입 누락 즉시 확인 필요 : {', '.join(rt_err_hos[:3]) or (str(ps['red_rt_cnt']) + '건')}")
    elif rt_wait_hos:
        sec1_items.append(f"상온({ps['rt_today_tab']}) CCP 이탈 없으나 확인시간 기입 대기 : {', '.join(rt_wait_hos[:3])}")
    else:
        sec1_items.append(f"상온({ps['rt_today_tab']}) {ps['active_rt_cnt']}품목 · 유제품(카톤 {ps['active_milk_carton_cnt']}·병 {ps['active_milk_bottle_cnt']}·발효 {ps['active_ferm_cnt']}) 기준이탈 없이 정상 가동")

    if soy_wait_hos:
        sec1_items.append(f"두유배양({soy_def} 개봉분) 판정 미입력 대기 호기 : {', '.join(soy_wait_hos[:4])}")
    else:
        sec1_items.append(f"두유배양({soy_def}) {ps['soy_done_cnt']}개 호기 전량 판정 완료 (오염 {ps['soy_contam_total']}건)")

    sec1_items.append(f"미생물 출고 판정 : '{mc.get('status', '출고적합')}' — {mc.get('note', '특이사항 없음')}")
    if ps.get("today_notes"):
        sec1_items.append(f"현장 전달사항 : {' / '.join(ps['today_notes'][:2])}")

    # 2) [법규 D-Day · 원자재 알레르기 · 표시심의 교차 리스크]
    all_laws = lw.get("all_laws", [])
    urgent_unready = [
        f"{x['법규명']}({x['d_day_label']})"
        for x in all_laws
        if x.get("d_day") is not None and 0 <= x["d_day"] <= 120 and not x.get("예고여부") and x.get("prep_status") == "검토전"
    ]
    prep_1yr_unready = [
        f"{x['법규명']}({x['d_day_label']})"
        for x in lw.get("dashboard_laws", [])
        if x.get("d_day") is not None and 0 <= x["d_day"] <= 365 and x.get("prep_status") != "반영완료"
    ]
    alg_bd = rw.get("allergen_breakdown", {})
    alg_nz = [f"{k} {v}건" for k, v in alg_bd.items() if v > 0]

    sec2_items = []
    if urgent_unready:
        sec2_items.append(f"시행 4개월 내 도래 '검토전' 고시 우선 착수 필요 : {', '.join(urgent_unready[:2])}")
    elif prep_1yr_unready:
        sec2_items.append(f"1년 내 불용방지 관리 대상 법규 {len(prep_1yr_unready)}건 추적 중 ({', '.join(prep_1yr_unready[:2])})")
    else:
        sec2_items.append(f"추적 법규 {lw['active_cnt']}건 모두 준비상태 정상 관리 중")

    if rw.get("allergy_cnt", 0) > 0:
        sec2_items.append(f"원자재 DB 신규 알레르기(4종) 함유 {rw['allergy_cnt']}건({'·'.join(alg_nz)}) 완제품 표시 대조 관리")
    sec2_items.append(f"표시사항 현재 심의 대기 {lb['reviewing_cnt']}건 ({lb['rev_summary_line']})")

    # 3) [8대 KPI 목표 미달 항목 및 연말 예측 격차 진단]
    cs_dept = cs["dept"]
    cost_gap_uk = round(cs["targets"]["부문"] - cs_dept["proj_uk"], 2)
    sup_gap = round(sp["target_exec"] - sp["exec_rate"], 2) if sp["exec_rate"] > 0 else 0.0
    aud_ks = au.get("kpi_sheet", {})
    aud_gap = round(aud_ks.get("score_target", 94.28) - aud_ks.get("score_actual", 0.0), 2) if aud_ks.get("score_actual", 0) > 0 else 0.0

    sec3_items = []
    if cost_gap_uk > 0:
        sec3_items.append(f"원가절감 : 현재 {cs_dept['actual_uk']}억({cs_dept['rate']}%) · 연말 추세 예측 {cs_dept['proj_uk']}억 (목표 {cs['targets']['부문']}억 대비 {cost_gap_uk}억 추가 과제 발굴 필요)")
    else:
        sec3_items.append(f"원가절감 : 현재 {cs_dept['actual_uk']}억({cs_dept['rate']}%) · 연말 예측 {cs_dept['proj_uk']}억으로 목표({cs['targets']['부문']}억) 달성 순항")

    sec3_items.append(
        f"클레임 발생률({cl.get('active_year', '2026년')}) : 현재 누적 {cl['actual_ppm']} PPM · 이상치 보정 계절성 반영 연말 누적 예상 {cl.get('cum_pred_ppm', cl['actual_ppm'])} PPM (목표 {cl['target_ppm']} 이하 {'안정권' if cl.get('cum_pred_ppm', cl['actual_ppm']) <= cl['target_ppm'] else '집중관리'})"
    )
    kpi_gaps = []
    if sup_gap > 0:
        kpi_gaps.append(f"협력업체 AUDIT 실행률 {sp['exec_rate']:.1f}%(목표 대비 -{sup_gap:.1f}%p)")
    if aud_gap > 0:
        kpi_gaps.append(f"외부심사 점수 {aud_ks['score_actual']}점(목표 대비 -{aud_gap:.2f}점)")
    if kpi_gaps:
        sec3_items.append("목표 미달 중점 보완 KPI : " + " / ".join(kpi_gaps))

    # 4) [심사 · 협력업체 · 클레임 실행 체크포인트]
    sec4_items = [f"방문·출장 일정 : {au['summary'].get('upcoming_banner', '-')}"]
    if sp.get("unsub_cnt", 0) > 0 or sp.get("out_cnt", 0) > 0:
        sec4_items.append(f"협력업체 서류심사 : 미제출 {sp['unsub_cnt']}개사 독려 및 등급 외 {sp['out_cnt']}개사 개선 피드백 필요")
    act_cnh = cnh.get("active_items", [])
    if act_cnh:
        names_cnh = [f"[{x['구분']}]{x['제품명']}({x['진행상태']})" for x in act_cnh[:2]]
        sec4_items.append(f"진행 중 클레임·부적합 {len(act_cnh)}건 추적 : {', '.join(names_cnh)}")
    else:
        sec4_items.append(f"현재 출고보류·추적 중인 클레임/부적합 0건 (시트 누적 {cnh['counts']['total']}건 종결 완료)")

    structured_sections = [
        {"badge": "공정 · 배양 점검", "tone": "warn" if (rt_err_hos or rt_wait_hos or soy_wait_hos) else "ok", "title": "현장 · 실험실 실시간 미결 및 출고 판정", "items": sec1_items},
        {"badge": "선제 리스크 차단", "tone": "warn" if (urgent_unready or rw.get("allergy_cnt", 0) > 0) else "info", "title": "법규 D-Day · 신규 알레르기 · 표시심의 교차 진단", "items": sec2_items},
        {"badge": "KPI 예측 · 격차", "tone": "warn" if (cost_gap_uk > 0 or kpi_gaps) else "ok", "title": "8대 KPI 연말 달성 예측 및 부족분 핀셋 진단", "items": sec3_items},
        {"badge": "심사 · 사후관리", "tone": "info", "title": "수검 일정 · 협력업체 미제출 · 클레임 추적", "items": sec4_items}
    ]

    fallback_plain = "\n\n".join([f"■ [{s['badge']}] {s['title']}\n" + "\n".join([f"• {it}" for it in s["items"]]) for s in structured_sections])

    api_key = os.environ.get("GEMINI_API_KEY", "").strip()
    if not api_key:
        return {"ok": True, "source": "Q-Safety 교차진단 엔진", "sections": structured_sections, "briefing": fallback_plain}

    prompt_txt = (
        "너는 연세유업 아산공장 품질안전부문 수석 데이터 분석가야. 아래 13개 시트에서 교차 추출한 팩트 진단 데이터를 바탕으로 "
        "경영진 보고용 4개 섹션 JSON을 다듬어줘. 추측이나 없는 숫자는 절대 만들지 말고, 각 항목(items)은 명사형·개조식 1줄로 간결·명료하게 작성해. "
        "반드시 순수 JSON 배열만 출력해: "
        '[{"badge":"공정 · 배양 점검","tone":"ok|warn","title":"...","items":["...","..."]}, ...]\n\n'
        f"[원본 팩트 JSON]\n{json.dumps(structured_sections, ensure_ascii=False)}"
    )

    for endpoint in [
        f"https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash:generateContent?key={api_key}",
        f"https://aiplatform.googleapis.com/v1/publishers/google/models/gemini-2.5-flash:generateContent?key={api_key}"
    ]:
        try:
            resp = requests.post(
                endpoint,
                json={"contents": [{"role": "user", "parts": [{"text": prompt_txt}]}]},
                timeout=11
            )
            if resp.status_code == 200:
                cands = resp.json().get("candidates", [])
                if cands:
                    parts = cands[0].get("content", {}).get("parts", [])
                    if parts and parts[0].get("text"):
                        raw_t = parts[0]["text"].strip()
                        raw_t = re.sub(r"^```(?:json)?\s*|\s*```$", "", raw_t, flags=re.IGNORECASE).strip()
                        parsed_sec = json.loads(raw_t)
                        if isinstance(parsed_sec, list) and len(parsed_sec) >= 3 and all("items" in x for x in parsed_sec):
                            plain_ai = "\n\n".join([f"■ [{s.get('badge','')}] {s.get('title','')}\n" + "\n".join([f"• {it}" for it in s.get("items", [])]) for s in parsed_sec])
                            return {"ok": True, "source": "Gemini 2.5 Flash 심층 교차진단", "sections": parsed_sec, "briefing": plain_ai}
        except Exception:
            continue

    return {"ok": True, "source": "Q-Safety 교차진단 엔진", "sections": structured_sections, "briefing": fallback_plain}
# ============================ [api/index.py - PART 4 / 4 끝] ============================
