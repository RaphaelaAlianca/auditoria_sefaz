from __future__ import annotations

from typing import List
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials


def _cliente_gspread(credentials_file: str, scopes: List[str]) -> gspread.Client:
    creds = Credentials.from_service_account_file(credentials_file, scopes=scopes)
    return gspread.authorize(creds)


def _ensure_ws(ss: gspread.Spreadsheet, title: str, rows: int = 2000, cols: int = 20) -> gspread.Worksheet:
    try:
        return ss.worksheet(title)
    except gspread.WorksheetNotFound:
        return ss.add_worksheet(title=title, rows=rows, cols=cols)


def _df_to_values(df: pd.DataFrame) -> List[List[str]]:
    df2 = df.copy().fillna("")
    return [df2.columns.tolist()] + df2.astype(str).values.tolist()


def escrever_df(ss: gspread.Spreadsheet, aba: str, df: pd.DataFrame, max_linhas: int) -> None:
    df_out = df.head(max_linhas).copy()
    rows = max(2000, min(max_linhas + 10, 200000))
    cols = max(10, len(df_out.columns) + 2)

    ws = _ensure_ws(ss, aba, rows=rows, cols=cols)
    ws.clear()
    ws.update(values=_df_to_values(df_out), range_name="A1")


def escrever_status(ss: gspread.Spreadsheet, aba: str, status_texto: str) -> None:
    ws = _ensure_ws(ss, aba, rows=80, cols=6)
    ws.clear()
    ws.update(values=[["STATUS"], [status_texto]], range_name="A1")


def exportar_para_sheets(
    spreadsheet_id: str,
    credentials_file: str,
    scopes: List[str],
    *,
    aba_resumo_pendencias: str,
    aba_detalhes: str,
    aba_status: str,
    df_resumo: pd.DataFrame,
    df_detalhes: pd.DataFrame,
    texto_status: str,
    max_linhas_resumo: int,
    max_linhas_detalhes: int,
) -> None:
    gc = _cliente_gspread(credentials_file, scopes)
    ss = gc.open_by_key(spreadsheet_id)

    escrever_df(ss, aba_resumo_pendencias, df_resumo, max_linhas=max_linhas_resumo)
    escrever_df(ss, aba_detalhes, df_detalhes, max_linhas=max_linhas_detalhes)
    escrever_status(ss, aba_status, texto_status)
