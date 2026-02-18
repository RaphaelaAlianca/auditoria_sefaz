from __future__ import annotations

import os
import json
import time
import hashlib
import shutil
from pathlib import Path
from datetime import datetime
from typing import Dict, Any, List, Tuple

import pandas as pd
import sqlite3

from .normalizar import normalizar_por_aba


# =========================
# Utilitários de arquivo
# =========================

def _hash_arquivo_md5(path: Path) -> str:
    h = hashlib.md5()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _nome_destino_unico(dest_dir: Path, nome: str) -> Path:
    dest = dest_dir / nome
    if not dest.exists():
        return dest
    stem = dest.stem
    suf = dest.suffix
    for i in range(2, 9999):
        cand = dest_dir / f"{stem} ({i}){suf}"
        if not cand.exists():
            return cand
    return dest_dir / f"{stem} ({int(time.time())}){suf}"


def mover_ou_copiar_para_processados(
    arquivo: Path,
    processados_dir: Path,
    tentativas: int = 6
) -> Tuple[str, str | None, str | None]:
    """
    1) tenta mover algumas vezes (resolve a maioria)
    2) se continuar preso, copia para processados e mantém o original
    Retorna: (status_move, erro_move_ou_none, destino_ou_none)
    """
    processados_dir.mkdir(parents=True, exist_ok=True)
    destino = _nome_destino_unico(processados_dir, arquivo.name)

    last_err: Exception | None = None

    # tenta mover
    for t in range(1, tentativas + 1):
        try:
            shutil.move(str(arquivo), str(destino))
            return "MOVIDO_PROCESSADOS", None, str(destino)
        except Exception as e:
            last_err = e
            time.sleep(0.35 * t)

    # tenta copiar
    try:
        shutil.copy2(str(arquivo), str(destino))
        return "COPIADO_PROCESSADOS", str(last_err), str(destino)
    except Exception as e2:
        return "NAO_MOVIDO_NEM_COPIADO", f"{last_err} | copy_err={e2}", None


# =========================
# Banco: schema mínimo
# =========================

def _conectar_db(db_path: Path) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(str(db_path))
    con.execute("PRAGMA journal_mode=WAL;")
    con.execute("PRAGMA synchronous=NORMAL;")
    return con


def _iniciar_schema(con: sqlite3.Connection) -> None:
    con.execute("""
    CREATE TABLE IF NOT EXISTS import_log (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      arquivo_origem TEXT,
      hash_md5 TEXT UNIQUE,
      data_importacao TEXT
    );
    """)

    con.execute("""
    CREATE TABLE IF NOT EXISTS pendencias_raw (
      id INTEGER PRIMARY KEY AUTOINCREMENT,

      cnpj TEXT,
      cgf TEXT,
      razao TEXT,

      tipo_pendencia TEXT,
      periodo TEXT,
      valor REAL,
      detalhe TEXT,
      data_referencia TEXT,

      arquivo_origem TEXT,
      aba_origem TEXT,
      linha_origem INTEGER,

      data_coleta TEXT,
      raw_json TEXT
    );
    """)

    con.execute("CREATE INDEX IF NOT EXISTS idx_raw_tipo ON pendencias_raw(tipo_pendencia);")
    con.execute("CREATE INDEX IF NOT EXISTS idx_raw_cnpj ON pendencias_raw(cnpj);")
    con.execute("CREATE INDEX IF NOT EXISTS idx_raw_cgf ON pendencias_raw(cgf);")
    con.commit()


def _ja_importado(con: sqlite3.Connection, hash_md5: str) -> bool:
    row = con.execute("SELECT 1 FROM import_log WHERE hash_md5=?", (hash_md5,)).fetchone()
    return row is not None


def _registrar_importacao(con: sqlite3.Connection, arquivo: str, hash_md5: str) -> None:
    con.execute(
        "INSERT OR IGNORE INTO import_log(arquivo_origem, hash_md5, data_importacao) VALUES (?,?,?)",
        (arquivo, hash_md5, datetime.now().isoformat(timespec="seconds")),
    )
    con.commit()


# =========================
# Leitura Excel
# =========================

ABAS_ACEITAS = {
    "Omissões de EFD",
    "Débitos",
    "Omissões e divergências de NFE",
    "NFe inexistente declarada",
    "Omissões e Divergências CFe",
    "CTE escriturado com divergência",
    "NFe sem REG_PAS",
    "Outros limitadores",
}

def _ler_excel(path: Path) -> Dict[str, pd.DataFrame]:
    # engine=openpyxl é o padrão para xlsx
    xls = pd.read_excel(path, sheet_name=None, engine="openpyxl")
    # mantém só as abas que interessam
    return {aba: df for aba, df in xls.items() if aba in ABAS_ACEITAS}


def _to_row_dict(df: pd.DataFrame, idx: int) -> Dict[str, Any]:
    row = df.iloc[idx].to_dict()
    # normaliza colunas (mantém nomes originais)
    return {k: (None if (isinstance(v, float) and pd.isna(v)) else v) for k, v in row.items()}


def _inserir_linha(
    con: sqlite3.Connection,
    base: Dict[str, Any],
    arquivo_origem: str,
    aba_origem: str,
    linha_origem: int,
    data_coleta: str,
    raw_json: str,
) -> None:
    con.execute(
        """
        INSERT INTO pendencias_raw (
          cnpj, cgf, razao,
          tipo_pendencia, periodo, valor, detalhe, data_referencia,
          arquivo_origem, aba_origem, linha_origem,
          data_coleta, raw_json
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
        """,
        (
            base.get("cnpj", "") or "",
            base.get("cgf", "") or "",
            base.get("razao", "") or "",
            base.get("tipo_pendencia", "") or "",
            base.get("periodo", "") or "",
            base.get("valor", None),
            base.get("detalhe", "") or "",
            base.get("data_referencia", "") or "",
            arquivo_origem,
            aba_origem,
            linha_origem,
            data_coleta,
            raw_json,
        ),
    )


# =========================
# API principal
# =========================

def importar_pasta(
    pasta_entrada: Path,
    pasta_processados: Path,
    pasta_erros: Path,
    db_path: Path,
) -> Dict[str, Any]:

    pasta_entrada.mkdir(parents=True, exist_ok=True)
    pasta_processados.mkdir(parents=True, exist_ok=True)
    pasta_erros.mkdir(parents=True, exist_ok=True)

    arquivos = sorted([p for p in pasta_entrada.glob("*.xlsx") if p.is_file()])
    detalhes: List[Dict[str, Any]] = []

    total = len(arquivos)
    importados = 0
    linhas_lidas_total = 0
    linhas_inseridas_total = 0

    con = _conectar_db(db_path)
    _iniciar_schema(con)

    data_coleta = datetime.now().isoformat(timespec="seconds")

    for path in arquivos:
        arquivo_nome = path.name

        try:
            hash_md5 = _hash_arquivo_md5(path)

            if _ja_importado(con, hash_md5):
                detalhes.append({"arquivo": arquivo_nome, "status": "JA_IMPORTADO"})
                continue

            # lê abas relevantes
            abas = _ler_excel(path)

            linhas_lidas = 0
            linhas_inseridas = 0

            # insere em transação (bem mais rápido)
            con.execute("BEGIN;")

            for aba, df in abas.items():
                # remove linhas completamente vazias
                df2 = df.dropna(how="all")
                if df2.empty:
                    continue

                for i in range(len(df2)):
                    rowdict = _to_row_dict(df2, i)

                    # normaliza (classifica)
                    base = normalizar_por_aba(aba, rowdict)

                    # guarda RAW completo (para auditoria)
                    raw = {
                        "aba": aba,
                        "row": rowdict
                    }
                    raw_json = json.dumps(raw, ensure_ascii=False, default=str)

                    # linha_origem: +2 porque 1 é header, e i é 0-indexed
                    linha_origem = int(i) + 2

                    _inserir_linha(
                        con,
                        base=base,
                        arquivo_origem=arquivo_nome,
                        aba_origem=aba,
                        linha_origem=linha_origem,
                        data_coleta=data_coleta,
                        raw_json=raw_json,
                    )

                    linhas_lidas += 1
                    linhas_inseridas += 1

            con.commit()

            # registra no log (dedupe por hash)
            _registrar_importacao(con, arquivo_nome, hash_md5)

            importados += 1
            linhas_lidas_total += linhas_lidas
            linhas_inseridas_total += linhas_inseridas

            detalhes.append({
                "arquivo": arquivo_nome,
                "status": "OK",
                "linhas_lidas": linhas_lidas,
                "linhas_inseridas": linhas_inseridas
            })

            # move/copia para processados
            status_move, erro_move, destino_final = mover_ou_copiar_para_processados(path, pasta_processados)

            if status_move == "MOVIDO_PROCESSADOS":
                # nada a fazer
                pass
            elif status_move == "COPIADO_PROCESSADOS":
                detalhes.append({
                    "arquivo": arquivo_nome,
                    "status": "COPIADO_PROCESSADOS",
                    "destino": destino_final,
                    "aviso": "Arquivo estava em uso (WinError 32). Copiado para processados e mantido na entrada.",
                    "erro_move": erro_move
                })
            else:
                detalhes.append({
                    "arquivo": arquivo_nome,
                    "status": "IMPORTADO_MAS_NAO_MOVIDO",
                    "erro_move": erro_move
                })

        except Exception as e:
            # tenta mandar para erros
            try:
                destino_err = _nome_destino_unico(pasta_erros, arquivo_nome)
                shutil.copy2(str(path), str(destino_err))
            except Exception:
                pass

            detalhes.append({
                "arquivo": arquivo_nome,
                "status": "ERRO",
                "erro": str(e)
            })

    con.close()

    return {
        "arquivos_total": total,
        "arquivos_importados": importados,
        "linhas_lidas": linhas_lidas_total,
        "linhas_inseridas": linhas_inseridas_total,
        "detalhes": detalhes
    }
