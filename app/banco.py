import sqlite3
import hashlib
from datetime import datetime
from typing import Dict, Any, Iterable, List

def conectar(db_path: str) -> sqlite3.Connection:
    con = sqlite3.connect(db_path)
    con.execute("PRAGMA journal_mode=WAL;")
    con.execute("PRAGMA synchronous=NORMAL;")
    con.execute("PRAGMA foreign_keys=ON;")
    return con

def iniciar(con: sqlite3.Connection) -> None:
    con.execute("""
    CREATE TABLE IF NOT EXISTS pendencias_raw (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      hash_registro TEXT NOT NULL UNIQUE,

      fonte TEXT,
      arquivo_origem TEXT,
      aba_origem TEXT,
      linha_origem INTEGER,

      cnpj TEXT,
      cgf TEXT,
      razao TEXT,

      tipo_pendencia TEXT,
      periodo TEXT,
      detalhe TEXT,

      valor NUMERIC,
      data_referencia TEXT,
      data_coleta TEXT NOT NULL,

      raw_json TEXT
    );
    """)

    con.execute("CREATE INDEX IF NOT EXISTS idx_raw_cnpj ON pendencias_raw(cnpj);")
    con.execute("CREATE INDEX IF NOT EXISTS idx_raw_cgf ON pendencias_raw(cgf);")
    con.execute("CREATE INDEX IF NOT EXISTS idx_raw_tipo ON pendencias_raw(tipo_pendencia);")
    con.execute("CREATE INDEX IF NOT EXISTS idx_raw_periodo ON pendencias_raw(periodo);")
    con.execute("CREATE INDEX IF NOT EXISTS idx_raw_fonte ON pendencias_raw(fonte);")
    con.execute("CREATE INDEX IF NOT EXISTS idx_raw_arquivo ON pendencias_raw(arquivo_origem);")

    con.execute("""
    CREATE TABLE IF NOT EXISTS import_log (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      arquivo_origem TEXT NOT NULL,
      hash_arquivo TEXT NOT NULL,
      data_importacao TEXT NOT NULL,
      linhas_lidas INTEGER NOT NULL,
      linhas_inseridas INTEGER NOT NULL,
      UNIQUE(arquivo_origem, hash_arquivo)
    );
    """)
    con.commit()

def _norm(v: Any) -> str:
    return "" if v is None else str(v).strip()

def hash_arquivo(bytes_data: bytes) -> str:
    return hashlib.sha256(bytes_data).hexdigest()

def hash_registro(row: Dict[str, Any]) -> str:
    # 1 linha do Excel = 1 registro
    parts = [
        _norm(row.get("arquivo_origem")),
        _norm(row.get("aba_origem")),
        _norm(row.get("linha_origem")),
    ]
    base = "||".join(parts).encode("utf-8", "ignore")
    return hashlib.sha256(base).hexdigest()

def arquivo_ja_importado(con: sqlite3.Connection, nome: str, h: str) -> bool:
    cur = con.execute(
        "SELECT 1 FROM import_log WHERE arquivo_origem=? AND hash_arquivo=? LIMIT 1",
        (nome, h),
    )
    return cur.fetchone() is not None

def inserir_raw(
    con: sqlite3.Connection,
    rows: Iterable[Dict[str, Any]],
    *,
    fonte: str,
    arquivo_origem: str,
    hash_arquivo_str: str,
) -> Dict[str, int]:
    now = datetime.now().isoformat(timespec="seconds")
    rows_list: List[Dict[str, Any]] = list(rows)
    lidas = len(rows_list)

    cur = con.cursor()
    inseridas = 0

    for i, r in enumerate(rows_list, start=1):
        rr = dict(r)

        rr["fonte"] = rr.get("fonte") or fonte
        rr["arquivo_origem"] = arquivo_origem
        rr["aba_origem"] = rr.get("aba_origem") or ""
        rr["linha_origem"] = rr.get("linha_origem") or i
        rr["data_coleta"] = rr.get("data_coleta") or now

        rr["hash_registro"] = hash_registro(rr)

        try:
            cur.execute("""
            INSERT INTO pendencias_raw (
              hash_registro, fonte, arquivo_origem, aba_origem, linha_origem,
              cnpj, cgf, razao, tipo_pendencia, periodo, detalhe,
              valor, data_referencia, data_coleta, raw_json
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);
            """, (
                rr.get("hash_registro"), rr.get("fonte"), rr.get("arquivo_origem"), rr.get("aba_origem"), rr.get("linha_origem"),
                rr.get("cnpj"), rr.get("cgf"), rr.get("razao"), rr.get("tipo_pendencia"), rr.get("periodo"), rr.get("detalhe"),
                rr.get("valor"), rr.get("data_referencia"), rr.get("data_coleta"), rr.get("raw_json")
            ))
            inseridas += 1
        except sqlite3.IntegrityError:
            pass

    cur.execute("""
    INSERT OR IGNORE INTO import_log
    (arquivo_origem, hash_arquivo, data_importacao, linhas_lidas, linhas_inseridas)
    VALUES (?,?,?,?,?);
    """, (arquivo_origem, hash_arquivo_str, now, lidas, inseridas))

    con.commit()
    return {"linhas_lidas": lidas, "linhas_inseridas": inseridas}
