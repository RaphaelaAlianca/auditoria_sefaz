from __future__ import annotations

import time
import hashlib
import shutil
from pathlib import Path
from datetime import datetime
import sqlite3


TENTATIVAS = 6


def hash_arquivo_md5(path: Path) -> str:
    h = hashlib.md5()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def nome_destino_unico(dest_dir: Path, nome: str) -> Path:
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


def mover_ou_copiar(arquivo: Path, processados_dir: Path, tentativas: int = TENTATIVAS):
    processados_dir.mkdir(parents=True, exist_ok=True)
    destino = nome_destino_unico(processados_dir, arquivo.name)

    last_err = None
    for t in range(1, tentativas + 1):
        try:
            shutil.move(str(arquivo), str(destino))
            return "MOVIDO", None, destino
        except Exception as e:
            last_err = e
            time.sleep(0.35 * t)

    try:
        shutil.copy2(str(arquivo), str(destino))
        return "COPIADO", str(last_err), destino
    except Exception as e2:
        return "FALHOU", f"{last_err} | copy_err={e2}", None


def _colunas_da_tabela(con: sqlite3.Connection, tabela: str) -> set[str]:
    try:
        rows = con.execute(f"PRAGMA table_info({tabela})").fetchall()
        # (cid, name, type, notnull, dflt_value, pk)
        return {r[1] for r in rows}
    except Exception:
        return set()


def ja_importado_no_db(db_path: Path, arquivo_nome: str, hash_md5: str) -> bool:
    """
    Compatível com DB antigo:
      - Se existir coluna hash_md5: usa ela (melhor)
      - Se NÃO existir: usa arquivo_origem (suficiente para nosso organizador)
    """
    if not db_path.exists():
        return False

    con = sqlite3.connect(str(db_path))
    try:
        # se não existir import_log, não tem como saber
        con.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='import_log'")
        exists = con.fetchone() if hasattr(con, "fetchone") else None  # compat
        rows = con.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='import_log'").fetchone()
        if not rows:
            return False

        cols = _colunas_da_tabela(con, "import_log")

        if "hash_md5" in cols:
            row = con.execute("SELECT 1 FROM import_log WHERE hash_md5=?", (hash_md5,)).fetchone()
            return row is not None

        # fallback: DB antigo sem hash -> usa arquivo_origem
        if "arquivo_origem" in cols:
            row = con.execute("SELECT 1 FROM import_log WHERE arquivo_origem=?", (arquivo_nome,)).fetchone()
            return row is not None

        return False
    finally:
        con.close()


def main():
    root = Path(__file__).resolve().parents[1]
    entrada = root / "entrada"
    processados = root / "processados"
    db_path = root / "banco" / "pendencias.db"

    entrada.mkdir(parents=True, exist_ok=True)
    processados.mkdir(parents=True, exist_ok=True)

    arquivos = sorted([p for p in entrada.glob("*.xlsx") if p.is_file()])

    print("🧹 ORGANIZADOR - iniciando")
    print("📥 Entrada:", entrada)
    print("📦 Processados:", processados)
    print("🗄️ DB:", db_path)
    print("📄 Arquivos encontrados:", len(arquivos))

    movidos = 0
    copiados = 0
    pulados = 0
    falhas = 0

    for arq in arquivos:
        try:
            h = hash_arquivo_md5(arq)

            if ja_importado_no_db(db_path, arq.name, h):
                status, err, dest = mover_ou_copiar(arq, processados)
                if status == "MOVIDO":
                    movidos += 1
                    print(f"✅ MOVIDO: {arq.name}")
                elif status == "COPIADO":
                    copiados += 1
                    print(f"⚠️ COPIADO (em uso): {arq.name}")
                else:
                    falhas += 1
                    print(f"❌ FALHOU: {arq.name} | {err}")
            else:
                pulados += 1
                print(f"⏭️ PULADO (ainda não importado): {arq.name}")

        except Exception as e:
            falhas += 1
            print(f"❌ ERRO: {arq.name} | {e}")

    agora = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print("\nSTATUS:", agora,
          "| movidos:", movidos,
          "| copiados:", copiados,
          "| pulados:", pulados,
          "| falhas:", falhas)


if __name__ == "__main__":
    main()
