import sqlite3

caminho = r".\banco\pendencias.db"

con = sqlite3.connect(caminho)
cur = con.cursor()

# Verificar colunas existentes
cur.execute("PRAGMA table_info(pendencias_raw)")
cols = [r[1] for r in cur.fetchall()]

print("Colunas atuais:", cols)

# Criar coluna fonte se não existir
if "fonte" not in cols:
    cur.execute("ALTER TABLE pendencias_raw ADD COLUMN fonte TEXT")
    con.commit()
    print("OK: coluna 'fonte' criada com sucesso.")
else:
    print("OK: coluna 'fonte' já existe.")

con.close()
