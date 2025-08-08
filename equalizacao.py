import os
from dotenv import load_dotenv
import pymssql

load_dotenv("config.env")

hmlg_servidor = os.getenv("SERVER")
hmlg_banco = os.getenv("DATABASE")
usuario = os.getenv("DB_USER")
senha = os.getenv("DB_PASS")

scripts_dir = os.getenv("SCRIPTS_DIR", "") 
log_path = os.getenv("LOG_PATH", "")        

def write_log(msg):
    with open(log_path, "a", encoding="utf-8") as log:
        log.write(msg + "\n")
    print(msg)


lista_bases_query = """
SELECT
       b.servidor,
       b.NOME_BASE
FROM tokens t
         join bases b on b.id = t.BASE_ID
WHERE 
  t.STATUS = 'ACTIVE'
  AND b.NOME_BASE not like '% %'
  AND b.SERVIDOR not like '% %'
GROUP BY b.servidor, b.NOME_BASE
ORDER BY  b.servidor;
"""

def ler_scripts_sql(diretorio):
    arquivos = [f for f in os.listdir(diretorio) if f.endswith('.sql')]
    arquivos.sort()  
    scripts = []
    for arq in arquivos:
        caminho = os.path.join(diretorio, arq)
        with open(caminho, "r", encoding="utf-8") as f:
            scripts.append((arq, f.read()))
    return scripts

def executar_query(conn, query):
    with conn.cursor() as cursor:
        cursor.execute(query)
        return cursor.fetchall()

def executar_script(conn, script):
    with conn.cursor() as cursor:
        cursor.execute(script)
    conn.commit()

def main():

    open(log_path, "w", encoding="utf-8").close()

    write_log(f"Conectando na base HMLG {hmlg_servidor} | {hmlg_banco} para obter lista de bases...")
    try:
        conn_hmlg = pymssql.connect(server=hmlg_servidor, user=usuario, password=senha, database=hmlg_banco)
        bases = executar_query(conn_hmlg, lista_bases_query)
        conn_hmlg.close()
    except Exception as e:
        write_log(f"Erro ao conectar/executar na base HMLG:\n{e}")
        return

    total_bases = len(bases)
    if total_bases == 0:
        write_log("Nenhuma base encontrada para processar.")
        return

    scripts = ler_scripts_sql(scripts_dir)
    if not scripts:
        write_log("Nenhum script SQL encontrado na pasta.")
        return


    for idx, (servidor, banco) in enumerate(bases, start=1):
        porcentagem = round((idx / total_bases) * 100, 2)
        write_log(f"\n[{idx} de {total_bases}] Executando no servidor: {servidor}, banco: {banco}... ({porcentagem}%)")

        try:
            conn = pymssql.connect(server=servidor, user=usuario, password=senha, database=banco)
            for nome_script, script_sql in scripts:
                write_log(f"Executando script {nome_script} em {servidor} | {banco}...")
                executar_script(conn, script_sql)
            write_log(f"Executado para {servidor} | {banco}...")
            conn.close()
        except Exception as e:
            write_log(f"Erro ao conectar/executar em {servidor} - {banco}:\n{e}")

if __name__ == "__main__":
    main()
