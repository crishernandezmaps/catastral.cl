#!/usr/bin/env python3
"""Create indexes after bulk load."""
import time
import psycopg
from config import DB_DSN

def main():
    print("Building indexes...")
    t0 = time.time()

    with psycopg.connect(DB_DSN) as conn:
        conn.autocommit = True
        with conn.cursor() as cur:
            indexes = [
                ("idx_actual_rol", "CREATE UNIQUE INDEX IF NOT EXISTS idx_actual_rol ON catastro_actual (comuna, manzana, predio)"),
                ("idx_actual_comuna", "CREATE INDEX IF NOT EXISTS idx_actual_comuna ON catastro_actual (comuna)"),
                ("idx_actual_destino", "CREATE INDEX IF NOT EXISTS idx_actual_destino ON catastro_actual (dc_cod_destino)"),
                ("idx_actual_sup", "CREATE INDEX IF NOT EXISTS idx_actual_sup ON catastro_actual (dc_sup_terreno) WHERE dc_sup_terreno IS NOT NULL"),
                ("idx_actual_avaluo", "CREATE INDEX IF NOT EXISTS idx_actual_avaluo ON catastro_actual (rc_avaluo_total) WHERE rc_avaluo_total IS NOT NULL"),
                ("idx_actual_direccion", "CREATE INDEX IF NOT EXISTS idx_actual_direccion ON catastro_actual USING gin (rc_direccion gin_trgm_ops)"),
            ]
            for name, sql in indexes:
                print(f"  Creating {name}...", end=" ", flush=True)
                it0 = time.time()
                cur.execute(sql)
                print(f"{time.time()-it0:.0f}s")

            print("  Running ANALYZE...", end=" ", flush=True)
            cur.execute("ANALYZE catastro_actual")
            cur.execute("ANALYZE catastro_historico")
            print("done")

    print(f"All indexes built in {time.time()-t0:.0f}s")

if __name__ == "__main__":
    main()
