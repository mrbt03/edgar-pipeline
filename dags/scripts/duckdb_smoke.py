import os
import duckdb


def main():
    duckdb_path = os.getenv("DUCKDB_PATH", "/data/edgar.duckdb")
    con = duckdb.connect(duckdb_path)
    try:
        rows = con.execute(
            "select form_type, count(*) as c from raw.edgar_master group by 1 order by 2 desc limit 10"
        ).fetchall()
        print("Top form_type counts:")
        for r in rows:
            print(f"{r[0]}\t{r[1]}")
    finally:
        con.close()


if __name__ == "__main__":
    main()


