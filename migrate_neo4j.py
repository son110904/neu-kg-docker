#!/usr/bin/env python3
"""
migrate_neo4j.py  —  Export toàn bộ graph từ Neo4j Aura → import vào Neo4j local Docker

Chiến lược: dùng Cypher thuần (không cần APOC trên Aura Free tier)
  1. Kết nối Aura Cloud → export tất cả nodes + relationships ra file JSON
  2. Kết nối Neo4j Local → batch MERGE nodes trước, rồi MERGE relationships

Chạy lệnh:
    python migrate_neo4j.py --export          # export từ Aura ra file
    python migrate_neo4j.py --import          # import file vào local
    python migrate_neo4j.py --export --import # cả hai bước liên tiếp
"""

import os
import json
import argparse
import time
from pathlib import Path
from neo4j import GraphDatabase

# ── Nguồn: Aura Cloud ─────────────────────────────────────────
CLOUD_URI  = "neo4j+s://bdfc7297.databases.neo4j.io"
CLOUD_USER = "bdfc7297"
CLOUD_PASS = "0WWAdQtwxMMeoPqTT62bTLBb0DOVqlZs3bjNlASrPDs"

# ── Đích: Neo4j Local Docker ───────────────────────────────────
LOCAL_URI  = os.getenv("DB_URL",      "bolt://localhost:7687")
LOCAL_USER = os.getenv("DB_USER",     "neo4j")
LOCAL_PASS = os.getenv("DB_PASSWORD", "Son@110904")

EXPORT_FILE = Path("./cache/neo4j_export.json")
BATCH_SIZE  = 500   # số nodes/rels mỗi transaction


# ══════════════════════════════════════════════════════════════
#  PHASE 1 — EXPORT từ Aura
# ══════════════════════════════════════════════════════════════

NODE_LABELS = ["MAJOR", "SUBJECT", "SKILL", "CAREER", "TEACHER", "PERSONALITY"]

def export_from_cloud():
    print("=" * 60)
    print("[EXPORT] Kết nối Neo4j Aura Cloud...")
    driver = GraphDatabase.driver(CLOUD_URI, auth=(CLOUD_USER, CLOUD_PASS))

    all_nodes = []
    all_rels  = []

    with driver.session() as session:
        # ── Export nodes theo từng label ──────────────────────
        for label in NODE_LABELS:
            print(f"  Exporting {label} nodes...", end=" ", flush=True)
            rows = session.run(f"""
                MATCH (n:{label})
                RETURN id(n) AS neo4j_id,
                       labels(n) AS labels,
                       properties(n) AS props
                ORDER BY id(n)
            """).data()
            all_nodes.extend(rows)
            print(f"{len(rows)} nodes")

        # ── Export relationships ───────────────────────────────
        REL_TYPES = [
            "MAJOR_OFFERS_SUBJECT", "PROVIDES", "TEACH",
            "REQUIRES", "PREREQUISITE_FOR", "LEADS_TO",
            "SUITS_MAJOR", "SUITS_CAREER",
            "REQUIRES_PERSONALITY", "CULTIVATES",
        ]
        for rtype in REL_TYPES:
            print(f"  Exporting :{rtype} relationships...", end=" ", flush=True)
            rows = session.run(f"""
                MATCH (a)-[r:{rtype}]->(b)
                RETURN id(a) AS from_id,
                       id(b) AS to_id,
                       labels(a)[0] AS from_label,
                       labels(b)[0] AS to_label,
                       type(r) AS rel_type,
                       properties(r) AS props,
                       a.code AS from_code,
                       a.name AS from_name,
                       a.personality_key AS from_pkey,
                       a.career_key AS from_ckey,
                       a.teacher_key AS from_tkey,
                       a.skill_key AS from_skey,
                       b.code AS to_code,
                       b.name AS to_name,
                       b.personality_key AS to_pkey,
                       b.career_key AS to_ckey,
                       b.teacher_key AS to_tkey,
                       b.skill_key AS to_skey
            """).data()
            all_rels.extend(rows)
            print(f"{len(rows)} rels")

    driver.close()

    EXPORT_FILE.parent.mkdir(parents=True, exist_ok=True)
    export_data = {"nodes": all_nodes, "relationships": all_rels}
    with open(EXPORT_FILE, "w", encoding="utf-8") as f:
        json.dump(export_data, f, ensure_ascii=False, indent=2)

    print(f"\n✅ Exported {len(all_nodes)} nodes + {len(all_rels)} rels → {EXPORT_FILE}")
    return export_data


# ══════════════════════════════════════════════════════════════
#  PHASE 2 — IMPORT vào Neo4j Local
# ══════════════════════════════════════════════════════════════

def _merge_key(label: str, props: dict) -> str | None:
    """Trả về Cypher MERGE key cho từng label."""
    if label == "MAJOR"       and props.get("code"):        return f"code: '{_esc(props['code'])}'"
    if label == "SUBJECT"     and props.get("code"):        return f"code: '{_esc(props['code'])}'"
    if label == "PERSONALITY" and props.get("personality_key"): return f"personality_key: '{_esc(props['personality_key'])}'"
    if label == "CAREER"      and props.get("name"):        return f"name: '{_esc(props['name'])}'"
    if label == "SKILL"       and props.get("skill_key"):   return f"skill_key: '{_esc(props['skill_key'])}'"
    if label == "SKILL"       and props.get("name"):        return f"name: '{_esc(props['name'])}'"
    if label == "TEACHER"     and props.get("teacher_key"): return f"teacher_key: '{_esc(props['teacher_key'])}'"
    if label == "TEACHER"     and props.get("name"):        return f"name: '{_esc(props['name'])}'"
    return None


def _esc(v: str) -> str:
    return str(v).replace("\\", "\\\\").replace("'", "\\'")


def _props_to_cypher(props: dict, skip_keys: set) -> str:
    """Serialize dict thành Cypher SET clauses."""
    parts = []
    for k, v in props.items():
        if k in skip_keys or v is None:
            continue
        if isinstance(v, bool):
            parts.append(f"n.{k} = {str(v).lower()}")
        elif isinstance(v, (int, float)):
            parts.append(f"n.{k} = {v}")
        elif isinstance(v, list):
            # Neo4j list property
            items = ", ".join(
                f"'{_esc(str(i))}'" if isinstance(i, str) else str(i)
                for i in v
            )
            parts.append(f"n.{k} = [{items}]")
        else:
            parts.append(f"n.{k} = '{_esc(str(v))}'")
    return ", ".join(parts)


def import_to_local(export_data: dict | None = None):
    if export_data is None:
        print(f"[IMPORT] Đọc file export: {EXPORT_FILE}")
        with open(EXPORT_FILE, encoding="utf-8") as f:
            export_data = json.load(f)

    all_nodes = export_data["nodes"]
    all_rels  = export_data["relationships"]

    print(f"[IMPORT] Kết nối Neo4j Local: {LOCAL_URI}")
    driver = GraphDatabase.driver(LOCAL_URI, auth=(LOCAL_USER, LOCAL_PASS))

    # ── Tạo constraints ───────────────────────────────────────
    with driver.session() as session:
        constraints = [
            "CREATE CONSTRAINT IF NOT EXISTS FOR (n:MAJOR)       REQUIRE n.code IS UNIQUE",
            "CREATE CONSTRAINT IF NOT EXISTS FOR (n:SUBJECT)     REQUIRE n.code IS UNIQUE",
            "CREATE CONSTRAINT IF NOT EXISTS FOR (n:PERSONALITY) REQUIRE n.personality_key IS UNIQUE",
            "CREATE INDEX IF NOT EXISTS FOR (n:CAREER)      ON (n.name)",
            "CREATE INDEX IF NOT EXISTS FOR (n:SKILL)       ON (n.skill_key)",
            "CREATE INDEX IF NOT EXISTS FOR (n:TEACHER)     ON (n.teacher_key)",
        ]
        for stmt in constraints:
            session.run(stmt)
        print(f"  Constraints/indexes OK")

    # ── Import nodes theo batch ───────────────────────────────
    ok = fail = 0
    print(f"[IMPORT] Importing {len(all_nodes)} nodes (batch={BATCH_SIZE})...")

    for i in range(0, len(all_nodes), BATCH_SIZE):
        batch = all_nodes[i:i + BATCH_SIZE]
        with driver.session() as session:
            def tx_nodes(tx, b=batch):
                nonlocal ok, fail
                for row in b:
                    labels = row.get("labels", [])
                    props  = row.get("props", {})
                    label  = labels[0] if labels else None
                    if not label:
                        continue
                    key_clause = _merge_key(label, props)
                    if not key_clause:
                        fail += 1
                        continue
                    # Tìm skip_keys (đã có trong MERGE clause)
                    skip_keys = set()
                    for kv in key_clause.split(","):
                        skip_keys.add(kv.strip().split(":")[0].strip())

                    set_clause = _props_to_cypher(props, skip_keys)
                    stmt = f"MERGE (n:{label} {{{key_clause}}})"
                    if set_clause:
                        stmt += f" SET {set_clause}"
                    try:
                        tx.run(stmt)
                        ok += 1
                    except Exception as e:
                        fail += 1
            session.execute_write(tx_nodes)
        print(f"  Nodes batch {i//BATCH_SIZE + 1}: {min(i+BATCH_SIZE, len(all_nodes))}/{len(all_nodes)}")

    print(f"  Nodes: ✓{ok}  ✗{fail}")

    # ── Import relationships theo batch ───────────────────────
    ok2 = fail2 = 0
    print(f"[IMPORT] Importing {len(all_rels)} relationships (batch={BATCH_SIZE})...")

    def _node_match(label: str, code, name, pkey, ckey, tkey, skey) -> str | None:
        """Trả về Cypher MATCH clause tìm node theo identity key."""
        if label == "MAJOR"   and code:  return f"(a:MAJOR {{code: '{_esc(code)}'}})"
        if label == "SUBJECT" and code:  return f"(a:SUBJECT {{code: '{_esc(code)}'}})"
        if label == "PERSONALITY" and pkey: return f"(a:PERSONALITY {{personality_key: '{_esc(pkey)}'}})"
        if label == "CAREER"  and name:  return f"(a:CAREER {{name: '{_esc(name)}'}})"
        if label == "SKILL"   and skey:  return f"(a:SKILL {{skill_key: '{_esc(skey)}'}})"
        if label == "SKILL"   and name:  return f"(a:SKILL {{name: '{_esc(name)}'}})"
        if label == "TEACHER" and tkey:  return f"(a:TEACHER {{teacher_key: '{_esc(tkey)}'}})"
        if label == "TEACHER" and name:  return f"(a:TEACHER {{name: '{_esc(name)}'}})"
        return None

    for i in range(0, len(all_rels), BATCH_SIZE):
        batch = all_rels[i:i + BATCH_SIZE]
        with driver.session() as session:
            def tx_rels(tx, b=batch):
                nonlocal ok2, fail2
                for row in b:
                    fl, tl = row.get("from_label"), row.get("to_label")
                    rtype = row.get("rel_type", "")
                    props = row.get("props", {})

                    from_match = _node_match(
                        fl,
                        row.get("from_code"), row.get("from_name"),
                        row.get("from_pkey"), row.get("from_ckey"),
                        row.get("from_tkey"), row.get("from_skey"),
                    )
                    to_match = _node_match(
                        tl,
                        row.get("to_code"),   row.get("to_name"),
                        row.get("to_pkey"),   row.get("to_ckey"),
                        row.get("to_tkey"),   row.get("to_skey"),
                    )
                    if not from_match or not to_match:
                        fail2 += 1
                        continue

                    # Đổi alias 'a' thành 'b' cho to-node
                    to_match_b = to_match.replace("(a:", "(b:", 1)
                    set_clause = _props_to_cypher(props, set())
                    stmt = (
                        f"MATCH {from_match}\n"
                        f"MATCH {to_match_b}\n"
                        f"MERGE (a)-[r:{rtype}]->(b)"
                    )
                    if set_clause:
                        stmt += f"\nSET {set_clause.replace('n.', 'r.')}"
                    try:
                        tx.run(stmt)
                        ok2 += 1
                    except Exception as e:
                        fail2 += 1
            session.execute_write(tx_rels)
        print(f"  Rels batch {i//BATCH_SIZE + 1}: {min(i+BATCH_SIZE, len(all_rels))}/{len(all_rels)}")

    driver.close()
    print(f"\n✅ Import hoàn tất!")
    print(f"   Nodes:         ✓{ok}  ✗{fail}")
    print(f"   Relationships: ✓{ok2}  ✗{fail2}")


# ══════════════════════════════════════════════════════════════
#  MAIN
# ══════════════════════════════════════════════════════════════

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Migrate Neo4j Aura → Local Docker")
    parser.add_argument("--export", action="store_true", help="Export từ Aura Cloud")
    parser.add_argument("--import", dest="do_import",
                        action="store_true", help="Import vào Neo4j Local")
    args = parser.parse_args()

    export_data = None
    if args.export:
        export_data = export_from_cloud()

    if args.do_import:
        import_to_local(export_data)

    if not args.export and not args.do_import:
        parser.print_help()
