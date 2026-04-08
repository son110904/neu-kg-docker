"""
Script 2 (OPTIMIZED v7): Load extracted KG JSON → generate Cypher TRỰC TIẾP (không dùng LLM)
→ push to Neo4j Aura

Thay đổi so với v6:
  [FIX-1] per_rel_cypher: SUITS_CAREER edge lưu thêm property major_code và group_name
          → script3 dùng để lọc nghề theo ngành khi user hỏi "ENFP + CNTT thì làm gì"

  [FIX-4] per_node_cypher: lưu thêm 3 list property vào PERSONALITY node:
            field_names        — list tên lĩnh vực
            group_names        — list tên nhóm ngành
            major_codes_index  — list mã ngành 7 chữ số phẳng
          → script3 query ngược "tính cách gì hợp làm IT/CNTT"

  [FIX-4] create_indexes: thêm index cho field_names và major_codes_index
"""

import os
import json
import logging
from pathlib import Path
from neo4j import GraphDatabase, exceptions as neo4j_exc
from dotenv import load_dotenv

load_dotenv()

# ─── LOGGING ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("./cache/ingestion.log", encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger(__name__)

# ─── CONFIG ───────────────────────────────────────────────────────────────────
NEO4J_URI      = os.getenv("DB_URL")
NEO4J_USERNAME = os.getenv("DB_USER", "neo4j")
NEO4J_PASSWORD = os.getenv("DB_PASSWORD")

LOCAL_OUT_DIR = Path("./cache/output")
FOLDERS = ["curriculum", "career_description", "syllabus", "personality"]
# ──────────────────────────────────────────────────────────────────────────────


def _s(value) -> str:
    """Safe strip: trả về chuỗi rỗng nếu value là None."""
    if value is None:
        return ""
    return str(value).strip()


def _esc(value) -> str:
    """Escape single quotes cho Cypher string literals. An toàn với None."""
    return _s(value).replace("\\", "\\\\").replace("'", "\\'")


def _json_prop(value) -> str:
    """Serialize giá trị phức tạp (dict/list) thành JSON string để lưu vào Neo4j property."""
    if value is None:
        return ""
    if isinstance(value, (dict, list)):
        return _esc(json.dumps(value, ensure_ascii=False))
    return _esc(str(value))


# ─── SCHEMA DETECTOR ──────────────────────────────────────────────────────────

def detect_schema(kg_data: dict) -> str:
    """
    Tự động nhận dạng loại file từ nội dung JSON.
    Returns: 'curriculum' | 'syllabus' | 'career' | 'personality' | 'unknown'

    Lưu ý: career_description (v4) có thể chứa node MAJOR (từ recommended_majors),
    nên không dùng sự vắng mặt của MAJOR để phân biệt career vs curriculum nữa.
    Thay vào đó ưu tiên nhận diện qua rel_type rõ ràng hơn.
    """
    nodes      = kg_data.get("nodes", [])
    rels       = kg_data.get("relationships", [])
    node_types = {n.get("type", "") for n in nodes}
    rel_types  = {r.get("rel_type", "") for r in rels}

    # personality: chứa node PERSONALITY hoặc relationship suits_major/suits_career
    if (
        "PERSONALITY" in node_types
        or "personality_suits_major"  in rel_types
        or "personality_suits_career" in rel_types
    ):
        return "personality"

    # syllabus: có TEACHER hoặc teacher_instructs_subject
    if "TEACHER" in node_types or "teacher_instructs_subject" in rel_types:
        return "syllabus"

    # curriculum: có major_offers_subject hoặc major_leads_to_career
    if "major_offers_subject" in rel_types or "major_leads_to_career" in rel_types:
        return "curriculum"

    # career_description: có CAREER + career_requires_skill
    if "CAREER" in node_types or "career_requires_skill" in rel_types:
        return "career"

    # fallback: nếu chỉ có MAJOR không rel → curriculum
    if "MAJOR" in node_types:
        return "curriculum"

    return "unknown"



def cur_node_cypher(node: dict) -> str | None:
    """CUR: MAJOR | SUBJECT | CAREER"""
    t = node.get("type", "")

    if t == "MAJOR":
        code    = _esc(node.get("major_code"))
        name_vi = _esc(node.get("major_name_vi"))
        if not code:
            return None

        sets = [
            f"n.name = '{name_vi}'",
            f"n.name_vi = '{name_vi}'",
        ]

        extended_fields = [
            ("philosophy_and_objectives",              "philosophy_and_objectives"),
            ("admission_requirements",                 "admission_requirements"),
            ("learning_outcomes",                      "learning_outcomes"),
            ("po_plo_matrix",                          "po_plo_matrix"),
            ("training_process_and_graduation_conditions", "training_process_and_graduation_conditions"),
            ("curriculum_structure_and_content",       "curriculum_structure_and_content"),
            ("teaching_and_assessment_methods",        "teaching_and_assessment_methods"),
            ("reference_programs",                     "reference_programs"),
            ("lecturer_and_teaching_assistant_standards", "lecturer_and_teaching_assistant_standards"),
            ("facilities_and_learning_resources",      "facilities_and_learning_resources"),
        ]
        for node_field, prop_name in extended_fields:
            val = node.get(node_field)
            if val is not None:
                sets.append(f"n.{prop_name} = '{_json_prop(val)}'")

        return (
            f"MERGE (n:MAJOR {{code: '{code}'}})"
            f" SET {', '.join(sets)}"
        )

    if t == "SUBJECT":
        code    = _esc(node.get("subject_code"))
        name_vi = _esc(node.get("subject_name_vi"))
        if not code:
            return None

        sets = [
            f"n.name = '{name_vi}'",
            f"n.name_vi = '{name_vi}'",
        ]

        credits = node.get("credits")
        if credits is not None:
            try:
                sets.append(f"n.credits = {int(credits)}")
            except (ValueError, TypeError):
                log.debug(f"  [skip credits] Invalid credits value: {credits}")

        return (
            f"MERGE (n:SUBJECT {{code: '{code}'}})"
            f" SET {', '.join(sets)}"
        )

    if t == "CAREER":
        key     = _esc(node.get("career_key"))
        name_vi = _esc(node.get("career_name_vi"))
        name_en = _esc(node.get("career_name_en"))
        name    = name_vi or name_en
        if not name:
            return None

        stmt = f"MERGE (n:CAREER {{name: '{name}'}})"
        sets = []
        if key:     sets.append(f"n.career_key = '{key}'")
        if name_vi: sets.append(f"n.name_vi = '{name_vi}'")
        if name_en: sets.append(f"n.name_en = '{name_en}'")

        if sets:
            stmt += " SET " + ", ".join(sets)
        return stmt

    return None


def cur_rel_cypher(rel: dict) -> str | None:
    """CUR: major_offers_subject | major_leads_to_career"""
    rtype = rel.get("rel_type", "")

    # ── MAJOR -[:MAJOR_OFFERS_SUBJECT]-> SUBJECT ──────────────────────────────
    if rtype == "major_offers_subject":
        major_code   = _esc(rel.get("from_major_code"))
        subject_code = _esc(rel.get("to_subject_code"))
        semester     = rel.get("semester")
        req_type     = _esc(rel.get("required_type"))
        if not major_code or not subject_code:
            return None
        if semester is not None and req_type:
            try:
                semester_int = int(semester)
                return (
                    f"MATCH (a:MAJOR {{code: '{major_code}'}}), (b:SUBJECT {{code: '{subject_code}'}})"
                    f" MERGE (a)-[:MAJOR_OFFERS_SUBJECT {{semester: {semester_int}, required_type: '{req_type}'}}]->(b)"
                )
            except (ValueError, TypeError):
                pass
        return (
            f"MATCH (a:MAJOR {{code: '{major_code}'}}), (b:SUBJECT {{code: '{subject_code}'}})"
            f" MERGE (a)-[:MAJOR_OFFERS_SUBJECT]->(b)"
        )

    # ── MAJOR -[:LEADS_TO]-> CAREER ───────────────────────────────────────────
    if rtype == "major_leads_to_career":
        major_code  = _esc(rel.get("from_major_code"))
        career_key  = _esc(rel.get("to_career_key"))
        if not major_code or not career_key:
            return None
        return (
            f"MATCH (a:MAJOR {{code: '{major_code}'}}), (b:CAREER {{career_key: '{career_key}'}})"
            f" MERGE (a)-[:LEADS_TO]->(b)"
        )

    return None


# ══════════════════════════════
#  SYLLABUS
# ══════════════════════════════

def syl_node_cypher(node: dict) -> str | None:
    """SYL: SUBJECT | TEACHER | SKILL"""
    t = node.get("type", "")

    if t == "SUBJECT":
        code    = _esc(node.get("subject_code"))
        name_vi = _esc(node.get("subject_name_vi"))
        if not code:
            return None

        sets = [
            f"n.name = '{name_vi}'",
            f"n.name_vi = '{name_vi}'",
        ]

        simple_fields = [
            "course_description",
            "learning_resources",
            "courses_goals",
            "assessment",
            "course_requirements_and_expectations",
            "syllabus_adjustment_time",
        ]
        for field in simple_fields:
            val = node.get(field)
            if val is not None:
                sets.append(f"n.{field} = '{_json_prop(val)}'")

        # lesson_plan: week_1, week_2, ... (mỗi tuần là 1 property)
        for key, val in node.items():
            if key.startswith("week_") and val is not None:
                sets.append(f"n.{key} = '{_json_prop(val)}'")

        return (
            f"MERGE (n:SUBJECT {{code: '{code}'}})"
            f" SET {', '.join(sets)}"
        )

    if t == "TEACHER":
        name  = _esc(node.get("name"))
        email = _esc(node.get("email"))
        title = _esc(node.get("title"))
        key   = _esc(node.get("teacher_key"))
        if not name:
            return None
        stmt = f"MERGE (n:TEACHER {{name: '{name}'}})"
        sets = []
        if email: sets.append(f"n.email = '{email}'")
        if title: sets.append(f"n.title = '{title}'")
        if key:   sets.append(f"n.teacher_key = '{key}'")
        if sets:
            stmt += " SET " + ", ".join(sets)
        return stmt

    if t == "SKILL":
        key        = _esc(node.get("skill_key"))
        name       = _esc(node.get("skill_name"))
        skill_type = _esc(node.get("skill_type"))
        clo_code   = _esc(node.get("clo_code"))
        if not key and not name:
            return None
        merge_key  = key if key else name
        merge_prop = "skill_key" if key else "name"
        stmt = f"MERGE (n:SKILL {{{merge_prop}: '{merge_key}'}})"
        sets = []
        if name:       sets.append(f"n.name = '{name}'")
        if key:        sets.append(f"n.skill_key = '{key}'")
        if skill_type: sets.append(f"n.skill_type = '{skill_type}'")
        if clo_code:   sets.append(f"n.clo_code = '{clo_code}'")
        if sets:
            stmt += " SET " + ", ".join(sets)
        return stmt

    return None


def syl_rel_cypher(rel: dict) -> str | None:
    """
    SYL:
      teacher_instructs_subject → TEACHER -[:TEACH]-> SUBJECT
      subject_provides_skill    → SUBJECT -[:PROVIDES]-> SKILL
      subject_is_prerequisite_of_subject → SUBJECT -[:PREREQUISITE_FOR]-> SUBJECT
    """
    rtype = rel.get("rel_type", "")

    # ── TEACHER -[:TEACH]-> SUBJECT ───────────────────────────────────────────
    if rtype == "teacher_instructs_subject":
        teacher_key  = _esc(rel.get("from_teacher_key"))
        subject_code = _esc(rel.get("to_subject_code"))
        if not teacher_key or not subject_code:
            return None
        return (
            f"MATCH (a:TEACHER {{teacher_key: '{teacher_key}'}}), (b:SUBJECT {{code: '{subject_code}'}})"
            f" MERGE (a)-[:TEACH]->(b)"
        )

    # ── SUBJECT -[:PROVIDES]-> SKILL ──────────────────────────────────────────
    if rtype == "subject_provides_skill":
        subject_code = _esc(rel.get("from_subject_code"))
        skill_key    = _esc(rel.get("to_skill_key"))
        mastery      = _esc(rel.get("mastery_level"))
        if not subject_code or not skill_key:
            return None
        if mastery:
            return (
                f"MATCH (a:SUBJECT {{code: '{subject_code}'}}), (b:SKILL {{skill_key: '{skill_key}'}})"
                f" MERGE (a)-[:PROVIDES {{mastery_level: '{mastery}'}}]->(b)"
            )
        return (
            f"MATCH (a:SUBJECT {{code: '{subject_code}'}}), (b:SKILL {{skill_key: '{skill_key}'}})"
            f" MERGE (a)-[:PROVIDES]->(b)"
        )

    # ── SUBJECT -[:PREREQUISITE_FOR]-> SUBJECT ────────────────────────────────
    if rtype == "subject_is_prerequisite_of_subject":
        from_code = _esc(rel.get("from_subject_code"))
        to_code   = _esc(rel.get("to_subject_code"))
        if not from_code or not to_code:
            return None
        return (
            f"MATCH (a:SUBJECT {{code: '{from_code}'}}), (b:SUBJECT {{code: '{to_code}'}})"
            f" MERGE (a)-[:PREREQUISITE_FOR]->(b)"
        )

    return None


# ══════════════════════════════
#  CAREER DESCRIPTION
# ══════════════════════════════

def car_node_cypher(node: dict) -> str | None:
    """CAR: CAREER | SKILL | MAJOR (recommended_majors từ script1 v2)"""
    t = node.get("type", "")

    if t == "CAREER":
        key      = _esc(node.get("career_key"))
        name_vi  = _esc(node.get("career_name_vi"))
        name_en  = _esc(node.get("career_name_en"))
        field    = _esc(node.get("field_name"))
        majors   = node.get("major_codes", [])
        name     = name_vi or name_en
        if not name:
            return None

        stmt = f"MERGE (n:CAREER {{name: '{name}'}})"
        sets = []
        if key:     sets.append(f"n.career_key = '{key}'")
        if name_vi: sets.append(f"n.name_vi = '{name_vi}'")
        if name_en: sets.append(f"n.name_en = '{name_en}'")
        if field:   sets.append(f"n.field_name = '{field}'")
        if majors:
            codes_lit = "[" + ", ".join(f"'{_esc(str(c))}'" for c in majors) + "]"
            sets.append(f"n.major_codes = {codes_lit}")

        extended_fields = [
            "description",
            "job_tasks",
            "education_certification",
            "market",
        ]
        for field_name in extended_fields:
            val = node.get(field_name)
            if val is not None:
                sets.append(f"n.{field_name} = '{_json_prop(val)}'")

        if sets:
            stmt += " SET " + ", ".join(sets)
        return stmt

    if t == "SKILL":
        key        = _esc(node.get("skill_key"))
        name       = _esc(node.get("skill_name"))
        skill_type = _esc(node.get("skill_type"))
        if not key and not name:
            return None
        merge_key  = key if key else name
        merge_prop = "skill_key" if key else "name"
        stmt = f"MERGE (n:SKILL {{{merge_prop}: '{merge_key}'}})"
        sets = []
        if name:       sets.append(f"n.name = '{name}'")
        if key:        sets.append(f"n.skill_key = '{key}'")
        if skill_type: sets.append(f"n.skill_type = '{skill_type}'")
        if sets:
            stmt += " SET " + ", ".join(sets)
        return stmt

    # MAJOR từ recommended_majors — chỉ có tên, chưa có code
    if t == "MAJOR":
        name_vi = _esc(node.get("major_name_vi"))
        code    = _esc(node.get("major_code"))
        name    = name_vi
        if not name:
            return None
        stmt = f"MERGE (n:MAJOR {{name: '{name}'}})"
        sets = []
        if name_vi: sets.append(f"n.name_vi = '{name_vi}'")
        if code:    sets.append(f"n.code = '{code}'")
        if sets:
            stmt += " SET " + ", ".join(sets)
        return stmt

    return None


def car_rel_cypher(rel: dict) -> str | None:
    """
    CAR:
      career_requires_skill  → CAREER -[:REQUIRES]-> SKILL
      major_leads_to_career  → MAJOR  -[:LEADS_TO]-> CAREER  (nếu có major_code từ Phase 2)
    """
    rtype = rel.get("rel_type", "")

    # ── CAREER -[:REQUIRES]-> SKILL ───────────────────────────────────────────
    if rtype == "career_requires_skill":
        career_key = _esc(rel.get("from_career_key"))
        skill_key  = _esc(rel.get("to_skill_key"))
        req_level  = _esc(rel.get("required_level"))
        if not career_key or not skill_key:
            return None
        if req_level:
            return (
                f"MATCH (a:CAREER {{career_key: '{career_key}'}}), (b:SKILL {{skill_key: '{skill_key}'}})"
                f" MERGE (a)-[:REQUIRES {{required_level: '{req_level}'}}]->(b)"
            )
        return (
            f"MATCH (a:CAREER {{career_key: '{career_key}'}}), (b:SKILL {{skill_key: '{skill_key}'}})"
            f" MERGE (a)-[:REQUIRES]->(b)"
        )

    # ── MAJOR -[:LEADS_TO]-> CAREER (từ recommended_majors nếu có code) ───────
    if rtype == "major_leads_to_career":
        major_code = _esc(rel.get("from_major_code"))
        career_key = _esc(rel.get("to_career_key"))
        if not major_code or not career_key:
            return None
        return (
            f"MATCH (a:MAJOR {{code: '{major_code}'}}), (b:CAREER {{career_key: '{career_key}'}})"
            f" MERGE (a)-[:LEADS_TO]->(b)"
        )

    return None


# ══════════════════════════════════════════════════════════════════════════════
#  PERSONALITY  (MBTI-based v7 — fixed)
# ══════════════════════════════════════════════════════════════════════════════

def per_node_cypher(node: dict) -> str | None:
    """
    PER: PERSONALITY — schema MBTI-based v7.
    MERGE theo personality_key (= MBTI code, e.g. "ESTP").

    [FIX-4] Thêm 3 list property mới so với v6:
      - field_names        (list[str]) — tên các lĩnh vực trong suitable_fields
      - group_names        (list[str]) — tên các nhóm ngành
      - major_codes_index  (list[str]) — mã ngành 7 chữ số phẳng
    Dùng để script3 query "tính cách gì hợp làm CNTT/IT" qua Phase 1c fallback.
    """
    t = node.get("type", "")
    if t != "PERSONALITY":
        return None

    key = _esc(node.get("personality_key") or node.get("code"))
    if not key:
        return None

    stmt = f"MERGE (n:PERSONALITY {{personality_key: '{key}'}})"
    sets = [
        f"n.name = '{key}'",
        f"n.code = '{key}'",
    ]

    desc = node.get("description")
    if desc:
        sets.append(f"n.description = '{_esc(desc)}'")

    structure = node.get("structure")
    if structure:
        sets.append(f"n.structure = '{_json_prop(structure)}'")

    strengths = node.get("strengths")
    if strengths:
        sets.append(f"n.strengths = '{_json_prop(strengths)}'")

    weaknesses = node.get("weaknesses")
    if weaknesses:
        sets.append(f"n.weaknesses = '{_json_prop(weaknesses)}'")

    work_env = node.get("work_environment")
    if work_env:
        sets.append(f"n.work_environment = '{_esc(work_env)}'")

    # suitable_fields: lưu nguyên JSON để script3 parse khi cần
    suitable = node.get("suitable_fields")
    if suitable:
        sets.append(f"n.suitable_fields = '{_json_prop(suitable)}'")

    # ── [FIX-4] 3 list property mới — lưu dạng Neo4j native list ─────────────
    field_names = node.get("field_names", [])
    if field_names:
        items = ", ".join(f"'{_esc(str(fn))}'" for fn in field_names)
        sets.append(f"n.field_names = [{items}]")

    group_names = node.get("group_names", [])
    if group_names:
        items = ", ".join(f"'{_esc(str(gn))}'" for gn in group_names)
        sets.append(f"n.group_names = [{items}]")

    major_codes_index = node.get("major_codes_index", [])
    if major_codes_index:
        items = ", ".join(f"'{_esc(str(mc))}'" for mc in major_codes_index)
        sets.append(f"n.major_codes_index = [{items}]")

    stmt += " SET " + ", ".join(sets)
    return stmt


def per_rel_cypher(rel: dict) -> str | None:
    """
    PER relationships (v7 — fixed):
      personality_suits_major  → PERSONALITY -[:SUITS_MAJOR]->  MAJOR
      personality_suits_career → PERSONALITY -[:SUITS_CAREER]-> CAREER

    [FIX-1] SUITS_CAREER edge lưu thêm property major_code và group_name
            → script3 dùng để lọc nghề theo ngành khi user đề cập cả MBTI lẫn ngành học.
    """
    rtype = rel.get("rel_type", "")

    # ── PERSONALITY -[:SUITS_MAJOR]-> MAJOR ─────────────────── (không đổi) ──
    if rtype == "personality_suits_major":
        pkey       = _esc(rel.get("from_personality_key"))
        major_code = _esc(rel.get("to_major_code"))
        if not pkey or not major_code:
            return None
        field_name = _esc(rel.get("field_name", ""))
        group_name = _esc(rel.get("group_name", ""))
        props = []
        if field_name: props.append(f"field_name: '{field_name}'")
        if group_name: props.append(f"group_name: '{group_name}'")
        props_str = "{" + ", ".join(props) + "}" if props else ""
        return (
            f"MATCH (a:PERSONALITY {{personality_key: '{pkey}'}}), "
            f"(b:MAJOR {{code: '{major_code}'}})"
            f" MERGE (a)-[:SUITS_MAJOR{' ' + props_str if props_str else ''}]->(b)"
        )

    # ── PERSONALITY -[:SUITS_CAREER]-> CAREER ──────────────────── [FIX-1] ───
    if rtype == "personality_suits_career":
        pkey        = _esc(rel.get("from_personality_key"))
        career_name = _esc(rel.get("to_career_name"))
        if not pkey or not career_name:
            return None
        major_name  = _esc(rel.get("major_name", ""))
        field_name  = _esc(rel.get("field_name", ""))
        group_name  = _esc(rel.get("group_name", ""))
        # ── [FIX-1] major_code mới — từ script1 patch ────────────────────────
        major_code  = _esc(rel.get("major_code", ""))

        # CAREER node: MERGE by name (tạo mới nếu chưa có)
        props = []
        if major_name:  props.append(f"major_name: '{major_name}'")
        if field_name:  props.append(f"field_name: '{field_name}'")
        if group_name:  props.append(f"group_name: '{group_name}'")
        if major_code:  props.append(f"major_code: '{major_code}'")   # THÊM MỚI
        props_str = "{" + ", ".join(props) + "}" if props else ""
        return (
            f"MERGE (b:CAREER {{name: '{career_name}'}})"
            f" WITH b"
            f" MATCH (a:PERSONALITY {{personality_key: '{pkey}'}})"
            f" MERGE (a)-[:SUITS_CAREER{' ' + props_str if props_str else ''}]->(b)"
        )

    return None


# ─── DISPATCH ─────────────────────────────────────────────────────────────────

NODE_BUILDERS = {
    "curriculum":  cur_node_cypher,
    "syllabus":    syl_node_cypher,
    "career":      car_node_cypher,
    "personality": per_node_cypher,
}

REL_BUILDERS = {
    "curriculum":  cur_rel_cypher,
    "syllabus":    syl_rel_cypher,
    "career":      car_rel_cypher,
    "personality": per_rel_cypher,
}


def kg_to_cypher_statements(kg_data: dict, schema: str) -> list[str]:
    """Chuyển KG JSON → list Cypher statements theo schema đã detect."""
    node_fn = NODE_BUILDERS.get(schema)
    rel_fn  = REL_BUILDERS.get(schema)

    if not node_fn:
        log.warning(f"  Schema '{schema}' không có builder, bỏ qua.")
        return []

    statements = []

    for node in kg_data.get("nodes", []):
        stmt = node_fn(node)
        if stmt:
            statements.append(stmt)
        else:
            log.debug(f"  [skip node] {node.get('type')} – thiếu key bắt buộc")

    for rel in kg_data.get("relationships", []):
        stmt = rel_fn(rel)
        if stmt:
            statements.append(stmt)
        else:
            log.debug(f"  [skip rel] {rel.get('rel_type')} – thiếu key bắt buộc")

    return statements


# ─── NEO4J ────────────────────────────────────────────────────────────────────

def get_driver():
    return GraphDatabase.driver(
        NEO4J_URI,
        auth=(NEO4J_USERNAME, NEO4J_PASSWORD),
        max_connection_pool_size=50,
        connection_timeout=30,
    )


def create_indexes(session):
    """
    [FIX-4] Thêm index cho PERSONALITY.field_names và PERSONALITY.major_codes_index.
    Neo4j v5+ hỗ trợ index trên array property.
    Nếu dùng Neo4j < v5, 2 index cuối sẽ bị skip (warning) nhưng không làm crash —
    ANY() query vẫn chạy được, chỉ chậm hơn một chút.
    """
    stmts = [
        # Unique constraints
        "CREATE CONSTRAINT IF NOT EXISTS FOR (n:MAJOR)       REQUIRE n.code IS UNIQUE",
        "CREATE CONSTRAINT IF NOT EXISTS FOR (n:SUBJECT)     REQUIRE n.code IS UNIQUE",
        "CREATE CONSTRAINT IF NOT EXISTS FOR (n:PERSONALITY) REQUIRE n.personality_key IS UNIQUE",
        # Node indexes (giữ nguyên từ v6)
        "CREATE INDEX IF NOT EXISTS FOR (n:SKILL)       ON (n.name)",
        "CREATE INDEX IF NOT EXISTS FOR (n:SKILL)       ON (n.skill_key)",
        "CREATE INDEX IF NOT EXISTS FOR (n:CAREER)      ON (n.name)",
        "CREATE INDEX IF NOT EXISTS FOR (n:CAREER)      ON (n.career_key)",
        "CREATE INDEX IF NOT EXISTS FOR (n:TEACHER)     ON (n.name)",
        "CREATE INDEX IF NOT EXISTS FOR (n:TEACHER)     ON (n.teacher_key)",
        "CREATE INDEX IF NOT EXISTS FOR (n:MAJOR)       ON (n.name)",
        "CREATE INDEX IF NOT EXISTS FOR (n:SUBJECT)     ON (n.name)",
        "CREATE INDEX IF NOT EXISTS FOR (n:PERSONALITY) ON (n.name)",
        "CREATE INDEX IF NOT EXISTS FOR (n:PERSONALITY) ON (n.code)",
        # ── [FIX-4] Index mới cho PERSONALITY list properties ─────────────────
        "CREATE INDEX IF NOT EXISTS FOR (n:PERSONALITY) ON (n.major_codes_index)",
        "CREATE INDEX IF NOT EXISTS FOR (n:PERSONALITY) ON (n.field_names)",
    ]
    for stmt in stmts:
        try:
            session.run(stmt)
        except Exception as e:
            log.warning(f"[Index] {e}")
    log.info("Indexes/constraints ready.")


def run_statements_in_tx(session, statements: list[str], label: str) -> tuple[int, int]:
    """Chạy tất cả statements trong 1 write transaction."""
    if not statements:
        return 0, 0

    ok = fail = 0

    def tx_func(tx):
        nonlocal ok, fail
        for stmt in statements:
            try:
                tx.run(stmt)
                ok += 1
            except neo4j_exc.CypherSyntaxError as e:
                fail += 1
                log.error(f"  [CypherError] {e.message}\n  → {stmt[:300]}")
            except neo4j_exc.ConstraintError:
                ok += 1  # MERGE race condition, bỏ qua
            except Exception as e:
                fail += 1
                log.error(f"  [StmtError] {e}\n  → {stmt[:300]}")

    try:
        session.execute_write(tx_func)
    except Exception as e:
        log.error(f"  [TX FAILED] {label}: {e}")
        return 0, len(statements)

    return ok, fail


# ─── PROCESS FILES ────────────────────────────────────────────────────────────

def process_files(driver):
    with driver.session() as session:
        log.info("Creating indexes / constraints...")
        create_indexes(session)

        total_ok = total_fail = total_files = 0

        for folder in FOLDERS:
            folder_path = LOCAL_OUT_DIR / folder
            if not folder_path.exists():
                log.warning(f"Folder không tồn tại: {folder_path}")
                continue

            files = sorted(folder_path.glob("*.json"))
            if not files:
                log.warning(f"Không có file JSON trong {folder_path}")
                continue

            log.info(f"\n{'='*60}")
            log.info(f"Folder: {folder} ({len(files)} files)")

            for jf in files:
                total_files += 1
                log.info(f"  → {jf.name}")

                try:
                    with open(jf, encoding="utf-8") as f:
                        kg_data = json.load(f)
                except json.JSONDecodeError as e:
                    log.error(f"    JSON parse error: {e}")
                    total_fail += 1
                    continue

                schema = detect_schema(kg_data)
                log.info(f"    Detected schema: {schema}")

                if schema == "unknown":
                    log.warning(f"    Không nhận dạng được schema, bỏ qua.")
                    continue

                statements = kg_to_cypher_statements(kg_data, schema)
                log.info(f"    Generated {len(statements)} statements")

                if not statements:
                    log.warning(f"    Không có statements nào, bỏ qua.")
                    continue

                ok, fail = run_statements_in_tx(session, statements, jf.name)
                total_ok   += ok
                total_fail += fail
                log.info(f"     {ok}   {fail}")

        log.info(f"\n{'='*60}")
        log.info(f"TỔNG KẾT: {total_files} files |  {total_ok} statements |  {total_fail} lỗi")


# ─── MAIN ─────────────────────────────────────────────────────────────────────

def main():
    log.info("Starting Neo4j ingestion pipeline (v7 – MBTI personality schema fixed)...")

    if not NEO4J_URI:
        raise ValueError("DB_URL không tìm thấy trong .env")

    driver = get_driver()
    try:
        process_files(driver)
    finally:
        driver.close()

    log.info("\nIngestion complete.")


if __name__ == "__main__":
    main()