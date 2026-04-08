import os
import re
import json
import uuid
import datetime
from pathlib import Path
from collections import defaultdict
from neo4j import GraphDatabase
from openai import OpenAI
from dotenv import load_dotenv
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

load_dotenv()

# ─── CONFIG ───────────────────────────────────────────────────────────────────
NEO4J_URI      = os.getenv("DB_URL")
NEO4J_USERNAME = os.getenv("DB_USER", "neo4j")
NEO4J_PASSWORD = os.getenv("DB_PASSWORD")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
OPENAI_MODEL   = os.getenv("OPENAI_MODEL", "gpt-4o-mini")

MAX_HOPS = int(os.getenv("MAX_HOPS", "3"))
# ──────────────────────────────────────────────────────────────────────────────

# ══════════════════════════════════════════════════════════════════════════════
# CHỈ TIÊU & ĐIỂM CHUẨN TUYỂN SINH 2025 — mapping thủ công từ tài liệu NEU
# Mỗi entry: danh sách chương trình thuộc cùng mã ngành / tên chương trình
# ══════════════════════════════════════════════════════════════════════════════

ADMISSION_DATA: list[dict] = [
    # ── Chương trình Đặc biệt (EP) ─────────────────────────────────────────
    {"so": 1,  "ten_chuong_trinh": "Công nghệ Marketing",              "ma_xet_tuyen": "EP19",      "ma_nganh": "7340115", "ten_nganh": "Marketing",                              "khoa_vien": "Khoa Marketing",                                  "chi_tieu": 50,  "diem_chuan_2025": None},
    {"so": 2,  "ten_chuong_trinh": "Công nghệ Logistics và Quản trị chuỗi cung ứng", "ma_xet_tuyen": "EP20", "ma_nganh": "7460108", "ten_nganh": "Khoa học dữ liệu",            "khoa_vien": "Khoa Khoa học dữ liệu và Trí tuệ nhân tạo",         "chi_tieu": 50,  "diem_chuan_2025": None},
    {"so": 3,  "ten_chuong_trinh": "Kiểm toán nội bộ",                 "ma_xet_tuyen": "EP21",      "ma_nganh": "7340302", "ten_nganh": "Kiểm toán",                              "khoa_vien": "Viện Kế toán - Kiểm toán",                        "chi_tieu": 50,  "diem_chuan_2025": None},
    {"so": 4,  "ten_chuong_trinh": "Kinh tế quốc tế (EP)",             "ma_xet_tuyen": "EP22",      "ma_nganh": "7310106", "ten_nganh": "Kinh tế quốc tế",                        "khoa_vien": "Viện Thương mại và Kinh tế quốc tế",              "chi_tieu": 50,  "diem_chuan_2025": None},
    {"so": 5,  "ten_chuong_trinh": "Kinh tế Y tế",                     "ma_xet_tuyen": "EP24",      "ma_nganh": "7310101", "ten_nganh": "Kinh tế",                                "khoa_vien": "Khoa Kinh tế học",                                "chi_tieu": 40,  "diem_chuan_2025": None},
    {"so": 6,  "ten_chuong_trinh": "Phát triển quốc tế",               "ma_xet_tuyen": "EP25",      "ma_nganh": "7310105", "ten_nganh": "Kinh tế phát triển",                     "khoa_vien": "Khoa Kế hoạch và Phát triển",                     "chi_tieu": 40,  "diem_chuan_2025": None},
    {"so": 7,  "ten_chuong_trinh": "Công nghệ môi trường và phát triển bền vững", "ma_xet_tuyen": "EP26", "ma_nganh": "7310101", "ten_nganh": "Kinh tế",                         "khoa_vien": "Khoa Môi trường, Biến đổi khí hậu và Đô thị",     "chi_tieu": 50,  "diem_chuan_2025": None},
    {"so": 8,  "ten_chuong_trinh": "Quản trị công nghiệp sáng tạo",    "ma_xet_tuyen": "EP27",      "ma_nganh": "7810101", "ten_nganh": "Du lịch",                                "khoa_vien": "Khoa Du lịch và Khách sạn",                       "chi_tieu": 50,  "diem_chuan_2025": None},
    {"so": 9,  "ten_chuong_trinh": "Quản trị nhân lực quốc tế",        "ma_xet_tuyen": "EP28",      "ma_nganh": "7340404", "ten_nganh": "Quản trị nhân lực",                      "khoa_vien": "Khoa Kinh tế và Quản lý nguồn nhân lực",          "chi_tieu": 40,  "diem_chuan_2025": None},
    {"so": 10, "ten_chuong_trinh": "Quản trị rủi ro định lượng",        "ma_xet_tuyen": "EP29",      "ma_nganh": "7310108", "ten_nganh": "Toán kinh tế",                           "khoa_vien": "Khoa Toán kinh tế",                               "chi_tieu": 50,  "diem_chuan_2025": None},
    {"so": 11, "ten_chuong_trinh": "Thẩm định giá (EP)",                "ma_xet_tuyen": "EP31",      "ma_nganh": "7340201", "ten_nganh": "Tài chính Ngân hàng",                    "khoa_vien": "Viện Ngân hàng - Tài chính",                      "chi_tieu": 50,  "diem_chuan_2025": None},
    {"so": 12, "ten_chuong_trinh": "Thống kê và Trí tuệ kinh doanh",   "ma_xet_tuyen": "EP32",      "ma_nganh": "7310107", "ten_nganh": "Thống kê kinh tế",                       "khoa_vien": "Khoa Thống kê",                                   "chi_tieu": 50,  "diem_chuan_2025": None},
    {"so": 13, "ten_chuong_trinh": "Kinh tế số (dự kiến)",              "ma_xet_tuyen": "EP23",      "ma_nganh": "7310109", "ten_nganh": "Kinh tế số",                             "khoa_vien": "Khoa Hệ thống thông tin quản lý",                 "chi_tieu": 50,  "diem_chuan_2025": None},
    {"so": 14, "ten_chuong_trinh": "Toán ứng dụng (dự kiến)",           "ma_xet_tuyen": "EP30",      "ma_nganh": "7460112", "ten_nganh": "Toán ứng dụng",                          "khoa_vien": "Khoa Khoa học Cơ sở",                             "chi_tieu": 50,  "diem_chuan_2025": None},
    {"so": 15, "ten_chuong_trinh": "Công nghệ tài chính (dự kiến)",     "ma_xet_tuyen": "7340205",   "ma_nganh": "7340205", "ten_nganh": "Công nghệ tài chính",                    "khoa_vien": "Viện Ngân hàng - Tài chính",                      "chi_tieu": 50,  "diem_chuan_2025": None},
    # ── POHE ───────────────────────────────────────────────────────────────
    {"so": 16, "ten_chuong_trinh": "Quản trị khách sạn (POHE)",         "ma_xet_tuyen": "POHE1",     "ma_nganh": "7810201", "ten_nganh": "Quản trị khách sạn",                     "khoa_vien": "Khoa Du lịch và Khách sạn",                       "chi_tieu": 50,  "diem_chuan_2025": 25.61},
    {"so": 17, "ten_chuong_trinh": "Quản trị lữ hành (POHE)",           "ma_xet_tuyen": "POHE2",     "ma_nganh": "7810103", "ten_nganh": "Quản trị dịch vụ du lịch và lữ hành",    "khoa_vien": "Khoa Du lịch và Khách sạn",                       "chi_tieu": 50,  "diem_chuan_2025": 24.64},
    {"so": 18, "ten_chuong_trinh": "Truyền thông Marketing (POHE)",     "ma_xet_tuyen": "POHE3",     "ma_nganh": "7340115", "ten_nganh": "Marketing",                              "khoa_vien": "Khoa Marketing",                                  "chi_tieu": 60,  "diem_chuan_2025": 27.61},
    {"so": 19, "ten_chuong_trinh": "Luật kinh doanh (POHE)",            "ma_xet_tuyen": "POHE4",     "ma_nganh": "7380107", "ten_nganh": "Luật kinh tế",                           "khoa_vien": "Khoa Luật",                                       "chi_tieu": 50,  "diem_chuan_2025": 25.5},
    {"so": 20, "ten_chuong_trinh": "Quản trị kinh doanh thương mại (POHE)", "ma_xet_tuyen": "POHE5", "ma_nganh": "7340121", "ten_nganh": "Kinh doanh thương mại",                 "khoa_vien": "Viện Thương mại và Kinh tế quốc tế",              "chi_tieu": 50,  "diem_chuan_2025": 26.29},
    {"so": 21, "ten_chuong_trinh": "Quản lý thị trường (POHE)",         "ma_xet_tuyen": "POHE6",     "ma_nganh": "7340121", "ten_nganh": "Kinh doanh thương mại",                  "khoa_vien": "Viện Thương mại và Kinh tế quốc tế",              "chi_tieu": 50,  "diem_chuan_2025": 24.66},
    {"so": 22, "ten_chuong_trinh": "Thẩm định giá (POHE)",              "ma_xet_tuyen": "POHE7",     "ma_nganh": "7340201", "ten_nganh": "Tài chính Ngân hàng",                    "khoa_vien": "Viện Ngân hàng - Tài chính",                      "chi_tieu": 50,  "diem_chuan_2025": 24.55},
    # ── E-BBA / EP01-EP18 ──────────────────────────────────────────────────
    {"so": 23, "ten_chuong_trinh": "Quản trị kinh doanh (E-BBA)",       "ma_xet_tuyen": "EBBA",      "ma_nganh": "7340101", "ten_nganh": "Quản trị kinh doanh",                    "khoa_vien": "Viện Quản trị Kinh doanh",                        "chi_tieu": 110, "diem_chuan_2025": 25.64},
    {"so": 24, "ten_chuong_trinh": "Khởi nghiệp và phát triển kinh doanh (BBAE)", "ma_xet_tuyen": "EP01", "ma_nganh": "7340101", "ten_nganh": "Quản trị kinh doanh",              "khoa_vien": "Viện Đào tạo Quốc tế",                            "chi_tieu": 90,  "diem_chuan_2025": 24.92},
    {"so": 25, "ten_chuong_trinh": "Khoa học tính toán trong Tài chính và Bảo hiểm", "ma_xet_tuyen": "EP02", "ma_nganh": "7310108", "ten_nganh": "Toán kinh tế",               "khoa_vien": "Khoa Toán kinh tế",                               "chi_tieu": 50,  "diem_chuan_2025": 25.5},
    {"so": 26, "ten_chuong_trinh": "Phân tích dữ liệu kinh tế (EDA)",   "ma_xet_tuyen": "EP03",      "ma_nganh": "7310108", "ten_nganh": "Toán kinh tế",                           "khoa_vien": "Khoa Toán kinh tế",                               "chi_tieu": 90,  "diem_chuan_2025": 26.78},
    {"so": 27, "ten_chuong_trinh": "Kế toán tích hợp chứng chỉ quốc tế (ICAEW CFAB)", "ma_xet_tuyen": "EP04", "ma_nganh": "7340301", "ten_nganh": "Kế toán",               "khoa_vien": "Viện Kế toán - Kiểm toán",                        "chi_tieu": 60,  "diem_chuan_2025": 25.9},
    {"so": 28, "ten_chuong_trinh": "Kinh doanh số (E-BDB)",              "ma_xet_tuyen": "EP05",      "ma_nganh": "7340101", "ten_nganh": "Quản trị kinh doanh",                    "khoa_vien": "Viện Quản trị Kinh doanh",                        "chi_tieu": 60,  "diem_chuan_2025": 26.4},
    {"so": 29, "ten_chuong_trinh": "Phân tích kinh doanh (BA)",          "ma_xet_tuyen": "EP06",      "ma_nganh": "7340101", "ten_nganh": "Quản trị kinh doanh",                    "khoa_vien": "Viện Đào tạo Tiên tiến, Chất lượng cao và POHE",  "chi_tieu": 60,  "diem_chuan_2025": 27.5},
    {"so": 30, "ten_chuong_trinh": "Quản trị điều hành thông minh (E-SOM)", "ma_xet_tuyen": "EP07",  "ma_nganh": "7340101", "ten_nganh": "Quản trị kinh doanh",                    "khoa_vien": "Khoa Quản trị kinh doanh",                        "chi_tieu": 70,  "diem_chuan_2025": 25.1},
    {"so": 31, "ten_chuong_trinh": "Quản trị chất lượng và Đổi mới (E-MQI)", "ma_xet_tuyen": "EP08", "ma_nganh": "7340101", "ten_nganh": "Quản trị kinh doanh",                   "khoa_vien": "Khoa Quản trị kinh doanh",                        "chi_tieu": 70,  "diem_chuan_2025": 24.2},
    {"so": 32, "ten_chuong_trinh": "Công nghệ tài chính và Ngân hàng số", "ma_xet_tuyen": "EP09",    "ma_nganh": "7340201", "ten_nganh": "Tài chính Ngân hàng",                    "khoa_vien": "Viện Ngân hàng - Tài chính",                      "chi_tieu": 100, "diem_chuan_2025": 26.29},
    {"so": 33, "ten_chuong_trinh": "Tài chính và Đầu tư (BFI)",          "ma_xet_tuyen": "EP10",      "ma_nganh": "7340201", "ten_nganh": "Tài chính Ngân hàng",                    "khoa_vien": "Viện Ngân hàng - Tài chính",                      "chi_tieu": 100, "diem_chuan_2025": 26.27},
    {"so": 34, "ten_chuong_trinh": "Quản trị khách sạn quốc tế (IHME)", "ma_xet_tuyen": "EP11",      "ma_nganh": "7810201", "ten_nganh": "Quản trị khách sạn",                     "khoa_vien": "Khoa Du lịch và Khách sạn",                       "chi_tieu": 50,  "diem_chuan_2025": 24.25},
    {"so": 35, "ten_chuong_trinh": "Kiểm toán tích hợp chứng chỉ quốc tế (ICAEW CFAB)", "ma_xet_tuyen": "EP12", "ma_nganh": "7340302", "ten_nganh": "Kiểm toán",            "khoa_vien": "Viện Kế toán - Kiểm toán",                        "chi_tieu": 60,  "diem_chuan_2025": 27.25},
    {"so": 36, "ten_chuong_trinh": "Kinh tế học tài chính (FE)",         "ma_xet_tuyen": "EP13",      "ma_nganh": "7310101", "ten_nganh": "Kinh tế",                                "khoa_vien": "Khoa Kinh tế học",                                "chi_tieu": 90,  "diem_chuan_2025": 25.41},
    {"so": 37, "ten_chuong_trinh": "Logistics và Quản lý CCU tích hợp chứng chỉ Logistics quốc tế (LSIC)", "ma_xet_tuyen": "EP14", "ma_nganh": "7510605", "ten_nganh": "Logistics và Quản lý chuỗi cung ứng", "khoa_vien": "Viện Thương mại và Kinh tế quốc tế", "chi_tieu": 100, "diem_chuan_2025": 27.69},
    {"so": 38, "ten_chuong_trinh": "Khoa học dữ liệu (EP15)",            "ma_xet_tuyen": "EP15",      "ma_nganh": "7460108", "ten_nganh": "Khoa học dữ liệu",                       "khoa_vien": "Khoa Khoa học dữ liệu và Trí tuệ nhân tạo",        "chi_tieu": 70,  "diem_chuan_2025": 26.13},
    {"so": 39, "ten_chuong_trinh": "Trí tuệ nhân tạo",                   "ma_xet_tuyen": "EP16",      "ma_nganh": "7480107", "ten_nganh": "Trí tuệ nhân tạo",                       "khoa_vien": "Khoa Khoa học dữ liệu và Trí tuệ nhân tạo",        "chi_tieu": 80,  "diem_chuan_2025": 25.44},
    {"so": 40, "ten_chuong_trinh": "Kỹ thuật phần mềm",                  "ma_xet_tuyen": "EP17",      "ma_nganh": "7480103", "ten_nganh": "Kỹ thuật phần mềm",                      "khoa_vien": "Khoa Công nghệ thông tin",                         "chi_tieu": 50,  "diem_chuan_2025": 24.68},
    {"so": 41, "ten_chuong_trinh": "Quản trị giải trí và sự kiện",       "ma_xet_tuyen": "EP18",      "ma_nganh": "7810101", "ten_nganh": "Du lịch",                                "khoa_vien": "Khoa Du lịch và Khách sạn",                       "chi_tieu": 50,  "diem_chuan_2025": 25.89},
    {"so": 42, "ten_chuong_trinh": "Quản lý công và Chính sách (E-PMP)", "ma_xet_tuyen": "EPMP",      "ma_nganh": "7340403", "ten_nganh": "Quản lý công",                           "khoa_vien": "Khoa Khoa học quản lý",                           "chi_tieu": 70,  "diem_chuan_2025": 23.04},
    # ── Hệ Đại trà / Chính quy ────────────────────────────────────────────
    {"so": 43, "ten_chuong_trinh": "An toàn thông tin",                   "ma_xet_tuyen": "7480202",   "ma_nganh": "7480202", "ten_nganh": "An toàn thông tin",                      "khoa_vien": "Khoa Công nghệ thông tin",                         "chi_tieu": 50,  "diem_chuan_2025": 25.59},
    {"so": 44, "ten_chuong_trinh": "Bảo hiểm",                            "ma_xet_tuyen": "7340204",   "ma_nganh": "7340204", "ten_nganh": "Bảo hiểm",                               "khoa_vien": "Khoa Bảo hiểm",                                   "chi_tieu": 80,  "diem_chuan_2025": 24.75},
    {"so": 45, "ten_chuong_trinh": "Bất động sản",                        "ma_xet_tuyen": "7340116",   "ma_nganh": "7340116", "ten_nganh": "Bất động sản",                           "khoa_vien": "Khoa Bất động sản và Kinh tế Tài nguyên",          "chi_tieu": 70,  "diem_chuan_2025": 25.41},
    {"so": 46, "ten_chuong_trinh": "Công nghệ thông tin",                 "ma_xet_tuyen": "7480201",   "ma_nganh": "7480201", "ten_nganh": "Công nghệ thông tin",                    "khoa_vien": "Khoa Công nghệ thông tin",                         "chi_tieu": 100, "diem_chuan_2025": 25.89},
    {"so": 47, "ten_chuong_trinh": "Hệ thống thông tin",                  "ma_xet_tuyen": "7480104",   "ma_nganh": "7480104", "ten_nganh": "Hệ thống thông tin",                     "khoa_vien": "Khoa Hệ thống thông tin quản lý",                  "chi_tieu": 50,  "diem_chuan_2025": 26.38},
    {"so": 48, "ten_chuong_trinh": "Hệ thống thông tin quản lý",          "ma_xet_tuyen": "7340405",   "ma_nganh": "7340405", "ten_nganh": "Hệ thống thông tin quản lý",             "khoa_vien": "Khoa Hệ thống thông tin quản lý",                  "chi_tieu": 100, "diem_chuan_2025": 27.54},
    {"so": 49, "ten_chuong_trinh": "Kế toán",                             "ma_xet_tuyen": "7340301",   "ma_nganh": "7340301", "ten_nganh": "Kế toán",                                "khoa_vien": "Viện Kế toán - Kiểm toán",                         "chi_tieu": 150, "diem_chuan_2025": 27.1},
    {"so": 50, "ten_chuong_trinh": "Khoa học máy tính",                   "ma_xet_tuyen": "7480101",   "ma_nganh": "7480101", "ten_nganh": "Khoa học máy tính",                      "khoa_vien": "Khoa Công nghệ thông tin",                         "chi_tieu": 50,  "diem_chuan_2025": 26.27},
    {"so": 51, "ten_chuong_trinh": "Khoa học quản lý",                    "ma_xet_tuyen": "7340401",   "ma_nganh": "7340401", "ten_nganh": "Khoa học quản lý",                       "khoa_vien": "Khoa Khoa học quản lý",                            "chi_tieu": 90,  "diem_chuan_2025": 26.06},
    {"so": 52, "ten_chuong_trinh": "Kiểm toán",                           "ma_xet_tuyen": "7340302",   "ma_nganh": "7340302", "ten_nganh": "Kiểm toán",                              "khoa_vien": "Viện Kế toán - Kiểm toán",                         "chi_tieu": 50,  "diem_chuan_2025": 28.38},
    {"so": 53, "ten_chuong_trinh": "Kinh doanh nông nghiệp",              "ma_xet_tuyen": "7620114",   "ma_nganh": "7620114", "ten_nganh": "Kinh doanh nông nghiệp",                 "khoa_vien": "Khoa Bất động sản và Kinh tế Tài nguyên",          "chi_tieu": 50,  "diem_chuan_2025": 23.75},
    {"so": 54, "ten_chuong_trinh": "Kinh doanh quốc tế",                  "ma_xet_tuyen": "7340120",   "ma_nganh": "7340120", "ten_nganh": "Kinh doanh quốc tế",                     "khoa_vien": "Viện Thương mại và Kinh tế quốc tế",               "chi_tieu": 100, "diem_chuan_2025": 28.65},
    {"so": 55, "ten_chuong_trinh": "Kinh doanh thương mại",               "ma_xet_tuyen": "7340121",   "ma_nganh": "7340121", "ten_nganh": "Kinh doanh thương mại",                  "khoa_vien": "Viện Thương mại và Kinh tế quốc tế",               "chi_tieu": 100, "diem_chuan_2025": 28.0},
    {"so": 56, "ten_chuong_trinh": "Kinh tế đầu tư",                      "ma_xet_tuyen": "7310104",   "ma_nganh": "7310104", "ten_nganh": "Kinh tế đầu tư",                         "khoa_vien": "Khoa Đầu tư",                                      "chi_tieu": 100, "diem_chuan_2025": 27.5},
    {"so": 57, "ten_chuong_trinh": "Kinh tế học",                         "ma_xet_tuyen": "7310101_1", "ma_nganh": "7310101", "ten_nganh": "Kinh tế",                                "khoa_vien": "Khoa Kinh tế học",                                 "chi_tieu": 50,  "diem_chuan_2025": 26.52},
    {"so": 58, "ten_chuong_trinh": "Kinh tế nông nghiệp",                 "ma_xet_tuyen": "7620115",   "ma_nganh": "7620115", "ten_nganh": "Kinh tế nông nghiệp",                    "khoa_vien": "Khoa Bất động sản và Kinh tế Tài nguyên",          "chi_tieu": 50,  "diem_chuan_2025": 24.35},
    {"so": 59, "ten_chuong_trinh": "Kinh tế phát triển",                  "ma_xet_tuyen": "7310105",   "ma_nganh": "7310105", "ten_nganh": "Kinh tế phát triển",                     "khoa_vien": "Khoa Kế hoạch và Phát triển",                      "chi_tieu": 80,  "diem_chuan_2025": 26.77},
    {"so": 60, "ten_chuong_trinh": "Kinh tế quốc tế",                     "ma_xet_tuyen": "7310106",   "ma_nganh": "7310106", "ten_nganh": "Kinh tế quốc tế",                        "khoa_vien": "Viện Thương mại và Kinh tế quốc tế",               "chi_tieu": 50,  "diem_chuan_2025": 28.13},
    {"so": 61, "ten_chuong_trinh": "Kinh tế tài nguyên thiên nhiên",      "ma_xet_tuyen": "7850102",   "ma_nganh": "7850102", "ten_nganh": "Kinh tế tài nguyên thiên nhiên",         "khoa_vien": "Khoa Bất động sản và Kinh tế Tài nguyên",          "chi_tieu": 50,  "diem_chuan_2025": 23.5},
    {"so": 62, "ten_chuong_trinh": "Kinh tế và quản lý đô thị",           "ma_xet_tuyen": "7310101_2", "ma_nganh": "7310101", "ten_nganh": "Kinh tế",                                "khoa_vien": "Khoa Môi trường, Biến đổi khí hậu và Đô thị",      "chi_tieu": 50,  "diem_chuan_2025": 25.86},
    {"so": 63, "ten_chuong_trinh": "Kinh tế và quản lý nguồn nhân lực",   "ma_xet_tuyen": "7310101_3", "ma_nganh": "7310101", "ten_nganh": "Kinh tế",                                "khoa_vien": "Khoa Kinh tế và Quản lý nguồn nhân lực",           "chi_tieu": 50,  "diem_chuan_2025": 26.79},
    {"so": 64, "ten_chuong_trinh": "Logistics và Quản lý chuỗi cung ứng", "ma_xet_tuyen": "7510605",   "ma_nganh": "7510605", "ten_nganh": "Logistics và Quản lý chuỗi cung ứng",    "khoa_vien": "Viện Thương mại và Kinh tế quốc tế",               "chi_tieu": 100, "diem_chuan_2025": 28.61},
    {"so": 65, "ten_chuong_trinh": "Luật",                                 "ma_xet_tuyen": "7380101",   "ma_nganh": "7380101", "ten_nganh": "Luật",                                   "khoa_vien": "Khoa Luật",                                        "chi_tieu": 50,  "diem_chuan_2025": 25.96},
    {"so": 66, "ten_chuong_trinh": "Luật kinh tế",                        "ma_xet_tuyen": "7380107",   "ma_nganh": "7380107", "ten_nganh": "Luật kinh tế",                           "khoa_vien": "Khoa Luật",                                        "chi_tieu": 80,  "diem_chuan_2025": 26.75},
    {"so": 67, "ten_chuong_trinh": "Luật thương mại quốc tế",             "ma_xet_tuyen": "7380109",   "ma_nganh": "7380109", "ten_nganh": "Luật thương mại quốc tế",                "khoa_vien": "Khoa Luật",                                        "chi_tieu": 50,  "diem_chuan_2025": 26.44},
    {"so": 68, "ten_chuong_trinh": "Marketing",                            "ma_xet_tuyen": "7340115",   "ma_nganh": "7340115", "ten_nganh": "Marketing",                              "khoa_vien": "Khoa Marketing",                                   "chi_tieu": 100, "diem_chuan_2025": 28.12},
    {"so": 69, "ten_chuong_trinh": "Ngôn ngữ Anh",                        "ma_xet_tuyen": "7220201",   "ma_nganh": "7220201", "ten_nganh": "Ngôn ngữ Anh",                           "khoa_vien": "Khoa Ngoại ngữ Kinh tế",                           "chi_tieu": 90,  "diem_chuan_2025": 26.51},
    {"so": 70, "ten_chuong_trinh": "Quan hệ công chúng",                   "ma_xet_tuyen": "7320108",   "ma_nganh": "7320108", "ten_nganh": "Quan hệ công chúng",                     "khoa_vien": "Khoa Marketing",                                   "chi_tieu": 50,  "diem_chuan_2025": 28.07},
    {"so": 71, "ten_chuong_trinh": "Quan hệ lao động",                    "ma_xet_tuyen": "7340408",   "ma_nganh": "7340408", "ten_nganh": "Quan hệ lao động",                       "khoa_vien": "Khoa Kinh tế và Quản lý nguồn nhân lực",           "chi_tieu": 40,  "diem_chuan_2025": 25.0},
    {"so": 72, "ten_chuong_trinh": "Quản lý công",                        "ma_xet_tuyen": "7340403",   "ma_nganh": "7340403", "ten_nganh": "Quản lý công",                           "khoa_vien": "Khoa Khoa học quản lý",                            "chi_tieu": 50,  "diem_chuan_2025": 25.42},
    {"so": 73, "ten_chuong_trinh": "Quản lý đất đai",                     "ma_xet_tuyen": "7850103",   "ma_nganh": "7850103", "ten_nganh": "Quản lý đất đai",                        "khoa_vien": "Khoa Bất động sản và Kinh tế Tài nguyên",          "chi_tieu": 50,  "diem_chuan_2025": 24.38},
    {"so": 74, "ten_chuong_trinh": "Quản lý dự án",                       "ma_xet_tuyen": "7340409",   "ma_nganh": "7340409", "ten_nganh": "Quản lý dự án",                          "khoa_vien": "Khoa Đầu tư",                                      "chi_tieu": 50,  "diem_chuan_2025": 26.63},
    {"so": 75, "ten_chuong_trinh": "Quản lý tài nguyên và môi trường",    "ma_xet_tuyen": "7850101",   "ma_nganh": "7850101", "ten_nganh": "Quản lý tài nguyên và môi trường",       "khoa_vien": "Khoa Môi trường, Biến đổi khí hậu và Đô thị",      "chi_tieu": 50,  "diem_chuan_2025": 24.17},
    {"so": 76, "ten_chuong_trinh": "Quản trị dịch vụ du lịch và lữ hành", "ma_xet_tuyen": "7810103",  "ma_nganh": "7810103", "ten_nganh": "Quản trị dịch vụ du lịch và lữ hành",    "khoa_vien": "Khoa Du lịch và Khách sạn",                        "chi_tieu": 60,  "diem_chuan_2025": 26.06},
    {"so": 77, "ten_chuong_trinh": "Quản trị khách sạn",                  "ma_xet_tuyen": "7810201",   "ma_nganh": "7810201", "ten_nganh": "Quản trị khách sạn",                     "khoa_vien": "Khoa Du lịch và Khách sạn",                        "chi_tieu": 50,  "diem_chuan_2025": 26.25},
    {"so": 78, "ten_chuong_trinh": "Quản trị kinh doanh",                 "ma_xet_tuyen": "7340101",   "ma_nganh": "7340101", "ten_nganh": "Quản trị kinh doanh",                    "khoa_vien": "Khoa Quản trị kinh doanh",                         "chi_tieu": 180, "diem_chuan_2025": 27.1},
    {"so": 79, "ten_chuong_trinh": "Quản trị nhân lực",                   "ma_xet_tuyen": "7340404",   "ma_nganh": "7340404", "ten_nganh": "Quản trị nhân lực",                      "khoa_vien": "Khoa Kinh tế và Quản lý nguồn nhân lực",           "chi_tieu": 70,  "diem_chuan_2025": 27.1},
    {"so": 80, "ten_chuong_trinh": "Tài chính - Ngân hàng",               "ma_xet_tuyen": "7340201",   "ma_nganh": "7340201", "ten_nganh": "Tài chính Ngân hàng",                    "khoa_vien": "Viện Ngân hàng - Tài chính",                       "chi_tieu": 230, "diem_chuan_2025": 27.34},
    {"so": 81, "ten_chuong_trinh": "Thống kê kinh tế",                    "ma_xet_tuyen": "7310107",   "ma_nganh": "7310107", "ten_nganh": "Thống kê kinh tế",                       "khoa_vien": "Khoa Thống kê",                                    "chi_tieu": 50,  "diem_chuan_2025": 26.79},
    {"so": 82, "ten_chuong_trinh": "Thương mại điện tử",                  "ma_xet_tuyen": "7340122",   "ma_nganh": "7340122", "ten_nganh": "Thương mại điện tử",                     "khoa_vien": "Viện Thương mại và Kinh tế quốc tế",               "chi_tieu": 50,  "diem_chuan_2025": 28.83},
    {"so": 83, "ten_chuong_trinh": "Toán kinh tế",                        "ma_xet_tuyen": "7310108",   "ma_nganh": "7310108", "ten_nganh": "Toán kinh tế",                           "khoa_vien": "Khoa Toán kinh tế",                                "chi_tieu": 50,  "diem_chuan_2025": 26.73},
    # ── TT1 / TT2 ─────────────────────────────────────────────────────────
    {"so": 84, "ten_chuong_trinh": "Kế toán (TT1)",                       "ma_xet_tuyen": "TT1",       "ma_nganh": "7340301", "ten_nganh": "Kế toán",                                "khoa_vien": "Viện Kế toán - Kiểm toán",                         "chi_tieu": 55,  "diem_chuan_2025": 24.75},
    {"so": 85, "ten_chuong_trinh": "Kế hoạch tài chính (TT1)",            "ma_xet_tuyen": "TT1",       "ma_nganh": "7340201", "ten_nganh": "Tài chính Ngân hàng",                    "khoa_vien": "Viện Ngân hàng - Tài chính",                       "chi_tieu": 55,  "diem_chuan_2025": 24.75},
    {"so": 86, "ten_chuong_trinh": "Quản trị kinh doanh (TT1)",           "ma_xet_tuyen": "TT1",       "ma_nganh": "7340101", "ten_nganh": "Quản trị kinh doanh",                    "khoa_vien": "Khoa Quản trị kinh doanh",                         "chi_tieu": 55,  "diem_chuan_2025": 24.75},
    {"so": 87, "ten_chuong_trinh": "Tài chính (TT2)",                     "ma_xet_tuyen": "TT2",       "ma_nganh": "7340201", "ten_nganh": "Tài chính Ngân hàng",                    "khoa_vien": "Viện Ngân hàng - Tài chính",                       "chi_tieu": 220, "diem_chuan_2025": 25.5},
    {"so": 88, "ten_chuong_trinh": "Kinh doanh quốc tế (TT2)",            "ma_xet_tuyen": "TT2",       "ma_nganh": "7340120", "ten_nganh": "Kinh doanh quốc tế",                     "khoa_vien": "Viện Thương mại và Kinh tế quốc tế",               "chi_tieu": 110, "diem_chuan_2025": 25.5},
    # ── CLC ───────────────────────────────────────────────────────────────
    {"so": 89, "ten_chuong_trinh": "Kinh tế phát triển (CLC1)",            "ma_xet_tuyen": "CLC1",      "ma_nganh": "7310105", "ten_nganh": "Kinh tế phát triển",                     "khoa_vien": "Khoa Kế hoạch và Phát triển",                      "chi_tieu": 55,  "diem_chuan_2025": 25.25},
    {"so": 90, "ten_chuong_trinh": "Ngân hàng (CLC1)",                    "ma_xet_tuyen": "CLC1",       "ma_nganh": "7340201", "ten_nganh": "Tài chính Ngân hàng",                    "khoa_vien": "Viện Ngân hàng - Tài chính",                       "chi_tieu": 55,  "diem_chuan_2025": 25.25},
    {"so": 91, "ten_chuong_trinh": "Công nghệ thông tin và chuyển đổi số (CLC1)", "ma_xet_tuyen": "CLC1", "ma_nganh": "7480201", "ten_nganh": "Công nghệ thông tin",               "khoa_vien": "Khoa Công nghệ thông tin",                          "chi_tieu": 55,  "diem_chuan_2025": 25.25},
    {"so": 92, "ten_chuong_trinh": "Bảo hiểm tích hợp chứng chỉ ANZIIF (CLC1)", "ma_xet_tuyen": "CLC1", "ma_nganh": "7340204", "ten_nganh": "Bảo hiểm",                           "khoa_vien": "Khoa Bảo hiểm",                                    "chi_tieu": 55,  "diem_chuan_2025": 25.25},
    {"so": 93, "ten_chuong_trinh": "Kinh tế đầu tư (CLC2)",               "ma_xet_tuyen": "CLC2",      "ma_nganh": "7310104", "ten_nganh": "Kinh tế đầu tư",                         "khoa_vien": "Khoa Đầu tư",                                      "chi_tieu": 160, "diem_chuan_2025": 26.5},
    {"so": 94, "ten_chuong_trinh": "Quản trị nhân lực (CLC2)",             "ma_xet_tuyen": "CLC2",      "ma_nganh": "7340404", "ten_nganh": "Quản trị nhân lực",                      "khoa_vien": "Khoa Kinh tế và Quản lý nguồn nhân lực",           "chi_tieu": 160, "diem_chuan_2025": 26.5},
    {"so": 95, "ten_chuong_trinh": "Quản trị kinh doanh (CLC2)",           "ma_xet_tuyen": "CLC2",      "ma_nganh": "7340101", "ten_nganh": "Quản trị kinh doanh",                    "khoa_vien": "Khoa Quản trị kinh doanh",                         "chi_tieu": 105, "diem_chuan_2025": 26.5},
    {"so": 96, "ten_chuong_trinh": "Quan hệ công chúng (CLC2)",            "ma_xet_tuyen": "CLC2",      "ma_nganh": "7320108", "ten_nganh": "Quan hệ công chúng",                     "khoa_vien": "Khoa Marketing",                                   "chi_tieu": 160, "diem_chuan_2025": 26.5},
    {"so": 97, "ten_chuong_trinh": "Tài chính doanh nghiệp (CLC3)",        "ma_xet_tuyen": "CLC3",      "ma_nganh": "7340201", "ten_nganh": "Tài chính Ngân hàng",                    "khoa_vien": "Viện Ngân hàng - Tài chính",                       "chi_tieu": 325, "diem_chuan_2025": 26.42},
    {"so": 98, "ten_chuong_trinh": "Marketing số (CLC3)",                  "ma_xet_tuyen": "CLC3",      "ma_nganh": "7340115", "ten_nganh": "Marketing",                              "khoa_vien": "Khoa Marketing",                                   "chi_tieu": 270, "diem_chuan_2025": 26.42},
    {"so": 99, "ten_chuong_trinh": "Quản trị Marketing (CLC3)",            "ma_xet_tuyen": "CLC3",      "ma_nganh": "7340115", "ten_nganh": "Marketing",                              "khoa_vien": "Khoa Marketing",                                   "chi_tieu": 165, "diem_chuan_2025": 26.42},
    {"so": 100,"ten_chuong_trinh": "Quản trị kinh doanh quốc tế (CLC3)",  "ma_xet_tuyen": "CLC3",      "ma_nganh": "7340120", "ten_nganh": "Kinh doanh quốc tế",                     "khoa_vien": "Viện Thương mại và Kinh tế quốc tế",               "chi_tieu": 270, "diem_chuan_2025": 26.42},
    {"so": 101,"ten_chuong_trinh": "Kinh tế quốc tế (CLC3)",               "ma_xet_tuyen": "CLC3",      "ma_nganh": "7310106", "ten_nganh": "Kinh tế quốc tế",                        "khoa_vien": "Viện Thương mại và Kinh tế quốc tế",               "chi_tieu": 270, "diem_chuan_2025": 26.42},
    {"so": 102,"ten_chuong_trinh": "Logistics và quản lý chuỗi cung ứng (CLC3)", "ma_xet_tuyen": "CLC3", "ma_nganh": "7510605", "ten_nganh": "Logistics và Quản lý chuỗi cung ứng",  "khoa_vien": "Viện Thương mại và Kinh tế quốc tế",               "chi_tieu": 165, "diem_chuan_2025": 26.42},
    {"so": 103,"ten_chuong_trinh": "Thương mại điện tử (CLC3)",             "ma_xet_tuyen": "CLC3",      "ma_nganh": "7340122", "ten_nganh": "Thương mại điện tử",                     "khoa_vien": "Viện Thương mại và Kinh tế quốc tế",               "chi_tieu": 165, "diem_chuan_2025": 26.42},
    {"so": 104,"ten_chuong_trinh": "Kiểm toán tích hợp chứng chỉ ACCA (CLC3)", "ma_xet_tuyen": "CLC3", "ma_nganh": "7340302", "ten_nganh": "Kiểm toán",                             "khoa_vien": "Viện Kế toán - Kiểm toán",                         "chi_tieu": 270, "diem_chuan_2025": 26.42},
]

# ── Pattern nhận diện câu hỏi về chỉ tiêu / điểm chuẩn ───────────────────────
_ADMISSION_PATTERN = re.compile(
    r"chỉ\s*tiêu"
    r"|điểm\s*chuẩn"
    r"|điểm\s*đầu\s*vào"
    r"|tuyển\s*sinh"
    r"|xét\s*tuyển"
    r"|mã\s*xét\s*tuyển"
    r"|POHE|CLC[123]|TT[12]|E-BBA|EP\d+",
    re.IGNORECASE | re.UNICODE,
)


def search_admission_data(question: str) -> list[dict]:
    """
    Tìm chương trình trong ADMISSION_DATA khớp với câu hỏi.
    Chiến lược:
      1. Khớp mã xét tuyển tường minh (EP19, POHE1, CLC1...)
      2. Khớp mã ngành 7 chữ số
      3. Trích xuất cụm tên ngành từ câu hỏi (bỏ từ ngữ cảnh),
         rồi so khớp trực tiếp substring với ten_chuong_trinh / ten_nganh
      4. Fallback: scoring từng từ nếu không có match trực tiếp
    """
    q_lower = question.lower()
    is_broad_program_request = bool(re.search(
        r"t[aấ]t\s*c[aả]|to[aà]n\s*b[ộo]|li[eê]t\s*k[eê]|danh\s*s[aá]ch|c[aá]c\s*chương\s*trình|c[aá]c\s*hệ",
        q_lower,
        re.IGNORECASE | re.UNICODE,
    ))

    # 1. Khớp mã xét tuyển tường minh
    ma_xt_pattern = re.compile(r"\b(EP\d+|POHE\d*|EBBA|EPMP|CLC[123]|TT[12])\b", re.IGNORECASE)
    found_codes = [m.group(1).upper() for m in ma_xt_pattern.finditer(question)]
    if found_codes:
        results = [e for e in ADMISSION_DATA if e["ma_xet_tuyen"].upper() in found_codes]
        if results:
            return results

    # 2. Khớp mã ngành 7 chữ số
    results = [e for e in ADMISSION_DATA if re.search(r"\b" + re.escape(e["ma_nganh"]) + r"\b", question)]
    if results:
        return results

    # 3. Trích xuất cụm tên ngành bằng cách bỏ từ ngữ cảnh
    CONTEXT_PAT = re.compile(
        r"điểm\s*chuẩn|điểm\s*đầu\s*vào|chỉ\s*tiêu|tuyển\s*sinh"
        r"|chương\s*trình\s*đào\s*tạo|chương\s*trình|đào\s*tạo"
        r"|ngành\s*học|ngành|ctđt|năm\s*\d{4}|\b20\d{2}\b"
        r"|bao\s*nhiêu|của\s*trường|tại\s*neu|tại\s*trường"
        r"|\blà\s*bao\s*nhiêu\b|\blà\s*gì\b|\blà\b"
        r"|\bnhư\s*thế\s*nào\b|\bthế\s*nào\b|\bcủa\b|\bcó\b"
        r"|\bcho\s*tôi\s*biết\b|\bcho\s*biết\b|\bxin\s*hỏi\b|\bhỏi\b",
        re.IGNORECASE | re.UNICODE,
    )
    term = CONTEXT_PAT.sub(" ", q_lower)
    term = re.sub(r"\s+", " ", term).strip()

    if not term:
        return []

    # Helper: chuẩn hóa chuỗi để so khớp (bỏ ký tự đặc biệt, chuẩn hóa khoảng trắng)
    def normalize(s: str) -> str:
        s = re.sub(r"[\(\)\[\]\-_/\\,\.]+", " ", s)
        return re.sub(r"\s+", " ", s).strip()

    # 3a. So khớp trực tiếp: term là substring của ten_chuong_trinh hoặc ten_nganh
    term_norm = normalize(term)
    exact_matches = []
    for entry in ADMISSION_DATA:
        name_norm  = normalize(entry["ten_chuong_trinh"].lower())
        nganh_norm = normalize(entry["ten_nganh"].lower())
        if term_norm in name_norm or term_norm in nganh_norm:
            exact_matches.append(entry)

    if exact_matches:
        name_hits = [e for e in exact_matches
                     if term_norm in normalize(e["ten_chuong_trinh"].lower())]
        if name_hits:
            exact_name_hits = [e for e in name_hits
                               if term_norm == normalize(e["ten_chuong_trinh"].lower())]
            if exact_name_hits:
                if len(exact_name_hits) == 1:
                    return exact_name_hits
                return exact_name_hits
            return name_hits
        term_words = set(term.split())
        nganh_hits = [
            e for e in exact_matches
            if any(w in normalize(e["ten_chuong_trinh"].lower()) for w in term_words)
        ]
        return nganh_hits if nganh_hits else exact_matches

    # 3b. Fallback: scoring từng từ (cho trường hợp câu hỏi viết tắt hoặc sai dấu nhẹ)
    words = [w for w in term.split() if len(w) >= 2]
    if not words:
        return []

    scored = []
    for entry in ADMISSION_DATA:
        name_lower  = entry["ten_chuong_trinh"].lower()
        nganh_lower = entry["ten_nganh"].lower()
        name_score = 0
        nganh_score = 0

        for length in range(min(len(words), 6), 1, -1):
            for i in range(len(words) - length + 1):
                phrase = " ".join(words[i:i + length])
                if phrase in name_lower:
                    name_score += length * 5
                if phrase in nganh_lower:
                    nganh_score += length * 2

        for w in words:
            if w in name_lower:
                name_score += 1
            if w in nganh_lower:
                nganh_score += 1

        total = name_score + nganh_score
        if total > 0:
            scored.append((name_score, total, entry))

    if not scored:
        return []

    has_name_match = any(ns > 0 for ns, _, _ in scored)
    if has_name_match:
        scored = [(ns, tot, e) for ns, tot, e in scored if ns > 0]
        max_score = max(ns for ns, _, _ in scored)
        filtered = [e for ns, _, e in scored if ns >= max_score * 0.75]
    else:
        max_score = max(tot for _, tot, _ in scored)
        filtered = [e for _, tot, e in scored if tot >= max_score * 0.75]

    seen = set()
    unique = []
    for e in filtered:
        key = e["ma_xet_tuyen"] + e["ma_nganh"]
        if key not in seen:
            seen.add(key)
            unique.append(e)
    return unique


def format_admission_answer(question: str, programs: list[dict]) -> str:
    """
    Trả lời về chỉ tiêu / điểm chuẩn — hoàn toàn bằng văn xuôi, không bảng.
    """
    if not programs:
        return (
            "Hiện chưa tìm thấy chương trình phù hợp với câu hỏi của bạn. "
        )

    q_lower = question.lower()
    want_diem    = bool(re.search(r"điểm.{0,5}chuẩn|điểm.{0,5}đầu vào", q_lower))
    want_chitieu = bool(re.search(r"chỉ.{0,3}tiêu", q_lower))

    def one_line(p: dict) -> str:
        ten      = p["ten_chuong_trinh"]
        ma       = p["ma_nganh"]
        ct       = p["chi_tieu"]
        dc       = p["diem_chuan_2025"]
        diem_str = str(dc) if dc is not None else None
        if want_diem and not want_chitieu:
            if diem_str is None:
                return f"- **{ten}** (mã {ma}): chương trình này chưa cập nhật điểm chuẩn"
            return f"- **{ten}** (mã {ma}): điểm chuẩn 2026 là **{diem_str}**"
        if want_chitieu and not want_diem:
            return f"- **{ten}** (mã {ma}): chỉ tiêu **{ct} sinh viên**"
        # Hỏi cả hai hoặc tổng quát
        diem_part = f"**{diem_str}**" if diem_str is not None else "chưa cập nhật"
        return f"- **{ten}** (mã {ma}): chỉ tiêu **{ct} sinh viên**, điểm chuẩn 2026: {diem_part}"

    if len(programs) == 1:
        p = programs[0]
        ten      = p["ten_chuong_trinh"]
        ma       = p["ma_nganh"]
        ct       = p["chi_tieu"]
        dc       = p["diem_chuan_2025"]
        diem_str = str(dc) if dc is not None else None
        if want_diem and not want_chitieu:
            if diem_str is None:
                return f"Chương trình **{ten}** (mã ngành {ma}) chưa cập nhật điểm chuẩn."
            return f"Chương trình **{ten}** (mã ngành {ma}) có điểm chuẩn 2026 là **{diem_str}**."
        if want_chitieu and not want_diem:
            return f"Chương trình **{ten}** (mã ngành {ma}) có chỉ tiêu tuyển sinh 2026 là **{ct} sinh viên**."
        # Hỏi cả hai hoặc tổng quát
        diem_part = f"**{diem_str}**" if diem_str is not None else "chưa cập nhật"
        return (f"Chương trình **{ten}** (mã ngành {ma}): "
                f"chỉ tiêu **{ct} sinh viên**, điểm chuẩn 2026: {diem_part}.")

    # Nhiều chương trình → liệt kê văn xuôi
    lines = ["Dưới đây là thông tin tuyển sinh các chương trình phù hợp:"]
    for p in programs:
        lines.append(one_line(p))
    return "\n".join(lines)


def handle_admission_question(question: str) -> str | None:
    """
    Nếu câu hỏi liên quan đến chỉ tiêu/điểm chuẩn → trả về answer string.
    Ngược lại trả về None để pipeline tiếp tục xử lý bình thường.
    """
    if not _ADMISSION_PATTERN.search(question):
        return None

    q_lower = question.lower()
    is_general = bool(re.search(
        r"t[aấ]t\s*c[aả]|to[àa]n\s*b[ộo]|c[aá]c\s*ng[àa]nh|danh\s*s[aá]ch|li[eê]t\s*k[eê]",
        q_lower,
    ))
    if is_general:
        return (
            "NEU có hơn 100 chương trình đào tạo. "
            "Bạn có thể hỏi cụ thể từng ngành để tôi tra chỉ tiêu và điểm chuẩn, "
            "hoặc xem toàn bộ danh sách tại tuyensinh.neu.edu.vn."
        )

    programs = search_admission_data(question)
    if not programs:
        return (
            "Hiện chưa tìm thấy thông tin tuyển sinh cho ngành bạn hỏi. "
            "Bạn có thể xem thêm tại tuyensinh.neu.edu.vn."
        )

    return format_admission_answer(question, programs)


EXCLUDED_SUBJECT_CODES = {
    "llnl1105", "llnl1106", "llnl1107", "lldl1102", "lltt1101",
    "khmi1101", "khma1101", "lucs1129",
}
EXCLUDED_SUBJECT_NAMES_PATTERNS = re.compile(
    r"tri\s*[eé]t\s*h[oọ]c\s*m[aá]c[\s\-]*l[eê][- ]?nin"
    r"|kinh\s*t[eế]\s*ch[ií]nh\s*tr[ij]\s*m[aá]c[\s\-]*l[eê][- ]?nin"
    r"|ch[uủ]\s*ngh[iĩ]a\s*x[aã]\s*h[oộ]i\s*khoa\s*h[oọ]c"
    r"|l[iị]ch\s*s[uử]\s*[dđ][aả]ng\s*c[oộ]ng\s*s[aả]n\s*vi[eệ]t\s*nam"
    r"|t[uư]\s*t[uư][oở]ng\s*h[oồ]\s*ch[ií]\s*minh"
    r"|gi[aá]o\s*d[uụ]c\s*th[eể]\s*ch[aấ]t"
    r"|gi[aá]o\s*d[uụ]c\s*qu[oố]c\s*ph[oò]ng"
    r"|gdtc|gdqp"
    r"|kinh\s*t[eế]\s*vi\s*m[oô]\s*1"
    r"|kinh\s*t[eế]\s*v[iĩ]\s*m[oô]\s*1"
    r"|ph[aá]p\s*lu[aậ]t\s*[dđ][aạ]i\s*c[uư][oơ]ng",
    re.IGNORECASE | re.UNICODE,
)

_F = re.IGNORECASE | re.UNICODE  # shorthand
_EXCLUDED_SUBJECT_KEYWORD_MAP: list[tuple[re.Pattern, str]] = [
    # Triết học — user có thể gõ "triết", "triet", "Triết học"
    (re.compile(r"tri[eếệề]t|triet", _F), "Triết học Mác-Lênin (LLNL1105)"),
    # Kinh tế chính trị
    (re.compile(r"kinh\s*t[eế]\s*ch[ií]nh\s*tr[ịi]|ktct", _F),
     "Kinh tế chính trị Mác-Lênin (LLNL1106)"),
    # Chủ nghĩa xã hội khoa học
    (re.compile(r"ch[uủ]\s*ngh[iĩ]a\s*x[aã]\s*h[oộ]i|cnxhkh", _F),
     "Chủ nghĩa xã hội khoa học (LLNL1107)"),
    # Lịch sử Đảng
    (re.compile(r"l[iị]ch\s*s[uử]\s*[dđ][aả]ng|lsd\b", _F),
     "Lịch sử Đảng Cộng sản Việt Nam (LLDL1102)"),
    # Tư tưởng Hồ Chí Minh
    (re.compile(r"t[uư]\s*t[uư][oở]ng\s*h[oồ]\s*ch[ií]\s*minh|tthcm", _F),
     "Tư tưởng Hồ Chí Minh (LLTT1101)"),
    # Giáo dục thể chất
    (re.compile(r"gdtc|gi[aá]o\s*d[uụ]c\s*th[eể]\s*ch[aấ]t", _F),
     "Giáo dục thể chất (GDTC)"),
    # Giáo dục quốc phòng
    (re.compile(r"gdqp|gi[aá]o\s*d[uụ]c\s*qu[oố]c\s*ph[oò]ng", _F),
     "Giáo dục quốc phòng và an ninh (GDQP)"),
    # Kinh tế vi mô 1 — phải đặt TRƯỚC vĩ mô để tránh overlap
    (re.compile(r"kinh\s*t[eế]\s*vi\s*m[oô]|vi\s*m[oô]\s*1", _F),
     "Kinh tế vi mô 1 (KHMI1101)"),
    # Kinh tế vĩ mô 1
    (re.compile(r"kinh\s*t[eế]\s*v[iĩ]\s*m[oô]|v[iĩ]\s*m[oô]\s*1", _F),
     "Kinh tế vĩ mô 1 (KHMA1101)"),
    # Pháp luật đại cương
    (re.compile(r"ph[aá]p\s*lu[aậ]t\s*[dđ][aạ]i\s*c[uư][oơ]ng", _F),
     "Pháp luật đại cương (LUCS1129)"),
]

# Pattern nhận diện câu hỏi dạng "ngành nào không (cần) học [môn]"
_WHICH_MAJOR_NOT_STUDY_PATTERN = re.compile(
    r"ng[àa]nh\s*(n[àa]o)?.{0,20}(kh[oô]ng|ko|ch[aẳ]ng)\s*(c[aầ]n\s*)?"
    r"(h[oọ]c|d[aạ]y|ph[aả]i\s*h[oọ]c)",
    re.IGNORECASE | re.UNICODE,
)


def handle_which_major_not_study(question: str) -> str | None:
    """
    Nếu user hỏi "ngành nào không học [môn đại cương bắt buộc]",
    trả về câu trả lời cứng vì các môn này bắt buộc toàn trường.
    Ngược lại trả về None để pipeline xử lý bình thường.
    """
    if not _WHICH_MAJOR_NOT_STUDY_PATTERN.search(question):
        return None

    # Kiểm tra xem môn được hỏi có phải môn đại cương bắt buộc không
    matched_subjects: list[str] = []
    for pattern, display_name in _EXCLUDED_SUBJECT_KEYWORD_MAP:
        if pattern.search(question):
            matched_subjects.append(display_name)

    if not matched_subjects:
        return None  # Hỏi về môn khác → để pipeline xử lý

    subjects_str = ", ".join(matched_subjects)
    return (
        f"Hiện tại không có ngành nào không học {subjects_str}. "
        f"Đây là môn học bắt buộc chung cho tất cả các ngành tại NEU."
    )


# Pattern để nhận diện câu hỏi "nên học môn gì" (câu hỏi gợi ý môn)
_RECOMMEND_SUBJECT_PATTERN = re.compile(
    r"n[eê]n\s+h[oọ]c\s+m[oô]n\s+g[iì]"
    r"|g[oợ]i\s+[yý]\s+m[oô]n"
    r"|m[oô]n\s+n[aà]o\s+n[eê]n\s+h[oọ]c"
    r"|ch[oọ]n\s+m[oô]n\s+h[oọ]c"
    r"|[dđ][aă]ng\s+k[yý]\s+m[oô]n\s+g[iì]"
    r"|n[eê]n\s+[dđ][aă]ng\s+k[yý]\s+m[oô]n\s+n[aà]o"
    r"|m[oô]n\s+t[uự]\s+ch[oọ]n\s+n[aà]o",
    re.IGNORECASE | re.UNICODE,
)

# Pattern nhận diện câu hỏi XÁC NHẬN sự tồn tại môn đại cương ("ngành X có học môn Y không?")
_CONFIRM_SUBJECT_PATTERN = re.compile(
    r"c[oó]\s+h[oọ]c\s+m[oô]n"
    r"|c[oó]\s+d[aạ]y\s+m[oô]n"
    r"|m[oô]n\s+.{0,40}\s+c[oó]\s+kh[oô]ng"
    r"|c[oó]\s+m[oô]n\s+.{0,40}\s+kh[oô]ng",
    re.IGNORECASE | re.UNICODE,
)


def is_recommend_subject_question(question: str) -> bool:
    """Trả về True nếu câu hỏi là hỏi gợi ý / nên học môn gì."""
    return bool(_RECOMMEND_SUBJECT_PATTERN.search(question))


def filter_excluded_subjects(nodes: list[dict], exclude: bool) -> list[dict]:
    """
    Nếu exclude=True: loại bỏ các SUBJECT node thuộc danh sách đại cương bắt buộc.
    Nếu exclude=False: giữ nguyên toàn bộ (dùng cho câu hỏi xác nhận sự tồn tại).
    """
    if not exclude:
        return nodes
    result = []
    for n in nodes:
        if n.get("label") != "SUBJECT":
            result.append(n)
            continue
        code = (n.get("code") or "").lower().strip()
        name = (n.get("name") or "").lower().strip()
        if code in EXCLUDED_SUBJECT_CODES:
            continue
        if EXCLUDED_SUBJECT_NAMES_PATTERNS.search(name):
            continue
        result.append(n)
    return result

# Module-level clients (reuse giữa các invocations trên cùng instance)
ai_client = OpenAI(api_key=OPENAI_API_KEY)
driver    = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USERNAME, NEO4J_PASSWORD))

app = FastAPI()

# CORS headers áp dụng cho mọi response
CORS_HEADERS = {
    "Access-Control-Allow-Origin":  "*",
    "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
    "Access-Control-Allow-Headers": "Content-Type, Authorization",
}

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Xử lý OPTIONS preflight cho mọi route 
@app.options("/{full_path:path}")
async def preflight_handler(full_path: str):
    return JSONResponse(content={}, headers=CORS_HEADERS)


COMMUNITY_LEVELS: dict[str, dict] = {

    "L1_GLOBAL": {
        "id":          "L1_GLOBAL",
        "level":       1,
        "name":        "Hệ sinh thái Đào tạo & Nghề nghiệp",
        "node_labels": {"MAJOR", "SUBJECT", "SKILL", "CAREER", "TEACHER", "PERSONALITY"},
        "purpose": (
            "Trả lời câu hỏi chiến lược: xu hướng đào tạo, liên kết toàn diện "
            "giữa chương trình học và thị trường lao động."
        ),
    },

    "L2_ACADEMIC": {
        "id":          "L2_ACADEMIC",
        "level":       2,
        "name":        "Cụm Học thuật (Academic Cluster)",
        # community_L2: MAJOR=2, SUBJECT=2, TEACHER=0 — không đồng nhất, dùng label filter
        "node_labels": {"MAJOR", "SUBJECT", "TEACHER"},
        "purpose": (
            "Trả lời về chương trình ngành, môn học, giảng viên phụ trách. "
            "Kết nối Teacher ↔ Subject ↔ Major."
        ),
    },

    "L2_CAREER_ALIGNMENT": {
        "id":          "L2_CAREER_ALIGNMENT",
        "level":       2,
        "name":        "Cụm Năng lực & Việc làm (Career Alignment Cluster)",
        # community_L2: SKILL=0, CAREER=1, SUBJECT=2 — không đồng nhất, dùng label filter
        "node_labels": {"SKILL", "CAREER", "SUBJECT", "PERSONALITY"},
        "purpose": (
            "Kết nối đầu ra môn học (Subject→Skill) với yêu cầu thực tế (Career→Skill). "
            "Trả lời về kỹ năng cần thiết, môn học liên quan đến nghề nghiệp. "
            "Bao gồm cả phẩm chất nhân cách nghề yêu cầu (Career→Personality)."
        ),
    },

    "L2_PERSONALITY_FIT": {
        "id":          "L2_PERSONALITY_FIT",
        "level":       2,
        "name":        "Cụm Tính cách MBTI & Ngành/Nghề (Personality Fit Cluster)",
        # community_L2: PERSONALITY=3, CAREER=1, MAJOR=2 — dùng label filter
        "node_labels": {"PERSONALITY", "CAREER", "MAJOR"},
        "purpose": (
            "Gợi ý ngành học và nghề nghiệp phù hợp với loại tính cách MBTI. "
            "Kích hoạt khi câu hỏi nhắc tới MBTI code (ESTP, ENTP...), "
            "'tính cách', 'hướng nội/hướng ngoại', 'hợp với nghề gì'. "
            "CŨNG xử lý câu hỏi ngược: 'tính cách gì hợp làm/học X' — "
            "tìm PERSONALITY phù hợp với ngành/lĩnh vực X qua suitable_fields hoặc SUITS_MAJOR/SUITS_CAREER."
        ),
    },

    "L3_MAJOR_CENTRIC": {
        "id":          "L3_MAJOR_CENTRIC",
        "level":       3,
        "name":        "Cộng đồng theo Ngành (Major-centric)",
        # community_L3: SUBJECT=0, TEACHER=1, SKILL=2 — không đồng nhất, dùng label filter
        "node_labels": {"SUBJECT", "TEACHER", "SKILL"},
        "purpose": (
            "Chi tiết lộ trình một ngành cụ thể: môn học, giảng viên, kỹ năng đầu ra. "
            "Kích hoạt khi câu hỏi nhắc tới Major Code cụ thể."
        ),
    },

    "L3_SKILL_CENTRIC": {
        "id":          "L3_SKILL_CENTRIC",
        "level":       3,
        "name":        "Cộng đồng theo Kỹ năng (Skill-centric)",
        "node_labels": {"SUBJECT", "CAREER"},
        "purpose": (
            "Giá trị của một kỹ năng cụ thể: môn nào dạy + nghề nào yêu cầu. "
            "Kích hoạt khi câu hỏi nhắc tới Skill cụ thể."
        ),
    },
}

# ── Ánh xạ intent → community ID ─────────────────────────────────────────────
INTENT_TO_COMMUNITY: dict[tuple, str] = {
    # Academic cluster
    ("MAJOR",   "SUBJECT"):  "L2_ACADEMIC",
    ("MAJOR",   "TEACHER"):  "L2_ACADEMIC",
    ("SUBJECT", "TEACHER"):  "L2_ACADEMIC",
    ("TEACHER", "SUBJECT"):  "L2_ACADEMIC",
    ("TEACHER", "MAJOR"):    "L2_ACADEMIC",
    # Self-queries học thuật
    ("SUBJECT", "SUBJECT"):  "L2_ACADEMIC",
    ("TEACHER", "TEACHER"):  "L2_ACADEMIC",
    ("MAJOR",   "MAJOR"):    "L1_GLOBAL",

    # Career cluster
    ("MAJOR",   "CAREER"):   "L2_CAREER_ALIGNMENT",
    ("MAJOR",   "SKILL"):    "L2_CAREER_ALIGNMENT",
    ("CAREER",  "SKILL"):    "L2_CAREER_ALIGNMENT",
    ("CAREER",  "SUBJECT"):  "L2_CAREER_ALIGNMENT",
    ("CAREER",  "MAJOR"):    "L2_CAREER_ALIGNMENT",
    ("SKILL",   "MAJOR"):    "L2_CAREER_ALIGNMENT",
    ("SKILL",   "CAREER"):   "L2_CAREER_ALIGNMENT",
    ("SKILL",   "SUBJECT"):  "L2_CAREER_ALIGNMENT",
    ("SUBJECT", "SKILL"):    "L2_CAREER_ALIGNMENT",
    ("SUBJECT", "CAREER"):   "L2_CAREER_ALIGNMENT",
    # Self-queries nghề nghiệp
    ("CAREER",  "CAREER"):   "L2_CAREER_ALIGNMENT",
    ("SKILL",   "SKILL"):    "L2_CAREER_ALIGNMENT",

    # Personality cluster
    ("PERSONALITY", "CAREER"):      "L2_PERSONALITY_FIT",
    ("PERSONALITY", "MAJOR"):       "L2_PERSONALITY_FIT",
    ("PERSONALITY", "SUBJECT"):     "L2_CAREER_ALIGNMENT",
    ("CAREER",      "PERSONALITY"): "L2_PERSONALITY_FIT",
    ("MAJOR",       "PERSONALITY"): "L2_PERSONALITY_FIT",
    ("SUBJECT",     "PERSONALITY"): "L2_CAREER_ALIGNMENT",
    ("PERSONALITY", "PERSONALITY"): "L2_PERSONALITY_FIT",
}


_PERSONALITY_KW_PATTERN = re.compile(
    r"tính cách|phẩm chất|personality|hướng nội|hướng ngoại|"
    r"cẩn thận|sáng tạo|lãnh đạo|đồng cảm|kiên nhẫn|tự tin|"
    r"điềm tĩnh|sâu sắc|kín đáo|nội tâm|tập trung|thận trọng|"
    r"suy tư|ôn hòa|trầm mặc|tinh tế|logic|phân tích|lý trí|"
    r"thấu cảm|ấm áp|nhân văn|nề nếp|kế hoạch|tổ chức|ngăn nắp|"
    r"linh hoạt|tự do|ngẫu hứng|thoải mái|phóng khoáng|"
    r"chiến lược|tầm nhìn|lý tưởng|đổi mới|tò mò|khám phá|"
    r"kỷ luật|trách nhiệm|quyết đoán|"
    r"hợp\s+(với\s+)?(nghề|ngành)|phù hợp\s+(với\s+)?(tôi|mình|người)|"
    r"\b(INTJ|INTP|ENTJ|ENTP|INFJ|INFP|ENFJ|ENFP"
    r"|ISTJ|ISFJ|ESTJ|ESFJ|ISTP|ISFP|ESTP|ESFP)\b",
    re.IGNORECASE | re.UNICODE,
)


def route_to_community(intent: dict) -> tuple[str, dict]:
    mentioned = intent.get("mentioned_labels") or []
    asked     = intent.get("asked_label", "UNKNOWN")
    keywords  = intent.get("keywords", [])

    # L2_PERSONALITY_FIT: hỏi về tính cách / phẩm chất nhân cách
    if (
        "PERSONALITY" in mentioned
        or asked == "PERSONALITY"
        or _PERSONALITY_KW_PATTERN.search(" ".join(keywords))
    ):
        return "L2_PERSONALITY_FIT", COMMUNITY_LEVELS["L2_PERSONALITY_FIT"]

    # L3_MAJOR_CENTRIC: keyword là mã ngành 7 chữ số
    MAJOR_CODE_PATTERN = re.compile(r"\b\d{7}\b")
    for kw in keywords:
        if MAJOR_CODE_PATTERN.search(str(kw)):
            return "L3_MAJOR_CENTRIC", COMMUNITY_LEVELS["L3_MAJOR_CENTRIC"]

    # L3_SKILL_CENTRIC: hỏi về skill cụ thể → career hoặc subject
    if asked in ("CAREER", "SUBJECT") and "SKILL" in mentioned:
        long_kws = [k for k in keywords if len(k.split()) >= 2]
        if long_kws:
            return "L3_SKILL_CENTRIC", COMMUNITY_LEVELS["L3_SKILL_CENTRIC"]

    # Lookup intent map
    first_mentioned = mentioned[0] if mentioned else None
    cid = INTENT_TO_COMMUNITY.get((first_mentioned, asked))
    if not cid:
        for m in mentioned:
            cid = INTENT_TO_COMMUNITY.get((m, asked))
            if cid:
                break
    if not cid:
        cid = "L1_GLOBAL"

    return cid, COMMUNITY_LEVELS[cid]


# ══════════════════════════════════════════════════════════════════════════════
# PHẦN 2: LOUVAIN COMMUNITY DETECTION
# ══════════════════════════════════════════════════════════════════════════════

def run_louvain_and_write(driver, community_def: dict) -> dict:
    level      = community_def["level"]
    cid        = community_def["id"]
    prop_key   = f"community_L{level}"
    graph_name = f"neo_edu_{cid.lower()}"

    stats = {"community_id": cid, "level": level, "nodes_written": 0, "error": None}

    if level == 1:
        with driver.session() as session:
            r = session.run(
                "MATCH (n) WHERE (n:MAJOR OR n:SUBJECT OR n:SKILL "
                "OR n:CAREER OR n:TEACHER OR n:PERSONALITY) "
                f"SET n.{prop_key} = 0 RETURN count(n) AS cnt"
            ).single()
            stats["nodes_written"] = r["cnt"] if r else 0
        return stats

    with driver.session() as session:
        try:
            session.run(f"CALL gds.graph.drop('{graph_name}', false)")
        except Exception:
            pass

        node_labels = list(community_def["node_labels"])
        rel_proj    = {
            rtype: {"type": rtype, "orientation": "UNDIRECTED",
                    "properties": {"weight": {"defaultValue": w}}}
            for rtype, w in community_def.get("rel_weights", {}).items()
        }

        try:
            if rel_proj:
                session.run(
                    "CALL gds.graph.project($gname, $nlabels, $rproj)",
                    gname=graph_name, nlabels=node_labels, rproj=rel_proj,
                )
            else:
                session.run(
                    "CALL gds.graph.project($gname, $nlabels, '*')",
                    gname=graph_name, nlabels=node_labels,
                )
        except Exception as e:
            stats["error"] = f"GDS project error: {e}"
            _fallback_community_assignment(driver, community_def, prop_key)
            return stats

        try:
            if rel_proj:
                session.run(
                    f"CALL gds.louvain.write('{graph_name}', "
                    f"{{relationshipWeightProperty: 'weight', writeProperty: '{prop_key}'}})"
                )
            else:
                session.run(
                    f"CALL gds.louvain.write('{graph_name}', "
                    f"{{writeProperty: '{prop_key}'}})"
                )
            r = session.run(
                f"MATCH (n) WHERE n.{prop_key} IS NOT NULL RETURN count(n) AS cnt"
            ).single()
            stats["nodes_written"] = r["cnt"] if r else 0
        except Exception as e:
            stats["error"] = f"GDS Louvain error: {e}"
            _fallback_community_assignment(driver, community_def, prop_key)
        finally:
            try:
                session.run(f"CALL gds.graph.drop('{graph_name}', false)")
            except Exception:
                pass

    return stats


def _fallback_community_assignment(driver, community_def: dict, prop_key: str):
    """
    Fallback assignment khớp với dữ liệu thực tế trong DB:
      L2: MAJOR=2, SUBJECT=2, CAREER=1, SKILL=0, TEACHER=0
      L3: SUBJECT=0, TEACHER=1, CAREER=1, SKILL=2
    """
    cid = community_def["id"]
    label_to_community = {
        "L2_ACADEMIC":          {"TEACHER": 0, "SUBJECT": 2, "MAJOR": 2},
        "L2_CAREER_ALIGNMENT":  {"SKILL": 0, "CAREER": 1, "SUBJECT": 2},
        "L2_PERSONALITY_FIT":   {"PERSONALITY": 3, "CAREER": 1, "MAJOR": 2},
        "L3_MAJOR_CENTRIC":     {"SUBJECT": 0, "TEACHER": 1, "SKILL": 2},
        "L3_SKILL_CENTRIC":     {"SUBJECT": 0, "CAREER": 1},
    }.get(cid, {})

    with driver.session() as session:
        for label, comm_val in label_to_community.items():
            session.run(f"MATCH (n:{label}) SET n.{prop_key} = {comm_val}")


def initialize_communities(driver, force_rebuild: bool = False):
    print("\n[Community Init] Bắt đầu khởi tạo 3 tầng cộng đồng...")

    if not force_rebuild:
        with driver.session() as session:
            r = session.run(
                "MATCH (n) WHERE n.community_L2 IS NOT NULL RETURN count(n) AS cnt LIMIT 1"
            ).single()
            if r and r["cnt"] > 0:
                print("[Community Init] Community L2/L3 đã tồn tại, bỏ qua rebuild.")
                return

    BUILD_ORDER = ["L1_GLOBAL", "L2_ACADEMIC", "L2_CAREER_ALIGNMENT",
                   "L2_PERSONALITY_FIT", "L3_MAJOR_CENTRIC", "L3_SKILL_CENTRIC"]

    for cid in BUILD_ORDER:
        cdef  = COMMUNITY_LEVELS[cid]
        level = cdef["level"]
        print(f"  [L{level}] Building: {cdef['name']}...")
        stats = run_louvain_and_write(driver, cdef)
        if stats.get("error"):
            print(f"    ⚠ Fallback (no GDS): {stats['error'][:80]}")
        else:
            print(f"    ✓ {stats['nodes_written']} nodes tagged (community_L{level})")

    print("[Community Init] Hoàn tất.\n")


# ══════════════════════════════════════════════════════════════════════════════
# PHẦN 3: AGGREGATION QUERY ROUTER
# ══════════════════════════════════════════════════════════════════════════════

_AGG_ALL_MAJOR_TOKENS = (
    r"tất cả(?: các)? ngành|mọi ngành|"
    r"các ngành đều|"
    r"ngành nào cũng|"
    r"chung cho(?: tất cả| mọi| các)?(?: các)? ngành|"
    r"môn chung|môn bắt buộc chung|môn(?: học)? bắt buộc"
)

AGGREGATION_PATTERNS: list[tuple[re.Pattern, str]] = [
    (re.compile(
        r"môn(?: học)?(?: nào)?(?: là)? chung(?: giữa| của)?(.*?)(?:\s+và\s+)(.*?)(?:\?|$)",
        re.IGNORECASE | re.UNICODE,
    ), "subject_intersection_two"),
    (re.compile(
        r"(?:môn(?: học)?(?: gì| nào)?.*?(?:" + _AGG_ALL_MAJOR_TOKENS + r")"
        r"|(?:" + _AGG_ALL_MAJOR_TOKENS + r").*?(?:môn|học phần))",
        re.IGNORECASE | re.UNICODE,
    ), "subject_intersection_all"),
    (re.compile(
        r"ngành(?: nào)?.{0,20}(?:nhiều môn|nhiều học phần).{0,15}nhất",
        re.IGNORECASE | re.UNICODE,
    ), "major_most_subjects"),
    (re.compile(
        r"(?:nghề|career|vị trí).{0,20}(?:nhiều kỹ năng|nhiều skill).{0,15}nhất",
        re.IGNORECASE | re.UNICODE,
    ), "career_most_skills"),
    (re.compile(
        r"môn(?: học)?(?: nào)?.{0,30}(?:nhiều ngành|phổ biến nhất|nhiều nhất)",
        re.IGNORECASE | re.UNICODE,
    ), "subject_most_majors"),
    (re.compile(
        r"(?:kỹ năng|skill)(?: nào)?.{0,30}(?:nhiều môn|phổ biến nhất)",
        re.IGNORECASE | re.UNICODE,
    ), "skill_most_subjects"),
    (re.compile(
        r"(?:có|tổng)(?: tất cả)? bao nhiêu (ngành|môn|nghề|kỹ năng|giảng viên)",
        re.IGNORECASE | re.UNICODE,
    ), "count_entities"),
]


def detect_aggregation_type(question: str) -> str | None:
    for pattern, agg_type in AGGREGATION_PATTERNS:
        if pattern.search(question):
            return agg_type
    return None


def run_aggregation_query(driver, question: str, agg_type: str) -> list[dict]:
    results = []
    with driver.session() as session:

        if agg_type == "subject_intersection_all":
            rows = session.run("""
                MATCH (m:MAJOR)
                WITH count(m) AS total_majors
                MATCH (s:SUBJECT)<-[:MAJOR_OFFERS_SUBJECT]-(m:MAJOR)
                WITH s, count(DISTINCT m) AS major_count, total_majors
                WHERE major_count = total_majors
                RETURN s.name AS name, s.code AS code, major_count
                ORDER BY s.name ASC
            """).data()
            if not rows:
                rows = session.run("""
                    MATCH (m:MAJOR)
                    WITH count(m) AS total_majors
                    MATCH (s:SUBJECT)<-[:MAJOR_OFFERS_SUBJECT]-(m:MAJOR)
                    WITH s, count(DISTINCT m) AS major_count, total_majors
                    WHERE major_count >= toInteger(total_majors * 0.8)
                    RETURN s.name AS name, s.code AS code,
                           major_count, total_majors
                    ORDER BY major_count DESC LIMIT 30
                """).data()
            for r in rows:
                results.append({
                    "name": r["name"], "label": "SUBJECT", "code": r["code"],
                    "major_count": r.get("major_count"), "hops": 1,
                    "_agg_meta": f"Xuất hiện trong {r.get('major_count')} ngành",
                })

        elif agg_type == "subject_intersection_two":
            rows = session.run("""
                MATCH (s:SUBJECT)<-[:MAJOR_OFFERS_SUBJECT]-(m:MAJOR)
                WITH s, collect(DISTINCT toLower(m.name)) AS major_names,
                     count(DISTINCT m) AS major_count
                WHERE major_count >= 2
                RETURN s.name AS name, s.code AS code,
                       major_names, major_count
                ORDER BY major_count DESC LIMIT 50
            """).data()
            for r in rows:
                results.append({
                    "name": r["name"], "label": "SUBJECT", "code": r["code"],
                    "major_names": r.get("major_names"),
                    "major_count": r.get("major_count"), "hops": 1,
                })

        elif agg_type == "major_most_subjects":
            rows = session.run("""
                MATCH (m:MAJOR)-[:MAJOR_OFFERS_SUBJECT]->(s:SUBJECT)
                WITH m, count(DISTINCT s) AS subject_count
                RETURN m.name AS name, m.code AS code, subject_count
                ORDER BY subject_count DESC LIMIT 10
            """).data()
            for r in rows:
                results.append({
                    "name": r["name"], "label": "MAJOR", "code": r["code"],
                    "subject_count": r.get("subject_count"), "hops": 1,
                    "_agg_meta": f"{r.get('subject_count')} môn học",
                })

        elif agg_type == "career_most_skills":
            rows = session.run("""
                MATCH (c:CAREER)-[:REQUIRES]->(sk:SKILL)
                WITH c, count(DISTINCT sk) AS skill_count
                RETURN c.name AS name, skill_count
                ORDER BY skill_count DESC LIMIT 10
            """).data()
            for r in rows:
                results.append({
                    "name": r["name"], "label": "CAREER",
                    "skill_count": r.get("skill_count"), "hops": 1,
                    "_agg_meta": f"{r.get('skill_count')} kỹ năng",
                })

        elif agg_type == "subject_most_majors":
            rows = session.run("""
                MATCH (m:MAJOR)-[:MAJOR_OFFERS_SUBJECT]->(s:SUBJECT)
                WITH s, count(DISTINCT m) AS major_count
                RETURN s.name AS name, s.code AS code, major_count
                ORDER BY major_count DESC LIMIT 15
            """).data()
            for r in rows:
                results.append({
                    "name": r["name"], "label": "SUBJECT", "code": r["code"],
                    "major_count": r.get("major_count"), "hops": 1,
                    "_agg_meta": f"Được dạy trong {r.get('major_count')} ngành",
                })

        elif agg_type == "skill_most_subjects":
            rows = session.run("""
                MATCH (s:SUBJECT)-[:PROVIDES]->(sk:SKILL)
                WITH sk, count(DISTINCT s) AS subject_count
                RETURN sk.name AS name, subject_count
                ORDER BY subject_count DESC LIMIT 15
            """).data()
            for r in rows:
                results.append({
                    "name": r["name"], "label": "SKILL",
                    "subject_count": r.get("subject_count"), "hops": 1,
                    "_agg_meta": f"Được cung cấp bởi {r.get('subject_count')} môn",
                })

        elif agg_type == "count_entities":
            q_lower = question.lower()
            if "ngành" in q_lower:        label, vn = "MAJOR",       "ngành"
            elif "nghề" in q_lower:       label, vn = "CAREER",      "nghề"
            elif "kỹ năng" in q_lower or "skill" in q_lower:
                                          label, vn = "SKILL",       "kỹ năng"
            elif "giảng viên" in q_lower: label, vn = "TEACHER",     "giảng viên"
            elif "phẩm chất" in q_lower or "personality" in q_lower or "tính cách" in q_lower:
                                          label, vn = "PERSONALITY", "phẩm chất"
            else:                         label, vn = "SUBJECT",     "môn học"
            cnt = session.run(f"MATCH (n:{label}) RETURN count(n) AS cnt").single()["cnt"]
            results.append({
                "name": f"Tổng số {vn}: {cnt}", "label": label,
                "count": cnt, "hops": 0,
                "_agg_meta": f"count={cnt}",
            })

    return results


# ══════════════════════════════════════════════════════════════════════════════
# PHẦN 4: SCHEMA + CONSTRAINTS + SYSTEM PROMPTS
# ══════════════════════════════════════════════════════════════════════════════

SCHEMA_DESC = """
Nodes (dữ liệu thực tế trong DB):
  MAJOR   (37 ngành):   code, name, name_vi, name_en
                        + philosophy_and_objectives, admission_requirements,
                          learning_outcomes, po_plo_matrix,
                          training_process_and_graduation_conditions,
                          curriculum_structure_and_content,
                          teaching_and_assessment_methods,
                          reference_programs, lecturer_and_teaching_assistant_standards,
                          facilities_and_learning_resources

  SUBJECT (802 môn):    code, name, name_vi, name_en
                        + course_description, courses_goals, assessment,
                          learning_resources, course_requirements_and_expectations,
                          syllabus_adjustment_time, week_1..week_N (kế hoạch giảng dạy)

  CAREER  (27 nghề):    career_key, name, name_vi, name_en, field_name
                        + description (JSON: short_description, role_in_organization),
                          job_tasks, education_certification, market

  SKILL   (5217 kỹ năng): skill_key, name, skill_type (hard|soft)

  TEACHER (695 GV):     teacher_key, name, email, title

  PERSONALITY (?):      personality_key, name_vi, name_en, category, description, indicators

Relationships (đồng bộ script1 v3, script2 v5):
  (MAJOR)       -[:MAJOR_OFFERS_SUBJECT {semester, required_type}]-> (SUBJECT)      (1421)
  (SUBJECT)     -[:PROVIDES {mastery_level}]->                       (SKILL)        (8069)
  (TEACHER)     -[:TEACH]->                                          (SUBJECT)      (3981)
  (CAREER)      -[:REQUIRES {required_level}]->                      (SKILL)         (223)
  (SUBJECT)     -[:PREREQUISITE_FOR]->                               (SUBJECT)        (24)
  (MAJOR)       -[:LEADS_TO]->                                       (CAREER)          (6)
  (PERSONALITY) -[:SUITS_MAJOR {field_name, group_name}]->           (MAJOR)         (MỚI v6)
  (PERSONALITY) -[:SUITS_CAREER {field_name, major_name}]->          (CAREER)        (MỚI v6)
  (CAREER)      -[:REQUIRES_PERSONALITY]->                           (PERSONALITY)   (dự phòng)
  (SUBJECT)     -[:CULTIVATES]->                                     (PERSONALITY)   (dự phòng)
"""

RELATIONSHIP_CONSTRAINTS = {
    ("MAJOR", "CAREER"): (
    "MAJOR -[:LEADS_TO]-> CAREER. "
    "Liệt kê Career mà Major dẫn đến. KHÔNG đề cập SUBJECT trừ khi được hỏi. "
    "QUAN TRỌNG: Chỉ liệt kê nghề nghiệp thực tế (vị trí công việc, chức danh trong tổ chức). "
    "TUYỆT ĐỐI KHÔNG liệt kê các mục bắt đầu bằng: "
    "'Cử nhân', 'Kỹ sư', 'Thạc sĩ', 'Tiến sĩ', 'Bác sĩ', 'Bachelor', 'Master', 'Engineer' — "
    "đây là danh hiệu học vị/bằng cấp, không phải vị trí công việc. "
    "Ví dụ SAI: 'Cử nhân Công nghệ thông tin', 'Kỹ sư phần mềm (danh hiệu)'. "
    "Ví dụ ĐÚNG: 'Chuyên viên phân tích dữ liệu', 'Lập trình viên', 'Nhà khoa học dữ liệu'. "
    "ĐỊNH DẠNG BẮT BUỘC: Trình bày dạng VĂN XUÔI, mỗi nghề một đoạn ngắn theo mẫu: "
    "'Có thể làm việc tại [field_name] với vai trò [tên nghề] — [short_description hoặc role_in_organization từ description]. "
    "Công việc chính bao gồm: [1-2 nhiệm vụ tiêu biểu từ job_tasks].' "
    "Nếu không có description hoặc job_tasks thì chỉ ghi: 'Có thể làm [tên nghề] trong lĩnh vực [field_name].' "
    "KHÔNG dùng bảng markdown cho câu hỏi loại này."
    ),    
    ("CAREER", "SKILL"):   (
        "CAREER -[:REQUIRES]-> SKILL và SUBJECT -[:PROVIDES]-> SKILL. "
        "Trả lời kỹ năng cần thiết, chỉ nêu kỹ năng cứng (hard skills, là các skill có skill_type = 'hard') + môn cung cấp kỹ năng đó."
    ),
    ("MAJOR", "SKILL"):    (
        "MAJOR -[:MAJOR_OFFERS_SUBJECT]-> SUBJECT -[:PROVIDES]-> SKILL. "
        "Kỹ năng đạt được từ các môn trong chương trình, chỉ nêu kỹ năng cứng (hard skills, là các skill có skill_type = 'hard'). Kèm tên môn (mã môn)."
    ),
    ("SKILL", "MAJOR"):    (
        "SKILL <-[:PROVIDES]- SUBJECT <-[:MAJOR_OFFERS_SUBJECT]- MAJOR. "
        "Ngành học có môn cung cấp kỹ năng đó. Kèm mã ngành, tên môn trung gian."
    ),
    ("CAREER", "SUBJECT"): (
        "CAREER -[:REQUIRES]-> SKILL <-[:PROVIDES]- SUBJECT. "
        "Môn học cung cấp kỹ năng nghề yêu cầu, chỉ nêu kỹ năng cứng (hard skills, là các skill có skill_type = 'hard'). Kèm mã môn + kỹ năng cứng tương ứng."
    ),
    ("MAJOR", "SUBJECT"):  (
        "MAJOR -[:MAJOR_OFFERS_SUBJECT]-> SUBJECT. "
        "Môn học thuộc chương trình ngành, kèm mã môn "
        "loại (required_type: required=bắt buộc, elective=tự chọn)."
    ),
    ("SKILL", "CAREER"):   (
        "SKILL <-[:REQUIRES]- CAREER. Nghề nghiệp yêu cầu kỹ năng đó."
    ),
    ("CAREER", "MAJOR"):   (
        "MAJOR -[:LEADS_TO]-> CAREER. Ngành học dẫn đến nghề đó, kèm mã ngành."
    ),
    ("SUBJECT", "SKILL"):  (
        "SUBJECT -[:PROVIDES]-> SKILL. Kỹ năng đạt được sau khi học môn đó."
    ),
    ("SKILL", "SUBJECT"):  (
        "SKILL <-[:PROVIDES]- SUBJECT. Môn học (kèm mã môn) cung cấp kỹ năng đó."
    ),
    ("SUBJECT", "TEACHER"): (
        "TEACHER -[:TEACH]-> SUBJECT. Giảng viên phụ trách môn đó."
    ),
    ("TEACHER", "SUBJECT"): (
        "TEACHER -[:TEACH]-> SUBJECT. Môn học thầy/cô đó phụ trách, kèm mã môn."
    ),
    ("MAJOR", "TEACHER"):  (
        "MAJOR -[:MAJOR_OFFERS_SUBJECT]-> SUBJECT <-[:TEACH]- TEACHER. "
        "Giảng viên dạy trong chương trình ngành đó."
    ),
    ("TEACHER", "MAJOR"):  (
        "TEACHER -[:TEACH]-> SUBJECT <-[:MAJOR_OFFERS_SUBJECT]- MAJOR. "
        "Ngành học thầy/cô đó tham gia giảng dạy."
    ),
    ("MAJOR", "MAJOR"):    (
        "So sánh: MAJOR -[:LEADS_TO]-> CAREER và MAJOR -[:MAJOR_OFFERS_SUBJECT]-> SUBJECT. "
        "So sánh cơ hội nghề nghiệp và môn học đặc trưng của từng ngành."
    ),
    # Self-queries
    ("SUBJECT", "SUBJECT"): (
        "Trả lời: mã môn (code), mô tả môn học (course_description), "
        "mục tiêu (courses_goals), đánh giá (assessment), "
        "môn tiên quyết nếu có (PREREQUISITE_FOR)."
    ),
    ("CAREER", "CAREER"):  (
        "Trả lời đầy đủ 4 phần: "
        "1. Mô tả nghề: lấy từ description (field short_description hoặc role_in_organization). "
        "2. Công việc chính: liệt kê từ job_tasks. "
        "3. Thị trường lao động: tóm tắt từ field market. "
        "4. ĐỀ XUẤT NGÀNH HỌC: BẮT BUỘC liệt kê các ngành theo recommended_majors "
        "(tên ngành + mã ngành). Nếu không có recommended_majors, "
        "dùng education_certification.recommended_majors làm tên gợi ý. "
        "Format: Tên ngành (mã ngành) - VD: Công nghệ thông tin (7480201). "
        "Nếu không có ngành nào trong DB - nói rõ chưa có dữ liệu ngành phù hợp."
    ),
    ("TEACHER", "TEACHER"): (
        "Trả lời: học hàm/học vị (title), email, "
        "môn đang dạy (TEACH→SUBJECT)."
    ),
    ("MAJOR", "MAJOR_DETAIL"): (
        "Trả lời chi tiết ngành: mục tiêu đào tạo (philosophy_and_objectives), "
        "chuẩn đầu ra (learning_outcomes), cơ hội nghề nghiệp (LEADS_TO→CAREER)."
    ),

    # Personality constraints — MBTI v6
    ("PERSONALITY", "CAREER"): (
        "PERSONALITY -[:SUITS_CAREER]-> CAREER. "
        "Liệt kê nghề nghiệp phù hợp với loại tính cách MBTI này. "
        "NGUỒN DỮ LIỆU ưu tiên theo thứ tự: "
        "(1) Các node CAREER trong [DỮ LIỆU GRAPH] có rel_types=['SUITS_CAREER']. "
        "(2) Trường suitable_fields trong node PERSONALITY (parse JSON string): "
        "    lấy từng field → groups → majors → careers. "
        "Định dạng BẮT BUỘC: bảng markdown | Lĩnh vực | Nhóm ngành | Nghề nghiệp |. "
        "TUYỆT ĐỐI không bịa thêm nghề không có trong dữ liệu."
    ),
    ("PERSONALITY", "MAJOR"): (
        "PERSONALITY -[:SUITS_MAJOR]-> MAJOR. "
        "Liệt kê ngành học phù hợp với loại tính cách MBTI này tại NEU. "
        "NGUỒN DỮ LIỆU ưu tiên theo thứ tự: "
        "(1) Các node MAJOR trong [DỮ LIỆU GRAPH] có rel_types=['SUITS_MAJOR']. "
        "(2) Trường suitable_fields trong node PERSONALITY (parse JSON string): "
        "    lấy từng field → groups → majors, lấy major_code và major_name. "
        "Định dạng BẮT BUỘC: bảng markdown | STT | Tên ngành | Mã ngành | Lĩnh vực |. "
        "Nếu major_code rỗng: ghi '—'. "
        "SAU BẢNG: thêm 1 đoạn ngắn giải thích TẠI SAO tính cách này phù hợp với "
        "các ngành đó (dựa vào strengths/work_environment trong node PERSONALITY). "
        "TUYỆT ĐỐI không liệt kê ngành ngoài dữ liệu."
    ),
    ("PERSONALITY", "PERSONALITY"): (
        "Trả lời đầy đủ về loại tính cách MBTI theo 4 phần: "
        "1. MÔ TẢ TỔNG QUAN (description). "
        "2. 4 CHIỀU TÍNH CÁCH (structure: IE/SN/TF/JP — mỗi chiều nêu dimension + description). "
        "3. ĐIỂM MẠNH (strengths) & ĐIỂM YẾU (weaknesses) — dạng bullet. "
        "4. MÔI TRƯỜNG LÀM VIỆC PHÙ HỢP (work_environment). "
        "Sau đó gợi ý xem thêm ngành/nghề phù hợp."
    ),
    ("CAREER", "PERSONALITY"): (
        "PERSONALITY -[:SUITS_CAREER]-> CAREER (chiều ngược). "
        "Liệt kê loại tính cách MBTI phù hợp với nghề/lĩnh vực này. "
        "Kèm mô tả ngắn tại sao phù hợp dựa vào structure/strengths của MBTI type đó. "
        "ĐỊNH DẠNG BẮT BUỘC: bảng markdown | MBTI | Tên tính cách | Lý do phù hợp |. "
        "Sau bảng, thêm đoạn tóm tắt: những đặc điểm tính cách chung của người phù hợp với lĩnh vực này."
    ),
    ("MAJOR", "PERSONALITY"): (
        "PERSONALITY -[:SUITS_MAJOR]-> MAJOR (chiều ngược). "
        "Liệt kê loại tính cách MBTI phù hợp với ngành học hoặc lĩnh vực này tại NEU. "
        "Nếu câu hỏi đề cập lĩnh vực rộng (VD: IT, CNTT), lấy TẤT CẢ personality có "
        "suitable_fields khớp với lĩnh vực đó (field_name chứa từ khóa liên quan). "
        "ĐỊNH DẠNG BẮT BUỘC: bảng markdown | MBTI | Tên tính cách | Lý do phù hợp |. "
        "Sau bảng, thêm đoạn tóm tắt: những đặc điểm tính cách chung của người phù hợp với lĩnh vực này."
    ),
    ("SUBJECT", "PERSONALITY"): (
        "SUBJECT -[:CULTIVATES]-> PERSONALITY (dự phòng). "
        "Nếu không có dữ liệu: thông báo chưa có thông tin tính cách cho môn học này."
    ),
    ("PERSONALITY", "SUBJECT"): (
        "PERSONALITY <-[:CULTIVATES]- SUBJECT (dự phòng). "
        "Nếu không có dữ liệu: thông báo chưa có thông tin."
    ),
}

ANSWER_SYSTEM_BASE = """Bạn là trợ lý tư vấn học thuật cho Đại học Kinh tế Quốc dân (NEU).

{schema}

==================================================
LUẬT TUYỆT ĐỐI:
==================================================
A. CHỈ dùng đúng tên/code/thông tin có trong [DỮ LIỆU GRAPH].
B. TUYỆT ĐỐI KHÔNG thêm kỹ năng, môn học, nghề nghiệp từ kiến thức bên ngoài.
C. TUYỆT ĐỐI KHÔNG liệt kê mục chung chung nếu không có trong [DỮ LIỆU GRAPH].
D. Mọi tên SKILL/SUBJECT/CAREER/MAJOR phải lấy nguyên văn từ [DỮ LIỆU GRAPH].
E. Mọi mã môn (code) phải lấy nguyên văn từ field "code".
F. Nếu [DỮ LIỆU GRAPH] trống → trả lời:
   "Dữ liệu hiện tại chưa đủ để tư vấn về [chủ đề]. Bạn có thể liên hệ phòng đào tạo."
G. TUYỆT ĐỐI KHÔNG liệt kê CAREER node có tên bắt đầu bằng danh hiệu học vị:
   "Cử nhân", "Kỹ sư" (khi là danh hiệu bằng cấp), "Thạc sĩ", "Tiến sĩ", "Bachelor", "Master".
   Đây là kết quả đào tạo, KHÔNG phải vị trí công việc. Bỏ qua hoàn toàn.
H. KHI GỢI Ý / TƯ VẤN "NÊN HỌC MÔN GÌ": TUYỆT ĐỐI KHÔNG đề cập, liệt kê, hay nhắc đến
   các môn đại cương bắt buộc chung sau đây (dù có trong dữ liệu hay không, dù viết hoa/thường/có dấu/không dấu):
     • Triết học Mác-Lênin (LLNL1105)
     • Kinh tế chính trị Mác-Lênin (LLNL1106)
     • Chủ nghĩa xã hội khoa học (LLNL1107)
     • Lịch sử Đảng Cộng sản Việt Nam (LLDL1102)
     • Tư tưởng Hồ Chí Minh (LLTT1101)
     • Giáo dục thể chất (GDTC)
     • Giáo dục quốc phòng và an ninh (GDQP)
     • Kinh tế vi mô 1 (KHMI1101)
     • Kinh tế vĩ mô 1 (KHMA1101)
     • Pháp luật đại cương (LUCS1129)
   Lý do: đây là môn bắt buộc chung mọi ngành, không cần tư vấn riêng.
   NGOẠI LỆ: Nếu người dùng HỎI TRỰC TIẾP "ngành X có học môn Y không?" → trả lời "Có".

ĐỊNH DẠNG ĐẦU RA — BẮT BUỘC TUÂN THỦ:
- Tiếng Việt tự nhiên, thân thiện.
- Khi người dùng phủ định (không giỏi X) → bỏ X khỏi gợi ý.
- KHÔNG hỏi ngược lại người dùng.

1. DANH SÁCH MÔN HỌC / KỸ NĂNG / NGHỀ NGHIỆP → DÙNG BẢNG MARKDOWN:
   Khi liệt kê từ 3 mục trở lên (môn học, kỹ năng, nghề nghiệp,...), BẮT BUỘC trình bày dạng bảng.

   Ví dụ bảng môn học:
   | STT | Tên môn | Mã môn | 
   |-----|---------|--------|
   | 1 | Toán rời rạc | TOCB1107 |

   Ví dụ bảng kỹ năng:
   | STT | Kỹ năng | Loại | 
   |-----|---------|------|
   | 1 | Lập trình Python | Kỹ năng chuyên môn | 

   Ví dụ bảng ngành học (đề xuất ngành):
   | STT | Tên ngành | Mã ngành | Môn học liên quan |
   |-----|-----------|----------|-------------------|
   | 1 | Công nghệ thông tin | 7480201 | Lập trình Python (ITBD2301) |

   Ví dụ bảng nghề nghiệp:
   | STT | Tên nghề |
   |-----|----------|
   | 1 | Kỹ sư phần mềm |

   Chọn cột phù hợp với dữ liệu thực có trong [DỮ LIỆU GRAPH]. Bỏ cột nếu không có dữ liệu.

2. THÔNG TIN CHI TIẾT (mô tả ngành, nghề, môn học) → DÙNG BULLET / NUMBERING:
   • Dùng chữ IN HOA cho tiêu đề mục (VD: MỤC TIÊU ĐÀO TẠO, CÔNG VIỆC CHÍNH).
   • Dùng ký tự • ở đầu dòng cho từng ý trong mỗi mục.
   • Dùng số thứ tự (1. 2. 3.) khi liệt kê các bước hoặc thứ tự ưu tiên.
   • Ví dụ:
     KỸ NĂNG YÊU CẦU:
     • Lập trình Python (hard skill, trung cấp)
     • Phân tích dữ liệu (hard skill, nâng cao)

3. CÂU TRẢ LỜI NGẮN (dưới 3 mục, hỏi thông tin đơn giản) → VĂN XUÔI BÌNH THƯỜNG.
   - Môn học: "Tên môn (mã môn)" — VD: "Toán rời rạc (TOCB1107)".
   - Ngành: "Tên ngành (mã ngành)" — VD: "Công nghệ thông tin (7480201)".

4. KẾT THÚC CÂU TRẢ LỜI: Thêm 1 dòng tóm tắt hoặc gợi ý tiếp theo nếu phù hợp.

SỬ DỤNG THUỘC TÍNH MỞ RỘNG KHI CÓ:
• SUBJECT:      dùng course_description, courses_goals khi hỏi nội dung môn học.
• CAREER:       dùng description, job_tasks, market khi hỏi về nghề nghiệp.
• MAJOR:        dùng philosophy_and_objectives, learning_outcomes khi hỏi về ngành.
• PERSONALITY:  dùng code (MBTI type), description (mô tả tổng quan), structure (4 chiều IE/SN/TF/JP), strengths/weaknesses, work_environment. Trường suitable_fields là JSON string → parse để lấy field_name, group_name, major_name, major_code, careers.
• Nếu field là JSON string → parse và trình bày ngắn gọn phần liên quan dùng ký tự •.

RÀNG BUỘC THEO LOẠI CÂU HỎI:
{constraint}

CỘNG ĐỒNG ĐÃ ĐƯỢC ĐỊNH TUYẾN:
{community_context}
"""

_MBTI_PATTERN = re.compile(
    r"\b(INTJ|INTP|ENTJ|ENTP|INFJ|INFP|ENFJ|ENFP"
    r"|ISTJ|ISFJ|ESTJ|ESFJ|ISTP|ISFP|ESTP|ESFP)\b",
    re.IGNORECASE,
)

# Map MBTI code → keywords để mở rộng query khi DB chưa có SUITS_MAJOR/SUITS_CAREER
# (fallback khi graph traversal không tìm được gì qua edge trực tiếp)
MBTI_KEYWORD_FALLBACK: dict[str, list[str]] = {
    "INTJ": ["chiến lược", "phân tích", "độc lập", "tầm nhìn"],
    "INTP": ["phân tích", "logic", "nghiên cứu", "lý luận"],
    "ENTJ": ["lãnh đạo", "chiến lược", "quyết đoán", "quản lý"],
    "ENTP": ["sáng tạo", "đổi mới", "lập luận", "linh hoạt"],
    "INFJ": ["đồng cảm", "tầm nhìn", "sáng tạo", "kiên nhẫn"],
    "INFP": ["sáng tạo", "đồng cảm", "lý tưởng", "linh hoạt"],
    "ENFJ": ["lãnh đạo", "đồng cảm", "giao tiếp", "tổ chức"],
    "ENFP": ["sáng tạo", "nhiệt huyết", "giao tiếp", "linh hoạt"],
    "ISTJ": ["kỷ luật", "cẩn thận", "trách nhiệm", "tổ chức"],
    "ISFJ": ["đồng cảm", "kiên nhẫn", "cẩn thận", "hỗ trợ"],
    "ESTJ": ["tổ chức", "kỷ luật", "lãnh đạo", "quyết đoán"],
    "ESFJ": ["giao tiếp", "đồng cảm", "hỗ trợ", "tổ chức"],
    "ISTP": ["phân tích", "thực tế", "kỹ thuật", "linh hoạt"],
    "ISFP": ["sáng tạo", "thực tế", "đồng cảm", "linh hoạt"],
    "ESTP": ["năng động", "thực tế", "quyết đoán", "lãnh đạo"],
    "ESFP": ["năng động", "giao tiếp", "linh hoạt", "thực tế"],
}


def expand_mbti(question: str) -> tuple[str, list[str]]:
    """
    Nhận diện MBTI code tường minh (INTJ, ESTP...) trong câu hỏi.
    Trả về (expanded_question, [mbti_code]) để query trực tiếp PERSONALITY node.
    """
    m = _MBTI_PATTERN.search(question)
    if not m:
        return question, []
    mbti_code = m.group(1).upper()
    hint = f"[GHI CHÚ: {mbti_code} là loại tính cách MBTI]"
    return question + "  " + hint, [mbti_code]


ABBREVIATION_MAP: dict[str, list[str]] = {
    "da":   ["data analyst", "phân tích dữ liệu"],
    "de":   ["data engineer", "kỹ sư dữ liệu"],
    "ds":   ["data scientist", "khoa học dữ liệu"],
    "data analyst":     ["phân tích dữ liệu", "chuyên viên phân tích dữ liệu"],
    "data engineer":    ["kỹ sư dữ liệu"],
    "data scientist":   ["khoa học dữ liệu", "nhà khoa học dữ liệu"],
    "data engineering": ["kỹ sư dữ liệu"],
    "data analysis":    ["phân tích dữ liệu"],
    "ba":   ["business analyst", "phân tích kinh doanh"],
    "pm":   ["project manager", "quản lý dự án"],
    "po":   ["product owner"],
    "qa":   ["kiểm thử", "quality assurance"],
    "dev":  ["lập trình viên", "developer"],
    "fe":   ["front end", "lập trình viên frontend"],
    "be":   ["back end", "lập trình viên backend"],
    "ml":   ["machine learning", "học máy"],
    "ai":   ["trí tuệ nhân tạo", "artificial intelligence"],
    "cntt": ["công nghệ thông tin"],
    "ktpm": ["kỹ thuật phần mềm"],
    "httt": ["hệ thống thông tin"],
    "qtkd": ["quản trị kinh doanh"],
    "tcnh": ["tài chính ngân hàng"],
    "kt":   ["kế toán", "kinh tế"],
    "mkt":  ["marketing"],
    "hr":   ["quản trị nhân lực", "nhân sự"],
    "mis":  ["hệ thống thông tin quản lý", "management information systems"],
    "fintech": ["công nghệ tài chính"],
    "ecom": ["thương mại điện tử"],
    "acct": ["kế toán"],
}


def expand_abbreviations(question: str) -> tuple[str, list[str]]:
    q_lower  = question.lower()
    expanded = question
    extras   = []
    found    = {}

    for abbrev, expansions in ABBREVIATION_MAP.items():
        if len(abbrev) <= 3:
            pat = r"(?<![\w\u00C0-\u024F])" + re.escape(abbrev.upper()) + r"(?![\w\u00C0-\u024F])"
            if not re.search(pat, question, re.UNICODE):
                continue
        pattern = r"(?<![\w\u00C0-\u024F])" + re.escape(abbrev) + r"(?![\w\u00C0-\u024F])"
        if re.search(pattern, q_lower, re.IGNORECASE | re.UNICODE):
            found[abbrev] = expansions
            extras.extend(expansions)

    if found:
        hints    = "; ".join(f"{k.upper()} = {' / '.join(v)}" for k, v in found.items())
        expanded = question + f"  [GHI CHÚ: {hints}]"

    return expanded, extras


# ══════════════════════════════════════════════════════════════════════════════
# PHẦN 6: INTENT EXTRACTION
# ══════════════════════════════════════════════════════════════════════════════

def extract_query_intent(ai_client: OpenAI, question: str) -> dict:
    system_msg = (
        "Bạn phân tích câu hỏi tư vấn học thuật và trả về JSON.\n"
        "Schema Node labels: MAJOR, SUBJECT, SKILL, CAREER, TEACHER, PERSONALITY\n\n"
        "Chuẩn hóa keyword:\n"
        "  data analyst/DA → phân tích dữ liệu, data analyst\n"
        "  business analyst/BA → phân tích kinh doanh\n"
        "  CNTT/IT → công nghệ thông tin\n"
        "  KTPM → kỹ thuật phần mềm | HTTT → hệ thống thông tin\n"
        "  developer/DEV → lập trình viên | tester/QA → kiểm thử\n\n"
        "Quy tắc xác định asked_label:\n"
        "  - Hỏi thông tin môn học (mô tả, mã môn, nội dung, kế hoạch giảng dạy) → asked=SUBJECT\n"
        "  - Hỏi thông tin nghề nghiệp (mô tả nghề, công việc, thị trường lao động, triển vọng, cơ hội nghề nghiệp) → asked=CAREER\n"
        "  - Hỏi thông tin giảng viên (email, học hàm, dạy môn gì) → asked=TEACHER\n"
        "  - Hỏi thông tin ngành học (chương trình, chuẩn đầu ra, mục tiêu) → asked=MAJOR\n"
        "  - Hỏi kỹ năng → asked=SKILL\n"
        "  - Hỏi về loại tính cách MBTI, personality fit, đặc điểm tính cách → asked=PERSONALITY\n"
        "  - Nếu đề cập tính cách/MBTI nhưng hỏi về nghề → mentioned=PERSONALITY, asked=CAREER\n"
        "  - Nếu đề cập tính cách/MBTI nhưng hỏi về ngành → mentioned=PERSONALITY, asked=MAJOR\n"
        "  - Keywords: luôn giữ nguyên MBTI code (ESTP, ENTP...) nếu có\n\n"
        "──────────────────────────────────────────\n"
        "TRƯỜNG ĐẶC BIỆT: mbti_dimensions\n"
        "──────────────────────────────────────────\n"
        "Nếu câu hỏi mô tả đặc điểm tính cách bằng từ ngữ tự nhiên (KHÔNG phải MBTI code),\n"
        "hãy suy luận các MBTI dimension letters phù hợp:\n\n"
        "  4 cặp dimension:\n"
        "    E / I  — năng lượng:  hướng ngoại (E) vs hướng nội, điềm tĩnh, kín đáo, suy tư (I)\n"
        "    S / N  — nhận thức:   thực tế, chi tiết, quy trình (S) vs sáng tạo, tầm nhìn, trực giác (N)\n"
        "    T / F  — quyết định:  logic, phân tích, lý trí (T) vs đồng cảm, ấm áp, cảm xúc (F)\n"
        "    J / P  — lối sống:    kế hoạch, ngăn nắp, kỷ luật (J) vs linh hoạt, ngẫu hứng, tự do (P)\n\n"
        "  Quy tắc:\n"
        "  - Chỉ trả về dimension mà câu hỏi có dấu hiệu rõ ràng. Không đoán mò.\n"
        "  - Nếu câu hỏi có cả 2 chiều đối lập (E lẫn I), bỏ cả 2, không trả về dimension đó.\n"
        "  - Nếu câu hỏi có MBTI code tường minh (INTJ, ESTP...), để mbti_dimensions = []\n"
        "    và đưa code đó vào keywords thay vào đó.\n"
        "  - Nếu không có dấu hiệu tính cách nào, để mbti_dimensions = [].\n\n"
        "  Ví dụ:\n"
        "  'Em hướng nội thì học ngành gì'        → mbti_dimensions: ['I']\n"
        "  'Người logic và kỷ luật hợp nghề gì'   → mbti_dimensions: ['T', 'J']\n"
        "  'Tôi sáng tạo, thích tầm nhìn xa'      → mbti_dimensions: ['N']\n"
        "  'Tôi vừa hướng nội vừa hướng ngoại'    → mbti_dimensions: []  (xung đột)\n"
        "  'Tôi là INTJ học ngành gì'              → mbti_dimensions: [], keywords: ['INTJ']\n\n"
        "Trả về JSON:\n"
        "{\n"
        '  "keywords": ["tên thực thể để tìm trong KG"],\n'
        '  "mentioned_labels": ["MAJOR|SUBJECT|SKILL|CAREER|TEACHER|PERSONALITY"],\n'
        '  "asked_label": "MAJOR|SUBJECT|SKILL|CAREER|TEACHER|PERSONALITY|UNKNOWN",\n'
        '  "negated_keywords": ["thực thể bị phủ định"],\n'
        '  "is_comparison": false,\n'
        '  "mbti_dimensions": ["I","T"]  // các dimension letters được suy luận, hoặc []\n'
        "}\n"
    )
    response = ai_client.chat.completions.create(
        model=OPENAI_MODEL,
        messages=[
            {"role": "system", "content": system_msg},
            {"role": "user",   "content": f"Phân tích: {question}"},
        ],
        temperature=0,
        response_format={"type": "json_object"},
    )
    parsed = json.loads(response.choices[0].message.content)
    return {
        "keywords":         parsed.get("keywords", []),
        "mentioned_labels": parsed.get("mentioned_labels", []),
        "asked_label":      parsed.get("asked_label", "UNKNOWN"),
        "negated_keywords": parsed.get("negated_keywords", []),
        "is_comparison":    parsed.get("is_comparison", False),
        "mbti_dimensions":  [
            d for d in parsed.get("mbti_dimensions", [])
            if d in ("E", "I", "S", "N", "T", "F", "J", "P")
        ],
    }


def resolve_mbti_codes_from_dimensions(dimensions: list[str]) -> list[str]:
    """
    Từ list dimension letters (e.g. ['I', 'T']) trả về tất cả MBTI codes
    chứa TẤT CẢ các dimensions đó.

    Ví dụ:
      ['I']       → [INTJ, INTP, INFJ, INFP, ISTJ, ISFJ, ISTP, ISFP]
      ['I', 'T']  → [INTJ, INTP, ISTJ, ISTP]
      ['T', 'J']  → [INTJ, ISTJ, ENTJ, ESTJ]
    """
    if not dimensions:
        return []
    all_types = [
        "INTJ","INTP","ENTJ","ENTP","INFJ","INFP","ENFJ","ENFP",
        "ISTJ","ISFJ","ESTJ","ESFJ","ISTP","ISFP","ESTP","ESFP",
    ]
    required = set(dimensions)
    return [t for t in all_types if required.issubset(set(t))]


# ══════════════════════════════════════════════════════════════════════════════
# PHẦN 6b: INTENT POST-PROCESSING RULES
# ══════════════════════════════════════════════════════════════════════════════

_COMPARE_CUE_PATTERN = re.compile(
    r"\b(vs|versus)\b|so sánh|phân vân|giữa.+và|nên chọn bên nào|nên chọn cái nào",
    re.IGNORECASE | re.UNICODE,
)
_CAREER_CUE_PATTERN = re.compile(
    r"nghề nào|làm gì|ra trường làm gì|nên chọn nghề|nên theo nghề|hợp làm|hợp nghề",
    re.IGNORECASE | re.UNICODE,
)
_ASK_PERSONALITY_PATTERN = re.compile(
    r"tính cách (gì|nào)|mbti (gì|nào)|loại tính cách",
    re.IGNORECASE | re.UNICODE,
)
_MAJOR_CUE_PATTERN = re.compile(
    r"\bngành\b|chuyên ngành|chương trình đào tạo|học ngành",
    re.IGNORECASE | re.UNICODE,
)
_SKILL_CUE_PATTERN = re.compile(
    r"\bsql\b|database|cơ sở dữ liệu|dữ liệu|data",
    re.IGNORECASE | re.UNICODE,
)
_NEGATED_CAREER_PATTERN = re.compile(
    r"(?:không|ko|chẳng|không muốn).{0,20}\b(sale|marketing)\b",
    re.IGNORECASE | re.UNICODE,
)
_CAREER_ALIAS_HINTS: list[tuple[re.Pattern, list[str]]] = [
    (re.compile(r"\b(tester|qa|quality assurance|kiểm thử)\b", re.IGNORECASE | re.UNICODE),
     ["kiểm thử", "tester", "quality assurance"]),
    (re.compile(r"\b(developer|dev|lập trình viên)\b", re.IGNORECASE | re.UNICODE),
     ["lập trình viên", "developer"]),
]
_DOMAIN_HINTS: list[tuple[re.Pattern, list[str], list[str]]] = [
    (re.compile(r"\b(cntt|it|công nghệ thông tin)\b", re.IGNORECASE | re.UNICODE),
     ["công nghệ thông tin"], ["MAJOR"]),
    (re.compile(r"\b(database|cơ sở dữ liệu)\b", re.IGNORECASE | re.UNICODE),
     ["database"], ["SKILL"]),
    (re.compile(r"\bsql\b", re.IGNORECASE | re.UNICODE),
     ["sql"], ["SKILL"]),
]

# Map pattern → field_name để inject field_context vào intent
# Dùng khi câu hỏi là "tính cách gì hợp làm X" → cần filter PERSONALITY theo lĩnh vực X
_FIELD_CONTEXT_HINTS: list[tuple[re.Pattern, str]] = [
    (re.compile(r"\b(cntt|it|công nghệ thông tin|lập trình|phần mềm|kỹ thuật phần mềm|hệ thống thông tin)\b",
                re.IGNORECASE | re.UNICODE), "Công nghệ thông tin"),
    (re.compile(r"\b(kinh tế|tài chính|kế toán|ngân hàng|kinh doanh|quản trị|marketing)\b",
                re.IGNORECASE | re.UNICODE), "Kinh tế - Quản trị"),
    (re.compile(r"\b(data|dữ liệu|phân tích dữ liệu|khoa học dữ liệu)\b",
                re.IGNORECASE | re.UNICODE), "Khoa học dữ liệu"),
    (re.compile(r"\b(giáo dục|sư phạm|giảng dạy|đào tạo)\b",
                re.IGNORECASE | re.UNICODE), "Giáo dục"),
    (re.compile(r"\b(y tế|bác sĩ|y khoa|dược|chăm sóc sức khỏe)\b",
                re.IGNORECASE | re.UNICODE), "Y tế - Sức khỏe"),
]


def _unique_keep_order(values: list[str]) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for v in values:
        sv = str(v).strip()
        if not sv:
            continue
        key = sv.lower()
        if key in seen:
            continue
        seen.add(key)
        out.append(sv)
    return out


def apply_intent_rules(question: str, intent: dict) -> dict:
    """
    Hậu xử lý intent để ổn định cho câu hỏi dài/ngữ cảnh mơ hồ.
    Không thay thế LLM, chỉ vá các case hay sai:
    - So sánh nghề (tester vs developer...)
    - Câu có tính cách + skill nhưng chưa hỏi rõ ngành/nghề
    - Câu hỏi "tính cách gì hợp làm IT"
    """
    q = question.strip()
    q_lower = q.lower()

    keywords = _unique_keep_order([*intent.get("keywords", [])])
    mentioned = [str(x).strip().upper() for x in intent.get("mentioned_labels", []) if str(x).strip()]
    mentioned = _unique_keep_order(mentioned)
    asked = str(intent.get("asked_label", "UNKNOWN")).upper()
    negated = _unique_keep_order([*intent.get("negated_keywords", [])])
    is_comp = bool(intent.get("is_comparison", False))

    # Inject thêm keyword/label domain.
    for pat, kws, labels in _DOMAIN_HINTS:
        if pat.search(q):
            keywords.extend(kws)
            mentioned.extend(labels)

    # Nhận diện job aliases để tăng recall query nghề.
    has_direct_career_alias = False
    for pat, kws in _CAREER_ALIAS_HINTS:
        if pat.search(q):
            has_direct_career_alias = True
            keywords.extend(kws)
            if "CAREER" not in mentioned:
                mentioned.append("CAREER")

    # Bổ sung phủ định nghề phổ biến (sale/marketing) khi LLM bỏ sót.
    for m in _NEGATED_CAREER_PATTERN.finditer(q):
        neg_kw = m.group(1).strip()
        if neg_kw not in negated:
            negated.append(neg_kw)

    # Phát hiện so sánh
    if _COMPARE_CUE_PATTERN.search(q):
        is_comp = True

    has_personality_signal = "PERSONALITY" in mentioned or _PERSONALITY_KW_PATTERN.search(q)

    # Rule 1: câu hỏi ngược "tính cách gì hợp làm/học X"
    if _ASK_PERSONALITY_PATTERN.search(q):
        asked = "PERSONALITY"
        # Inject field_context nếu câu hỏi đề cập lĩnh vực cụ thể (IT, kinh tế,...)
        for pat, field_name in _FIELD_CONTEXT_HINTS:
            if pat.search(q):
                intent["field_context"] = field_name
                break

    # Rule 2: so sánh giữa 2 nghề => asked = CAREER.
    if is_comp and has_direct_career_alias:
        asked = "CAREER"

    # Rule 3: câu mơ hồ nhưng có dấu hiệu nghề + tính cách/skill => hỏi nghề.
    if asked == "UNKNOWN":
        if _CAREER_CUE_PATTERN.search(q):
            asked = "CAREER"
        elif has_personality_signal and (_SKILL_CUE_PATTERN.search(q) or bool(negated)):
            asked = "CAREER"
        elif _MAJOR_CUE_PATTERN.search(q):
            asked = "MAJOR"

    # Rule 4: nếu user vừa nói personality vừa hỏi nghề/ngành, ưu tiên theo câu hỏi.
    # NGOẠI LỆ QUAN TRỌNG: Nếu câu hỏi hỏi rõ "tính cách gì/nào" → GIỮ NGUYÊN asked=PERSONALITY
    if asked == "PERSONALITY":
        is_asking_personality_explicitly = bool(_ASK_PERSONALITY_PATTERN.search(q))
        if is_asking_personality_explicitly:
            pass  # Giữ asked=PERSONALITY, chỉ inject field_context để biết cần filter theo lĩnh vực
        elif _CAREER_CUE_PATTERN.search(q) and "CAREER" in mentioned:
            asked = "CAREER"
        elif _MAJOR_CUE_PATTERN.search(q) and "MAJOR" in mentioned and "CAREER" not in mentioned:
            asked = "MAJOR"

    # Sắp thứ tự mentioned để targeted query đi đúng hướng hơn.
    if asked == "CAREER":
        if is_comp:
            priority = ["CAREER", "SKILL", "MAJOR", "PERSONALITY", "SUBJECT", "TEACHER"]
        else:
            priority = ["SKILL", "MAJOR", "PERSONALITY", "CAREER", "SUBJECT", "TEACHER"]
    elif asked == "MAJOR":
        priority = ["PERSONALITY", "SKILL", "CAREER", "MAJOR", "SUBJECT", "TEACHER"]
    elif asked == "PERSONALITY":
        if re.search(r"hợp làm|hợp nghề|làm\s+\w+", q_lower, re.IGNORECASE):
            priority = ["CAREER", "MAJOR", "PERSONALITY", "SKILL", "SUBJECT", "TEACHER"]
        else:
            priority = ["MAJOR", "CAREER", "PERSONALITY", "SKILL", "SUBJECT", "TEACHER"]
    else:
        priority = ["PERSONALITY", "SKILL", "MAJOR", "CAREER", "SUBJECT", "TEACHER"]

    mentioned_set = set(mentioned)
    mentioned = [lbl for lbl in priority if lbl in mentioned_set]
    mentioned.extend([lbl for lbl in mentioned_set if lbl not in mentioned])

    intent["keywords"] = _unique_keep_order(keywords)
    intent["mentioned_labels"] = mentioned
    intent["asked_label"] = asked if asked in {"MAJOR", "SUBJECT", "SKILL", "CAREER", "TEACHER", "PERSONALITY", "UNKNOWN"} else "UNKNOWN"
    intent["negated_keywords"] = _unique_keep_order(negated)
    intent["is_comparison"] = is_comp
    return intent


def get_relationship_constraint(intent: dict) -> str:
    mentioned = intent.get("mentioned_labels", [])
    asked     = intent.get("asked_label", "UNKNOWN")
    is_comp   = intent.get("is_comparison", False)

    if is_comp and "MAJOR" in mentioned:
        return RELATIONSHIP_CONSTRAINTS.get(("MAJOR", "MAJOR"), "")

    # So sánh 2 nghề
    if is_comp and (asked == "CAREER" or "CAREER" in mentioned):
        return (
            "So sánh 2 nghề nghiệp được nêu trong câu hỏi dựa trên dữ liệu graph. "
            "BẮT BUỘC gồm 4 phần: "
            "1) Mô tả ngắn từng nghề (description/role). "
            "2) Công việc chính từng nghề (job_tasks). "
            "3) Cơ hội/triển vọng (market). "
            "4) Ngành học đề xuất cho từng nghề (major_codes hoặc recommended_majors trong DB). "
            "Cuối cùng kết luận nên ưu tiên nghề nào dựa trên tính cách/ưu tiên user nêu trong câu hỏi. "
            "Tuyệt đối không dùng kiến thức ngoài graph."
        )

    for m in ([mentioned[0]] if mentioned else []) + mentioned:
        key = (m, asked)
        if key in RELATIONSHIP_CONSTRAINTS:
            return RELATIONSHIP_CONSTRAINTS[key]

    # Self-query fallback
    if asked != "UNKNOWN":
        self_key = (asked, asked)
        if self_key in RELATIONSHIP_CONSTRAINTS:
            return RELATIONSHIP_CONSTRAINTS[self_key]

    return "Trả lời theo đúng câu hỏi, chỉ dùng dữ liệu trong Knowledge Graph."


# ══════════════════════════════════════════════════════════════════════════════
# PHẦN 7: COMMUNITY-AWARE TRAVERSAL
# ══════════════════════════════════════════════════════════════════════════════

# Extended props được fetch từ DB và đưa vào context cho LLM
EXTENDED_PROPS: dict[str, list[str]] = {
    "SUBJECT": [
        "course_description", "courses_goals", "assessment",
        "learning_resources", "course_requirements_and_expectations",
    ],
    "CAREER": [
        "description", "job_tasks", "field_name", "market",
    ],
    "MAJOR": [
        "philosophy_and_objectives", "admission_requirements",
        "learning_outcomes", "curriculum_structure_and_content",
    ],
    "TEACHER":     ["email", "title"],
    "SKILL":       ["skill_type"],
    "PERSONALITY": [
        "code", "description", "structure",
        "strengths", "weaknesses", "work_environment", "suitable_fields",
    ],
}

# Targeted Queries — trả về các columns chuẩn: name, label, code, rel_types, node_names, hops
# + extended cols: course_description, semester, required_type
TARGETED_QUERIES: dict[tuple[str, str], str] = {

    # ── Academic ──────────────────────────────────────────────────────────────
    ("MAJOR", "SUBJECT"): """
        MATCH (start:MAJOR)-[r:MAJOR_OFFERS_SUBJECT]->(n:SUBJECT)
        WHERE toLower(start.name) CONTAINS toLower($kw) OR start.code = $kw
        RETURN n.name AS name, labels(n)[0] AS label, n.code AS code,
               ['MAJOR_OFFERS_SUBJECT'] AS rel_types,
               [start.name, n.name] AS node_names,
               1 AS hops,
               r.semester AS semester,
               r.required_type AS required_type,
               n.course_description AS course_description
        ORDER BY r.required_type DESC, r.semester ASC, n.name ASC
        LIMIT 100
    """,
    ("MAJOR", "TEACHER"): """
        MATCH (n:TEACHER)-[:TEACH]->(sub:SUBJECT)<-[:MAJOR_OFFERS_SUBJECT]-(start:MAJOR)
        WHERE toLower(start.name) CONTAINS toLower($kw) OR start.code = $kw
        RETURN n.name AS name, labels(n)[0] AS label, null AS code,
               ['MAJOR_OFFERS_SUBJECT','TEACH'] AS rel_types,
               [start.name, sub.name, n.name] AS node_names,
               2 AS hops,
               null AS semester, null AS required_type, null AS course_description
        ORDER BY n.name LIMIT 50
    """,
    ("TEACHER", "SUBJECT"): """
        MATCH (start:TEACHER)-[:TEACH]->(n:SUBJECT)
        WHERE toLower(start.name) CONTAINS toLower($kw)
           OR toLower(start.teacher_key) CONTAINS toLower($kw)
        RETURN n.name AS name, labels(n)[0] AS label, n.code AS code,
               ['TEACH'] AS rel_types, [start.name, n.name] AS node_names,
               1 AS hops,
               null AS semester, null AS required_type,
               n.course_description AS course_description
        ORDER BY n.name LIMIT 50
    """,
    ("TEACHER", "MAJOR"): """
        MATCH (start:TEACHER)-[:TEACH]->(sub:SUBJECT)<-[:MAJOR_OFFERS_SUBJECT]-(n:MAJOR)
        WHERE toLower(start.name) CONTAINS toLower($kw)
           OR toLower(start.teacher_key) CONTAINS toLower($kw)
        RETURN n.name AS name, labels(n)[0] AS label, n.code AS code,
               ['TEACH','MAJOR_OFFERS_SUBJECT'] AS rel_types,
               [start.name, sub.name, n.name] AS node_names,
               2 AS hops,
               null AS semester, null AS required_type, null AS course_description
        ORDER BY n.name LIMIT 50
    """,
    ("SUBJECT", "TEACHER"): """
        MATCH (n:TEACHER)-[:TEACH]->(start:SUBJECT)
        WHERE toLower(start.name) CONTAINS toLower($kw) OR start.code = $kw
        RETURN n.name AS name, labels(n)[0] AS label, null AS code,
               ['TEACH'] AS rel_types, [n.name, start.name] AS node_names,
               1 AS hops,
               null AS semester, null AS required_type, null AS course_description
        ORDER BY n.name LIMIT 50
    """,
    # Self: thông tin chi tiết môn học + môn tiên quyết
    ("SUBJECT", "SUBJECT"): """
        MATCH (start:SUBJECT)
        WHERE toLower(start.name) CONTAINS toLower($kw) OR start.code = $kw
        RETURN start.name AS name, labels(start)[0] AS label, start.code AS code,
               [] AS rel_types, [start.name] AS node_names, 0 AS hops,
               null AS semester, null AS required_type,
               start.course_description AS course_description
        ORDER BY start.name LIMIT 10
        UNION
        MATCH (start:SUBJECT)-[:PREREQUISITE_FOR]->(n:SUBJECT)
        WHERE toLower(start.name) CONTAINS toLower($kw) OR start.code = $kw
        RETURN n.name AS name, labels(n)[0] AS label, n.code AS code,
               ['PREREQUISITE_FOR'] AS rel_types,
               [start.name, n.name] AS node_names, 1 AS hops,
               null AS semester, null AS required_type, null AS course_description
        ORDER BY n.name LIMIT 30
    """,
    # Self: thông tin giảng viên
    ("TEACHER", "TEACHER"): """
        MATCH (start:TEACHER)
        WHERE toLower(start.name) CONTAINS toLower($kw)
           OR toLower(start.teacher_key) CONTAINS toLower($kw)
        RETURN start.name AS name, labels(start)[0] AS label, null AS code,
               [] AS rel_types, [start.name] AS node_names, 0 AS hops,
               null AS semester, null AS required_type, null AS course_description
        ORDER BY start.name LIMIT 10
    """,

    # ── Career cluster ────────────────────────────────────────────────────────
    ("MAJOR", "CAREER"): """
        MATCH (start:MAJOR)-[:LEADS_TO]->(n:CAREER)
        WHERE toLower(start.name) CONTAINS toLower($kw) OR start.code = $kw
        RETURN n.name AS name, labels(n)[0] AS label, null AS code,
               ['LEADS_TO'] AS rel_types, [start.name, n.name] AS node_names,
               1 AS hops,
               null AS semester, null AS required_type, null AS course_description
        ORDER BY n.name LIMIT 50
    """,
    ("CAREER", "SKILL"): """
        MATCH (start:CAREER)-[:REQUIRES]->(n:SKILL)
        WHERE toLower(start.name) CONTAINS toLower($kw)
           OR toLower(start.career_key) CONTAINS toLower($kw)
        RETURN n.name AS name, labels(n)[0] AS label, null AS code,
               ['REQUIRES'] AS rel_types, [start.name, n.name] AS node_names,
               1 AS hops,
               null AS semester, null AS required_type, null AS course_description
        ORDER BY n.name LIMIT 50
    """,
    ("CAREER", "SUBJECT"): """
        MATCH (start:CAREER)-[:REQUIRES]->(sk:SKILL)<-[:PROVIDES]-(n:SUBJECT)
        WHERE toLower(start.name) CONTAINS toLower($kw)
           OR toLower(start.career_key) CONTAINS toLower($kw)
        OPTIONAL MATCH (m:MAJOR)-[:MAJOR_OFFERS_SUBJECT]->(n)
        WHERE m.code IN start.major_codes
        WITH start, sk, n,
             count(DISTINCT m) AS major_match,
             size([(s2:SUBJECT)-[:PROVIDES]->(sk) | s2]) AS skill_breadth
        ORDER BY major_match DESC, skill_breadth ASC, n.name ASC
        RETURN n.name AS name, labels(n)[0] AS label, n.code AS code,
               ['REQUIRES','PROVIDES'] AS rel_types,
               [start.name, sk.name, n.name] AS node_names,
               2 AS hops,
               null AS semester, null AS required_type,
               n.course_description AS course_description
        LIMIT 30
    """,
    ("CAREER", "MAJOR"): """
        MATCH (n:MAJOR)-[:LEADS_TO]->(start:CAREER)
        WHERE toLower(start.name) CONTAINS toLower($kw)
           OR toLower(start.career_key) CONTAINS toLower($kw)
        RETURN n.name AS name, labels(n)[0] AS label, n.code AS code,
               ['LEADS_TO'] AS rel_types, [n.name, start.name] AS node_names,
               1 AS hops,
               null AS semester, null AS required_type, null AS course_description
        ORDER BY n.name LIMIT 50
    """,
    ("MAJOR", "SKILL"): """
        MATCH (start:MAJOR)-[:MAJOR_OFFERS_SUBJECT]->(sub:SUBJECT)-[:PROVIDES]->(n:SKILL)
        WHERE toLower(start.name) CONTAINS toLower($kw) OR start.code = $kw
        RETURN n.name AS name, labels(n)[0] AS label, null AS code,
               ['MAJOR_OFFERS_SUBJECT','PROVIDES'] AS rel_types,
               [start.name, sub.name, n.name] AS node_names,
               2 AS hops,
               null AS semester, null AS required_type, null AS course_description
        ORDER BY n.name LIMIT 50
    """,
    ("SKILL", "MAJOR"): """
        MATCH (n:MAJOR)-[:MAJOR_OFFERS_SUBJECT]->(sub:SUBJECT)-[:PROVIDES]->(start:SKILL)
        WHERE toLower(start.name) CONTAINS toLower($kw)
           OR toLower(start.skill_key) CONTAINS toLower($kw)
        RETURN n.name AS name, labels(n)[0] AS label, n.code AS code,
               ['MAJOR_OFFERS_SUBJECT','PROVIDES'] AS rel_types,
               [n.name, sub.name, start.name] AS node_names,
               2 AS hops,
               null AS semester, null AS required_type, null AS course_description
        ORDER BY n.name LIMIT 50
    """,
    ("SKILL", "CAREER"): """
        MATCH (n:CAREER)-[:REQUIRES]->(start:SKILL)
        WHERE toLower(start.name) CONTAINS toLower($kw)
           OR toLower(start.skill_key) CONTAINS toLower($kw)
        RETURN n.name AS name, labels(n)[0] AS label, null AS code,
               ['REQUIRES'] AS rel_types, [n.name, start.name] AS node_names,
               1 AS hops,
               null AS semester, null AS required_type, null AS course_description
        ORDER BY n.name LIMIT 50
    """,
    ("SKILL", "SUBJECT"): """
        MATCH (n:SUBJECT)-[:PROVIDES]->(start:SKILL)
        WHERE toLower(start.name) CONTAINS toLower($kw)
           OR toLower(start.skill_key) CONTAINS toLower($kw)
        RETURN n.name AS name, labels(n)[0] AS label, n.code AS code,
               ['PROVIDES'] AS rel_types, [n.name, start.name] AS node_names,
               1 AS hops,
               null AS semester, null AS required_type,
               n.course_description AS course_description
        ORDER BY n.name LIMIT 50
    """,
    ("SUBJECT", "SKILL"): """
        MATCH (start:SUBJECT)-[:PROVIDES]->(n:SKILL)
        WHERE toLower(start.name) CONTAINS toLower($kw) OR start.code = $kw
        RETURN n.name AS name, labels(n)[0] AS label, null AS code,
               ['PROVIDES'] AS rel_types, [start.name, n.name] AS node_names,
               1 AS hops,
               null AS semester, null AS required_type, null AS course_description
        ORDER BY n.name LIMIT 50
    """,
    ("SUBJECT", "CAREER"): """
        MATCH (start:SUBJECT)-[:PROVIDES]->(sk:SKILL)<-[:REQUIRES]-(n:CAREER)
        WHERE toLower(start.name) CONTAINS toLower($kw) OR start.code = $kw
        RETURN n.name AS name, labels(n)[0] AS label, null AS code,
               ['PROVIDES','REQUIRES'] AS rel_types,
               [start.name, sk.name, n.name] AS node_names,
               2 AS hops,
               null AS semester, null AS required_type, null AS course_description
        ORDER BY n.name LIMIT 50
    """,
    # Self: thông tin chi tiết nghề nghiệp + ngành học đề xuất qua major_codes
    ("CAREER", "CAREER"): """
        MATCH (start:CAREER)
        WHERE toLower(start.name) CONTAINS toLower($kw)
           OR toLower(start.career_key) CONTAINS toLower($kw)
        RETURN start.name AS name, labels(start)[0] AS label, null AS code,
               [] AS rel_types, [start.name] AS node_names, 0 AS hops,
               null AS semester, null AS required_type, null AS course_description
        ORDER BY start.name LIMIT 10
        UNION
        MATCH (start:CAREER)
        WHERE toLower(start.name) CONTAINS toLower($kw)
           OR toLower(start.career_key) CONTAINS toLower($kw)
        MATCH (m:MAJOR) WHERE m.code IN start.major_codes
        RETURN m.name AS name, labels(m)[0] AS label, m.code AS code,
               ['RECOMMENDED_MAJOR'] AS rel_types,
               [start.name, m.name] AS node_names, 1 AS hops,
               null AS semester, null AS required_type, null AS course_description
        ORDER BY m.name LIMIT 20
    """,
    # Skill self-lookup
    ("SKILL", "SKILL"): """
        MATCH (start:SKILL)
        WHERE toLower(start.name) CONTAINS toLower($kw)
           OR toLower(start.skill_key) CONTAINS toLower($kw)
        RETURN start.name AS name, labels(start)[0] AS label, null AS code,
               [] AS rel_types, [start.name] AS node_names, 0 AS hops,
               null AS semester, null AS required_type, null AS course_description
        ORDER BY start.name LIMIT 10
    """,

    # ── Personality cluster (MBTI v6 — dùng SUITS_MAJOR / SUITS_CAREER) ───────
    # MBTI → Career (primary edge SUITS_CAREER)
    ("PERSONALITY", "CAREER"): """
        MATCH (start:PERSONALITY)-[:SUITS_CAREER]->(n:CAREER)
        WHERE start.personality_key = toUpper($kw)
           OR toLower(start.name) CONTAINS toLower($kw)
        RETURN n.name AS name, labels(n)[0] AS label, null AS code,
               ['SUITS_CAREER'] AS rel_types,
               [start.name, n.name] AS node_names,
               1 AS hops,
               null AS semester, null AS required_type, null AS course_description
        ORDER BY n.name LIMIT 50
    """,
    # MBTI → Major (primary edge SUITS_MAJOR)
    ("PERSONALITY", "MAJOR"): """
        MATCH (start:PERSONALITY)-[:SUITS_MAJOR]->(n:MAJOR)
        WHERE start.personality_key = toUpper($kw)
           OR toLower(start.name) CONTAINS toLower($kw)
        RETURN n.name AS name, labels(n)[0] AS label, n.code AS code,
               ['SUITS_MAJOR'] AS rel_types,
               [start.name, n.name] AS node_names,
               1 AS hops,
               null AS semester, null AS required_type, null AS course_description
        ORDER BY n.name LIMIT 30
    """,
    # MBTI self-lookup (trả về node đầy đủ để LLM dùng suitable_fields)
    ("PERSONALITY", "PERSONALITY"): """
        MATCH (start:PERSONALITY)
        WHERE start.personality_key = toUpper($kw)
           OR toLower(start.name) CONTAINS toLower($kw)
        RETURN start.name AS name, labels(start)[0] AS label, null AS code,
               [] AS rel_types, [start.name] AS node_names, 0 AS hops,
               null AS semester, null AS required_type, null AS course_description
        ORDER BY start.name LIMIT 5
    """,
    # Career → MBTI (nghề này hợp tính cách nào)
    ("CAREER", "PERSONALITY"): """
        MATCH (n:PERSONALITY)-[:SUITS_CAREER]->(start:CAREER)
        WHERE toLower(start.name) CONTAINS toLower($kw)
           OR toLower(start.career_key) CONTAINS toLower($kw)
        RETURN n.name AS name, labels(n)[0] AS label, null AS code,
               ['SUITS_CAREER'] AS rel_types,
               [start.name, n.name] AS node_names,
               1 AS hops,
               null AS semester, null AS required_type, null AS course_description
        ORDER BY n.name LIMIT 20
    """,
    # Major → MBTI (ngành này hợp tính cách nào) — từ cạnh SUITS_MAJOR
    ("MAJOR", "PERSONALITY"): """
        MATCH (n:PERSONALITY)-[:SUITS_MAJOR]->(start:MAJOR)
        WHERE toLower(start.name) CONTAINS toLower($kw) OR start.code = $kw
        RETURN n.name AS name, labels(n)[0] AS label, null AS code,
               ['SUITS_MAJOR'] AS rel_types,
               [start.name, n.name] AS node_names,
               1 AS hops,
               null AS semester, null AS required_type, null AS course_description
        ORDER BY n.name LIMIT 20
    """,
    # Lĩnh vực → MBTI: tìm tất cả PERSONALITY có suitable_fields chứa lĩnh vực $kw
    # Dùng khi câu hỏi là "tính cách gì hợp làm IT" — keyword là tên lĩnh vực chứ không phải ngành
    ("FIELD", "PERSONALITY"): """
        MATCH (n:PERSONALITY)
        WHERE n.suitable_fields IS NOT NULL
          AND toLower(n.suitable_fields) CONTAINS toLower($kw)
        RETURN n.name AS name, labels(n)[0] AS label, null AS code,
               ['suitable_fields_match'] AS rel_types,
               [n.name] AS node_names,
               0 AS hops,
               null AS semester, null AS required_type, null AS course_description
        ORDER BY n.name LIMIT 20
    """,
    # Subject → MBTI (dự phòng CULTIVATES)
    ("SUBJECT", "PERSONALITY"): """
        MATCH (start:SUBJECT)-[:CULTIVATES]->(n:PERSONALITY)
        WHERE toLower(start.name) CONTAINS toLower($kw) OR start.code = $kw
        RETURN n.name AS name, labels(n)[0] AS label, null AS code,
               ['CULTIVATES'] AS rel_types,
               [start.name, n.name] AS node_names,
               1 AS hops,
               null AS semester, null AS required_type, null AS course_description
        ORDER BY n.name LIMIT 20
    """,
    # MBTI → Subject (dự phòng CULTIVATES)
    ("PERSONALITY", "SUBJECT"): """
        MATCH (n:SUBJECT)-[:CULTIVATES]->(start:PERSONALITY)
        WHERE start.personality_key = toUpper($kw)
           OR toLower(start.name) CONTAINS toLower($kw)
        RETURN n.name AS name, labels(n)[0] AS label, n.code AS code,
               ['CULTIVATES'] AS rel_types,
               [start.name, n.name] AS node_names,
               1 AS hops,
               n.course_description AS course_description,
               null AS semester, null AS required_type
        ORDER BY n.name LIMIT 20
    """,
}


def _add_node_and_paths(rec, all_nodes: list, all_paths: list):
    """Thêm node và path vào context, kèm extended props."""
    node = {
        "name":  rec["name"],
        "label": rec["label"],
        "code":  rec.get("code"),
        "hops":  rec["hops"],
    }
    # Extended props từ targeted query
    for field in ("course_description", "semester", "required_type"):
        val = rec.get(field)
        if val is not None:
            node[field] = val

    all_nodes.append(node)

    node_names = rec.get("node_names") or []
    rel_types  = rec.get("rel_types") or []
    for i, rel in enumerate(rel_types):
        all_paths.append({
            "from":     node_names[i]   if i < len(node_names) else "",
            "to":       node_names[i+1] if i+1 < len(node_names) else "",
            "relation": rel,
            "hop":      i + 1,
        })


def fetch_node_details(driver, nodes: list[dict]) -> list[dict]:
    """
    Enrich nodes với extended properties từ DB.
    Chỉ fetch khi node chưa có extended props và là SUBJECT/CAREER/MAJOR.
    """
    to_fetch: dict[str, list[str]] = {
        "SUBJECT": [], "CAREER": [], "MAJOR": [], "PERSONALITY": []
    }
    node_map: dict[str, dict] = {}

    for n in nodes:
        label = n.get("label", "")
        name  = n.get("name", "")
        if not name:
            continue
        node_map[name] = n
        if label in to_fetch:
            has_extended = any(n.get(p) for p in EXTENDED_PROPS.get(label, []))
            if not has_extended:
                to_fetch[label].append(name)

    with driver.session() as session:
        if to_fetch["SUBJECT"]:
            rows = session.run("""
                MATCH (n:SUBJECT) WHERE n.name IN $names
                RETURN n.name AS name,
                       n.course_description AS course_description,
                       n.courses_goals AS courses_goals,
                       n.assessment AS assessment,
                       n.learning_resources AS learning_resources,
                       n.course_requirements_and_expectations AS course_requirements_and_expectations
            """, names=to_fetch["SUBJECT"]).data()
            for r in rows:
                if r["name"] in node_map:
                    for k, v in r.items():
                        if k != "name" and v is not None:
                            node_map[r["name"]][k] = v

        if to_fetch["CAREER"]:
            rows = session.run("""
                MATCH (n:CAREER) WHERE n.name IN $names
                OPTIONAL MATCH (m:MAJOR) WHERE m.code IN n.major_codes
                WITH n, collect({name: m.name, code: m.code}) AS recommended_majors
                RETURN n.name AS name,
                       n.description AS description,
                       n.job_tasks AS job_tasks,
                       n.field_name AS field_name,
                       n.market AS market,
                       n.education_certification AS education_certification,
                       n.major_codes AS major_codes,
                       recommended_majors
            """, names=to_fetch["CAREER"]).data()
            for r in rows:
                if r["name"] in node_map:
                    for k, v in r.items():
                        if k != "name" and v is not None:
                            node_map[r["name"]][k] = v

        if to_fetch["MAJOR"]:
            rows = session.run("""
                MATCH (n:MAJOR) WHERE n.name IN $names
                RETURN n.name AS name,
                       n.philosophy_and_objectives AS philosophy_and_objectives,
                       n.admission_requirements AS admission_requirements,
                       n.learning_outcomes AS learning_outcomes,
                       n.curriculum_structure_and_content AS curriculum_structure_and_content
            """, names=to_fetch["MAJOR"]).data()
            for r in rows:
                if r["name"] in node_map:
                    for k, v in r.items():
                        if k != "name" and v is not None:
                            node_map[r["name"]][k] = v

        if to_fetch["PERSONALITY"]:
            rows = session.run("""
                MATCH (n:PERSONALITY) WHERE n.name IN $names
                RETURN n.name             AS name,
                       n.code             AS code,
                       n.description      AS description,
                       n.structure        AS structure,
                       n.strengths        AS strengths,
                       n.weaknesses       AS weaknesses,
                       n.work_environment AS work_environment,
                       n.suitable_fields  AS suitable_fields
            """, names=to_fetch["PERSONALITY"]).data()
            for r in rows:
                if r["name"] in node_map:
                    for k, v in r.items():
                        if k != "name" and v is not None:
                            node_map[r["name"]][k] = v

    return nodes


def multihop_traversal_community_aware(
    driver,
    keywords:      list[str],
    max_hops:      int = MAX_HOPS,
    intent:        dict | None = None,
    community_def: dict | None = None,
) -> tuple[list[dict], list[dict]]:
    """
    Traversal 3-phase community-aware:
    Phase 1 — TARGETED Cypher theo intent.
    Phase 2 — BFS label-scoped (KHÔNG dùng community number filter vì không đồng nhất).
    Phase 3 — CROSS-CLUSTER BRIDGE (L2/L3).
    """
    all_nodes:  list[dict] = []
    all_paths:  list[dict] = []
    seen_names: set[str]   = set()

    mentioned_labels = (intent or {}).get("mentioned_labels", [])
    asked_label      = (intent or {}).get("asked_label", "UNKNOWN")
    first_mentioned  = mentioned_labels[0] if mentioned_labels else None

    if community_def:
        allowed_labels = community_def["node_labels"]
        level          = community_def["level"]
        comm_id        = community_def["id"]
    else:
        allowed_labels = {"MAJOR", "SUBJECT", "SKILL", "CAREER", "TEACHER", "PERSONALITY"}
        level          = 1
        comm_id        = "L1_GLOBAL"

    print(f"  [community] Routing to: {comm_id} (Level {level})")
    print(f"  [community] Scope labels: {allowed_labels}")

    # ── Phase 1: Targeted query ───────────────────────────────────────────────
    targeted_key    = (first_mentioned, asked_label) if first_mentioned else None
    targeted_cypher = TARGETED_QUERIES.get(targeted_key) if targeted_key else None

    # Fallback: self-lookup
    if not targeted_cypher and asked_label != "UNKNOWN":
        self_key = (asked_label, asked_label)
        if self_key in TARGETED_QUERIES:
            targeted_key    = self_key
            targeted_cypher = TARGETED_QUERIES[self_key]

    if targeted_cypher:
        with driver.session() as session:
            for kw in keywords:
                try:
                    for rec in session.run(targeted_cypher, kw=kw):
                        _add_node_and_paths(rec, all_nodes, all_paths)
                except Exception as e:
                    print(f"  [targeted] WARNING: {e}")
        if all_nodes:
            print(f"  [targeted] ({targeted_key}) → {len(all_nodes)} nodes")

    # ── Phase 1b: MBTI fallback — nếu targeted query không tìm được gì ────────
    # Đọc node PERSONALITY đầy đủ (có suitable_fields) để LLM tự parse ngành/nghề
    if not all_nodes and asked_label in ("MAJOR", "CAREER", "PERSONALITY", "UNKNOWN"):
        mbti_kws = [kw for kw in keywords
                    if re.match(r'^(INTJ|INTP|ENTJ|ENTP|INFJ|INFP|ENFJ|ENFP'
                                r'|ISTJ|ISFJ|ESTJ|ESFJ|ISTP|ISFP|ESTP|ESFP)$',
                                kw, re.IGNORECASE)]
        if mbti_kws:
            with driver.session() as session:
                for mbti_code in mbti_kws:
                    try:
                        rows = session.run("""
                            MATCH (p:PERSONALITY)
                            WHERE p.personality_key = toUpper($code)
                               OR p.code = toUpper($code)
                            RETURN p.personality_key AS name,
                                   'PERSONALITY'     AS label,
                                   null              AS code,
                                   []                AS rel_types,
                                   [p.personality_key] AS node_names,
                                   0                 AS hops,
                                   null AS semester, null AS required_type,
                                   null AS course_description
                        """, code=mbti_code).data()
                        for rec in rows:
                            _add_node_and_paths(rec, all_nodes, all_paths)
                    except Exception as e:
                        print(f"  [mbti fallback] WARNING: {e}")
            if all_nodes:
                print(f"  [mbti fallback] Found PERSONALITY node for {mbti_kws}")

    # ── Phase 1c: Field-context PERSONALITY lookup ────────────────────────────
    # Khi câu hỏi là "tính cách gì hợp làm IT" → asked=PERSONALITY, field_context="Công nghệ thông tin"
    # Cần tìm tất cả PERSONALITY có suitable_fields chứa lĩnh vực đó
    field_context = (intent or {}).get("field_context")
    if asked_label == "PERSONALITY" and field_context:
        field_query = TARGETED_QUERIES.get(("FIELD", "PERSONALITY"))
        if field_query:
            _FIELD_ALIASES: dict[str, list[str]] = {
                "Công nghệ thông tin": [
                    "Công nghệ thông tin", "Information Technology", "CNTT", "IT",
                    "công nghệ", "technology", "phần mềm", "software",
                ],
                "Kinh tế - Quản trị": [
                    "Kinh tế", "Quản trị", "Economics", "Business", "Management",
                    "Tài chính", "Finance", "Kế toán", "Accounting",
                ],
                "Khoa học dữ liệu": [
                    "Khoa học dữ liệu", "Data Science", "Data", "dữ liệu",
                    "phân tích dữ liệu", "Data Analytics",
                ],
                "Giáo dục": ["Giáo dục", "Education", "sư phạm", "đào tạo"],
                "Y tế - Sức khỏe": ["Y tế", "Health", "y khoa", "dược"],
            }
            aliases = _FIELD_ALIASES.get(field_context, [field_context])

            with driver.session() as session:
                rows = []
                for alias_kw in aliases:
                    try:
                        found = list(session.run(field_query, kw=alias_kw))
                        rows.extend(found)
                        if found:
                            print(f"  [field_ctx] alias='{alias_kw}' → {len(found)} hits")
                    except Exception as e:
                        print(f"  [field_ctx] WARNING alias='{alias_kw}': {e}")

                seen_fc: set[str] = set()
                for rec in rows:
                    if rec.get("name") not in seen_fc:
                        seen_fc.add(rec.get("name", ""))
                        _add_node_and_paths(rec, all_nodes, all_paths)
                if rows:
                    print(f"  [field_ctx] total ({field_context}) → {len(seen_fc)} personality nodes")

        # Cũng tìm MAJOR thuộc lĩnh vực đó để làm context cho LLM
        _intent_kws = (intent or {}).get("keywords", [])
        major_kws = [kw for kw in _intent_kws if kw.lower() in (
            "công nghệ thông tin", "it", "cntt", "kinh tế", "tài chính", "data", "dữ liệu"
        )]
        if not major_kws:
            major_kws = [field_context.split()[0].lower()] if field_context else []
        major_query = TARGETED_QUERIES.get(("MAJOR", "PERSONALITY"))
        if major_query and major_kws:
            with driver.session() as session:
                for kw in major_kws:
                    try:
                        for rec in session.run(major_query, kw=kw):
                            _add_node_and_paths(rec, all_nodes, all_paths)
                    except Exception as e:
                        print(f"  [field_ctx_major] WARNING: {e}")

    # ── Phase 2: BFS label-scoped ─────────────────────────────────────────────
    # Dùng allowed_labels filter, KHÔNG filter theo community number
    # (vì MAJOR=2, SUBJECT=2, TEACHER=0 tại L2 — không đồng nhất)
    label_clauses = " OR ".join(f"n:{lbl}" for lbl in allowed_labels)

    with driver.session() as session:
        for kw in keywords:
            seed_rows = session.run("""
                MATCH (seed)
                WHERE (seed:MAJOR OR seed:SUBJECT OR seed:SKILL
                       OR seed:CAREER OR seed:TEACHER OR seed:PERSONALITY)
                  AND (toLower(seed.name) CONTAINS toLower($kw)
                       OR (seed.code IS NOT NULL AND seed.code = $kw)
                       OR (seed.career_key IS NOT NULL
                           AND toLower(seed.career_key) CONTAINS toLower($kw))
                       OR (seed.teacher_key IS NOT NULL
                           AND toLower(seed.teacher_key) CONTAINS toLower($kw))
                       OR (seed.skill_key IS NOT NULL
                           AND toLower(seed.skill_key) CONTAINS toLower($kw))
                       OR (seed.personality_key IS NOT NULL
                           AND toLower(seed.personality_key) CONTAINS toLower($kw))
                       OR (seed.category IS NOT NULL
                           AND toLower(seed.category) CONTAINS toLower($kw)))
                WITH seed, size([(seed)-[]-() | 1]) AS degree
                RETURN seed
                ORDER BY degree DESC
                LIMIT 3
            """, kw=kw).data()
            seeds = [r["seed"] for r in seed_rows]

            for seed in seeds:
                seed_name = seed.get("name", "")
                if seed_name in seen_names:
                    continue
                seen_names.add(seed_name)

                # Thêm seed node vào context (kèm extended props)
                seed_labels = list(seed.labels) if hasattr(seed, "labels") else []
                seed_label  = seed_labels[0] if seed_labels else "UNKNOWN"
                seed_node   = {
                    "name":  seed_name,
                    "label": seed_label,
                    "code":  seed.get("code"),
                    "hops":  0,
                }
                for prop in EXTENDED_PROPS.get(seed_label, []):
                    val = seed.get(prop)
                    if val is not None:
                        seed_node[prop] = val
                all_nodes.append(seed_node)

                # BFS label-scoped traversal
                traversal_query = f"""
                    MATCH path = (start)-[*1..{max_hops}]-(n)
                    WHERE start.name = $seed_name
                      AND ({label_clauses})
                    WITH n, path,
                         [r IN relationships(path) | type(r)] AS rel_types,
                         [x IN nodes(path) | x.name]          AS node_names
                    RETURN DISTINCT
                        n.name                 AS name,
                        labels(n)[0]           AS label,
                        n.code                 AS code,
                        n.course_description   AS course_description,
                        null                   AS semester,
                        null                   AS required_type,
                        rel_types,
                        node_names,
                        length(path)           AS hops
                    ORDER BY hops ASC
                    LIMIT 60
                """
                try:
                    for rec in session.run(traversal_query, seed_name=seed_name):
                        _add_node_and_paths(rec, all_nodes, all_paths)
                except Exception as e:
                    print(f"  [BFS] WARNING seed={seed_name}: {e}")

    # ── Phase 3: Cross-cluster bridge (L2/L3) ────────────────────────────────
    if level >= 2 and asked_label not in (None, "UNKNOWN"):
        bridge_pairs = [
            ("L2_ACADEMIC", "CAREER",
             "MATCH (m:MAJOR)-[:LEADS_TO]->(n:CAREER) WHERE m.name IN $names "
             "RETURN n.name AS name, 'CAREER' AS label, null AS code, "
             "['LEADS_TO'] AS rel_types, [m.name, n.name] AS node_names, 1 AS hops, "
             "null AS semester, null AS required_type, null AS course_description"),

            ("L2_CAREER_ALIGNMENT", "SUBJECT",
             "MATCH (c:CAREER)-[:REQUIRES]->(sk:SKILL)<-[:PROVIDES]-(n:SUBJECT) "
             "WHERE c.name IN $names "
             "OPTIONAL MATCH (m:MAJOR)-[:MAJOR_OFFERS_SUBJECT]->(n) WHERE m.code IN c.major_codes "
             "WITH c, sk, n, count(DISTINCT m) AS major_match, "
             "size([(s2:SUBJECT)-[:PROVIDES]->(sk) | s2]) AS skill_breadth "
             "ORDER BY major_match DESC, skill_breadth ASC "
             "RETURN n.name AS name, 'SUBJECT' AS label, n.code AS code, "
             "['REQUIRES','PROVIDES'] AS rel_types, [c.name, sk.name, n.name] AS node_names, 2 AS hops, "
             "null AS semester, null AS required_type, n.course_description AS course_description "
             "LIMIT 20"),

            # Bridge: L2_PERSONALITY_FIT → Career (SUITS_CAREER)
            ("L2_PERSONALITY_FIT", "CAREER",
             "MATCH (p:PERSONALITY)-[:SUITS_CAREER]->(n:CAREER) "
             "WHERE p.name IN $names OR p.personality_key IN $names "
             "RETURN n.name AS name, 'CAREER' AS label, null AS code, "
             "['SUITS_CAREER'] AS rel_types, [p.name, n.name] AS node_names, 1 AS hops, "
             "null AS semester, null AS required_type, null AS course_description "
             "LIMIT 50"),

            # Bridge: L2_PERSONALITY_FIT → Major (SUITS_MAJOR)
            ("L2_PERSONALITY_FIT", "MAJOR",
             "MATCH (p:PERSONALITY)-[:SUITS_MAJOR]->(n:MAJOR) "
             "WHERE p.name IN $names OR p.personality_key IN $names "
             "RETURN n.name AS name, 'MAJOR' AS label, n.code AS code, "
             "['SUITS_MAJOR'] AS rel_types, [p.name, n.name] AS node_names, 1 AS hops, "
             "null AS semester, null AS required_type, null AS course_description "
             "LIMIT 30"),

            # Bridge ngược: MAJOR → PERSONALITY (khi hỏi "tính cách gì hợp làm/học X")
            # Dùng khi seed_names chứa MAJOR nodes → tìm PERSONALITY suits những MAJOR đó
            ("L2_PERSONALITY_FIT", "PERSONALITY",
             "MATCH (n:PERSONALITY)-[:SUITS_MAJOR]->(m:MAJOR) "
             "WHERE m.name IN $names "
             "RETURN n.name AS name, 'PERSONALITY' AS label, null AS code, "
             "['SUITS_MAJOR'] AS rel_types, [m.name, n.name] AS node_names, 1 AS hops, "
             "null AS semester, null AS required_type, null AS course_description "
             "LIMIT 20"),

            # Bridge ngược qua CAREER: CAREER → PERSONALITY (khi seed là CAREER trong lĩnh vực IT)
            ("L2_PERSONALITY_FIT", "PERSONALITY",
             "MATCH (n:PERSONALITY)-[:SUITS_CAREER]->(c:CAREER) "
             "WHERE c.name IN $names "
             "RETURN n.name AS name, 'PERSONALITY' AS label, null AS code, "
             "['SUITS_CAREER'] AS rel_types, [c.name, n.name] AS node_names, 1 AS hops, "
             "null AS semester, null AS required_type, null AS course_description "
             "LIMIT 20"),
        ]
        seed_names = list({n["name"] for n in all_nodes if n.get("name")})[:20]

        if seed_names:
            with driver.session() as session:
                for bridge_cid, bridge_label, bridge_q in bridge_pairs:
                    if comm_id != bridge_cid:
                        continue
                    if bridge_label != asked_label and asked_label != "UNKNOWN":
                        continue
                    try:
                        for rec in session.run(bridge_q, names=seed_names):
                            _add_node_and_paths(rec, all_nodes, all_paths)
                        print(f"  [bridge] {bridge_cid}→{bridge_label}: added")
                    except Exception as e:
                        print(f"  [bridge] WARNING: {e}")

    return all_nodes, all_paths


# ══════════════════════════════════════════════════════════════════════════════
# PHẦN 8: GENERATE ANSWER
# ══════════════════════════════════════════════════════════════════════════════

def generate_answer(
    ai_client:    OpenAI,
    question:     str,
    ranked_nodes: list[dict],
    traversal_paths: list[dict],
    intent:       dict,
    community_def: dict | None = None,
    override_constraint: str | None = None,
) -> str:
    context = json.dumps({
        "ranked_results":  ranked_nodes,
        "traversal_paths": traversal_paths[:60],
    }, ensure_ascii=False, indent=2)

    constraint = (
        override_constraint if override_constraint is not None
        else get_relationship_constraint(intent)
    )

    negated = intent.get("negated_keywords", [])
    if negated:
        constraint += (
            f"\n\nLƯU Ý PHỦ ĐỊNH: Người dùng KHÔNG giỏi/thích: {negated}. "
            "Loại bỏ khỏi gợi ý."
        )

    if community_def:
        community_context = (
            f"Tầng {community_def['level']} — {community_def['name']}\n"
            f"Mục tiêu: {community_def['purpose']}"
        )
    else:
        community_context = "L1 Global — Toàn bộ hệ sinh thái đào tạo"

    system_prompt = ANSWER_SYSTEM_BASE.format(
        schema=SCHEMA_DESC,
        constraint=constraint,
        community_context=community_context,
    )

    no_data_hint = ""
    if not ranked_nodes:
        no_data_hint = (
            "\n[CẢNH BÁO: Không tìm thấy dữ liệu trong Knowledge Graph. "
            "Thông báo lịch sự, không bịa thông tin.]"
        )

    # Nhắc LLM không nhắc đến môn đại cương khi đang trả lời câu hỏi gợi ý môn
    excluded_hint = ""
    if intent.get("_exclude_common_subjects"):
        excluded_hint = (
            "\n[LUẬT BỔ SUNG — ÁP DỤNG CHO CÂU TRẢ LỜI NÀY]: "
            "Đây là câu hỏi gợi ý môn học. "
            "TUYỆT ĐỐI KHÔNG đề cập hoặc liệt kê các môn sau (dù tên viết hoa, thường, có dấu hay không): "
            "Triết học Mác-Lênin, Kinh tế chính trị Mác-Lênin, Chủ nghĩa xã hội khoa học, "
            "Lịch sử Đảng Cộng sản Việt Nam, Tư tưởng Hồ Chí Minh, "
            "Giáo dục thể chất (GDTC), Giáo dục quốc phòng và an ninh (GDQP), "
            "Kinh tế vi mô 1 (KHMI1101), Kinh tế vĩ mô 1 (KHMA1101), Pháp luật đại cương (LUCS1129). "
            "Đây là các môn bắt buộc chung mọi ngành — không cần tư vấn riêng.]"
        )

    # Nhắc LLM filter theo lĩnh vực khi câu hỏi là "tính cách gì hợp làm X"
    field_context_hint = ""
    field_context = intent.get("field_context")
    if field_context and intent.get("asked_label") == "PERSONALITY":
        field_context_hint = (
            f"\n[HƯỚNG DẪN ĐẶC BIỆT — LĨNH VỰC: {field_context}]: "
            f"Câu hỏi hỏi tính cách phù hợp với lĩnh vực '{field_context}'. "
            f"Từ [DỮ LIỆU GRAPH], CHỈ liệt kê các PERSONALITY node có suitable_fields "
            f"chứa lĩnh vực '{field_context}' hoặc đã được liên kết (SUITS_MAJOR/SUITS_CAREER) "
            f"với ngành/nghề thuộc lĩnh vực '{field_context}'. "
            f"Với mỗi tính cách, giải thích ngắn gọn TẠI SAO phù hợp với lĩnh vực này "
            f"(dựa vào strengths/structure trong node PERSONALITY). "
            f"ĐỊNH DẠNG: bảng markdown | MBTI | Tên tính cách | Lý do phù hợp |, "
            f"sau đó thêm đoạn tóm tắt đặc điểm chung.]"
        )

    response = ai_client.chat.completions.create(
        model=OPENAI_MODEL,
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user",   "content": (
                f"Câu hỏi: {question}\n\n"
                f"[DỮ LIỆU GRAPH]:\n{context}"
                f"{no_data_hint}"
                f"{excluded_hint}"
                f"{field_context_hint}\n\n"
                "Trả lời CHỈ dùng tên/code từ [DỮ LIỆU GRAPH]:"
            )},
        ],
        temperature=0,
    )
    raw = response.choices[0].message.content.strip()

    # Post-process: đảm bảo mỗi bullet • luôn bắt đầu trên dòng mới
    # Xử lý trường hợp LLM trả về "text • item" liền nhau không xuống dòng
    fixed = re.sub(r'(?<!\n)\s*•\s*', '\n• ', raw)
    # Dọn dẹp khoảng trắng thừa đầu dòng (giữ indent tối đa 4 space)
    fixed = re.sub(r'\n {5,}•', '\n    •', fixed)
    return fixed.strip()


# ══════════════════════════════════════════════════════════════════════════════
# PHẦN 9: PIPELINE CHÍNH
# ══════════════════════════════════════════════════════════════════════════════
_CTDT_PATTERN = re.compile(
    r"(?:xem|tìm|tải|download|file|chương trình đào tạo|ctđt|ct đt)\s*"
    r"(?:file\s*)?(?:ctđt|ct\s*đt|chương trình đào tạo)?\s*(?:ngành|của ngành)?\s*"
    r"(.+?)(?:\s*(?:ở đâu|tại đâu|tải ở đâu|xem ở đâu|download ở đâu)|\s*\?|$)",
    re.IGNORECASE | re.UNICODE,
)

def detect_ctdt_question(question: str) -> str | None:
    """
    Nếu câu hỏi hỏi về 'xem file CTĐT ngành X ở đâu' (hoặc biến thể),
    trả về tên ngành X. Ngược lại trả về None.
    """
    q = question.strip()
    if not re.search(r"ctđt|ct\s*đt|chương trình đào tạo", q, re.IGNORECASE | re.UNICODE):
        return None
    if not re.search(r"ở đâu|tại đâu|xem|tìm|tải|download|file", q, re.IGNORECASE | re.UNICODE):
        return None
    m = _CTDT_PATTERN.search(q)
    if m:
        major_name = m.group(1).strip(" ?")
        # Loại bỏ các từ thừa ở cuối: "thì", "thì xem", "thì tải"...
        major_name = re.sub(
            r"\s+(?:thì|thì xem|thì tải|thì download|thì ở đâu|thì tại đâu)\s*$",
            "", major_name, flags=re.IGNORECASE | re.UNICODE,
        ).strip(" ?")
        return major_name if major_name else "ngành bạn quan tâm"
    return "ngành bạn quan tâm"

def kg_ask(driver, ai_client: OpenAI, question: str, query_id: str | None = None) -> dict:
    if query_id is None:
        query_id = "q" + uuid.uuid4().hex[:6]

    print(f"\n{'='*60}")
    print(f"Q [{query_id}]: {question}")
    ctdt_major = detect_ctdt_question(question)
    if ctdt_major is not None:
        answer = (
            f"Để xem thêm thì hãy vào trang courses.neu.edu.vn "
            f"và tìm ngành {ctdt_major} nhé!"
        )
        print(f"\nA: {answer}")
        return _build_record(
            query_id, question, answer, [ctdt_major],
            {"asked_label": "CTDT_REDIRECT", "mentioned_labels": [],
             "keywords": [ctdt_major], "negated_keywords": [],
             "community_id": "CTDT_REDIRECT"},
            [], [], "ctdt_redirect",
        )

    # ── Bước 0-pre: Chỉ tiêu & Điểm chuẩn tuyển sinh ────────────────────────
    admission_answer = handle_admission_question(question)
    if admission_answer is not None:
        print(f"\nA (admission): {admission_answer[:120]}...")
        return _build_record(
            query_id, question, admission_answer, [],
            {"asked_label": "ADMISSION", "mentioned_labels": [],
             "keywords": [], "negated_keywords": [],
             "community_id": "ADMISSION_STATIC"},
            [], [], "admission_static_lookup",
        )

    # ── Bước 0-pre2: Môn đại cương bắt buộc — câu hỏi "ngành nào không học X" ──
    not_study_answer = handle_which_major_not_study(question)
    if not_study_answer is not None:
        print(f"\nA (not_study_excluded): {not_study_answer}")
        return _build_record(
            query_id, question, not_study_answer, [],
            {"asked_label": "SUBJECT", "mentioned_labels": ["MAJOR", "SUBJECT"],
             "keywords": [], "negated_keywords": [],
             "community_id": "EXCLUDED_SUBJECT_STATIC"},
            [], [], "excluded_subject_static_reply",
        )

    # ── Bước 0: Aggregation Router ────────────────────────────────────────────
    agg_type = detect_aggregation_type(question)
    if agg_type:
        print(f"  [aggregation] {agg_type}")
        agg_nodes = run_aggregation_query(driver, question, agg_type)
        print(f"  [aggregation] {len(agg_nodes)} nodes")

        agg_intent = {
            "keywords": [], "mentioned_labels": [], "negated_keywords": [],
            "is_comparison": False, "agg_type": agg_type,
            "asked_label": (
                "SUBJECT" if "subject" in agg_type else
                "MAJOR"   if "major"   in agg_type else
                "CAREER"  if "career"  in agg_type else
                "SKILL"   if "skill"   in agg_type else "UNKNOWN"
            ),
        }
        agg_constraint = (
            "Câu hỏi thống kê/tập hợp. Dữ liệu đã tổng hợp từ graph. "
            "Trình bày rõ ràng, kèm mã môn/ngành, số liệu (_agg_meta). "
            "Nếu intersection: giải thích đây là môn tất cả ngành đều học. "
            "Nếu ranking: liệt kê từ cao xuống thấp."
        )
        answer = generate_answer(
            ai_client, question, agg_nodes, [],
            intent=agg_intent,
            community_def=COMMUNITY_LEVELS["L1_GLOBAL"],
            override_constraint=agg_constraint,
        )
        print(f"\nA: {answer}")
        return _build_record(query_id, question, answer, [], agg_intent,
                             agg_nodes, [], "aggregation")

    # ── Bước 0b: Expand MBTI code tường minh → keyword ──────────────────────
    expanded_question, mbti_keywords = expand_mbti(question)
    if mbti_keywords:
        print(f"  [mbti] {mbti_keywords}")

    # ── Bước 0c: Expand viết tắt ──────────────────────────────────────────────
    expanded_question, abbrev_keywords = expand_abbreviations(expanded_question)
    if abbrev_keywords:
        print(f"  [abbrev] {abbrev_keywords}")

    # ── Bước 1: Extract intent (LLM) — trả về cả mbti_dimensions ─────────────
    intent = extract_query_intent(ai_client, expanded_question)
    intent["keywords"] = list(dict.fromkeys(
        intent["keywords"] + mbti_keywords + abbrev_keywords
    ))
    # Lần 1: apply rules trước MBTI resolve (để vá labels/asked sớm)
    intent = apply_intent_rules(question, intent)

    # ── Bước 1b: Resolve MBTI codes ──────────────────────────────────────────
    # Ưu tiên 1: MBTI code tường minh (INTJ, ESTP...) từ regex expand_mbti
    # Ưu tiên 2: dimensions do LLM suy luận từ từ đồng nghĩa tính cách
    dimensions = intent.get("mbti_dimensions", [])

    if mbti_keywords:
        all_mbti_keywords = mbti_keywords
        # Explicit code → dùng trực tiếp, bỏ qua dimensions
        print(f"  [mbti override] source=explicit code={mbti_keywords[0].upper()}")
    elif dimensions:
        # LLM suy luận dimensions → expand thành MBTI codes
        all_mbti_keywords = resolve_mbti_codes_from_dimensions(dimensions)
        print(f"  [mbti override] source=llm-dimensions dims={dimensions} "
              f"→ {len(all_mbti_keywords)} codes: {all_mbti_keywords}")
    else:
        all_mbti_keywords = []

    if all_mbti_keywords:
        mbti_code = all_mbti_keywords[0].upper()
        # Đảm bảo PERSONALITY có trong mentioned_labels
        if "PERSONALITY" not in intent.get("mentioned_labels", []):
            intent["mentioned_labels"] = ["PERSONALITY"] + [
                l for l in intent.get("mentioned_labels", [])
                if l != "PERSONALITY"
            ]
        # Nếu asked=UNKNOWN → set PERSONALITY
        if intent.get("asked_label") == "UNKNOWN":
            intent["asked_label"] = "PERSONALITY"
        # Inject tất cả MBTI codes vào keywords (để traversal query từng cái)
        existing_kws = [k for k in intent["keywords"]
                        if k.upper() not in {c.upper() for c in all_mbti_keywords}]
        intent["keywords"] = all_mbti_keywords + existing_kws
    # Lần 2: apply rules sau MBTI resolve (để sắp xếp lại mentioned theo MBTI)
    intent = apply_intent_rules(question, intent)
    keywords = intent["keywords"]
    print(f"  Keywords: {keywords}")
    print(f"  Intent: mentioned={intent['mentioned_labels']} "
          f"asked={intent['asked_label']} negated={intent['negated_keywords']}")

    # ── Bước 2: Community Routing ─────────────────────────────────────────────
    community_id, community_def = route_to_community(intent)
    intent["community_id"] = community_id

    # ── Bước 3: Community-aware Traversal ────────────────────────────────────
    raw_nodes, traversal_paths = multihop_traversal_community_aware(
        driver, keywords, max_hops=MAX_HOPS,
        intent=intent, community_def=community_def,
    )
    print(f"  Traversal: {len(raw_nodes)} nodes | {len(traversal_paths)} paths")

    # ── Bước 4: Dedup + Negation filter ──────────────────────────────────────
    negated_lower = [kw.lower() for kw in intent.get("negated_keywords", [])]
    seen: dict[tuple, dict] = {}
    for n in raw_nodes:
        key = (n.get("label", ""), n.get("name", ""))
        if key not in seen or (n.get("hops") or 99) < (seen[key].get("hops") or 99):
            seen[key] = n
    context_nodes = [
        n for n in seen.values()
        if not any(neg in (n.get("name") or "").lower() for neg in negated_lower)
    ]

    # ── Bước 4-excluded: Lọc môn đại cương bắt buộc khi câu hỏi là gợi ý môn ──
    _is_confirm = bool(_CONFIRM_SUBJECT_PATTERN.search(question))
    _is_recommend = is_recommend_subject_question(question)
    # Lọc khỏi context nếu là câu hỏi gợi ý MÀ KHÔNG phải câu hỏi xác nhận
    _should_exclude = _is_recommend and not _is_confirm
    context_nodes = filter_excluded_subjects(context_nodes, exclude=_should_exclude)
    # Gắn flag vào intent để generate_answer biết
    intent["_exclude_common_subjects"] = _should_exclude

    # ── Bước 4b-pre: Lọc CAREER node dạng bằng cấp (không phải vị trí công việc) ──
    _DEGREE_PREFIXES = (
        "cử nhân", "kỹ sư", "thạc sĩ", "tiến sĩ", "bác sĩ",
        "bachelor", "master", "engineer",
    )
    def _is_degree_career(node: dict) -> bool:
        if node.get("label") != "CAREER":
            return False
        name_lower = (node.get("name") or "").lower().strip()
        return any(name_lower.startswith(prefix) for prefix in _DEGREE_PREFIXES)

    context_nodes = [n for n in context_nodes if not _is_degree_career(n)]

    # ── Bước 4b: Enrich extended props khi cần ───────────────────────────────
    asked = intent.get("asked_label", "UNKNOWN")
    if asked in ("SUBJECT", "CAREER", "MAJOR", "PERSONALITY") and len(context_nodes) <= 20:
        context_nodes = fetch_node_details(driver, context_nodes)
        print(f"  [enrich] Extended props fetched for: {asked}")
    elif asked == "CAREER" and len(context_nodes) > 20:
        # Luôn enrich CAREER nodes ngay cả khi tổng context_nodes lớn
        # (vì chỉ có tối đa 27 CAREER trong DB)
        career_nodes_exist = any(n.get("label") == "CAREER" for n in context_nodes)
        if career_nodes_exist:
            context_nodes = fetch_node_details(driver, context_nodes)
            print(f"  [enrich] Extended props force-fetched for CAREER (total nodes={len(context_nodes)})")
    elif len(context_nodes) > 20:
        # Luôn enrich PERSONALITY ngay cả khi nhiều nodes (số lượng PERSONALITY có giới hạn)
        pers_exist = any(n.get("label") == "PERSONALITY" for n in context_nodes)
        if pers_exist:
            context_nodes = fetch_node_details(driver, context_nodes)
            print(f"  [enrich] Extended props force-fetched for PERSONALITY (total nodes={len(context_nodes)})")

    # ── Bước 5: LLM answer ───────────────────────────────────────────────────
    answer = generate_answer(
        ai_client, question, context_nodes, traversal_paths,
        intent=intent, community_def=community_def,
    )
    print(f"\nA: {answer}")

    return _build_record(
        query_id, question, answer, keywords, intent,
        context_nodes, traversal_paths,
        f"Targeted+BFS label-scoped [{community_id}]",
    )


def _build_record(
    query_id, question, answer, keywords, intent,
    context_nodes, traversal_paths, algorithm_desc,
) -> dict:
    return {
        "query_id":         query_id,
        "query":            question,
        "generated_answer": answer,
        "keywords":         keywords,
        "intent":           intent,
        "community_id":     intent.get("community_id", ""),
        "retrieved_nodes": [
            {
                "node_id":  f"node{i+1:03d}",
                "content":  json.dumps(n, ensure_ascii=False),
                "entities": [n.get("name", "")],
            }
            for i, n in enumerate(context_nodes)
        ],
        "traversal_path": traversal_paths[:20],
        "timestamp":      datetime.datetime.now().isoformat(),
        "algorithm": {
            "community_detection": "Louvain weighted (GDS) + rule-based fallback",
            "traversal":           algorithm_desc,
        },
    }


# ══════════════════════════════════════════════════════════════════════════════
# RUN PIPELINE — wrapper cho Vercel endpoint
# ══════════════════════════════════════════════════════════════════════════════

def run_pipeline(question: str, query_id: str) -> dict:
    return kg_ask(driver, ai_client, question, query_id=query_id)




# FASTAPI ENDPOINTS


@app.get("/metadata")
async def metadata():
    payload = {
        "name":        "NEU Advisory Agent",
        "description": "Chatbot tư vấn đào tạo dựa trên Knowledge Graph — Đại học Kinh tế Quốc dân",
        "developer":     "Nhóm Hà Anh Hồng Sơn",
        "capabilities":     ["search", "knowledge-graph"],
        "supported_models": [{"model_id": OPENAI_MODEL, "name": OPENAI_MODEL}],
        "pipeline": [
            "Intent Detection (keywords, labels, negation)",
            "Seed Entity Fetch",
            "Community Detection Filter",
            "Multi-hop BFS Traversal + Targeted Queries",
            "PageRank Ranking + Negation Filter",
            "LLM Answer Generation with Relationship Constraints",
        ],
        "status": "active",
        "sample_prompts": [
            "Tôi có thế mạnh về ngoại ngữ và muốn làm việc trong môi trường quốc tế thì nên học ngành gì tại NEU?",
            "Tôi thích tự kinh doanh, khởi nghiệp sau khi ra trường thì nên học ngành gì tại NEU?",
            "Môn Trí tuệ nhân tạo dạy những kiến thức gì?",
            "Học công nghệ thông tin ở NEU có ưu điểm gì không?",
        ],
    }
    return JSONResponse(content=payload, headers=CORS_HEADERS)


@app.post("/ask")
async def ask(request: Request):
    data       = await request.json()
    question   = data.get("prompt", "").strip()
    session_id = data.get("session_id", str(uuid.uuid4()))

    if not question:
        return JSONResponse(
            content={
                "session_id":       session_id,
                "status":           "error",
                "content_markdown": "Vui lòng nhập câu hỏi.",
            },
            headers=CORS_HEADERS,
        )

    # ── Kiểm tra câu hỏi về chỉ tiêu & điểm chuẩn ───────────────────────────────
    admission_answer = handle_admission_question(question)
    if admission_answer is not None:
        return JSONResponse(
            content={
                "session_id":       session_id,
                "status":           "success",
                "content_markdown": admission_answer,
                "debug": {
                    "query_id":   "admission_static",
                    "keywords":   [],
                    "intent":     {"asked_label": "ADMISSION"},
                    "node_count": 0,
                },
            },
            headers=CORS_HEADERS,
        )

    # ── Kiểm tra câu hỏi "ngành nào không học [môn đại cương]" ──────────────────
    not_study_answer = handle_which_major_not_study(question)
    if not_study_answer is not None:
        return JSONResponse(
            content={
                "session_id":       session_id,
                "status":           "success",
                "content_markdown": not_study_answer,
                "debug": {
                    "query_id":   "excluded_subject_static",
                    "keywords":   [],
                    "intent":     {"asked_label": "SUBJECT"},
                    "node_count": 0,
                },
            },
            headers=CORS_HEADERS,
        )

    # ── Kiểm tra câu hỏi xem file CTĐT ──────────────────────────────────────────
    ctdt_major = detect_ctdt_question(question)
    if ctdt_major is not None:
        answer = (
            f"Để xem thêm thì hãy vào trang "
            f"[courses.neu.edu.vn](https://courses.neu.edu.vn) "
            f"và tìm ngành **{ctdt_major}** nhé! 📚"
        )
        return JSONResponse(
            content={
                "session_id":       session_id,
                "status":           "success",
                "content_markdown": answer,
                "debug": {
                    "query_id":   "ctdt_redirect",
                    "keywords":   [ctdt_major],
                    "intent":     {"asked_label": "CTDT_REDIRECT"},
                    "node_count": 0,
                },
            },
            headers=CORS_HEADERS,
        )

    try:
        query_id = "q" + uuid.uuid4().hex[:6]
        result   = run_pipeline(question, query_id)

        return JSONResponse(
            content={
                "session_id":       session_id,
                "status":           "success",
                "content_markdown": result["generated_answer"],
                "debug": {
                    "query_id":   result["query_id"],
                    "keywords":   result["keywords"],
                    "intent":     result["intent"],
                    "node_count": len(result["retrieved_nodes"]),
                },
            },
            headers=CORS_HEADERS,
        )

    except Exception as e:
        return JSONResponse(
            content={
                "session_id":       session_id,
                "status":           "error",
                "content_markdown": f"Đã xảy ra lỗi khi xử lý câu hỏi. Vui lòng thử lại.\n\n`{str(e)}`",
            },
            headers=CORS_HEADERS,
        )