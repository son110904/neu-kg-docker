# Hướng dẫn Migration: Vercel + Neo4j Aura → Portainer + Docker

## Tổng quan kiến trúc sau migration

```
Internet
    │
    ├── :8000  ──→  [Docker: neu_api]   FastAPI + Uvicorn
    └── :7474  ──→  [Docker: neu_neo4j] Neo4j Browser (admin)
                         │
                   bolt://neo4j:7687
                   (internal Docker network)
```

---

## Cấu trúc thư mục cần chuẩn bị

```
neu-kg/
├── Dockerfile
├── docker-compose.yml
├── portainer-stack.yml      ← dùng cho Portainer UI
├── requirements.txt
├── .env                     ← KHÔNG commit lên git
├── .gitignore
├── index.py                 ← FastAPI app (file hiện tại của bạn)
├── script1.py               ← Extraction pipeline
├── script2.py               ← Neo4j ingestion
├── script3a.py              ← Chatbot logic
├── migrate_neo4j.py         ← Script migration dữ liệu
└── cache/
    └── output/              ← Dữ liệu đã extract (nếu còn giữ)
```

---

## BƯỚC 1 — Chọn chiến lược migrate dữ liệu

### So sánh 2 phương án

| Tiêu chí | Phương án A: Dump & Restore | Phương án B: Re-import từ cache |
|---|---|---|
| Tốc độ | Nhanh hơn (~10 phút) | ~20-30 phút tùy dữ liệu |
| Yêu cầu | `./cache/output/` phải còn | Có `./cache/output/*.json` |
| Độ an toàn | Dữ liệu 1:1 với cloud | Đảm bảo sạch, không rác |
| Khuyến nghị | **Dùng nếu mất cache** | **Dùng nếu còn cache JSON** |

**→ Khuyến nghị: Phương án A (migrate_neo4j.py) vì nhanh và an toàn nhất.**

---

## BƯỚC 2 — Chuẩn bị môi trường trên server của trường

### 2.1 Verify Docker và Portainer

```bash
# Kiểm tra Docker đã cài chưa
docker --version
docker compose version

# Portainer phải đang chạy tại port 9000 hoặc 9443
# (do admin trường cấu hình)
```

### 2.2 Tạo thư mục project trên server

```bash
mkdir -p /opt/neu-kg
cd /opt/neu-kg

# Upload tất cả file (dùng scp hoặc Git)
# Option 1: Git
git clone https://github.com/<your-repo>/neu-kg.git .

# Option 2: SCP từ máy local
scp -r ./neu-kg/* user@<server-ip>:/opt/neu-kg/
```

### 2.3 Tạo file .env trên server

```bash
# Copy template và sửa mật khẩu
cp .env.example .env      # hoặc tạo mới từ .env đã chuẩn bị
nano .env

# ĐỔI DB_PASSWORD thành mật khẩu mạnh!
# DB_PASSWORD=Neu@Graph2025!Secure
```

---

## BƯỚC 3 — Khởi động Neo4j Local

```bash
cd /opt/neu-kg

# Chỉ khởi động Neo4j trước (để migrate dữ liệu)
docker compose up -d neo4j

# Chờ Neo4j sẵn sàng (khoảng 60 giây)
docker compose logs -f neo4j
# → Chờ đến khi thấy: "Started."

# Kiểm tra Neo4j Browser
# Mở trình duyệt: http://<server-ip>:7474
# Login: neo4j / <NEO4J_PASSWORD trong .env>
```

---

## BƯỚC 4 — Migrate dữ liệu từ Aura Cloud

### 4.1 Chạy migration script (trên máy local hoặc server)

```bash
# Cài dependencies nếu chưa có
pip install neo4j python-dotenv

# BƯỚC 4.1: Export từ Aura Cloud → file JSON local
python migrate_neo4j.py --export
# → Tạo file: ./cache/neo4j_export.json

# BƯỚC 4.2: Copy file export lên server (nếu chạy từ máy local)
scp ./cache/neo4j_export.json user@<server-ip>:/opt/neu-kg/cache/

# BƯỚC 4.3: Import vào Neo4j local (chạy trên server)
# Trỏ LOCAL_URI về server nếu chạy từ máy local:
LOCAL_URI="bolt://<server-ip>:7687" python migrate_neo4j.py --import

# Hoặc chạy cả 2 bước cùng lúc (từ server):
python migrate_neo4j.py --export --import
```

### 4.2 Xác minh dữ liệu sau migration

```bash
# Vào Neo4j Browser: http://<server-ip>:7474
# Chạy Cypher query:

MATCH (n) RETURN labels(n)[0] AS label, count(n) AS total
ORDER BY total DESC;

# Kết quả mong đợi:
# SKILL        | 5217
# SUBJECT      |  802
# TEACHER      |  695
# MAJOR        |   37
# CAREER       |   27
# PERSONALITY  |   16
```

### 4.3 Nếu muốn re-import từ cache/output thay vì từ Aura

```bash
# Copy thư mục cache/output lên server
scp -r ./cache/output user@<server-ip>:/opt/neu-kg/cache/

# Chạy script2 trong Docker (môi trường đã có đủ dependencies)
docker compose run --rm migration
# (migration service trong docker-compose.yml chạy python script2.py)
```

---

## BƯỚC 5 — Deploy FastAPI App

### 5.1 Build Docker image

```bash
cd /opt/neu-kg

# Build image
docker compose build api

# Hoặc build thủ công với tag
docker build -t neu-kg-api:latest .
```

### 5.2 Khởi động toàn bộ stack

```bash
docker compose up -d

# Kiểm tra status
docker compose ps

# Xem logs API
docker compose logs -f api
# → Chờ: "Application startup complete."

# Test API
curl http://localhost:8000/health
# → {"status": "ok"}

curl -X POST http://localhost:8000/chat \
  -H "Content-Type: application/json" \
  -d '{"question": "ngành CNTT có những môn gì?"}'
```

---

## BƯỚC 6 — Deploy trên Portainer UI

### Cách 1: Upload Stack file (khuyến nghị)

1. Mở Portainer: `http://<server-ip>:9000`
2. **Stacks** → **Add Stack**
3. Đặt tên: `neu-kg`
4. Chọn **Upload** → upload file `portainer-stack.yml`
5. Kéo xuống **Environment variables**, thêm:
   ```
   NEO4J_PASSWORD   = Neu@Graph2025!Secure
   OPENAI_API_KEY   = sk-proj-...
   MINIO_ACCESS_KEY = course2
   MINIO_SECRET_KEY = course2-s3-uiauia
   ```
6. Click **Deploy the stack**

### Cách 2: Web Editor

1. **Stacks** → **Add Stack** → **Web editor**
2. Dán nội dung `portainer-stack.yml` vào editor
3. Điền Environment variables như trên
4. Deploy

### Build image trước khi deploy trên Portainer

Portainer cần image đã được build sẵn (nếu không dùng Git build):

```bash
# Trên server, build image
cd /opt/neu-kg
docker build -t neu-kg-api:latest .

# Portainer sẽ tìm thấy image này trong local registry
```

---

## BƯỚC 7 — Cập nhật Frontend (nếu có)

### Thay đổi API endpoint

Nếu frontend đang gọi API từ Vercel (ví dụ `https://your-app.vercel.app`),
cần đổi sang:

```
http://<server-ip>:8000
```

Hoặc nếu có domain + Nginx:
```
https://kg.neu.edu.vn
```

### Cấu hình Nginx reverse proxy (tùy chọn)

```nginx
# /etc/nginx/sites-available/neu-kg
server {
    listen 80;
    server_name kg.neu.edu.vn;

    location /api/ {
        proxy_pass http://localhost:8000/;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_read_timeout 120s;
    }
}
```

---

## BƯỚC 8 — Thay đổi connection string trong code

> **Không cần sửa code!** Tất cả connection string đọc từ env vars.

Chỉ cần đảm bảo `.env` hoặc Portainer env vars có:

| Biến | Giá trị cũ (Vercel/Aura) | Giá trị mới (Docker) |
|---|---|---|
| `DB_URL` | `neo4j+s://bdfc7297.databases.neo4j.io` | `bolt://neo4j:7687` |
| `DB_USER` | `bdfc7297` | `neo4j` |
| `DB_PASSWORD` | `0WWAdQtw...` | `<mật khẩu mới>` |

Protocol thay đổi:
- `neo4j+s://` → kết nối TLS với Aura
- `bolt://` → kết nối nội bộ Docker (không cần TLS)

---

## Xử lý sự cố thường gặp

### Neo4j không start được

```bash
# Xem log chi tiết
docker compose logs neo4j

# Kiểm tra RAM (Neo4j cần ít nhất 1GB)
free -h

# Giảm memory nếu cần (sửa docker-compose.yml):
# NEO4J_dbms_memory_heap_max__size: "512m"
# NEO4J_dbms_memory_pagecache_size: "256m"
```

### API không kết nối được Neo4j

```bash
# Kiểm tra network
docker compose exec api ping neo4j

# Kiểm tra credentials
docker compose exec api python -c "
from neo4j import GraphDatabase
d = GraphDatabase.driver('bolt://neo4j:7687', auth=('neo4j', 'YourPassword'))
print(d.verify_connectivity())
"
```

### Migration bị lỗi node không tìm thấy

Một số relationships có thể fail nếu node chưa được import.
Script đã xử lý batch order (nodes trước, rels sau).
Nếu vẫn fail, chạy lại `--import` lần 2 — idempotent do dùng MERGE.

### Port 7687 hoặc 8000 đã bị chiếm

```bash
# Tìm process đang dùng port
sudo lsof -i :7687
sudo lsof -i :8000

# Hoặc đổi port trong docker-compose.yml:
# "8001:8000"  → truy cập qua port 8001
```

---

## Checklist hoàn thành migration

- [ ] Neo4j local khởi động thành công
- [ ] Dữ liệu đã được migrate (verify bằng count query)
- [ ] API build và chạy không có lỗi
- [ ] Test endpoint `/chat` trả về kết quả đúng
- [ ] Frontend đã trỏ sang endpoint mới
- [ ] Portainer stack hiển thị tất cả services "running"
- [ ] Tắt Vercel deployment (tránh chi phí)
- [ ] Backup file `neo4j_export.json` vào nơi an toàn

---

## Backup định kỳ

```bash
# Tạo script backup (chạy bằng cron)
# crontab -e → thêm:
# 0 2 * * * /opt/neu-kg/backup.sh

cat > /opt/neu-kg/backup.sh << 'EOF'
#!/bin/bash
DATE=$(date +%Y%m%d)
# Dump Neo4j data
docker exec neu_neo4j neo4j-admin database dump neo4j \
  --to-path=/data/backup_${DATE}.dump

# Copy ra ngoài container
docker cp neu_neo4j:/data/backup_${DATE}.dump \
  /opt/neu-kg/backups/

echo "Backup done: backup_${DATE}.dump"
EOF
chmod +x /opt/neu-kg/backup.sh
```
