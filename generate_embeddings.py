#!/usr/bin/env python3
"""
Generate semantic embeddings for all USG job listings.

Run this ONCE from the usg-jobs directory:

    pip install sentence-transformers
    python3 generate_embeddings.py

This creates two files in the same directory:
    job_embeddings.bin       -- Float32 vectors, shape [n_jobs × 384]
    job_embeddings_meta.json -- {"jobId": rowIndex, ...} mapping

These files are loaded by the website for semantic CV matching.
After running, commit and push both files:

    git add job_embeddings.bin job_embeddings_meta.json
    git commit -m "Add semantic job embeddings"
    git push
"""

import json, sys, numpy as np
from pathlib import Path

try:
    from sentence_transformers import SentenceTransformer
except ImportError:
    print("ERROR: sentence-transformers is not installed.")
    print("       Run:  pip install sentence-transformers")
    sys.exit(1)

# ── Load jobs ──────────────────────────────────────────────────────────────────
jobs_path = Path(__file__).parent / "jobs.json"
if not jobs_path.exists():
    print(f"ERROR: jobs.json not found at {jobs_path}")
    print("       Run this script from the usg-jobs directory.")
    sys.exit(1)

with open(jobs_path) as f:
    data = json.load(f)

jobs = data.get("jobs", [])
print(f"Loaded {len(jobs)} jobs from {jobs_path}")

# ── Build one text string per job ─────────────────────────────────────────────
# Weight title most heavily by repeating it; include dept and summary.
def job_text(job):
    title   = (job.get("title",      "") or "").strip()
    dept    = (job.get("department", "") or "").strip()
    summary = (job.get("summary",    "") or "").strip()
    # Repeat title 3× so the model treats it as the most important field
    parts = [title, title, title, dept, summary]
    return " ".join(p for p in parts if p)

texts = [job_text(j) for j in jobs]
ids   = [j["id"] for j in jobs]

# ── Load model ────────────────────────────────────────────────────────────────
MODEL = "all-MiniLM-L6-v2"
print(f"\nLoading model '{MODEL}'...")
print("  (First run downloads ~90 MB from HuggingFace and caches it locally)\n")
model = SentenceTransformer(MODEL)

# ── Compute embeddings ────────────────────────────────────────────────────────
print("Computing embeddings (this takes ~1-2 minutes)...")
embeddings = model.encode(
    texts,
    batch_size=64,
    show_progress_bar=True,
    normalize_embeddings=True,   # L2-normalise so cosine sim = dot product
    convert_to_numpy=True,
).astype(np.float32)

print(f"\nEmbeddings shape: {embeddings.shape}  (jobs × dimensions)")

# ── Save binary embedding matrix ─────────────────────────────────────────────
out_dir  = Path(__file__).parent
bin_path = out_dir / "job_embeddings.bin"
with open(bin_path, "wb") as f:
    f.write(embeddings.tobytes())
mb = bin_path.stat().st_size / 1024 / 1024
print(f"Saved {bin_path.name}  ({mb:.1f} MB)")

# ── Save id→rowIndex mapping ──────────────────────────────────────────────────
meta      = {job_id: idx for idx, job_id in enumerate(ids)}
meta_path = out_dir / "job_embeddings_meta.json"
with open(meta_path, "w") as f:
    json.dump(meta, f, separators=(",", ":"))
print(f"Saved {meta_path.name}  ({meta_path.stat().st_size / 1024:.0f} KB)")

# ── Done ──────────────────────────────────────────────────────────────────────
print("\n✅  Done! Now commit and push to deploy:")
print("    git add job_embeddings.bin job_embeddings_meta.json")
print('    git commit -m "Add semantic job embeddings"')
print("    git push")
