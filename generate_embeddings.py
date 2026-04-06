#!/usr/bin/env python3
"""
Generate semantic embeddings for all USG job listings.

Run this ONCE from the usg-jobs directory:

    pip install fastembed "numpy<2"
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
    from fastembed import TextEmbedding
except ImportError:
    print("ERROR: fastembed is not installed.")
    print("       Run:  pip install fastembed 'numpy<2'")
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
MODEL = "sentence-transformers/all-MiniLM-L6-v2"
print(f"\nLoading model '{MODEL}'...")
print("  (First run downloads ~90 MB from HuggingFace and caches it locally)\n")
model = TextEmbedding(model_name=MODEL)

# ── Compute embeddings ────────────────────────────────────────────────────────
print(f"Computing embeddings for {len(texts)} jobs (this takes ~1-2 minutes)...")
embeddings_iter = model.embed(texts)

# fastembed returns a generator — collect into a list then stack
emb_list = list(embeddings_iter)
embeddings = np.array(emb_list, dtype=np.float32)

# L2-normalise so cosine similarity = dot product (matches browser-side behaviour)
norms = np.linalg.norm(embeddings, axis=1, keepdims=True)
norms = np.where(norms == 0, 1, norms)
embeddings = (embeddings / norms).astype(np.float32)

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

# ── Compute top-5 similar jobs and inject into jobs.json ─────────────────────
# Dot product matrix — since vectors are L2-normalised this equals cosine sim.
# We process in chunks to avoid OOM on large corpora.
print("\nComputing pairwise similarities for 'Similar Jobs' feature…")
TOP_K    = 5
CHUNK    = 256          # rows processed per batch
n        = len(jobs)
similar  = [None] * n   # will hold list of top-K neighbour IDs per job

for start in range(0, n, CHUNK):
    end   = min(start + CHUNK, n)
    chunk = embeddings[start:end]          # (chunk_size, dims)
    sims  = chunk @ embeddings.T           # (chunk_size, n)   dot products
    for local_i, global_i in enumerate(range(start, end)):
        row   = sims[local_i]
        row[global_i] = -1                 # exclude self
        top   = row.argsort()[::-1][:TOP_K]
        similar[global_i] = [ids[j] for j in top]

# Reload and patch jobs.json in-place
jobs_data = json.loads(jobs_path.read_text())
job_list  = jobs_data.get("jobs", [])
id_to_sim = {ids[i]: similar[i] for i in range(n)}
for job in job_list:
    sim = id_to_sim.get(job["id"])
    if sim:
        job["similar"] = sim
    else:
        job.pop("similar", None)

jobs_path.write_text(json.dumps(jobs_data, separators=(",", ":"), ensure_ascii=False))
print(f"Injected 'similar' field into {len(job_list)} jobs in {jobs_path.name}")

# ── Done ──────────────────────────────────────────────────────────────────────
print("\n✅  Done! Now commit and push to deploy:")
print("    git add job_embeddings.bin job_embeddings_meta.json")
print('    git commit -m "Add semantic job embeddings"')
print("    git push")
