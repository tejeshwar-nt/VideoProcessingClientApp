const express = require("express");
const multer = require("multer");
const path = require("path");
const fs = require("fs");
const crypto = require("crypto");
const Database = require("better-sqlite3");

const app = express();
const PORT = 3000;

app.use(express.json());

// Storage dirs
const UPLOAD_DIR = path.join(__dirname, "uploads");
if (!fs.existsSync(UPLOAD_DIR)) fs.mkdirSync(UPLOAD_DIR);

const HDF5_DIR = path.join(__dirname, "uploads", "hdf5");
if (!fs.existsSync(HDF5_DIR)) fs.mkdirSync(HDF5_DIR, { recursive: true });

// Multer for video uploads
const storage = multer.diskStorage({
  destination: (req, file, cb) => cb(null, UPLOAD_DIR),
  filename: (req, file, cb) => {
    const ext = path.extname(file.originalname) || ".mp4";
    cb(null, `${Date.now()}${ext}`);
  },
});
const upload = multer({ storage });

// Multer for HDF5 uploads
const hdf5Storage = multer.diskStorage({
  destination: (req, file, cb) => cb(null, HDF5_DIR),
  filename: (req, file, cb) => {
    const base = (req.body && req.body.videoId) ? req.body.videoId : `${Date.now()}`;
    cb(null, `${base}.h5`);
  },
});
const uploadHdf5 = multer({
  storage: hdf5Storage,
  fileFilter: (req, file, cb) => {
    const ext = path.extname(file.originalname).toLowerCase();
    if (ext === ".h5") return cb(null, true);
    cb(new Error("Only .h5 files are accepted"));
  },
});

const REFERENCE_DIR = path.join(__dirname, "data/processed/popsign_v2/mltk-hands/popsign_v2/reference");

// SQLite database
const DATA_DIR = path.join(__dirname, "data");
if (!fs.existsSync(DATA_DIR)) fs.mkdirSync(DATA_DIR);
const db = new Database(path.join(DATA_DIR, "state.db"));

db.exec(`
  CREATE TABLE IF NOT EXISTS phones (
    phone_id   TEXT PRIMARY KEY,
    last_seen_ms INTEGER,
    user_agent TEXT
  );

  CREATE TABLE IF NOT EXISTS jobs (
    video_id          TEXT PRIMARY KEY,
    status            TEXT NOT NULL DEFAULT 'pending',
    created_ms        INTEGER,
    leased_to         TEXT,
    lease_until_ms    INTEGER,
    leased_ms         INTEGER,
    completed_ms      INTEGER,
    processed_by      TEXT,
    hdf5_file         TEXT,
    fail_reason       TEXT,
    processing_time_ms INTEGER
  );
`);

const nowMs = () => Date.now();
const PHONE_ONLINE_WINDOW_MS = 30_000;
const LEASE_DURATION_MS = 10 * 60 * 1000;

function listUploadFilesSorted() {
  return fs.readdirSync(UPLOAD_DIR)
    .filter(f => !f.startsWith(".") && fs.statSync(path.join(UPLOAD_DIR, f)).isFile())
    .sort();
}

function syncJobsWithUploads() {
  const insertJob = db.prepare(`INSERT OR IGNORE INTO jobs (video_id, status, created_ms) VALUES (?, 'pending', ?)`);
  for (const videoId of listUploadFilesSorted()) {
    insertJob.run(videoId, nowMs());
  }
  // Mark jobs as failed if their uploaded file is missing
  for (const { video_id } of db.prepare(`SELECT video_id FROM jobs WHERE status != 'done'`).all()) {
    if (!fs.existsSync(path.join(UPLOAD_DIR, video_id))) {
      db.prepare(`UPDATE jobs SET status = 'failed', fail_reason = ? WHERE video_id = ? AND status != 'done'`)
        .run("File missing from uploads directory", video_id);
    }
  }
}

function isLeaseExpired(job) {
  return job.status === "leased" && typeof job.lease_until_ms === "number" && job.lease_until_ms <= nowMs();
}

function ensurePhone(phoneId, req) {
  if (!phoneId || typeof phoneId !== "string") return false;
  db.prepare(`INSERT INTO phones (phone_id, last_seen_ms, user_agent) VALUES (?, ?, ?)
    ON CONFLICT(phone_id) DO UPDATE SET last_seen_ms = excluded.last_seen_ms, user_agent = excluded.user_agent`)
    .run(phoneId, nowMs(), req.headers["user-agent"] || null);
  return true;
}

// Upload a single video
app.post("/upload", upload.single("video"), (req, res) => {
  const filename = req.file.filename;
  db.prepare(`INSERT OR IGNORE INTO jobs (video_id, status, created_ms) VALUES (?, 'pending', ?)`).run(filename, nowMs());
  res.json({ videoId: filename, downloadUrl: `/videos/${filename}` });
});

// Upload many videos at once
app.post("/upload-many", upload.array("videos"), (req, res) => {
  const insertJob = db.prepare(`INSERT OR IGNORE INTO jobs (video_id, status, created_ms) VALUES (?, 'pending', ?)`);
  const files = (req.files || []).map((f) => {
    insertJob.run(f.filename, nowMs());
    return { videoId: f.filename, downloadUrl: `/videos/${f.filename}`, originalName: f.originalname, size: f.size };
  });
  res.json({ count: files.length, files });
});

// Download a video
app.get("/videos/:id", (req, res) => {
  const filePath = path.join(UPLOAD_DIR, req.params.id);
  if (!fs.existsSync(filePath)) return res.status(404).json({ error: "Video not found" });
  res.sendFile(filePath);
});

// List all uploaded videos
app.get("/list", (req, res) => {
  const files = listUploadFilesSorted();
  res.json({ count: files.length, videoIds: files });
});

// Register a phone
app.post("/register", (req, res) => {
  const phoneId = crypto.randomUUID();
  db.prepare(`INSERT INTO phones (phone_id, last_seen_ms, user_agent) VALUES (?, ?, ?)`)
    .run(phoneId, nowMs(), req.headers["user-agent"] || null);
  res.json({ phoneId });
});

// Heartbeat
app.post("/heartbeat", (req, res) => {
  const { phoneId } = req.body || {};
  if (!ensurePhone(phoneId, req)) return res.status(400).json({ error: "Missing or invalid phoneId" });
  res.json({ ok: true, serverTimeMs: nowMs() });
});

// Claim the next available job
app.post("/claim", (req, res) => {
  const { phoneId } = req.body || {};
  if (!ensurePhone(phoneId, req)) return res.status(400).json({ error: "Missing or invalid phoneId" });

  syncJobsWithUploads();

  // Reclaim expired leases
  db.prepare(`UPDATE jobs SET status = 'pending', leased_to = NULL, lease_until_ms = NULL, leased_ms = NULL
    WHERE status = 'leased' AND lease_until_ms <= ?`).run(nowMs());

  const job = db.prepare(`SELECT * FROM jobs WHERE status = 'pending' ORDER BY video_id ASC LIMIT 1`).get();
  if (!job) return res.status(204).send();

  const leaseUntilMs = nowMs() + LEASE_DURATION_MS;
  db.prepare(`UPDATE jobs SET status = 'leased', leased_to = ?, lease_until_ms = ?, leased_ms = ? WHERE video_id = ?`)
    .run(phoneId, leaseUntilMs, nowMs(), job.video_id);

  res.json({ videoId: job.video_id, downloadUrl: `/videos/${job.video_id}`, leaseUntilMs });
});

// Mark a job as complete
app.post("/complete", (req, res) => {
  const { phoneId, videoId } = req.body || {};
  if (!ensurePhone(phoneId, req)) return res.status(400).json({ error: "Missing or invalid phoneId" });
  if (!videoId || typeof videoId !== "string") return res.status(400).json({ error: "Missing or invalid videoId" });

  syncJobsWithUploads();

  const job = db.prepare(`SELECT * FROM jobs WHERE video_id = ?`).get(videoId);
  if (!job) return res.status(404).json({ error: "Unknown job/videoId" });

  if (job.status === "leased" && job.leased_to && job.leased_to !== phoneId && !isLeaseExpired(job)) {
    return res.status(409).json({ error: "Job is leased to another phone" });
  }

  const completedMs = nowMs();
  const processingTimeMs = job.leased_ms ? completedMs - job.leased_ms : null;

  db.prepare(`UPDATE jobs SET status = 'done', completed_ms = ?, processed_by = ?, processing_time_ms = ?,
    leased_to = NULL, lease_until_ms = NULL WHERE video_id = ?`)
    .run(completedMs, phoneId, processingTimeMs, videoId);

  res.json({ ok: true });
});

// Mark a job as failed
app.post("/fail", (req, res) => {
  const { phoneId, videoId, reason } = req.body || {};
  if (!ensurePhone(phoneId, req)) return res.status(400).json({ error: "Missing or invalid phoneId" });
  if (!videoId || typeof videoId !== "string") return res.status(400).json({ error: "Missing or invalid videoId" });

  syncJobsWithUploads();

  const job = db.prepare(`SELECT * FROM jobs WHERE video_id = ?`).get(videoId);
  if (!job) return res.status(404).json({ error: "Unknown job/videoId" });

  db.prepare(`UPDATE jobs SET status = 'failed', fail_reason = ?, leased_to = NULL, lease_until_ms = NULL WHERE video_id = ?`)
    .run(typeof reason === "string" ? reason : "Unknown", videoId);

  res.json({ ok: true });
});

// See which phones are online
app.get("/phones", (req, res) => {
  const cutoff = nowMs() - PHONE_ONLINE_WINDOW_MS;
  const phones = db.prepare(`SELECT * FROM phones ORDER BY last_seen_ms DESC`).all()
    .map(p => ({ phoneId: p.phone_id, lastSeenMs: p.last_seen_ms, userAgent: p.user_agent, online: p.last_seen_ms >= cutoff }));
  res.json({ count: phones.length, onlineCount: phones.filter(p => p.online).length, phones });
});

// Queue stats
app.get("/queue", (req, res) => {
  syncJobsWithUploads();
  const rows = db.prepare(`SELECT status, COUNT(*) as count FROM jobs GROUP BY status`).all();
  const counts = { pending: 0, leased: 0, done: 0, failed: 0 };
  for (const { status, count } of rows) counts[status] = count;
  res.json(counts);
});

// Stats per phone: how many videos processed and average processing time
app.get("/phone-stats", (req, res) => {
  const rows = db.prepare(`
    SELECT processed_by as phoneId,
           COUNT(*) as videosProcessed,
           AVG(processing_time_ms) as avgProcessingTimeMs,
           MIN(processing_time_ms) as minProcessingTimeMs,
           MAX(processing_time_ms) as maxProcessingTimeMs
    FROM jobs WHERE status = 'done' AND processed_by IS NOT NULL
    GROUP BY processed_by
  `).all();
  res.json(rows);
});

// Delete all fully processed videos
app.delete("/cleanup", (req, res) => {
  const doneJobs = db.prepare(`SELECT video_id FROM jobs WHERE status = 'done'`).all();
  let deleted = 0;
  const errors = [];
  for (const { video_id } of doneJobs) {
    try {
      const filePath = path.join(UPLOAD_DIR, video_id);
      if (fs.existsSync(filePath)) fs.unlinkSync(filePath);
      db.prepare(`DELETE FROM jobs WHERE video_id = ?`).run(video_id);
      deleted++;
    } catch (e) {
      errors.push({ videoId: video_id, error: e.message });
    }
  }
  res.json({ deleted, errors });
});

// Cancel all pending jobs
app.post("/cancel-pending", (req, res) => {
  const result = db.prepare(`DELETE FROM jobs WHERE status = 'pending'`).run();
  res.json({ ok: true, cancelled: result.changes });
});

// Reset everything
app.post("/reset", (req, res) => {
  const files = listUploadFilesSorted();
  const errors = [];
  for (const file of files) {
    try { fs.unlinkSync(path.join(UPLOAD_DIR, file)); }
    catch (e) { errors.push({ file, error: e.message }); }
  }
  db.prepare(`DELETE FROM phones`).run();
  db.prepare(`DELETE FROM jobs`).run();
  res.json({ ok: true, deletedFiles: files.length, errors });
});

// Receive processed HDF5 from a phone
app.post("/upload-hdf5", (req, res, next) => {
  uploadHdf5.single("hdf5")(req, res, (err) => {
    if (err) return res.status(400).json({ error: err.message });
    next();
  });
}, (req, res) => {
  const { phoneId, videoId } = req.body || {};
  if (!ensurePhone(phoneId, req)) return res.status(400).json({ error: "Missing or invalid phoneId" });
  if (!videoId || typeof videoId !== "string") return res.status(400).json({ error: "Missing or invalid videoId" });
  if (!req.file) return res.status(400).json({ error: "No HDF5 file received" });

  const job = db.prepare(`SELECT * FROM jobs WHERE video_id = ?`).get(videoId);
  if (job && job.status !== "done") {
    const completedMs = nowMs();
    const processingTimeMs = job.leased_ms ? completedMs - job.leased_ms : null;
    db.prepare(`UPDATE jobs SET status = 'done', completed_ms = ?, hdf5_file = ?, processed_by = ?,
      processing_time_ms = ?, leased_to = NULL, lease_until_ms = NULL WHERE video_id = ?`)
      .run(completedMs, req.file.filename, phoneId, processingTimeMs, videoId);
  }

  res.json({ ok: true, hdf5Id: req.file.filename });
});

// Serve a reference H5 file for testing
app.get("/test-hdf5", (req, res) => {
  const files = fs.readdirSync(REFERENCE_DIR).filter(f => f.endsWith(".h5")).sort();
  if (files.length === 0) return res.status(404).json({ error: "No H5 files found in reference directory" });
  res.sendFile(path.join(REFERENCE_DIR, files[0]));
});

syncJobsWithUploads();

app.listen(PORT, () => {
  console.log(`Server running on http://localhost:${PORT}`);
});
