const express = require("express");
const multer = require("multer");
const path = require("path");
const fs = require("fs");
const crypto = require("crypto");

const app = express();
const PORT = 3000;

// Parse JSON for the worker endpoints
app.use(express.json());

// Storage: uploaded files
const UPLOAD_DIR = path.join(__dirname, "uploads");
if (!fs.existsSync(UPLOAD_DIR)) fs.mkdirSync(UPLOAD_DIR);

// store uploads on disk
const storage = multer.diskStorage({
  destination: (req, file, cb) => cb(null, UPLOAD_DIR),
  filename: (req, file, cb) => {
    const ext = path.extname(file.originalname) || ".mp4";
    cb(null, `${Date.now()}${ext}`);
  },
});
const upload = multer({ storage });

// HDF5 results from phones
const HDF5_DIR = path.join(__dirname, "uploads", "hdf5");
if (!fs.existsSync(HDF5_DIR)) fs.mkdirSync(HDF5_DIR, { recursive: true });

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

// Reference H5 files for testing
const REFERENCE_DIR = path.join(__dirname, "data/processed/popsign_v2/mltk-hands/popsign_v2/reference");

// Simple persistent state in JSON
const STATE_PATH = path.join(__dirname, "state.json");


function loadState() {
  try {
    if (!fs.existsSync(STATE_PATH)) return { phones: {}, jobs: {} };
    const raw = fs.readFileSync(STATE_PATH, "utf8");
    const parsed = JSON.parse(raw);
    return {
      phones: parsed.phones || {},
      jobs: parsed.jobs || {},
    };
  } catch (e) {
    console.error("Failed to load state.json; starting fresh:", e.message);
    return { phones: {}, jobs: {} };
  }
}

function saveState() {
  try {
    fs.writeFileSync(STATE_PATH, JSON.stringify(state, null, 2), "utf8");
  } catch (e) {
    console.error("Failed to save state.json:", e.message);
  }
}

let state = loadState();

const nowMs = () => Date.now();

// consider a phone online if it heartbeated recently
const PHONE_ONLINE_WINDOW_MS = 30_000;

// lease duration (how long a phone has to download/process before job can be reclaimed)
const LEASE_DURATION_MS = 10 * 60 * 1000; // 10 minutes

function listUploadFilesSorted() {
  return fs
    .readdirSync(UPLOAD_DIR)
    .filter((f) => !f.startsWith("."))
    .sort();
}

// Ensure every file in uploads has a job record
function syncJobsWithUploads() {
  const files = listUploadFilesSorted();
  for (const videoId of files) {
    if (!state.jobs[videoId]) {
      state.jobs[videoId] = {
        status: "pending",
        createdMs: nowMs(),
      };
    }
  }
  // if a job exists but file is deleted, mark failed
  for (const videoId of Object.keys(state.jobs)) {
    const filePath = path.join(UPLOAD_DIR, videoId);
    if (!fs.existsSync(filePath) && state.jobs[videoId].status !== "done") {
      state.jobs[videoId].status = "failed";
      state.jobs[videoId].failReason = "File missing from uploads directory";
    }
  }
  saveState();
}

function isLeaseExpired(job) {
  return job.status === "leased" && typeof job.leaseUntilMs === "number" && job.leaseUntilMs <= nowMs();
}

function ensurePhone(phoneId, req) {
  if (!phoneId || typeof phoneId !== "string") return false;
  if (!state.phones[phoneId]) {
    state.phones[phoneId] = { lastSeenMs: nowMs(), userAgent: req.headers["user-agent"] };
  } else {
    state.phones[phoneId].lastSeenMs = nowMs();
    state.phones[phoneId].userAgent = req.headers["user-agent"];
  }
  return true;
}


// upload a single video
app.post("/upload", upload.single("video"), (req, res) => {
  const filename = req.file.filename;

  // Create a pending job for this upload
  if (!state.jobs[filename]) {
    state.jobs[filename] = { status: "pending", createdMs: nowMs() };
    saveState();
  }

  res.json({
    videoId: filename,
    downloadUrl: `/videos/${filename}`,
  });
});

// upload many videos at once (pass in an array of files, usually from a folder)
app.post("/upload-many", upload.array("videos", 500), (req, res) => {
  const files = (req.files || []).map((f) => {
    // Create jobs for each uploaded file
    if (!state.jobs[f.filename]) {
      state.jobs[f.filename] = { status: "pending", createdMs: nowMs() };
    }
    return {
      videoId: f.filename,
      downloadUrl: `/videos/${f.filename}`,
      originalName: f.originalname,
      size: f.size,
    };
  });

  saveState();

  res.json({
    count: files.length,
    files,
  });
});

// download endpoints
app.get("/videos/:id", (req, res) => {
  const filePath = path.join(UPLOAD_DIR, req.params.id);
  if (!fs.existsSync(filePath)) {
    return res.status(404).json({ error: "Video not found" });
  }
  res.sendFile(filePath);
});

// List all uploaded videos/filenames
app.get("/list", (req, res) => {
  const files = listUploadFilesSorted();
  res.json({ count: files.length, videoIds: files });
});


// Register a phone (call once; store phoneId on device)
app.post("/register", (req, res) => {
  const phoneId = crypto.randomUUID();
  state.phones[phoneId] = {
    lastSeenMs: nowMs(),
    userAgent: req.headers["user-agent"],
  };
  saveState();
  res.json({ phoneId });
});

// heartbeat while app is open
app.post("/heartbeat", (req, res) => {
  const { phoneId } = req.body || {};
  if (!ensurePhone(phoneId, req)) {
    return res.status(400).json({ error: "Missing or invalid phoneId" });
  }
  saveState();
  res.json({ ok: true, serverTimeMs: nowMs() });
});

// Claim the next available job (one video) for this phone.
// Returns No Content if none are available.
app.post("/claim", (req, res) => {
  const { phoneId } = req.body || {};
  if (!ensurePhone(phoneId, req)) {
    return res.status(400).json({ error: "Missing or invalid phoneId" });
  }

  // Make sure jobs exist for all upload files
  syncJobsWithUploads();

  // Reclaim expired leases
  for (const job of Object.values(state.jobs)) {
    if (isLeaseExpired(job)) {
      job.status = "pending";
      delete job.leasedTo;
      delete job.leaseUntilMs;
    }
  }

  // Choose the first pending job
  const pendingIds = Object.keys(state.jobs)
    .filter((id) => state.jobs[id].status === "pending")
    .sort();

  if (pendingIds.length === 0) {
    saveState();
    return res.status(204).send();
  }

  const videoId = pendingIds[0];
  const job = state.jobs[videoId];

  job.status = "leased";
  job.leasedTo = phoneId;
  job.leaseUntilMs = nowMs() + LEASE_DURATION_MS;

  saveState();

  res.json({
    videoId,
    downloadUrl: `/videos/${videoId}`,
    leaseUntilMs: job.leaseUntilMs,
  });
});

// Mark a job as complete after processing
app.post("/complete", (req, res) => {
  const { phoneId, videoId } = req.body || {};
  if (!ensurePhone(phoneId, req)) {
    return res.status(400).json({ error: "Missing or invalid phoneId" });
  }
  if (!videoId || typeof videoId !== "string") {
    return res.status(400).json({ error: "Missing or invalid videoId" });
  }

  syncJobsWithUploads();

  const job = state.jobs[videoId];
  if (!job) return res.status(404).json({ error: "Unknown job/videoId" });

  // Only allow the leasing phone to complete it, unless lease expired
  if (job.status === "leased" && job.leasedTo && job.leasedTo !== phoneId && !isLeaseExpired(job)) {
    return res.status(409).json({ error: "Job is leased to another phone" });
  }

  job.status = "done";
  job.completedMs = nowMs();
  delete job.leasedTo;
  delete job.leaseUntilMs;

  saveState();
  res.json({ ok: true });
});

// Mark a job as failed so it can be retried later
app.post("/fail", (req, res) => {
  const { phoneId, videoId, reason } = req.body || {};
  if (!ensurePhone(phoneId, req)) {
    return res.status(400).json({ error: "Missing or invalid phoneId" });
  }
  if (!videoId || typeof videoId !== "string") {
    return res.status(400).json({ error: "Missing or invalid videoId" });
  }

  syncJobsWithUploads();

  const job = state.jobs[videoId];
  if (!job) return res.status(404).json({ error: "Unknown job/videoId" });

  job.status = "failed";
  job.failReason = typeof reason === "string" ? reason : "Unknown";
  delete job.leasedTo;
  delete job.leaseUntilMs;

  saveState();
  res.json({ ok: true });
});

// See which phones are online right now based on recent heartbeats
app.get("/phones", (req, res) => {
  const cutoff = nowMs() - PHONE_ONLINE_WINDOW_MS;
  const phones = Object.entries(state.phones)
    .map(([phoneId, info]) => ({ phoneId, ...info, online: info.lastSeenMs >= cutoff }))
    .sort((a, b) => b.lastSeenMs - a.lastSeenMs);

  res.json({
    count: phones.length,
    onlineCount: phones.filter((p) => p.online).length,
    phones,
  });
});

// Queue stats (pending/leased/done/failed)
app.get("/queue", (req, res) => {
  syncJobsWithUploads();

  const counts = { pending: 0, leased: 0, done: 0, failed: 0 };
  for (const job of Object.values(state.jobs)) {
    counts[job.status] = (counts[job.status] || 0) + 1;
  }
  res.json(counts);
});

// Delete all fully processed videos
app.delete("/cleanup", (req, res) => {
  const doneIds = Object.keys(state.jobs).filter((id) => state.jobs[id].status === "done");

  let deleted = 0;
  let errors = [];

  for (const videoId of doneIds) {
    const filePath = path.join(UPLOAD_DIR, videoId);
    try {
      if (fs.existsSync(filePath)) fs.unlinkSync(filePath);
      delete state.jobs[videoId];
      deleted++;
    } catch (e) {
      errors.push({ videoId, error: e.message });
    }
  }

  saveState();
  res.json({ deleted, errors });
});

// Reset the queue and states, delete all files in uploads directory
app.post("/reset", (req, res) => {
  const files = listUploadFilesSorted();
  const errors = [];

  for (const file of files) {
    try {
      fs.unlinkSync(path.join(UPLOAD_DIR, file));
    } catch (e) {
      errors.push({ file, error: e.message });
    }
  }

  state.phones = {};
  state.jobs = {};
  saveState();

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

  if (!ensurePhone(phoneId, req)) {
    return res.status(400).json({ error: "Missing or invalid phoneId" });
  }
  if (!videoId || typeof videoId !== "string") {
    return res.status(400).json({ error: "Missing or invalid videoId" });
  }
  if (!req.file) {
    return res.status(400).json({ error: "No HDF5 file received" });
  }

  const job = state.jobs[videoId];
  if (job && job.status !== "done") {
    job.status = "done";
    job.completedMs = nowMs();
    job.hdf5File = req.file.filename;
    delete job.leasedTo;
    delete job.leaseUntilMs;
    saveState();
  }

  res.json({ ok: true, hdf5Id: req.file.filename });
});

// Serve a reference H5 file for testing
app.get("/test-hdf5", (req, res) => {
  const files = fs.readdirSync(REFERENCE_DIR)
    .filter(f => f.endsWith(".h5"))
    .sort();
  if (files.length === 0)
    return res.status(404).json({ error: "No H5 files found in reference directory" });
  res.sendFile(path.join(REFERENCE_DIR, files[0]));
});

// Initialize job list on startup
syncJobsWithUploads();

app.listen(PORT, () => {
  console.log(`Server running on http://localhost:${PORT}`);
});
