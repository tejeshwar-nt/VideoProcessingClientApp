package com.example.myapplication

import android.os.Bundle
import androidx.activity.ComponentActivity
import androidx.activity.compose.setContent
import androidx.compose.foundation.layout.Column
import androidx.compose.material3.Button
import androidx.compose.material3.LinearProgressIndicator
import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.Surface
import androidx.compose.material3.Text
import androidx.compose.runtime.*
import androidx.compose.ui.platform.LocalContext
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.withContext
import java.io.File

class MainActivity : ComponentActivity() {
    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)

        setContent {
            val ctx = LocalContext.current

            var status by remember { mutableStateOf("Idle") }
            var running by remember { mutableStateOf(true) }

            var currentVideo by remember { mutableStateOf<String?>(null) }
            var progress by remember { mutableStateOf<Float?>(null) }

            suspend fun ensurePhoneId(): String {
                val existing = PhoneIdStore.get(ctx)
                if (existing != null) return existing

                status = "Registering phone…"
                val reg = ApiClient.api.register()
                PhoneIdStore.set(ctx, reg.phoneId)
                return reg.phoneId
            }

            suspend fun downloadWithProgress(videoId: String): File {
                currentVideo = videoId
                progress = 0f

                val outFile = File(ctx.filesDir, videoId)
                // If a previous partial file exists, remove it before redownloading
                if (outFile.exists()) outFile.delete()

                val resp = ApiClient.api.downloadVideo(videoId)
                if (!resp.isSuccessful || resp.body() == null) {
                    val errText = resp.errorBody()?.string()
                    throw RuntimeException("Download failed HTTP ${resp.code()} ${errText ?: ""}".trim())
                }

                val body = resp.body()!!
                val total = body.contentLength() // may be -1 if unknown
                var read = 0L

                // downloading video
                withContext(Dispatchers.IO) {
                    body.byteStream().use { input ->
                        outFile.outputStream().use { output ->
                            val buf = ByteArray(8 * 1024)
                            while (true) {
                                val n = input.read(buf)
                                if (n <= 0) break
                                output.write(buf, 0, n)
                                read += n

                                progress =
                                    if (total > 0) (read.toDouble() / total.toDouble()).toFloat()
                                    else null
                            }
                            output.flush()
                        }
                    }
                }

                // Validate full download when content length is known
                if (total > 0 && read != total) {
                    outFile.delete()
                    throw RuntimeException("Incomplete download for $videoId: wrote $read bytes, expected $total")
                }

                progress = null
                currentVideo = null
                return outFile
            }

            // Placeholder processing step
            suspend fun processVideo(file: File) {
                // TODO: run Mediapipe processing
                delay(500)
            }

            // Worker loop runs while app is open
            LaunchedEffect(running) {
                if (!running) return@LaunchedEffect

                while (isActive && running) {
                    var claimedVideoId: String? = null
                    try {
                        val phoneId = ensurePhoneId()
                        status = "Worker running (phoneId=$phoneId)…"

                        // heartbeat
                        ApiClient.api.heartbeat(PhoneIdBody(phoneId))

                        // claim a job
                        val claimResp = ApiClient.api.claim(PhoneIdBody(phoneId))

                        if (claimResp.code() == 204) {
                            status = "No jobs. Waiting…"
                            delay(3000)
                            continue
                        }

                        if (!claimResp.isSuccessful || claimResp.body() == null) {
                            status = "Claim error: HTTP ${claimResp.code()}"
                            delay(3000)
                            continue
                        }

                        val job = claimResp.body()!!
                        val videoId = job.videoId
                        claimedVideoId = videoId

                        // skip if already downloaded
                        val existing = File(ctx.filesDir, videoId)
                        if (existing.exists()) {
                            status = "Already have $videoId, completing…"
                            ApiClient.api.complete(CompleteBody(phoneId, videoId))
                            claimedVideoId = null
                            continue
                        }

                        // download
                        status = "Downloading $videoId…"
                        val file = downloadWithProgress(videoId)

                        // process
                        status = "Processing $videoId…"
                        processVideo(file)

                        // complete
                        status = "Completing $videoId…"
                        ApiClient.api.complete(CompleteBody(phoneId, videoId))
                        claimedVideoId = null

                        status = "Done $videoId. Claiming next…"
                    } catch (e: Exception) {
                        val vid = claimedVideoId
                        if (vid != null) {
                            try {
                                // Remove partial file if it exists
                                File(ctx.filesDir, vid).delete()
                                // Mark job failed so it can be retried
                                val reason = "${e::class.simpleName ?: e.javaClass.simpleName}: ${e.message ?: "Unknown error"}"
                                val pid = PhoneIdStore.get(ctx)
                                if (pid != null) ApiClient.api.fail(FailBody(pid, vid, reason))
                            } catch (_: Exception) {
                                // ignore secondary failures
                            } finally {
                                claimedVideoId = null
                            }
                        }
                        status = "Error: ${e.message}"
                        delay(3000)
                    }
                }
            }

            MaterialTheme {
                Surface {
                    Column {
                        Text(status)

                        if (currentVideo != null) {
                            Text("Current: $currentVideo")
                            when (val p = progress) {
                                null -> LinearProgressIndicator()
                                else -> LinearProgressIndicator(progress = { p })
                            }
                        }

                        Button(onClick = { running = !running }) {
                            Text(if (running) "Stop worker" else "Start worker")
                        }

                        Button(onClick = {
                            ctx.filesDir.listFiles()?.forEach { it.delete() }
                            status = "All videos wiped."
                        }) {
                            Text("Wipe downloaded videos")
                        }
                    }
                }
            }
        }
    }
}