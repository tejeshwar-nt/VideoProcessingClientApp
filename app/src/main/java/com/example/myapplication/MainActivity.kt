package com.example.myapplication

import android.media.MediaExtractor
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
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import okhttp3.MediaType.Companion.toMediaType
import okhttp3.MultipartBody
import okhttp3.RequestBody.Companion.asRequestBody
import okhttp3.RequestBody.Companion.toRequestBody
import edu.gatech.ccg.gtk.extraction.ExtractionPipeline
import java.io.File

class MainActivity : ComponentActivity() {

    companion object {
        init {
            System.loadLibrary("mediapipe_jni")
        }
    }

    private lateinit var pipeline: ExtractionPipeline
    private var videosProcessed = 0

    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)

        com.google.mediapipe.framework.AndroidAssetUtil.initializeNativeAssetManager(this.baseContext)
        pipeline = ExtractionPipeline()
        setContent {
            val ctx = LocalContext.current

            var status by remember { mutableStateOf("Idle") }
            var running by remember { mutableStateOf(true) }

            var currentVideo by remember { mutableStateOf<String?>(null) }
            var progress by remember { mutableStateOf<Float?>(null) }
            val scope = rememberCoroutineScope()

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

                // validate full download when content length is known
                if (total > 0 && read != total) {
                    outFile.delete()
                    throw RuntimeException("Incomplete download for $videoId")
                }

                progress = null
                currentVideo = null
                return outFile
            }

            suspend fun processVideo(file: File): File {
                val hdf5File = File(ctx.filesDir, file.nameWithoutExtension + ".h5")
                withContext(Dispatchers.Default) {
                    val start = System.currentTimeMillis()
                    android.util.Log.d("Pipeline", "Starting processing: ${file.name}")
                    PhoneIdStore.setProcessingVideo(ctx, file.name)
                    pipeline.process(file.absolutePath, hdf5File.absolutePath)
                    PhoneIdStore.setProcessingVideo(ctx, null)
                    val elapsed = System.currentTimeMillis() - start
                    android.util.Log.d("Pipeline", "Finished processing: ${file.name} in ${elapsed}ms (total this session: ${++videosProcessed})")
                }
                return hdf5File
            }

            // upload result to server

            suspend fun uploadHdf5(phoneId: String, videoId: String, hdf5File: File) {
                val phoneIdPart = phoneId.toRequestBody("text/plain".toMediaType())
                val videoIdPart = videoId.toRequestBody("text/plain".toMediaType())
                val filePart = MultipartBody.Part.createFormData(
                    "hdf5", hdf5File.name,
                    hdf5File.asRequestBody("application/octet-stream".toMediaType())
                )
                val resp = ApiClient.api.uploadHdf5(phoneIdPart, videoIdPart, filePart)
                if (!resp.ok) throw RuntimeException("HDF5 upload rejected for $videoId")
            }

            // Worker loop runs while app is open
            LaunchedEffect(running) {
                if (!running) return@LaunchedEffect

                while (isActive && running) {
                    var claimedVideoId: String? = null
                    try {
                        val phoneId = ensurePhoneId()

                        val crashedVideo = PhoneIdStore.getProcessingVideo(ctx)
                        if (crashedVideo != null) {
                            android.util.Log.e("Pipeline", "Previous run crashed while processing: $crashedVideo")
                            PhoneIdStore.setProcessingVideo(ctx, null)
                            // delete partial files from phone
                            File(ctx.filesDir, crashedVideo).delete()
                            File(ctx.filesDir, File(crashedVideo).nameWithoutExtension + ".h5").delete()
                            // wipe cache to clear any corrupted pipeline asset extractions
                            ctx.cacheDir.listFiles()?.forEach { it.delete() }
                        }

                        status = "Worker running (phoneId=$phoneId)"

                        // heartbeat
                        ApiClient.api.heartbeat(PhoneIdBody(phoneId))

                        // claim a job
                        val claimResp = ApiClient.api.claim(PhoneIdBody(phoneId))

                        if (claimResp.code() == 204) {
                            status = "No jobs currently"
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
                            status = "Already have $videoId"
                            ApiClient.api.complete(CompleteBody(phoneId, videoId))
                            claimedVideoId = null
                            continue
                        }

                        // download
                        status = "Downloading $videoId"
                        val file = downloadWithProgress(videoId)

                        // process
                        status = "Processing $videoId"
                        val hdf5File = processVideo(file)

                        // upload result
                        status = "Uploading HDF5 for $videoId"
                        uploadHdf5(phoneId, videoId, hdf5File)

                        // complete
                        status = "Completing $videoId"
                        ApiClient.api.complete(CompleteBody(phoneId, videoId))
                        claimedVideoId = null

                        // free up storage
                        file.delete()
                        hdf5File.delete()

                        status = "Done $videoId. Claiming next."
                    } catch (e: Exception) {
                        val vid = claimedVideoId
                        if (vid != null) {
                            try {
                                // remove partial files if they exist
                                File(ctx.filesDir, vid).delete()
                                File(ctx.filesDir, File(vid).nameWithoutExtension + ".h5").delete()
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
                        Text("Device ID: ${PhoneIdStore.get(ctx) ?: "Not registered"}")
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
                            ctx.filesDir.listFiles()
                                ?.filter { it.extension == "mp4" || it.extension == "h5" }
                                ?.forEach { it.delete() }
                            status = "All videos wiped."
                        }) {
                            Text("Wipe downloaded videos")
                        }

//                        Button(onClick = {
//                            scope.launch {
//                                try {
//                                    val phoneId = ensurePhoneId()
//                                    status = "Downloading test H5"
//                                    val resp = ApiClient.api.downloadTestHdf5()
//                                    if (!resp.isSuccessful || resp.body() == null)
//                                        throw RuntimeException("Failed to fetch test H5: HTTP ${resp.code()}")
//                                    val testFile = File(ctx.filesDir, "test_upload.h5")
//                                    withContext(Dispatchers.IO) {
//                                        resp.body()!!.byteStream().use { input ->
//                                            testFile.outputStream().use { it.write(input.readBytes()) }
//                                        }
//                                    }
//                                    status = "Uploading test H5"
//                                    uploadHdf5(phoneId, "test_upload", testFile)
//                                    status = "Test upload succeeded!"
//                                } catch (e: Exception) {
//                                    status = "Test upload failed: ${e.message}"
//                                }
//                            }
//                        }) {
//                            Text("Test HDF5 Upload")
//                        }
                    }
                }
            }
        }
    }
}