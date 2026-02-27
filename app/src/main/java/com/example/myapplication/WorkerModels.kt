package com.example.myapplication

data class RegisterResponse(val phoneId: String)

data class PhoneIdBody(val phoneId: String)

data class HeartbeatResponse(
    val ok: Boolean,
    val serverTimeMs: Long
)

data class ClaimResponse(
    val videoId: String,
    val downloadUrl: String,
    val leaseUntilMs: Long
)

data class CompleteBody(
    val phoneId: String,
    val videoId: String
)

data class FailBody(
    val phoneId: String,
    val videoId: String,
    val reason: String
)

data class CompleteResponse(val ok: Boolean)