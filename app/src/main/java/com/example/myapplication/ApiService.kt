package com.example.myapplication

import okhttp3.ResponseBody
import retrofit2.http.Body
import retrofit2.http.GET
import retrofit2.http.POST
import retrofit2.http.Path
import retrofit2.http.Streaming


interface ApiService {
    @POST("register")
    suspend fun register(): RegisterResponse

    @POST("heartbeat")
    suspend fun heartbeat(@Body body: PhoneIdBody): HeartbeatResponse

    @POST("claim")
    suspend fun claim(@Body body: PhoneIdBody): retrofit2.Response<ClaimResponse>

    @POST("complete")
    suspend fun complete(@Body body: CompleteBody): CompleteResponse

    @POST("fail")
    suspend fun fail(@Body body: FailBody): CompleteResponse

    // download
    @Streaming
    @GET("videos/{id}")
    suspend fun downloadVideo(@Path("id") id: String): retrofit2.Response<ResponseBody>
}