package com.example.myapplication

import android.content.Context

object PhoneIdStore {
    private const val PREFS = "worker_prefs"
    private const val KEY_PHONE_ID = "phone_id"

    fun get(context: Context): String? {
        return context.getSharedPreferences(PREFS, Context.MODE_PRIVATE)
            .getString(KEY_PHONE_ID, null)
    }

    fun set(context: Context, phoneId: String) {
        context.getSharedPreferences(PREFS, Context.MODE_PRIVATE)
            .edit()
            .putString(KEY_PHONE_ID, phoneId)
            .apply()
    }
}