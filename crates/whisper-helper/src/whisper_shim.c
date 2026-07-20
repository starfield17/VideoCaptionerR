/* Thin C wrapper so Rust never hand-rolls whisper_full_params layout. */
#include "whisper.h"
#include <stdlib.h>
#include <string.h>

struct vc_whisper_ctx {
    struct whisper_context *ctx;
};

struct vc_whisper_ctx *vc_whisper_load(const char *path) {
    struct whisper_context_params cparams = whisper_context_default_params();
    cparams.use_gpu = false;
    struct whisper_context *ctx = whisper_init_from_file_with_params(path, cparams);
    if (!ctx) {
        return NULL;
    }
    struct vc_whisper_ctx *wrap = (struct vc_whisper_ctx *)calloc(1, sizeof(*wrap));
    if (!wrap) {
        whisper_free(ctx);
        return NULL;
    }
    wrap->ctx = ctx;
    return wrap;
}

void vc_whisper_free(struct vc_whisper_ctx *wrap) {
    if (!wrap) {
        return;
    }
    if (wrap->ctx) {
        whisper_free(wrap->ctx);
    }
    free(wrap);
}

int vc_whisper_full(
    struct vc_whisper_ctx *wrap,
    const float *samples,
    int n_samples,
    int n_threads,
    const char *language,
    int detect_language
) {
    if (!wrap || !wrap->ctx) {
        return -1;
    }
    struct whisper_full_params params = whisper_full_default_params(WHISPER_SAMPLING_GREEDY);
    params.n_threads = n_threads > 0 ? n_threads : 4;
    params.translate = false;
    params.print_special = false;
    params.print_progress = false;
    params.print_realtime = false;
    params.print_timestamps = false;
    params.token_timestamps = true;
    params.language = language;
    params.detect_language = detect_language ? true : false;
    return whisper_full(wrap->ctx, params, samples, n_samples);
}

int vc_whisper_n_segments(struct vc_whisper_ctx *wrap) {
    return wrap && wrap->ctx ? whisper_full_n_segments(wrap->ctx) : 0;
}

int vc_whisper_n_tokens(struct vc_whisper_ctx *wrap, int i_segment) {
    return wrap && wrap->ctx ? whisper_full_n_tokens(wrap->ctx, i_segment) : 0;
}

const char *vc_whisper_token_text(struct vc_whisper_ctx *wrap, int i_segment, int i_token) {
    return wrap && wrap->ctx ? whisper_full_get_token_text(wrap->ctx, i_segment, i_token) : NULL;
}

void vc_whisper_token_times(
    struct vc_whisper_ctx *wrap,
    int i_segment,
    int i_token,
    int64_t *t0,
    int64_t *t1,
    float *prob
) {
    if (!wrap || !wrap->ctx) {
        if (t0) *t0 = 0;
        if (t1) *t1 = 0;
        if (prob) *prob = -1.0f;
        return;
    }
    struct whisper_token_data data = whisper_full_get_token_data(wrap->ctx, i_segment, i_token);
    if (t0) *t0 = data.t0;
    if (t1) *t1 = data.t1;
    if (prob) *prob = data.p;
}

const char *vc_whisper_segment_text(struct vc_whisper_ctx *wrap, int i_segment) {
    return wrap && wrap->ctx ? whisper_full_get_segment_text(wrap->ctx, i_segment) : NULL;
}

int64_t vc_whisper_segment_t0(struct vc_whisper_ctx *wrap, int i_segment) {
    return wrap && wrap->ctx ? whisper_full_get_segment_t0(wrap->ctx, i_segment) : 0;
}

int64_t vc_whisper_segment_t1(struct vc_whisper_ctx *wrap, int i_segment) {
    return wrap && wrap->ctx ? whisper_full_get_segment_t1(wrap->ctx, i_segment) : 0;
}
