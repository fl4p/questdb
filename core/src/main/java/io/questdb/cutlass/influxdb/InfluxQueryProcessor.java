/*+*****************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 ******************************************************************************/

package io.questdb.cutlass.influxdb;

import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ImplicitCastException;
import io.questdb.cairo.sql.NetworkSqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cutlass.http.HttpChunkedResponse;
import io.questdb.cutlass.http.HttpConnectionContext;
import io.questdb.cutlass.http.HttpConstants;
import io.questdb.cutlass.http.HttpPostPutProcessor;
import io.questdb.cutlass.http.HttpRequestHandler;
import io.questdb.cutlass.http.HttpRequestHeader;
import io.questdb.cutlass.http.HttpRequestProcessor;
import io.questdb.cutlass.http.LocalValue;
import io.questdb.cutlass.http.processors.JsonQueryProcessorConfiguration;
import io.questdb.griffin.CompiledQuery;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.network.NoSpaceLeftInResponseBufferException;
import io.questdb.network.PeerDisconnectedException;
import io.questdb.network.PeerIsSlowToReadException;
import io.questdb.network.ServerDisconnectException;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.str.DirectUtf8Sequence;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8String;
import io.questdb.std.str.Utf8s;

import java.io.Closeable;

import static io.questdb.cutlass.http.HttpRequestValidator.METHOD_GET;
import static io.questdb.cutlass.http.HttpRequestValidator.METHOD_POST;
import static io.questdb.cutlass.http.HttpRequestValidator.NON_MULTIPART_REQUEST;
import static java.net.HttpURLConnection.HTTP_BAD_REQUEST;
import static java.net.HttpURLConnection.HTTP_OK;

/**
 * Implements the InfluxDB v1 {@code /query} HTTP endpoint so that QuestDB can
 * stand in for an InfluxDB v1 datasource (e.g. in Grafana). It accepts an
 * InfluxQL statement in the {@code q} parameter (URL query for GET, or a
 * form-encoded body for POST), translates the Grafana builder subset of
 * InfluxQL to QuestDB SQL via {@link InfluxQlTranslator}, runs it through the
 * SQL engine, and streams the result as InfluxDB v1 JSON.
 */
public class InfluxQueryProcessor implements HttpRequestProcessor, HttpRequestHandler, HttpPostPutProcessor, Closeable {
    private static final Log LOG = LogFactory.getLog(InfluxQueryProcessor.class);
    private static final LocalValue<InfluxQueryProcessorState> LV = new LocalValue<>();
    private static final Utf8String URL_PARAM_Q = new Utf8String("q");
    // Set in onHeadersReady() and read by onChunk() while this worker receives
    // the request body, mirroring LineHttpProcessorImpl. Per-connection durable
    // state still lives on the context via LV.
    private InfluxQueryProcessorState bodyState;
    private final JsonQueryProcessorConfiguration configuration;
    private final CairoEngine engine;
    private final byte requiredAuthType;
    private final int sharedWorkerCount;
    private final InfluxQlTranslator translator = new InfluxQlTranslator();

    public InfluxQueryProcessor(
            JsonQueryProcessorConfiguration configuration,
            CairoEngine engine,
            int sharedQueryWorkerCount
    ) {
        this.configuration = configuration;
        this.engine = engine;
        this.sharedWorkerCount = sharedQueryWorkerCount;
        this.requiredAuthType = configuration.getRequiredAuthType();
    }

    @Override
    public void close() {
    }

    @Override
    public HttpRequestProcessor getProcessor(HttpRequestHeader requestHeader) {
        return this;
    }

    @Override
    public byte getRequiredAuthType() {
        return requiredAuthType;
    }

    @Override
    public short getSupportedRequestTypes() {
        return METHOD_GET | METHOD_POST | NON_MULTIPART_REQUEST;
    }

    @Override
    public void onChunk(long lo, long hi) {
        if (bodyState != null && hi > lo) {
            Utf8s.utf8ToUtf16(lo, hi, bodyState.getPostBody());
        }
    }

    @Override
    public void onHeadersReady(HttpConnectionContext context) {
        bodyState = getOrCreateState(context);
        bodyState.getPostBody().clear();
    }

    @Override
    public void onRequestComplete(HttpConnectionContext context) throws PeerDisconnectedException, PeerIsSlowToReadException {
        final InfluxQueryProcessorState state = getOrCreateState(context);
        final HttpChunkedResponse response = context.getChunkedResponse();

        // resolve the InfluxQL text: URL "q" param first, then a form-encoded body
        final StringSink query = state.getQuery();
        query.clear();
        final DirectUtf8Sequence q = context.getRequestHeader().getUrlParam(URL_PARAM_Q);
        if (q != null && q.size() > 0) {
            Utf8s.utf8ToUtf16(q.lo(), q.hi(), query);
        } else {
            extractFormParam(state.getPostBody(), query);
        }

        if (query.length() == 0) {
            sendError(state, response, "missing required parameter \"q\"");
            return;
        }

        state.resetTranslation();
        try {
            translator.translate(query, engine, state);
            state.finishTranslation();
        } catch (InfluxQlException e) {
            sendError(state, response, e.getMessage());
            return;
        } catch (CairoException e) {
            sendError(state, response, e.getFlyweightMessage());
            return;
        }

        compileStatements(context, state);

        // stream the InfluxDB-shaped JSON
        try {
            header(response, HTTP_OK);
        } catch (PeerIsSlowToReadException | PeerDisconnectedException e) {
            // headers populated; sending will resume in resumeSend()
            state.beginStreaming();
            throw e;
        }
        state.beginStreaming();
        doResumeSend(state, context);
    }

    @Override
    public void resumeSend(HttpConnectionContext context) throws PeerDisconnectedException, PeerIsSlowToReadException {
        final InfluxQueryProcessorState state = LV.get(context);
        if (state != null) {
            context.resumeResponseSend();
            doResumeSend(state, context);
        }
    }

    private void compileStatements(HttpConnectionContext context, InfluxQueryProcessorState state) {
        final NetworkSqlExecutionCircuitBreaker circuitBreaker = context.getOrCreateCircuitBreaker(engine);
        final SqlExecutionContextImpl sqlExecutionContext = context.getOrCreateSqlExecutionContext(engine, sharedWorkerCount);
        circuitBreaker.resetTimer();
        sqlExecutionContext.with(context.getSecurityContext(), null, null, context.getFd(), circuitBreaker.of(context.getFd()));
        sqlExecutionContext.initNow();
        circuitBreaker.resetMaxTimeToDefault();

        final int n = state.getStatementCount();
        try (SqlCompiler compiler = engine.getSqlCompiler()) {
            for (int i = 0; i < n; i++) {
                final TranslatedQuery tq = state.getStatement(i);
                if (tq.synthesized || tq.statementError != null || tq.sql == null) {
                    state.setExec(i, null, null, null);
                    continue;
                }
                RecordCursorFactory factory = null;
                RecordCursor cursor = null;
                try {
                    CompiledQuery cc = compiler.compile(tq.sql, sqlExecutionContext);
                    factory = cc.getRecordCursorFactory();
                    if (factory == null) {
                        cc.closeAllButSelect();
                        tq.statementError = "statement is not a query";
                        state.setExec(i, null, null, null);
                        continue;
                    }
                    cursor = factory.getCursor(sqlExecutionContext);
                    state.setExec(i, factory, cursor, factory.getMetadata());
                } catch (SqlException | CairoException | ImplicitCastException e) {
                    Misc.free(cursor);
                    Misc.free(factory);
                    tq.statementError = messageOf(e);
                    state.setExec(i, null, null, null);
                } catch (Throwable e) {
                    Misc.free(cursor);
                    Misc.free(factory);
                    tq.statementError = e.getMessage() != null ? e.getMessage() : "internal error";
                    state.setExec(i, null, null, null);
                }
            }
        }
    }

    private void doResumeSend(InfluxQueryProcessorState state, HttpConnectionContext context) throws PeerDisconnectedException, PeerIsSlowToReadException {
        final HttpChunkedResponse response = context.getChunkedResponse();
        while (true) {
            try {
                state.resume(response);
                break;
            } catch (NoSpaceLeftInResponseBufferException ignored) {
                if (response.resetToBookmark()) {
                    response.sendChunk(false);
                } else {
                    LOG.error().$("influxdb /query response buffer too small [fd=").$(context.getFd()).I$();
                    throw CairoException.nonCritical().put("response buffer is too small for a single row");
                }
            }
        }
    }

    // Decodes the value of the "q" key from an application/x-www-form-urlencoded body.
    // Percent escapes are decoded in the ASCII range, which covers the InfluxQL the
    // Grafana builder emits; '+' decodes to a space.
    private void extractFormParam(StringSink body, StringSink out) {
        final int n = body.length();
        int i = 0;
        while (i < n) {
            int keyStart = i;
            while (i < n && body.charAt(i) != '=' && body.charAt(i) != '&') {
                i++;
            }
            final boolean isQ = i - keyStart == 1 && body.charAt(keyStart) == 'q';
            if (i < n && body.charAt(i) == '=') {
                i++; // skip '='
                while (i < n && body.charAt(i) != '&') {
                    char c = body.charAt(i);
                    if (c == '+') {
                        if (isQ) {
                            out.put(' ');
                        }
                        i++;
                    } else if (c == '%' && i + 2 < n) {
                        int hi = hexDigit(body.charAt(i + 1));
                        int lo = hexDigit(body.charAt(i + 2));
                        if (hi >= 0 && lo >= 0) {
                            if (isQ) {
                                out.put((char) ((hi << 4) | lo));
                            }
                            i += 3;
                        } else {
                            if (isQ) {
                                out.put(c);
                            }
                            i++;
                        }
                    } else {
                        if (isQ) {
                            out.put(c);
                        }
                        i++;
                    }
                }
            }
            if (i < n && body.charAt(i) == '&') {
                i++;
            }
            if (isQ) {
                return;
            }
        }
    }

    private InfluxQueryProcessorState getOrCreateState(HttpConnectionContext context) {
        InfluxQueryProcessorState state = LV.get(context);
        if (state == null) {
            LV.set(context, state = new InfluxQueryProcessorState(context, configuration.getKeepAliveHeader()));
        }
        return state;
    }

    private void header(HttpChunkedResponse response, int statusCode) throws PeerDisconnectedException, PeerIsSlowToReadException {
        response.status(statusCode, HttpConstants.CONTENT_TYPE_JSON);
        response.headers().setKeepAlive(configuration.getKeepAliveHeader());
        response.sendHeader();
    }

    private static int hexDigit(char c) {
        if (c >= '0' && c <= '9') {
            return c - '0';
        }
        if (c >= 'a' && c <= 'f') {
            return c - 'a' + 10;
        }
        if (c >= 'A' && c <= 'F') {
            return c - 'A' + 10;
        }
        return -1;
    }

    private static String messageOf(Throwable e) {
        if (e instanceof SqlException se) {
            return se.getFlyweightMessage().toString();
        }
        if (e instanceof CairoException ce) {
            return ce.getFlyweightMessage().toString();
        }
        if (e instanceof ImplicitCastException ice) {
            return ice.getFlyweightMessage().toString();
        }
        return e.getMessage() != null ? e.getMessage() : "error";
    }

    private void sendError(InfluxQueryProcessorState state, HttpChunkedResponse response, CharSequence message) throws PeerDisconnectedException, PeerIsSlowToReadException {
        header(response, HTTP_BAD_REQUEST);
        response.putAscii("{\"error\":");
        response.putQuote().escapeJsonStr(message != null ? message : "bad request").putQuote();
        response.putAscii('}');
        response.sendChunk(true);
    }
}
