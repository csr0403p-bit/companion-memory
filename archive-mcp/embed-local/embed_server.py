#!/usr/bin/env python3
# embed-server —— 本地 embedding 服务（Qwen3-Embedding-0.6B，CPU 推理）
# POST /embed {"texts": ["...", ...]} → {"ok":true,"dims":1024,"vectors":[[...],...]}
# GET /healthz → {"ok":true}
# 只监听 127.0.0.1，供 archive-mcp 和回填脚本调用（MODEL_DIR/PORT 环境变量可改）。
import json
import os
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import torch
from sentence_transformers import SentenceTransformer

MODEL_DIR = os.environ.get("MODEL_DIR", os.path.join(os.path.dirname(__file__), "models/qwen3-emb-0.6b"))
PORT = int(os.environ.get("PORT", 3013))
MAX_CHARS = 1024  # 单条文本截断，防止超长输入把单线程卡死

# 并发 encode 会让 torch 线程互相饿死 → 串行化 + 限线程数
_encode_lock = threading.Lock()

print("loading model...", flush=True)
model = SentenceTransformer(MODEL_DIR, device="cpu")
# 权重是 bf16，CPU 推理极慢 → 转 fp32（4 核 AVX2 快 3-5 倍；int8 动态量化在 6500T 无 VNNI 无增益，弃用）
model._first_module().auto_model = model._first_module().auto_model.float()
print("model loaded (fp32)", flush=True)

class H(BaseHTTPRequestHandler):
    def log_message(self, *a):
        pass

    def do_GET(self):
        if self.path == "/healthz":
            self._json(200, {"ok": True})
        else:
            self._json(404, {"ok": False, "error": "not found"})

    def do_POST(self):
        if self.path != "/embed":
            return self._json(404, {"ok": False, "error": "not found"})
        try:
            n = int(self.headers.get("Content-Length") or 0)
            body = json.loads(self.rfile.read(n) or b"{}")
            texts = body.get("texts")
            if not isinstance(texts, list) or not texts:
                return self._json(400, {"ok": False, "error": "texts[] required"})
            # 清洗：lone surrogate（历史脏数据里的半截 emoji）无法 UTF-8 编码，
            # 会让 Rust tokenizer 抛 TextEncodeInput；encode-ignore 直接丢弃
            texts = [str(t).encode("utf-8", "ignore").decode("utf-8", "ignore")[:MAX_CHARS] for t in texts]
            with _encode_lock:
                vecs = model.encode(texts, normalize_embeddings=True)
            self._json(200, {
                "ok": True,
                "dims": int(vecs.shape[1]),
                "vectors": [v.tolist() for v in vecs],
            })
        except Exception as e:
            self._json(500, {"ok": False, "error": str(e)})

    def _json(self, code, obj):
        data = json.dumps(obj, ensure_ascii=False).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

if __name__ == "__main__":
    ThreadingHTTPServer(("127.0.0.1", PORT), H).serve_forever()
