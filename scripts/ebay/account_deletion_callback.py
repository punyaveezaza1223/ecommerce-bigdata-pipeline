import hashlib
import os

from flask import Flask, jsonify, request


app = Flask(__name__)


def get_required_env(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


@app.get("/ebay/account-deletion")
def verify_endpoint():
    challenge_code = request.args.get("challenge_code", "").strip()

    if not challenge_code:
        return jsonify({"error": "Missing challenge_code"}), 400

    verification_token = get_required_env("EBAY_VERIFICATION_TOKEN")
    endpoint = get_required_env("EBAY_NOTIFICATION_ENDPOINT")

    value_to_hash = (
        challenge_code
        + verification_token
        + endpoint
    )

    challenge_response = hashlib.sha256(
        value_to_hash.encode("utf-8")
    ).hexdigest()

    return jsonify({
        "challengeResponse": challenge_response
    }), 200


@app.post("/ebay/account-deletion")
def receive_account_deletion():
    payload = request.get_json(silent=True)

    if payload is None:
        return jsonify({"error": "Invalid JSON payload"}), 400

    # Do not log the payload because it may contain eBay user identifiers.
    # Actual deletion processing will be implemented separately.
    return "", 204


@app.get("/health")
def health():
    return jsonify({"status": "ok"}), 200


if __name__ == "__main__":
    app.run(
        host="0.0.0.0",
        port=int(os.getenv("EBAY_CALLBACK_PORT", "5000")),
        debug=False,
    )